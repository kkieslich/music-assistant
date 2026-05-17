"""
Experimental Qobuz Connect receiver for Music Assistant.

This provider implements enough of Qobuz Connect locally to expose Music
Assistant as a Qobuz Connect target while keeping playback inside MA's
native Qobuz provider and player queue.

Owns:
- ``QobuzConnectProvider`` (a ``PluginProvider``): config schema,
  lifecycle hooks (``loaded_in_mass`` / ``unload`` / ``update_config``),
  target-player resolution (the ``__auto__`` heuristic vs. a pinned
  player), and persisting the user's quality choice from the Qobuz app
  back into both this provider's config *and* the native ``qobuz``
  music provider's ``CONF_QUALITY``.
- The wiring between the four collaborators: it owns the
  ``QobuzConnectDiscovery``, ``QobuzConnectSession`` and
  ``QobuzConnectSyncEngine`` instances and threads callbacks between
  them.
- The device-identity derivation that keeps the mDNS serial and the
  Qobuz cloud device UUID stable across restarts (``uuid5`` over
  ``instance_id``).

Exposes:
- ``setup``, ``get_config_entries`` (the provider-protocol hooks).
- ``QobuzConnectProvider`` for typing.
- Module-level constants ``CONF_TARGET_PLAYER`` / ``CONF_PUBLISH_NAME``
  / ``CONF_HTTP_PORT`` / ``CONF_MAX_QUALITY`` / ``CONF_INITIAL_VOLUME``.

Depends on:
- :mod:`.discovery`, :mod:`.session`, :mod:`.sync`, :mod:`.models`.
- The native ``qobuz`` music provider (``mass.get_provider("qobuz")``)
  must be configured — looked up lazily via ``get_qobuz_provider()``,
  which raises ``InvalidDataError`` if absent.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the end-to-end flow.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.config_entries import ConfigEntry, ConfigValueOption
from music_assistant_models.enums import ConfigEntryType, EventType
from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.errors import InvalidDataError

from music_assistant.models.plugin import PluginProvider
from music_assistant.providers.qobuz import CONF_QUALITY as QOBUZ_CONF_QUALITY

from .discovery import QobuzConnectDiscovery
from .models import PROTOCOL_TO_QUALITY, QUALITY_TO_PROTOCOL, ConnectTokens, DeviceConfig
from .session import QobuzConnectSession, SessionCallbacks
from .sync import QobuzConnectSyncEngine

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.event import MassEvent
    from music_assistant_models.provider import ProviderManifest

    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType
    from music_assistant.providers.qobuz import QobuzProvider


CONF_TARGET_PLAYER = "target_player"
CONF_PUBLISH_NAME = "publish_name"
CONF_HTTP_PORT = "http_port"
CONF_MAX_QUALITY = "max_quality"
CONF_INITIAL_VOLUME = "initial_volume"

PLAYER_ID_AUTO = "__auto__"
DEFAULT_INITIAL_VOLUME = 25
AUTO_QUALITY = 0
SUPPORTED_QUALITIES = frozenset(QUALITY_TO_PROTOCOL)

# Stable namespace for deriving the Qobuz device UUID from MA's instance_id, so
# the mDNS-advertised serial and the cloud JOIN_SESSION device UUID match and
# remain identical across restarts (otherwise the Qobuz app shows duplicates).
_DEVICE_UUID_NAMESPACE = uuid.UUID("4e8b9a3c-6f1d-4a2e-9b7c-3d5e8f1a2b6c")


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Initialize provider instance."""
    return QobuzConnectProvider(mass, manifest, config)


async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,  # noqa: ARG001
    action: str | None = None,  # noqa: ARG001
    values: dict[str, ConfigValueType] | None = None,  # noqa: ARG001
) -> tuple[ConfigEntry, ...]:
    """Return config entries for this provider."""
    return (
        ConfigEntry(
            key=CONF_TARGET_PLAYER,
            type=ConfigEntryType.STRING,
            label="Target Music Assistant player",
            default_value=PLAYER_ID_AUTO,
            required=True,
            options=[
                ConfigValueOption("Auto (prefer playing player)", PLAYER_ID_AUTO),
                *(
                    ConfigValueOption(player.display_name, player.player_id)
                    for player in sorted(
                        mass.players.all_players(False, False), key=lambda x: x.display_name
                    )
                ),
            ],
        ),
        ConfigEntry(
            key=CONF_PUBLISH_NAME,
            type=ConfigEntryType.STRING,
            label="Name shown in the Qobuz app",
            default_value="Music Assistant",
            required=True,
        ),
        ConfigEntry(
            key=CONF_HTTP_PORT,
            type=ConfigEntryType.INTEGER,
            label="HTTP discovery port",
            description=(
                "Port the Qobuz app connects to on this host. "
                "Change only if it clashes with another service or you need to open it "
                "explicitly through a firewall."
            ),
            default_value=8695,
            required=True,
        ),
        ConfigEntry(
            key=CONF_MAX_QUALITY,
            type=ConfigEntryType.STRING,
            label="Maximum Qobuz quality to advertise",
            default_value="27",
            required=True,
            options=[
                ConfigValueOption("Hi-Res 192kHz/24 bit", "27"),
                ConfigValueOption("Hi-Res 96kHz/24 bit", "7"),
                ConfigValueOption("CD Quality 44.1kHz/16 bit", "6"),
                ConfigValueOption("MP3 320kbps", "5"),
                ConfigValueOption("Auto", str(AUTO_QUALITY)),
            ],
        ),
        ConfigEntry(
            key=CONF_INITIAL_VOLUME,
            type=ConfigEntryType.INTEGER,
            label="Fallback connect volume",
            description=(
                "Volume reported to the Qobuz app on connect when the target player's "
                "current volume can't be read (e.g. Auto mode with no player available). "
                "When a target player is known, its current volume is used instead."
            ),
            default_value=DEFAULT_INITIAL_VOLUME,
            required=True,
        ),
    )


class QobuzConnectProvider(PluginProvider):
    """Qobuz Connect provider that controls native MA queue playback."""

    def __init__(
        self, mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
    ) -> None:
        """Initialize provider."""
        super().__init__(mass, manifest, config, set())
        self._target_player_id = cast("str", config.get_value(CONF_TARGET_PLAYER)) or PLAYER_ID_AUTO
        self._publish_name = cast("str", config.get_value(CONF_PUBLISH_NAME)) or self.name
        self._http_port = int(cast("int | str", config.get_value(CONF_HTTP_PORT)) or 8695)
        self._max_quality = int(cast("str", config.get_value(CONF_MAX_QUALITY)) or "27")
        self._initial_volume = max(
            0,
            min(
                100,
                int(
                    cast("int | str | None", config.get_value(CONF_INITIAL_VOLUME))
                    or DEFAULT_INITIAL_VOLUME
                ),
            ),
        )

        self._device_uuid = str(uuid.uuid5(_DEVICE_UUID_NAMESPACE, self.instance_id))

        self._device_config = DeviceConfig(
            name=self._publish_name,
            uuid=self._device_uuid,
            http_port=self._http_port,
            bind_address=self.mass.streams.bind_ip,
            max_quality=self._max_quality,
        )
        self._discovery: QobuzConnectDiscovery | None = None
        self._session: QobuzConnectSession | None = None
        self._sync = QobuzConnectSyncEngine(self)
        self._ws_setup_lock = asyncio.Lock()
        self._unsubscribe_queue_events: Callable[[], None] | None = None

    async def loaded_in_mass(self) -> None:
        """Start Qobuz Connect discovery after provider load."""
        logging.getLogger("websockets.client").setLevel(logging.WARNING)
        logging.getLogger("websockets.protocol").setLevel(logging.WARNING)
        await self._sync.start()
        self._unsubscribe_queue_events = self.mass.subscribe(
            self._on_ma_queue_event,
            EventType.QUEUE_UPDATED,
        )
        self._discovery = QobuzConnectDiscovery(
            device=self._device_config,
            on_connect=self._on_app_connected,
            quality_getter=lambda: self._max_quality,
        )
        await self._discovery.start()
        self.logger.info(
            "Qobuz Connect target '%s' listening on %s:%s",
            self._publish_name,
            self.mass.streams.bind_ip,
            self._http_port,
        )

    async def unload(self, is_removed: bool = False) -> None:
        """Unload provider and stop network services."""
        if self._unsubscribe_queue_events is not None:
            self._unsubscribe_queue_events()
            self._unsubscribe_queue_events = None
        await self._sync.stop()
        if self._session:
            await self._session.stop()
        if self._discovery:
            await self._discovery.stop()

    async def update_config(self, config: ProviderConfig, changed_keys: set[str]) -> None:
        """Handle dynamic provider config updates."""
        if changed_keys == {f"values/{CONF_MAX_QUALITY}"}:
            self.config = config
            self._max_quality = int(cast("str", config.get_value(CONF_MAX_QUALITY)) or "27")
            self._device_config.max_quality = self._max_quality
            return
        await super().update_config(config, changed_keys)

    @property
    def qobuz_session(self) -> QobuzConnectSession | None:
        """Return active Qobuz Connect websocket session."""
        return self._session

    def get_target_player_id(self) -> str | None:
        """Resolve configured target player."""
        if self._target_player_id != PLAYER_ID_AUTO:
            if self.mass.players.get_player(self._target_player_id):
                return self._target_player_id
            self.logger.warning(
                "Configured target player no longer exists: %s", self._target_player_id
            )
            return None

        players = list(self.mass.players.all_players(False, False))
        for player in players:
            if player.state.playback_state == MAPlaybackState.PLAYING:
                return player.player_id
        return players[0].player_id if players else None

    def get_qobuz_provider(self) -> QobuzProvider:
        """Return the configured Music Assistant Qobuz music provider."""
        provider = self.mass.get_provider("qobuz")
        if provider is None:
            raise InvalidDataError("The Qobuz music provider must be configured first")
        return cast("QobuzProvider", provider)

    def _on_app_connected(self, tokens: ConnectTokens) -> None:
        """Handle Qobuz app connection callback."""
        self.mass.create_task(self._setup_websocket(tokens))

    async def _setup_websocket(self, tokens: ConnectTokens) -> None:
        """Set up Qobuz Connect WebSocket command handling."""
        async with self._ws_setup_lock:
            if self._session is not None:
                self._session.set_tokens(tokens)
                await self._broadcast_current_volume()
                await self._session.send_quality_reports(self._max_quality)
                return

            self._session = QobuzConnectSession(
                self._device_config,
                SessionCallbacks(
                    on_set_state=self._sync.handle_qobuz_set_state,
                    on_queue_load_ack=self._sync.handle_queue_load_ack,
                    on_queue_error=self._sync.handle_queue_error,
                    on_queue_version=self._sync.handle_queue_version,
                    on_queue_state=self._sync.handle_queue_state,
                    on_queue_tracks_added=self._sync.handle_queue_tracks_added,
                    on_queue_tracks_inserted=self._sync.handle_queue_tracks_inserted,
                    on_queue_tracks_removed=self._sync.handle_queue_tracks_removed,
                    on_queue_tracks_reordered=self._sync.handle_queue_tracks_reordered,
                    on_queue_cleared=self._sync.handle_queue_cleared,
                    on_volume=self._on_volume_command,
                    on_volume_delta=self._on_volume_delta_command,
                    on_quality=self._on_quality_change,
                    on_loop_mode=self._sync.handle_loop_mode,
                    on_shuffle_mode=self._sync.handle_shuffle_mode,
                    on_autoplay_mode=self._sync.handle_autoplay_mode,
                    on_state_request=self._sync.report_state,
                    on_set_active=self._on_set_active,
                ),
            )
            self._session.set_tokens(tokens)
            await self._session.start()
            await self._broadcast_current_volume()
            await self._session.send_quality_reports(self._max_quality)
            self.logger.info("Qobuz Connect WebSocket connected")

    async def _on_quality_change(self, new_quality: int) -> None:
        """Remember quality selected in Qobuz app."""
        quality = _normalize_quality_id(new_quality)
        if quality is None:
            self.logger.warning("Ignoring unsupported Qobuz Connect quality value: %s", new_quality)
            if self._session:
                await self._session.send_quality_reports(self._max_quality)
            return
        self.logger.info("Qobuz Connect quality changed: %s -> %s", self._max_quality, quality)
        self._max_quality = quality
        self._device_config.max_quality = quality
        await self._update_connect_quality_config(quality)
        await self._update_qobuz_stream_quality(quality)
        if self._session:
            await self._session.send_quality_reports(quality)

    async def _update_connect_quality_config(self, quality: int) -> None:
        """Persist selected Connect quality to this provider's config."""
        current_quality = int(cast("str", self.config.get_value(CONF_MAX_QUALITY)) or "27")
        if current_quality == quality:
            return
        await self.mass.config.save_provider_config(
            self.domain,
            {CONF_MAX_QUALITY: str(quality)},
            self.instance_id,
        )

    async def _update_qobuz_stream_quality(self, quality: int) -> None:
        """Persist selected Connect quality to the native Qobuz stream provider."""
        qobuz_provider = self.get_qobuz_provider()
        current_quality = int(
            cast("str", qobuz_provider.config.get_value(QOBUZ_CONF_QUALITY)) or "27"
        )
        if current_quality == quality:
            return
        await self.mass.config.save_provider_config(
            qobuz_provider.domain,
            {QOBUZ_CONF_QUALITY: str(quality)},
            qobuz_provider.instance_id,
        )

    async def _on_volume_command(self, volume: int) -> None:
        """Handle absolute or delta volume command from Qobuz."""
        await self._sync.set_volume(volume)

    async def _on_volume_delta_command(self, delta: int) -> None:
        """Handle relative volume command from Qobuz."""
        await self._sync.set_volume_delta(delta)

    async def _on_set_active(self, active: bool) -> None:
        """Handle SRVR_RNDR_SET_ACTIVE from the Qobuz cloud.

        Sent when the user picks a different renderer in the Qobuz app —
        we have to release the MA player so two devices don't keep streaming
        in parallel.
        """
        if active:
            self.logger.info("Qobuz Connect activated")
            await self._broadcast_current_volume()
            if self._session:
                await self._session.send_quality_reports(self._max_quality)
            return
        self.logger.info("Qobuz Connect deactivated by cloud; releasing MA player")
        player_id = self.get_target_player_id()
        if player_id:
            with contextlib.suppress(Exception):
                await self.mass.player_queues.stop(player_id)
        self._sync.reset_for_deactivation()

    async def _broadcast_current_volume(self) -> None:
        """Report current MA player volume to Qobuz."""
        if not self._session:
            return
        volume = self._initial_volume
        player_id = self.get_target_player_id()
        if player_id and (player := self.mass.players.get_player(player_id)):
            if player.state.volume_level is not None:
                volume = player.state.volume_level
        await self._session.send_volume_changed(volume)

    async def _on_ma_queue_event(self, event: MassEvent) -> None:
        """Forward MA queue updates into the Qobuz sync engine."""
        await self._sync.handle_ma_queue_event(event)

    def get_qobuz_track_id_from_queue_item(self, queue_item: Any) -> str | None:
        """Extract a Qobuz provider track id from an MA QueueItem."""
        media_item = getattr(queue_item, "media_item", None)
        if media_item is None or getattr(media_item, "media_type", None) is None:
            return None
        media_type = getattr(media_item.media_type, "value", media_item.media_type)
        if media_type != "track":
            return None

        qobuz_provider = self.get_qobuz_provider()
        provider = getattr(media_item, "provider", None)
        if provider in (qobuz_provider.instance_id, "qobuz"):
            return str(media_item.item_id)

        for mapping in getattr(media_item, "provider_mappings", ()) or ():
            provider_domain = getattr(mapping, "provider_domain", None)
            provider_instance = getattr(mapping, "provider_instance", None)
            if provider_domain == "qobuz" or provider_instance == qobuz_provider.instance_id:
                return str(mapping.item_id)
        return None


def _normalize_quality_id(value: int) -> int | None:
    """Normalize Qobuz Connect protocol quality values to MA Qobuz format IDs."""
    if value in SUPPORTED_QUALITIES:
        return value
    return PROTOCOL_TO_QUALITY.get(value)
