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
import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.config_entries import ConfigEntry, ConfigValueOption
from music_assistant_models.enums import ConfigEntryType, EventType
from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.errors import InvalidDataError

from music_assistant.helpers.app_vars import app_var
from music_assistant.models.plugin import PluginProvider
from music_assistant.providers.qobuz import CONF_QUALITY as QOBUZ_CONF_QUALITY

from .controller import QobuzConnectController
from .discovery import QobuzConnectDiscovery
from .models import (
    PROTOCOL_TO_QUALITY,
    QUALITY_TO_PROTOCOL,
    ConnectTokens,
    DeviceConfig,
    JWTConnectToken,
)
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
CONF_ENABLE_CONTROLLER = "enable_controller"

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
            default_value=PLAYER_ID_AUTO,
            required=True,
            options=[
                # Static option title lives in strings.json; player names are
                # dynamic data so their title is supplied inline.
                ConfigValueOption(PLAYER_ID_AUTO),
                *(
                    ConfigValueOption(player.player_id, title=player.display_name)
                    for player in sorted(
                        mass.players.all_players(False, False), key=lambda x: x.display_name
                    )
                ),
            ],
        ),
        ConfigEntry(
            key=CONF_PUBLISH_NAME,
            type=ConfigEntryType.STRING,
            default_value="Music Assistant",
            required=True,
        ),
        ConfigEntry(
            key=CONF_HTTP_PORT,
            type=ConfigEntryType.INTEGER,
            default_value=8695,
            required=True,
        ),
        ConfigEntry(
            key=CONF_MAX_QUALITY,
            type=ConfigEntryType.STRING,
            default_value="27",
            required=True,
            options=[
                # Option titles are authored in strings.json (config_entries.
                # max_quality.options.<value>); pass value-only here.
                ConfigValueOption("27"),
                ConfigValueOption("7"),
                ConfigValueOption("6"),
                ConfigValueOption("5"),
                ConfigValueOption(str(AUTO_QUALITY)),
            ],
        ),
        ConfigEntry(
            key=CONF_INITIAL_VOLUME,
            type=ConfigEntryType.INTEGER,
            default_value=DEFAULT_INITIAL_VOLUME,
            required=True,
        ),
        ConfigEntry(
            key=CONF_ENABLE_CONTROLLER,
            type=ConfigEntryType.BOOLEAN,
            default_value=True,
            required=False,
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
        self._enable_controller = bool(config.get_value(CONF_ENABLE_CONTROLLER))
        self.controller: QobuzConnectController | None = None
        self._sync = QobuzConnectSyncEngine(self)
        self._ws_setup_lock = asyncio.Lock()
        self._unsubscribe_queue_events: Callable[[], None] | None = None
        self._unsubscribe_queue_items_events: Callable[[], None] | None = None
        self._unsubscribe_player_events: Callable[[], None] | None = None
        # Last value we pushed to the Qobuz cloud, so a MA ``PLAYER_UPDATED``
        # event for an unchanged volume/mute doesn't trigger duplicate sends.
        self._last_sent_volume: int | None = None
        self._last_sent_muted: bool | None = None

    async def loaded_in_mass(self) -> None:
        """Start Qobuz Connect discovery after provider load."""
        logging.getLogger("websockets.client").setLevel(logging.WARNING)
        logging.getLogger("websockets.protocol").setLevel(logging.WARNING)
        await self._sync.start()
        self._unsubscribe_queue_events = self.mass.subscribe(
            self._on_ma_queue_event,
            EventType.QUEUE_UPDATED,
        )
        # QUEUE_ITEMS_UPDATED carries user-driven queue mutations (drag-
        # reorder, remove, add) — feed those into the MA→Qobuz outbound
        # differ so cloud sees them.
        self._unsubscribe_queue_items_events = self.mass.subscribe(
            self._on_ma_queue_items_event,
            EventType.QUEUE_ITEMS_UPDATED,
        )
        # PLAYER_UPDATED carries volume + mute changes from MA's UI. Without
        # this subscription, MA-side slider drags never propagate to the
        # Qobuz app and its volume display drifts away from MA's actual
        # level — the cloud's view stays pinned at whatever we last sent
        # via ``_broadcast_current_volume`` (only fired on connect /
        # SET_ACTIVE). The result is the "huge volume drop" the user
        # reported: cloud thinks MA is at 25, user moves Qobuz slider to
        # 30, MA snaps from 75 to 30.
        self._unsubscribe_player_events = self.mass.subscribe(
            self._on_ma_player_updated,
            EventType.PLAYER_UPDATED,
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
        if self._unsubscribe_queue_items_events is not None:
            self._unsubscribe_queue_items_events()
            self._unsubscribe_queue_items_events = None
        if self._unsubscribe_player_events is not None:
            self._unsubscribe_player_events()
            self._unsubscribe_player_events = None
        await self._sync.stop()
        if self.controller is not None:
            await self.controller.stop()
            self.controller = None
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
                await self._ensure_controller()
                return

            self._session = QobuzConnectSession(
                self._device_config,
                self._build_session_callbacks(),
                token_refresher=self._refresh_ws_token,
            )
            self._session.set_tokens(tokens)
            await self._session.start()
            await self._broadcast_current_volume()
            await self._session.send_quality_reports(self._max_quality)
            await self._ensure_controller()
            self.logger.info("Qobuz Connect WebSocket connected")

    def _build_session_callbacks(self) -> SessionCallbacks:
        """
        Build the callback bundle shared by the renderer and controller sessions.

        The cloud routes renderer-directed unicast frames to the most
        recently joined connection with our deviceUuid, so both connections
        must dispatch them into the same handlers.
        """
        return SessionCallbacks(
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
            on_session_state=self._sync.handle_session_state,
        )

    async def _ensure_controller(self) -> None:
        """Start the controller-role connection if enabled and not yet running."""
        if not self._enable_controller or self.controller is not None:
            return
        self.controller = QobuzConnectController(
            self._device_config,
            uuid.UUID(self._device_uuid).bytes,
            self._refresh_ws_token,
            self.logger,
            self._build_session_callbacks(),
        )
        await self.controller.start()
        self.logger.info("Qobuz Connect controller connection started")

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

    async def _refresh_ws_token(self) -> JWTConnectToken | None:
        """
        Mint a fresh Qobuz Connect websocket token via the native Qobuz login.

        The Qobuz app only hands the device a short-lived token during the local
        handshake. This calls the same ``qws/createToken`` endpoint the Qobuz web
        client uses, authenticated with the native Qobuz provider's logged-in
        user token, so the cloud session survives token expiry without the app.
        """
        try:
            qobuz_provider = self.get_qobuz_provider()
        except InvalidDataError:
            self.logger.warning("Cannot refresh Qobuz Connect token: Qobuz provider missing")
            return None
        auth_token = await qobuz_provider._auth_token()
        if not auth_token:
            self.logger.warning("Cannot refresh Qobuz Connect token: not logged in to Qobuz")
            return None
        try:
            async with self.mass.http_session.post(
                "https://www.qobuz.com/api.json/0.2/qws/createToken",
                headers={
                    "X-App-Id": app_var("qobuz_app_id"),
                    "X-User-Auth-Token": auth_token,
                },
                data={"jwt": "jwt_qws"},
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except Exception as err:
            self.logger.warning("Failed to refresh Qobuz Connect token: %s", err)
            return None
        jwt_qws = payload.get("jwt_qws") or {}
        token = JWTConnectToken(
            jwt=jwt_qws.get("jwt", ""),
            exp=int(jwt_qws.get("exp", 0)),
            endpoint=jwt_qws.get("endpoint", ""),
        )
        if not token.is_valid():
            self.logger.warning("Qobuz createToken returned an invalid token payload")
            return None
        self.logger.debug("Refreshed Qobuz Connect websocket token (exp=%s)", token.exp)
        return token

    async def _on_set_active(self, active: bool) -> None:
        """
        Handle SRVR_RNDR_SET_ACTIVE from the Qobuz cloud.

        Sent when the user picks a different renderer in the Qobuz app —
        we have to release the MA player so two devices don't keep streaming
        in parallel.
        """
        if active:
            self.logger.info("Qobuz Connect activated")
            self._sync.set_active(active=True)
            await self._broadcast_current_volume()
            if self._session:
                await self._session.send_quality_reports(self._max_quality)
            return
        self.logger.info("Qobuz Connect deactivated by cloud; releasing MA player")
        await self._sync.release_target_player()

    async def _broadcast_current_volume(self) -> None:
        """Report current MA player volume to Qobuz."""
        if not self._session:
            return
        volume = self._initial_volume
        muted: bool | None = None
        player_id = self.get_target_player_id()
        if player_id and (player := self.mass.players.get_player(player_id)):
            # group_volume/group_volume_muted resolve to the player's own level for
            # a single player and to the aggregate for a sync group / group player,
            # so this works whether the target is a plain player or a group.
            if player.group_volume is not None:
                volume = player.group_volume
            muted = player.group_volume_muted
        await self._session.send_volume_changed(volume)
        self._last_sent_volume = volume
        if muted is not None and muted != self._last_sent_muted:
            await self._session.send_volume_muted(muted)
            self._last_sent_muted = muted

    async def _on_ma_queue_event(self, event: MassEvent) -> None:
        """Forward MA queue updates into the Qobuz sync engine."""
        await self._sync.handle_ma_queue_event(event)

    async def _on_ma_queue_items_event(self, event: MassEvent) -> None:
        """Forward MA queue-item mutations to the MA→Qobuz outbound differ."""
        await self._sync.handle_ma_queue_items_updated(event)

    async def _on_ma_player_updated(self, event: MassEvent) -> None:
        """
        Propagate MA-side volume + mute changes to the Qobuz cloud.

        Fires for every ``PLAYER_UPDATED`` event MA emits. Filters on our
        target player id and skips when:
        - The session isn't up yet.
        - The engine is in QOBUZ origin scope (= we're applying an inbound
          ``SET_VOLUME``; the resulting MA event would otherwise echo
          straight back to the cloud).
        - The value hasn't actually changed since our last send (dedup).

        Volume + mute live on the player state and share the same source
        event, so both are handled here.
        """
        from .models import Origin  # noqa: PLC0415

        if self._session is None:
            return
        if self._sync.origin == Origin.QOBUZ:
            return
        player_id = self.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        player = event.data
        if player is None:
            return
        # group_volume/group_volume_muted give the aggregate for a sync group /
        # group player and the player's own level for a single player. The bare
        # ``volume_level``/``volume_muted`` on a group are unset, which is why a
        # sync-group target never synced its volume to the Qobuz app.
        volume = getattr(player, "group_volume", None)
        if volume is not None and volume != self._last_sent_volume:
            self.logger.debug(
                "MA->Qobuz volume: sending group_volume=%s (player=%s)", volume, event.object_id
            )
            await self._session.send_volume_changed(volume)
            self._last_sent_volume = volume
        muted = getattr(player, "group_volume_muted", None)
        if muted is not None and muted != self._last_sent_muted:
            await self._session.send_volume_muted(muted)
            self._last_sent_muted = muted

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
