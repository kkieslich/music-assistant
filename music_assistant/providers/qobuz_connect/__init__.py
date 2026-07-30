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
- The wiring between the collaborators: it owns the
  ``QobuzConnectDiscovery`` and ``QobuzConnectSession`` instances, plus the
  reducer-based sync core (``QobuzConnectCoordinator`` + ``EffectRunner``)
  that replaced the retired ``QobuzConnectSyncEngine``, and threads
  callbacks between them.
- The device-identity derivation that keeps the mDNS serial and the
  Qobuz cloud device UUID stable across restarts (``uuid5`` over
  ``instance_id``).

Exposes:
- ``setup``, ``get_config_entries`` (the provider-protocol hooks).
- ``QobuzConnectProvider`` for typing.
- Module-level constants ``CONF_TARGET_PLAYER`` / ``CONF_PUBLISH_NAME``
  / ``CONF_HTTP_PORT`` / ``CONF_MAX_QUALITY`` / ``CONF_INITIAL_VOLUME``
  / ``CONF_QOBUZ_PROVIDER``.

Depends on:
- :mod:`.discovery`, :mod:`.session`, :mod:`.coordinator`, :mod:`.effect_runner`,
  :mod:`.models`.
- The selected native ``qobuz`` music-provider instance must be configured
  and loaded — looked up lazily via ``get_qobuz_provider()``, which raises
  ``InvalidDataError`` if absent.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the end-to-end flow.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.config_entries import ConfigEntry, ConfigValueOption
from music_assistant_models.enums import ConfigEntryType, EventType
from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.errors import InvalidDataError

from music_assistant.helpers.app_vars import app_var
from music_assistant.models.plugin import PluginProvider
from music_assistant.providers.qobuz import CONF_QUALITY as QOBUZ_CONF_QUALITY

from .coordinator import QobuzConnectCoordinator
from .discovery import QobuzConnectDiscovery
from .effect_runner import EffectRunner
from .flight_recorder import FlightRecorder
from .ma_bridge import MABridge
from .metadata_resolver import MetadataResolver
from .models import (
    PROTOCOL_TO_QUALITY,
    QUALITY_TO_PROTOCOL,
    AudioQualityReport,
    ConnectTokens,
    DeviceConfig,
    JWTConnectToken,
    quality_id_for_format,
)
from .outbound_reporter import OutboundReporter
from .quality_reporter import QualityReporter
from .session import QobuzConnectSession, SessionCallbacks
from .sync_types import CloudSetActive, Event

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
CONF_QOBUZ_PROVIDER = "qobuz_provider"

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


class QobuzConnectProvider(PluginProvider):
    """Qobuz Connect provider that controls native MA queue playback."""

    def __init__(
        self, mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
    ) -> None:
        """Initialize provider."""
        super().__init__(mass, manifest, config, set())
        self._target_player_id = (
            cast("str", self._get_setup_or_legacy_value(CONF_TARGET_PLAYER)) or PLAYER_ID_AUTO
        )
        self._publish_name = (
            cast("str", self._get_setup_or_legacy_value(CONF_PUBLISH_NAME)) or self.name
        )
        self._http_port = int(
            cast("int | str", self._get_setup_or_legacy_value(CONF_HTTP_PORT)) or 8695
        )
        self._qobuz_provider_id = cast(
            "str | None", self._get_setup_or_legacy_value(CONF_QOBUZ_PROVIDER)
        )
        self._configured_max_quality = int(cast("str", config.get_value(CONF_MAX_QUALITY)) or "27")
        self._max_quality = self._resolve_max_quality(self._configured_max_quality)
        self._initial_volume = max(
            0,
            min(
                100,
                int(
                    cast(
                        "int | str | None",
                        self._get_setup_or_legacy_value(CONF_INITIAL_VOLUME),
                    )
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
        self._flight_recorder_started = False
        self._reporter_started = False
        # Sync core: a pure reducer (reducer.py/sync_types.py) driving an
        # impure coordinator + effect runner. MABridge is the single seam to
        # MA; MetadataResolver and OutboundReporter take explicit getters for
        # exactly what they read (no duck-typed engine host).
        self._bridge = MABridge(self)
        self._metadata = MetadataResolver(
            qobuz_provider_getter=self._bridge.qobuz_music_provider,
            logger=self.logger,
        )
        self._reporter = OutboundReporter(
            session_getter=lambda: self._session,
            state_getter=lambda: self._coordinator.state,
            duration_getter=self._current_track_duration_ms,
            active_getter=lambda: (
                self._coordinator.state.active
                and self._coordinator.state.own_rid is not None
                and self._coordinator.state.active_rid == self._coordinator.state.own_rid
                and self._bridge.target_player_id() is not None
            ),
            current_index_getter=self._current_queue_index,
            logger=self.logger,
        )
        self._quality_reporter = QualityReporter(
            session_getter=lambda: self._session,
            file_quality_getter=self._current_file_quality,
            logger=self.logger,
        )
        # EffectRunner's ``session`` parameter wants a concrete session, but
        # ours is created later (in ``_setup_websocket``) and gets replaced
        # on reconnect — ``_LiveSessionProxy`` forwards every ``send_*`` call
        # to whichever session is current, and safely no-ops before one
        # exists.
        self._effect_runner = EffectRunner(
            session=cast(
                "QobuzConnectSession", _LiveSessionProxy(lambda: self._session, self.logger)
            ),
            bridge=self._bridge,
            metadata=self._metadata,
            reporter=self._reporter,
            own_rid_getter=lambda: self._coordinator.state.own_rid,
            generation_getter=lambda: self._coordinator.queue_generation,
        )
        # Persistent diagnostics: bounded in-memory ring of reducer steps +
        # WARNING/ERROR logs, dumped to <storage_path>/qobuz_connect/<instance>
        # on errors and on a rolling interval, so instability seen on a
        # headless server can be diagnosed after the fact.
        self._flight_recorder = FlightRecorder(
            Path(self.mass.storage_path) / "qobuz_connect" / self.instance_id,
            state_getter=lambda: self._coordinator.state,
        )
        self._coordinator: QobuzConnectCoordinator = QobuzConnectCoordinator(
            runner=self._effect_runner,
            bridge=self._bridge,
            device_uuid=uuid.UUID(self._device_uuid).bytes,
            recorder=self._flight_recorder,
            unresolvable_getter=lambda: self._metadata.unresolvable_track_ids,
        )
        self._ws_setup_lock = asyncio.Lock()
        self._unloaded = False
        self._unsubscribe_queue_events: Callable[[], None] | None = None
        self._unsubscribe_queue_items_events: Callable[[], None] | None = None
        self._unsubscribe_player_events: Callable[[], None] | None = None
        # Last mute state we pushed to the Qobuz cloud, so an unchanged value
        # doesn't trigger duplicate sends (volume dedup lives in the
        # coordinator).
        self._last_sent_muted: bool | None = None
        # Auto-mode target resolution: the player picked at session start is
        # pinned while the Connect session is active (see
        # ``get_target_player_id``); warn-once bookkeeping for a vanished
        # pinned player, since target resolution runs on every MA event.
        self._pinned_target_id: str | None = None
        self._warned_missing_target: str | None = None
        self._ma_autoplay_lease: tuple[str, bool] | None = None

    async def handle_async_init(self) -> None:
        """Validate dependencies and start availability-critical services."""
        self.get_qobuz_provider()
        logging.getLogger("websockets.client").setLevel(logging.WARNING)
        logging.getLogger("websockets.protocol").setLevel(logging.WARNING)
        try:
            # Started first so it captures anything that goes wrong in the rest
            # of the load sequence (discovery port conflicts, mDNS failures, ...).
            self._flight_recorder_started = True
            await self._flight_recorder.start(extra_logger=self.logger)
            self._reporter_started = True
            await self._reporter.start()
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
            # SET_ACTIVE).
            self._unsubscribe_player_events = self.mass.subscribe(
                self._on_ma_player_updated,
                EventType.PLAYER_UPDATED,
            )
            self._discovery = QobuzConnectDiscovery(
                device=self._device_config,
                on_connect=self._on_app_connected,
                quality_getter=lambda: self._max_quality,
                zeroconf=self._shared_zeroconf(),
            )
            await self._discovery.start()
        except BaseException:
            self.logger.exception(
                "Qobuz Connect initialization failed — the device will not be "
                "reachable (is port %s already in use, e.g. by another instance?)",
                self._http_port,
            )
            await self._stop_runtime()
            raise
        self.logger.info(
            "Qobuz Connect target '%s' listening on %s:%s",
            self._publish_name,
            self.mass.streams.bind_ip,
            self._http_port,
        )

    async def loaded_in_mass(self) -> None:
        """Start the cloud session after the initialized provider is registered."""
        # Connect to the cloud eagerly, self-minting the websocket token via
        # qws/createToken. Waiting for the app's local handshake would lose the
        # FIRST handoff: the phone's SET_ACTIVE races our connect+join and the
        # app bounces playback back when no renderer answers.
        self.mass.create_task(self._setup_websocket(None))

    async def unload(self, is_removed: bool = False) -> None:
        """Unload provider and stop network services."""
        # Flag first: any in-flight handshake/_setup_websocket task checks it
        # under the setup lock and bails instead of spawning a zombie session
        # that would fight the next provider instance for the cloud session.
        self._unloaded = True
        self._release_active_target()
        self._coordinator.close()
        async with self._ws_setup_lock:
            if self._session:
                await self._session.stop()
                self._session = None
        await self._stop_runtime()

    async def update_config(self, config: ProviderConfig, changed_keys: set[str]) -> None:
        """Handle dynamic provider config updates."""
        if changed_keys == {f"values/{CONF_MAX_QUALITY}"}:
            self.config = config
            self._configured_max_quality = int(
                cast("str", config.get_value(CONF_MAX_QUALITY)) or "27"
            )
            self._max_quality = self._resolve_max_quality(self._configured_max_quality)
            self._device_config.max_quality = self._max_quality
            if self._configured_max_quality != AUTO_QUALITY:
                try:
                    await self._update_qobuz_stream_quality(self._max_quality)
                except Exception as err:
                    self.logger.warning("Failed to update native Qobuz quality: %s", err)
            await self._quality_reporter.report_current(self._max_quality)
            return
        await super().update_config(config, changed_keys)

    async def get_config_entries(self) -> tuple[ConfigEntry, ...]:
        """Return dynamically editable provider options."""
        return (_quality_config_entry(),)

    @property
    def qobuz_session(self) -> QobuzConnectSession | None:
        """Return active Qobuz Connect websocket session."""
        return self._session

    def get_target_player_id(self) -> str | None:
        """Resolve configured target player, falling back to auto if a pinned one is gone."""
        active = self._coordinator.state.active
        pinned = self._pinned_target_id
        resolved: str | None
        if pinned is not None and active and self.mass.players.get_player(pinned):
            return pinned

        if self._target_player_id != PLAYER_ID_AUTO:
            if self.mass.players.get_player(self._target_player_id):
                self._warned_missing_target = None
                resolved = self._target_player_id
                self._pinned_target_id = resolved
                if active and resolved != pinned:
                    self._transfer_active_target(resolved)
                return resolved
            # A pinned target that no longer resolves must NOT silently kill
            # playback: a stale saved id (e.g. MA's player-id scheme drifted, or
            # the player is briefly offline) previously returned None, so every
            # MaPlayTrack/MaResyncQueue was dropped and the receiver went
            # active-but-silent ("connect, press play, nothing happens"). Warn
            # once per disappearance (this runs on every MA event, so an
            # unconditional warning floods the log) and fall through to
            # auto-resolution so the receiver still plays somewhere.
            if self._warned_missing_target != self._target_player_id:
                self._warned_missing_target = self._target_player_id
                self.logger.warning(
                    "Configured target player %s no longer exists; falling back to "
                    "auto-resolving an available player",
                    self._target_player_id,
                )

        # While the Connect session is ACTIVE the resolved target is pinned:
        # re-resolving "any PLAYING player, else first" on every event meant a
        # pause from the app (target no longer PLAYING) or another player
        # starting playback silently redirected commands and state sync to a
        # different player mid-session. Only a vanished pin re-resolves.
        players = list(self.mass.players.all_players(False, False))
        resolved = next(
            (p.player_id for p in players if p.state.playback_state == MAPlaybackState.PLAYING),
            players[0].player_id if players else None,
        )
        self._pinned_target_id = resolved
        if active and resolved != pinned:
            self._transfer_active_target(resolved)
        return resolved

    def get_qobuz_provider(self) -> QobuzProvider:
        """Return the configured Music Assistant Qobuz music provider."""
        if not self._qobuz_provider_id:
            raise InvalidDataError("A specific Qobuz music provider instance must be selected")
        provider = self.mass.get_provider(self._qobuz_provider_id)
        if provider is None or provider.domain != "qobuz":
            raise InvalidDataError(
                f"The selected Qobuz music provider {self._qobuz_provider_id!r} "
                "must be configured and loaded"
            )
        return cast("QobuzProvider", provider)

    def get_qobuz_track_id_from_queue_item(self, queue_item: Any) -> str | None:
        """Extract a Qobuz provider track id from an MA QueueItem."""
        track_ids = self.get_qobuz_track_ids_from_queue_item(queue_item)
        return track_ids[0] if track_ids else None

    def get_qobuz_track_ids_from_queue_item(self, queue_item: Any) -> tuple[str, ...]:
        """Extract every Qobuz provider track id from an MA QueueItem."""
        media_item = getattr(queue_item, "media_item", None)
        if media_item is None or getattr(media_item, "media_type", None) is None:
            return ()
        media_type = getattr(media_item.media_type, "value", media_item.media_type)
        if media_type != "track":
            return ()

        track_ids: list[str] = []
        provider = getattr(media_item, "provider", None)
        if provider in (self._qobuz_provider_id, "qobuz"):
            track_ids.append(str(media_item.item_id))

        for mapping in getattr(media_item, "provider_mappings", ()) or ():
            provider_domain = getattr(mapping, "provider_domain", None)
            provider_instance = getattr(mapping, "provider_instance", None)
            if provider_domain == "qobuz" or provider_instance == self._qobuz_provider_id:
                mapping_id = str(mapping.item_id)
                if mapping_id not in track_ids:
                    track_ids.append(mapping_id)
        return tuple(track_ids)

    def _resolve_max_quality(self, configured_quality: int) -> int:
        """Resolve the auto setting against the native Qobuz provider."""
        if configured_quality != AUTO_QUALITY:
            return _normalize_quality_id(configured_quality) or 27
        try:
            provider = self.get_qobuz_provider()
            native_quality = int(cast("str", provider.config.get_value(QOBUZ_CONF_QUALITY)) or "27")
        except AttributeError, InvalidDataError, TypeError, ValueError:
            return 27
        return _normalize_quality_id(native_quality) or 27

    def _shared_zeroconf(self) -> Any | None:
        """Return MA's shared ``Zeroconf`` instance, or ``None`` if unavailable."""
        try:
            return self.mass.discovery.aiozc.zeroconf
        except AttributeError:
            return None

    def _on_app_connected(self, tokens: ConnectTokens) -> None:
        """Handle Qobuz app connection callback."""
        if self._unloaded:
            return
        self.mass.create_task(self._setup_websocket(tokens))

    async def _setup_websocket(self, tokens: ConnectTokens | None) -> None:
        """Set up Qobuz Connect WebSocket command handling."""
        async with self._ws_setup_lock:
            if self._unloaded:
                return
            if self._session is not None:
                # Swapping tokens closes and reopens the socket — never do that
                # to a healthy connection: the local handshake happens at the
                # exact moment the phone hands off, and the cloud needs the
                # connection up to route SET_ACTIVE. (The handshake token adds
                # nothing there; we self-mint via qws/createToken.) Only feed
                # tokens to a session that is still struggling to connect.
                if tokens is not None and not self._session.is_connected:
                    self._session.set_tokens(tokens)
                await self._broadcast_current_volume()
                await self._quality_reporter.report_current(self._max_quality)
                return

            # A single controller socket, like the reference web client: joined
            # via CtrlSrvrJoinSession it both reports renderer state and sends
            # controller verbs, so there is no second connection to race
            # against.
            self._session = QobuzConnectSession(
                self._device_config,
                self._build_session_callbacks(),
                token_refresher=self._refresh_ws_token,
            )
            if tokens is not None:
                self._session.set_tokens(tokens)
            await self._session.start()
            self.logger.info("Qobuz Connect WebSocket session started")

    def _build_session_callbacks(self) -> SessionCallbacks:
        """
        Build the callback bundle for the controller session.

        Delegates translation of every cloud message into a reducer event to
        ``self._coordinator``, with two provider-level overrides that do
        strictly more than the reducer: ``on_set_active`` broadcasts the
        current MA volume + a quality report on activation before handing
        off to the coordinator's takeover/deactivate logic, and
        ``on_quality`` persists the app's quality pick to both this
        provider's config and the native ``qobuz`` provider's config (the
        reducer's ``CloudQuality`` handling is a pure no-op — quality isn't
        an MA-drivable setting, it's config).
        """
        callbacks = self._coordinator.build_session_callbacks()
        return dataclasses.replace(
            callbacks,
            submit=self._submit_cloud_event,
            on_set_active=self._on_set_active,
            on_quality=self._on_quality_change,
            on_connected=self._on_session_connected,
            on_disconnected=self._on_session_disconnected,
        )

    async def _submit_cloud_event(self, event: Event) -> None:
        """Submit a cloud event and reconcile any resulting ownership loss."""
        was_active = self._coordinator.state.active
        try:
            await self._coordinator.submit(event)
        finally:
            if was_active and not self._coordinator.state.active:
                self._release_active_target()

    async def _on_session_connected(self) -> None:
        """Log a confirmed Qobuz websocket connection."""
        self.logger.info("Qobuz Connect WebSocket connected")
        await self._broadcast_current_volume()
        await self._quality_reporter.report_current(self._max_quality)

    async def _on_session_disconnected(self) -> None:
        """Reset connection-scoped reporting state."""
        self._last_sent_muted = None
        self._quality_reporter.reset()
        try:
            await self._coordinator._on_disconnected()
        finally:
            self._release_active_target()

    async def _on_quality_change(self, new_quality: int) -> None:
        """Remember quality selected in Qobuz app."""
        quality = _normalize_quality_id(new_quality)
        if quality is None:
            self.logger.warning("Ignoring unsupported Qobuz Connect quality value: %s", new_quality)
            await self._quality_reporter.report_current(self._max_quality)
            return
        self.logger.info("Qobuz Connect quality changed: %s -> %s", self._max_quality, quality)
        self._configured_max_quality = quality
        self._max_quality = quality
        self._device_config.max_quality = quality
        # This runs as a session dispatcher callback: persistence failures
        # (qobuz provider briefly absent, config write error) must not
        # propagate into the receive loop and cost the connection.
        try:
            await self._update_connect_quality_config(quality)
        except Exception as err:
            self.logger.warning("Failed to persist Qobuz Connect quality change: %s", err)
        try:
            await self._update_qobuz_stream_quality(quality)
        except Exception as err:
            self.logger.warning("Failed to persist native Qobuz quality change: %s", err)
        await self._quality_reporter.report_current(quality)

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

        Sent when the user picks a different renderer in the Qobuz app, or
        when this renderer is (re)selected. Broadcasts the current MA
        volume + a quality report on activation (matching the Qobuz app's
        activation UX), then hands off to the coordinator's own
        translation, whose reducer takeover plays the canonical current
        track (or releases the MA player on deactivation) — the direct
        ``QobuzConnectCoordinator._submit`` call mirrors exactly what
        ``coordinator.build_session_callbacks()``'s own ``on_set_active``
        would have done, since that field is overridden here to add the
        volume/quality broadcast first.
        """
        if self._unload_started():
            return
        if active:
            self.logger.info("Qobuz Connect activated")
            await self._broadcast_current_volume()
            await self._quality_reporter.report_current(self._max_quality)
            if self._unload_started():
                return
            self._suppress_ma_autoplay()
            await self._coordinator.submit(
                CloudSetActive(now_ms=int(time.time() * 1000), active=True)
            )
        else:
            self.logger.info("Qobuz Connect deactivated by cloud; releasing MA player")
            try:
                if self._coordinator.state.active:
                    self.get_target_player_id()
                await self._coordinator.submit(
                    CloudSetActive(now_ms=int(time.time() * 1000), active=False)
                )
            finally:
                self._release_active_target()

    def _suppress_ma_autoplay(self) -> None:
        """Suppress MA autoplay while Qobuz Connect owns the target queue."""
        if self._unloaded:
            return
        if self._ma_autoplay_lease is None:
            player_id = self.get_target_player_id()
            if not player_id:
                return
            queue = self._bridge.get_queue(player_id)
            if queue is None:
                return
            self._ma_autoplay_lease = (player_id, bool(queue.autoplay_enabled))
        else:
            player_id, _enabled = self._ma_autoplay_lease
            queue = self._bridge.get_queue(player_id)
            if queue is None:
                return
        if queue.autoplay_enabled:
            self._bridge.set_autoplay(player_id, False)

    def _restore_ma_autoplay(self) -> None:
        """Restore MA autoplay on the queue captured during activation."""
        lease, self._ma_autoplay_lease = self._ma_autoplay_lease, None
        if lease is None:
            return
        player_id, enabled = lease
        if self._bridge.get_queue(player_id) is not None:
            self._bridge.set_autoplay(player_id, enabled)

    def _transfer_active_target(self, player_id: str | None) -> None:
        """Move coordinator and autoplay ownership to a replacement target."""
        self._coordinator.transfer_target(player_id)
        if self._ma_autoplay_lease is not None:
            leased_player_id, _enabled = self._ma_autoplay_lease
            if leased_player_id == player_id:
                return
            self._restore_ma_autoplay()
        if player_id is None:
            return
        queue = self._bridge.get_queue(player_id)
        if queue is None:
            return
        self._ma_autoplay_lease = (player_id, bool(queue.autoplay_enabled))
        if queue.autoplay_enabled:
            self._bridge.set_autoplay(player_id, False)

    def _release_active_target(self) -> None:
        """Restore the active target's autoplay setting and clear its pin."""
        self._restore_ma_autoplay()
        self._pinned_target_id = None

    def _unload_started(self) -> bool:
        """Return whether provider unload has started."""
        return self._unloaded

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
        if muted is not None and muted != self._last_sent_muted:
            await self._session.send_volume_muted(muted)
            self._last_sent_muted = muted

    def _current_track_duration_ms(self) -> int:
        """
        Return the target player's current queue-item duration in milliseconds.

        ``CanonicalState`` carries no track duration, so the reporter reads it
        live from MA's queue (``QueueItem.duration`` is in seconds); falls back
        to ``0`` when no queue / current item / duration is available.
        """
        player_id = self._bridge.target_player_id()
        queue = self._bridge.get_queue(player_id) if player_id else None
        current_item = getattr(queue, "current_item", None) if queue is not None else None
        duration_s = getattr(current_item, "duration", None) if current_item is not None else None
        return int(duration_s * 1000) if duration_s else 0

    def _current_queue_index(self) -> int | None:
        """Return the target MA queue's current occurrence index."""
        player_id = self._bridge.target_player_id()
        queue = self._bridge.get_queue(player_id) if player_id else None
        current_index = getattr(queue, "current_index", None) if queue is not None else None
        return current_index if isinstance(current_index, int) else None

    def _current_file_quality(self) -> AudioQualityReport | None:
        """Return actual audio properties from the target queue's resolved stream."""
        player_id = self._bridge.target_player_id()
        queue = self._bridge.get_queue(player_id) if player_id else None
        current_item = getattr(queue, "current_item", None) if queue is not None else None
        streamdetails = (
            getattr(current_item, "streamdetails", None) if current_item is not None else None
        )
        audio_format = (
            getattr(streamdetails, "audio_format", None) if streamdetails is not None else None
        )
        if audio_format is None:
            return None
        sampling_rate = int(getattr(audio_format, "sample_rate", 0) or 0)
        bit_depth = int(getattr(audio_format, "bit_depth", 0) or 0)
        channels = int(getattr(audio_format, "channels", 0) or 0)
        if sampling_rate <= 0 or bit_depth <= 0 or channels <= 0:
            return None
        content_type = getattr(audio_format, "content_type", "")
        return AudioQualityReport(
            quality=quality_id_for_format(content_type, sampling_rate, bit_depth),
            sampling_rate=sampling_rate,
            bit_depth=bit_depth,
            channels=channels,
        )

    async def _on_ma_queue_event(self, event: MassEvent) -> None:
        """
        Forward MA transport/mode changes (``QUEUE_UPDATED``) into the coordinator.

        Both the transport lane (playing/paused/position) and the modes
        lane (loop) derive from the same queue-updated snapshot.
        """
        player_id = self.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        if self._coordinator.state.active:
            self._suppress_ma_autoplay()
        await self._coordinator.on_ma_transport_event(player_id)
        await self._coordinator.on_ma_modes_event(player_id)
        await self._quality_reporter.report_file(self._current_file_quality())

    async def _on_ma_queue_items_event(self, event: MassEvent) -> None:
        """Forward MA queue-item mutations (``QUEUE_ITEMS_UPDATED``) into the coordinator."""
        player_id = self.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        await self._coordinator.on_ma_queue_event(player_id)
        await self._quality_reporter.report_file(self._current_file_quality())

    async def _on_ma_player_updated(self, event: MassEvent) -> None:
        """
        Propagate MA-side volume + mute changes (``PLAYER_UPDATED``) into the coordinator.

        Fires for every ``PLAYER_UPDATED`` event MA emits; filtered to our
        target player id. Volume + mute live on the player state and share
        the same source event, so both go through one coordinator call.
        """
        player_id = self.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        await self._coordinator.on_ma_volume_event(player_id)

    def _get_setup_or_legacy_value(
        self, key: str, default: ConfigValueType = None
    ) -> ConfigValueType:
        """Return a setup-flow value, falling back to the legacy option value."""
        setup_value = self.get_setup_value(key)
        if setup_value is not None:
            return setup_value
        return self.config.get_value(key, default)

    async def _stop_runtime(self) -> None:
        """Stop availability-critical services without skipping later owners."""
        if self._discovery is not None:
            discovery, self._discovery = self._discovery, None
            try:
                await discovery.stop()
            except Exception as err:
                self.logger.warning("Failed to stop Qobuz Connect discovery: %s", err)
        for attribute in (
            "_unsubscribe_player_events",
            "_unsubscribe_queue_items_events",
            "_unsubscribe_queue_events",
        ):
            if unsubscribe := getattr(self, attribute):
                setattr(self, attribute, None)
                try:
                    unsubscribe()
                except Exception as err:
                    self.logger.warning("Failed to unsubscribe Qobuz Connect event: %s", err)
        if self._reporter_started:
            self._reporter_started = False
            try:
                await self._reporter.stop()
            except Exception as err:
                self.logger.warning("Failed to stop Qobuz Connect reporter: %s", err)
        if self._flight_recorder_started:
            self._flight_recorder_started = False
            try:
                # Last, so the final rolling dump includes any shutdown warnings.
                await self._flight_recorder.stop()
            except Exception as err:
                self.logger.warning("Failed to stop Qobuz Connect flight recorder: %s", err)


class _LiveSessionProxy:
    """
    Stand-in for ``EffectRunner``'s ``session: QobuzConnectSession`` parameter.

    ``EffectRunner`` is constructed once, in the provider's ``__init__``,
    before the Qobuz Connect websocket exists (``_setup_websocket`` creates
    ``self._session`` later, and reconnects replace it), and it calls
    ``self._session.send_*(...)`` directly with no null-check. This proxy is
    what actually gets bound as ``EffectRunner``'s ``session``: every
    attribute access forwards to whatever ``session_getter()`` currently
    returns, so cloud effects transparently follow reconnects and quietly
    no-op (rather than raising ``AttributeError``) while no socket is up yet.
    """

    def __init__(
        self,
        session_getter: Callable[[], QobuzConnectSession | None],
        logger: logging.Logger,
    ) -> None:
        """Bind the proxy to a getter for the provider's current live session."""
        self._session_getter = session_getter
        self._logger = logger

    def __getattr__(self, name: str) -> Any:
        """Forward to the live session's attribute, or a logging no-op coroutine."""
        session = self._session_getter()
        if session is not None:
            return getattr(session, name)

        async def _noop(*_args: Any, **_kwargs: Any) -> bool:
            self._logger.debug("Qobuz Connect effect '%s' dropped: no active cloud session", name)
            return False

        return _noop


def _quality_config_entry() -> ConfigEntry:
    """Build the runtime maximum-quality option."""
    return ConfigEntry(
        key=CONF_MAX_QUALITY,
        type=ConfigEntryType.STRING,
        default_value="27",
        required=True,
        options=[
            ConfigValueOption("27"),
            ConfigValueOption("7"),
            ConfigValueOption("6"),
            ConfigValueOption("5"),
            ConfigValueOption(str(AUTO_QUALITY)),
        ],
    )


def _normalize_quality_id(value: int) -> int | None:
    """Normalize Qobuz Connect protocol quality values to MA Qobuz format IDs."""
    if value in SUPPORTED_QUALITIES:
        return value
    return PROTOCOL_TO_QUALITY.get(value)
