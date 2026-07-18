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
  / ``CONF_HTTP_PORT`` / ``CONF_MAX_QUALITY`` / ``CONF_INITIAL_VOLUME``.

Depends on:
- :mod:`.discovery`, :mod:`.session`, :mod:`.coordinator`, :mod:`.effect_runner`,
  :mod:`.models`.
- The native ``qobuz`` music provider (``mass.get_provider("qobuz")``)
  must be configured — looked up lazily via ``get_qobuz_provider()``,
  which raises ``InvalidDataError`` if absent.

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
    ConnectTokens,
    DeviceConfig,
    JWTConnectToken,
    QobuzMirror,
    SessionRole,
)
from .outbound_reporter import OutboundReporter
from .session import QobuzConnectSession, SessionCallbacks
from .sync_types import CloudSetActive

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.event import MassEvent
    from music_assistant_models.provider import ProviderManifest

    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType
    from music_assistant.providers.qobuz import QobuzProvider

    from .sync_types import CanonicalState


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
        # Sync core: a pure reducer (reducer.py/sync_types.py) driving an
        # impure coordinator + effect runner, replacing the retired
        # QobuzConnectSyncEngine. MABridge stands alone (it already only
        # wraps ``self``); MetadataResolver/OutboundReporter are built
        # against a retired-engine-shaped host (below) instead of the real
        # engine — see ``_MetadataHost``/``_ReporterHost``.
        self._bridge = MABridge(self)
        self._metadata = MetadataResolver(cast("Any", _MetadataHost(self._bridge)))
        self._reporter = OutboundReporter(
            cast("Any", _ReporterHost(self._bridge, lambda: self._coordinator.state))
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
            controller_enabled=self._enable_controller,
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

    async def loaded_in_mass(self) -> None:
        """Start Qobuz Connect discovery after provider load."""
        logging.getLogger("websockets.client").setLevel(logging.WARNING)
        logging.getLogger("websockets.protocol").setLevel(logging.WARNING)
        # Started first so it captures anything that goes wrong in the rest
        # of the load sequence (discovery port conflicts, mDNS failures, ...).
        await self._flight_recorder.start(extra_logger=self.logger)
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
            zeroconf=self._shared_zeroconf(),
        )
        try:
            await self._discovery.start()
        except Exception:
            # loaded_in_mass runs in a fire-and-forget task whose exception
            # MA only logs at DEBUG — surface the failure (port conflict,
            # mDNS error) at ERROR so it's visible and flight-recorded.
            self.logger.exception(
                "Qobuz Connect discovery failed to start — the device will not be "
                "reachable (is port %s already in use, e.g. by another instance?)",
                self._http_port,
            )
            raise
        self.logger.info(
            "Qobuz Connect target '%s' listening on %s:%s",
            self._publish_name,
            self.mass.streams.bind_ip,
            self._http_port,
        )
        # Controller mode connects to the cloud eagerly, self-minting the
        # websocket token via qws/createToken. Waiting for the app's local
        # handshake (the legacy renderer behavior) loses the FIRST handoff:
        # the phone's SET_ACTIVE races our connect+join and the app bounces
        # playback back when no renderer answers.
        if self._enable_controller:
            self.mass.create_task(self._setup_websocket(None))

    async def unload(self, is_removed: bool = False) -> None:
        """Unload provider and stop network services."""
        # Flag first: any in-flight handshake/_setup_websocket task checks it
        # under the setup lock and bails instead of spawning a zombie session
        # that would fight the next provider instance for the cloud session.
        self._unloaded = True
        if self._unsubscribe_queue_events is not None:
            self._unsubscribe_queue_events()
            self._unsubscribe_queue_events = None
        if self._unsubscribe_queue_items_events is not None:
            self._unsubscribe_queue_items_events()
            self._unsubscribe_queue_items_events = None
        if self._unsubscribe_player_events is not None:
            self._unsubscribe_player_events()
            self._unsubscribe_player_events = None
        await self._reporter.stop()
        self._coordinator.close()
        async with self._ws_setup_lock:
            if self._session:
                await self._session.stop()
                self._session = None
        if self._discovery:
            await self._discovery.stop()
            self._discovery = None
        # Last, so the final rolling dump includes any shutdown warnings.
        await self._flight_recorder.stop()

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
                self._warned_missing_target = None
                return self._target_player_id
            # Warn once per disappearance: this resolver runs on every MA
            # event of every player, so an unconditional warning flooded the
            # log for as long as the player stayed gone.
            if self._warned_missing_target != self._target_player_id:
                self._warned_missing_target = self._target_player_id
                self.logger.warning(
                    "Configured target player no longer exists: %s", self._target_player_id
                )
            return None

        # While the Connect session is ACTIVE the resolved target is pinned:
        # re-resolving "any PLAYING player, else first" on every event meant a
        # pause from the app (target no longer PLAYING) or another player
        # starting playback silently redirected commands and state sync to a
        # different player mid-session. Only a vanished pin re-resolves.
        pinned = self._pinned_target_id
        if (
            pinned is not None
            and self._coordinator.state.active
            and self.mass.players.get_player(pinned)
        ):
            return pinned

        players = list(self.mass.players.all_players(False, False))
        resolved = next(
            (p.player_id for p in players if p.state.playback_state == MAPlaybackState.PLAYING),
            players[0].player_id if players else None,
        )
        self._pinned_target_id = resolved
        return resolved

    def get_qobuz_provider(self) -> QobuzProvider:
        """Return the configured Music Assistant Qobuz music provider."""
        provider = self.mass.get_provider("qobuz")
        if provider is None:
            raise InvalidDataError("The Qobuz music provider must be configured first")
        return cast("QobuzProvider", provider)

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
                # Swapping tokens closes and reopens the socket — never do
                # that to a healthy controller-role connection: the local
                # handshake happens at the exact moment the phone hands off,
                # and the cloud needs the connection up to route SET_ACTIVE.
                # (The handshake token adds nothing there; we self-mint via
                # qws/createToken.) Only feed tokens to a session that is
                # still struggling to connect, or to a legacy renderer-role
                # session, which needs the handshake session uuid to join.
                if tokens is not None and not (
                    self._enable_controller and self._session.is_connected
                ):
                    self._session.set_tokens(tokens)
                await self._broadcast_current_volume()
                await self._session.send_quality_reports(self._max_quality)
                return

            # Single dual-role socket, like the reference web client: joined
            # via CtrlSrvrJoinSession it both reports renderer state and
            # sends controller verbs, so there is no second connection to
            # race against (renderer-role join is the legacy fallback when
            # the controller feature is disabled).
            self._session = QobuzConnectSession(
                self._device_config,
                self._build_session_callbacks(),
                token_refresher=self._refresh_ws_token,
                role=SessionRole.CONTROLLER if self._enable_controller else SessionRole.RENDERER,
            )
            if tokens is not None:
                self._session.set_tokens(tokens)
            await self._session.start()
            await self._broadcast_current_volume()
            await self._session.send_quality_reports(self._max_quality)
            self.logger.info("Qobuz Connect WebSocket connected")

    def _build_session_callbacks(self) -> SessionCallbacks:
        """
        Build the callback bundle shared by the renderer and controller sessions.

        The cloud routes renderer-directed unicast frames to the most
        recently joined connection with our deviceUuid, so both connections
        must dispatch them into the same handlers.

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
            on_set_active=self._on_set_active,
            on_quality=self._on_quality_change,
        )

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
        # This runs as a session dispatcher callback: persistence failures
        # (qobuz provider briefly absent, config write error) must not
        # propagate into the receive loop and cost the connection.
        try:
            await self._update_connect_quality_config(quality)
            await self._update_qobuz_stream_quality(quality)
        except Exception as err:
            self.logger.warning("Failed to persist Qobuz Connect quality change: %s", err)
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
        if active:
            self.logger.info("Qobuz Connect activated")
            await self._broadcast_current_volume()
            if self._session:
                await self._session.send_quality_reports(self._max_quality)
        else:
            self.logger.info("Qobuz Connect deactivated by cloud; releasing MA player")
        await self._coordinator._submit(
            CloudSetActive(now_ms=int(time.time() * 1000), active=active)
        )

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

    async def _on_ma_queue_event(self, event: MassEvent) -> None:
        """
        Forward MA transport/mode changes (``QUEUE_UPDATED``) into the coordinator.

        Both the transport lane (playing/paused/position) and the modes
        lane (loop) derive from the same queue-updated snapshot.
        """
        player_id = self.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        await self._coordinator.on_ma_transport_event(player_id)
        await self._coordinator.on_ma_modes_event(player_id)

    async def _on_ma_queue_items_event(self, event: MassEvent) -> None:
        """Forward MA queue-item mutations (``QUEUE_ITEMS_UPDATED``) into the coordinator."""
        player_id = self.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        await self._coordinator.on_ma_queue_event(player_id)

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


class _MetadataHost:
    """
    Minimal duck-typed 'engine' satisfying ``MetadataResolver``'s constructor.

    ``MetadataResolver`` was built against the retired ``QobuzConnectSyncEngine``
    and only ever reads ``engine.bridge`` through the one method the effect
    runner actually calls (``get_track_or_none``), so this host needs nothing
    beyond ``bridge``.
    """

    __slots__ = ("bridge",)

    def __init__(self, bridge: MABridge) -> None:
        """Wrap the provider's MABridge for MetadataResolver's benefit."""
        self.bridge = bridge


class _ReporterHost:
    """
    Minimal duck-typed 'engine' satisfying ``OutboundReporter``'s constructor.

    ``qobuz_state`` is a live projection of the coordinator's
    ``CanonicalState`` (rather than a field some caller has to remember to
    keep in sync) so the heartbeat / ``ReportState`` effect keeps reporting
    current position/track/playing data off a single source of truth.
    ``CanonicalState`` doesn't
    carry ``duration_ms``/``buffer_state``/``next_item`` (those lived on the
    retired ``QobuzMirror`` only); ``buffer_state``/``next_item`` fall back to
    safe defaults, while ``duration_ms`` is read live from MA's current queue
    item so the Qobuz app's progress bar isn't stuck at a zero-length track.
    """

    __slots__ = ("_state_getter", "bridge")

    def __init__(self, bridge: MABridge, state_getter: Callable[[], CanonicalState]) -> None:
        """Wrap the provider's MABridge and a getter for the coordinator's live state."""
        self.bridge = bridge
        self._state_getter = state_getter

    @property
    def qobuz_state(self) -> QobuzMirror:
        """Project the coordinator's current ``CanonicalState`` as a ``QobuzMirror``."""
        state = self._state_getter()
        current_item = None
        if state.current_id is not None:
            current_item = next(
                (t for t in state.tracks if t.track_id == str(state.current_id)), None
            )
        return QobuzMirror(
            queue_version=state.cloud_version,
            current_item=current_item,
            playing_state=state.playing,
            position_ms=state.position_ms,
            position_timestamp_ms=state.position_anchor_ms,
            duration_ms=self._current_duration_ms(),
            tracks=list(state.tracks),
            loop_mode=state.loop,
            autoplay_mode=state.autoplay,
        )

    @property
    def _is_active(self) -> bool:
        """Whether the heartbeat should report: active AND a target player exists."""
        # Without the target-player check, a removed/vanished player froze
        # canonical state at its last value and the heartbeat kept reporting
        # stale PLAYING forever — ghost playback in the Qobuz app.
        return self._state_getter().active and self.bridge.target_player_id() is not None

    def _current_duration_ms(self) -> int:
        """
        Return the target player's current queue-item duration in milliseconds.

        ``CanonicalState`` carries no track duration, so read it live from MA's
        queue (``QueueItem.duration`` is in seconds); falls back to ``0`` when no
        queue / current item / duration is available.
        """
        player_id = self.bridge.target_player_id()
        queue = self.bridge.get_queue(player_id) if player_id else None
        current_item = getattr(queue, "current_item", None) if queue is not None else None
        duration_s = getattr(current_item, "duration", None) if current_item is not None else None
        return int(duration_s * 1000) if duration_s else 0


def _normalize_quality_id(value: int) -> int | None:
    """Normalize Qobuz Connect protocol quality values to MA Qobuz format IDs."""
    if value in SUPPORTED_QUALITIES:
        return value
    return PROTOCOL_TO_QUALITY.get(value)
