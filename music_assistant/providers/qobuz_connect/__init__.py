"""Experimental Qobuz Connect receiver for Music Assistant.

This prototype reuses qobuz-proxy's Connect discovery/WebSocket/protobuf layer,
but maps playback commands to Music Assistant's native player queue. That keeps
streaming, seeking, buffering and provider auth inside Music Assistant instead
of forwarding audio through a DLNA bridge.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _installed_pkg_version
from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.config_entries import ConfigEntry, ConfigValueOption
from music_assistant_models.enums import ConfigEntryType, QueueOption
from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.errors import InvalidDataError, PlayerUnavailableError

from music_assistant.models.plugin import PluginProvider

# Pin the qobuz-proxy version expected by this provider. Must match the @<tag>
# at the end of the `requirements` URL in manifest.json. If the installed version
# drifts (e.g. after bumping the manifest tag), we raise ImportError so MA's
# provider loader reinstalls from the manifest URL.
EXPECTED_QOBUZ_PROXY_VERSION = "1.3.5"

try:
    _qobuz_proxy_installed_version: str | None = _installed_pkg_version("qobuz-proxy")
except PackageNotFoundError:
    _qobuz_proxy_installed_version = None

if (
    _qobuz_proxy_installed_version is not None
    and _qobuz_proxy_installed_version != EXPECTED_QOBUZ_PROXY_VERSION
):
    raise ImportError(
        f"qobuz-proxy {EXPECTED_QOBUZ_PROXY_VERSION} required, "
        f"but {_qobuz_proxy_installed_version} is installed"
    )

from qobuz_proxy.auth.oauth import OAUTH_APP_ID  # noqa: E402
from qobuz_proxy.backends.types import BufferStatus, PlaybackState  # noqa: E402
from qobuz_proxy.config import (  # noqa: E402
    AUTO_QUALITY,
    BackendConfig,
    Config,
    DeviceConfig,
    LoggingConfig,
    QobuzConfig,
    ServerConfig,
)
from qobuz_proxy.connect import ConnectTokens, DiscoveryService, WsManager  # noqa: E402
from qobuz_proxy.playback import (  # noqa: E402
    PlaybackCommandHandler,
    QobuzQueue,
    QueueHandler,
    QueueTrack,
    StateReporter,
    VolumeCommandHandler,
)

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.media_items import Track
    from music_assistant_models.provider import ProviderManifest
    from qobuz_proxy.playback.state_reporter import PlaybackStateReport

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

        self._queue = QobuzQueue()
        self._player = MAQueueBackedQobuzPlayer(self)
        self._discovery: DiscoveryService | None = None
        self._ws_manager: WsManager | None = None
        self._state_reporter: StateReporter | None = None
        self._queue_handler: QueueHandler | None = None
        self._playback_handler: PlaybackCommandHandler | None = None
        self._volume_handler: VolumeCommandHandler | None = None
        self._ws_setup_lock = asyncio.Lock()

    async def loaded_in_mass(self) -> None:
        """Start Qobuz Connect discovery after provider load."""
        await self._queue.start()
        await self._player.start()
        self._discovery = DiscoveryService(
            config=self._build_qobuz_proxy_config(),
            app_id=OAUTH_APP_ID,
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
        if self._state_reporter:
            await self._state_reporter.stop()
        if self._ws_manager:
            await self._ws_manager.stop()
        if self._discovery:
            await self._discovery.stop()
        await self._player.stop()
        await self._queue.stop()

    @property
    def qobuz_queue(self) -> QobuzQueue:
        """Return qobuz-proxy queue state."""
        return self._queue

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

    def _build_qobuz_proxy_config(self) -> Config:
        """Build the minimal qobuz-proxy config needed by discovery/ws code."""
        return Config(
            qobuz=QobuzConfig(max_quality=self._max_quality),
            device=DeviceConfig(name=self._publish_name, uuid=self._device_uuid),
            backend=BackendConfig(type="stub"),
            server=ServerConfig(
                http_port=self._http_port,
                bind_address=self.mass.streams.bind_ip,
            ),
            logging=LoggingConfig(level="debug"),
        )

    def _on_app_connected(self, tokens: ConnectTokens) -> None:
        """Handle Qobuz app connection callback."""
        self.mass.create_task(self._setup_websocket(tokens))

    async def _setup_websocket(self, tokens: ConnectTokens) -> None:
        """Set up Qobuz Connect WebSocket command handling."""
        async with self._ws_setup_lock:
            if self._ws_manager is not None:
                self._ws_manager.set_tokens(tokens)
                return

            self._ws_manager = WsManager(config=self._build_qobuz_proxy_config())
            self._ws_manager.set_tokens(tokens)
            self._ws_manager.set_max_audio_quality(self._max_quality)

            # MAQueueBackedQobuzPlayer duck-types qobuz_proxy.playback.QobuzPlayer
            # rather than inheriting; cast to Any so mypy stops complaining.
            ma_player_as_any = cast("Any", self._player)

            self._queue_handler = QueueHandler(self._queue)
            self._playback_handler = PlaybackCommandHandler(
                ma_player_as_any,
                queue=self._queue,
                on_quality_change=self._on_quality_change,
            )
            self._volume_handler = VolumeCommandHandler(ma_player_as_any)

            # qobuz_proxy expects sync handlers; we dispatch async work via
            # mass.create_task and drop the returned Task, so cast to Any.
            for msg_type in self._queue_handler.get_message_types():
                self._ws_manager.register_handler(
                    msg_type,
                    cast(
                        "Any",
                        lambda mt, msg, h=self._queue_handler: self.mass.create_task(
                            h.handle_message(mt, msg)
                        ),
                    ),
                )
            for msg_type in self._playback_handler.get_message_types():
                self._ws_manager.register_handler(
                    msg_type,
                    cast(
                        "Any",
                        lambda mt, msg, h=self._playback_handler: self.mass.create_task(
                            h.handle_message(mt, msg)
                        ),
                    ),
                )
            for msg_type in self._volume_handler.get_message_types():
                self._ws_manager.register_handler(
                    msg_type,
                    cast(
                        "Any",
                        lambda mt, msg, h=self._volume_handler: self.mass.create_task(
                            h.handle_message(mt, msg)
                        ),
                    ),
                )

            self._state_reporter = StateReporter(
                player=ma_player_as_any,
                queue=self._queue,
                send_callback=cast("Any", self._send_state_report),
            )
            self._player.set_state_reporter(self._state_reporter)

            await self._ws_manager.start()
            await self._state_reporter.start()
            await self._player.broadcast_current_volume()
            self.logger.info("Qobuz Connect WebSocket connected")

    async def _on_quality_change(self, new_quality: int) -> None:
        """Remember quality selected in Qobuz app."""
        self.logger.info("Qobuz Connect quality changed: %s -> %s", self._max_quality, new_quality)
        self._max_quality = new_quality

    async def _send_state_report(self, report: PlaybackStateReport) -> None:
        """Send playback state to Qobuz Connect."""
        if not self._ws_manager:
            return
        playing_state = report.playing_state
        if playing_state in (PlaybackState.LOADING, PlaybackState.ERROR):
            playing_state = PlaybackState.STOPPED
        await self._ws_manager.send_state_update(
            playing_state=int(playing_state),
            buffer_state=int(report.buffer_state),
            position_ms=report.position_value_ms,
            position_timestamp_ms=report.position_timestamp_ms,
            duration_ms=report.duration_ms,
            queue_item_id=report.current_queue_item_id,
            queue_version_major=report.queue_version_major,
            queue_version_minor=report.queue_version_minor,
        )


class MAQueueBackedQobuzPlayer:
    """Small adapter with the QobuzPlayer surface expected by qobuz-proxy handlers."""

    def __init__(self, provider: QobuzConnectProvider) -> None:
        """Initialize adapter."""
        self.provider = provider
        self.mass = provider.mass
        self.queue = provider.qobuz_queue
        self.backend = self
        self._current_track: QueueTrack | None = None
        self._duration_ms = 0
        self._state = PlaybackState.STOPPED
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)
        self._state_reporter: StateReporter | None = None
        self._volume_initialized = False
        self._last_volume = provider._initial_volume

    async def start(self) -> None:
        """Start adapter."""

    async def stop(self) -> None:
        """Stop adapter."""
        await self.stop_playback()

    def set_state_reporter(self, reporter: StateReporter) -> None:
        """Set Qobuz Connect state reporter."""
        self._state_reporter = reporter

    async def broadcast_current_volume(self) -> None:
        """Report the target player's current volume to the Qobuz app on connect."""
        if not self.provider._ws_manager:
            return
        if self._volume_initialized:
            volume = await self.get_volume()
        else:
            volume = self.provider._initial_volume
            player_id = self.provider.get_target_player_id()
            if player_id and (player := self.mass.players.get_player(player_id)):
                if player.state.volume_level is not None:
                    volume = player.state.volume_level
            self._last_volume = volume
            self._volume_initialized = True
        await self.provider._ws_manager.send_volume_changed(volume)

    async def set_volume(self, level: int) -> int:
        """Set MA player volume from Qobuz app."""
        player_id = self._require_target_player_id()
        volume = max(0, min(100, int(level)))
        await self.mass.players.cmd_volume_set(player_id, volume)
        self._last_volume = volume
        self._volume_initialized = True
        if self.provider._ws_manager:
            await self.provider._ws_manager.send_volume_changed(volume)
        return volume

    async def set_volume_delta(self, delta: int) -> int:
        """Adjust MA player volume from Qobuz app."""
        return await self.set_volume(await self.get_volume() + int(delta))

    async def get_volume(self) -> int:
        """Return current MA player volume."""
        player_id = self.provider.get_target_player_id()
        if player_id and (player := self.mass.players.get_player(player_id)):
            volume = player.state.volume_level
            if volume is not None:
                self._last_volume = volume
                return volume
        return self._last_volume

    async def set_loop_mode(self, mode: int) -> None:
        """Accept Qobuz loop-mode commands for now."""
        self.provider.logger.debug("Qobuz Connect loop mode ignored by prototype: %s", mode)

    async def set_shuffle_mode(self, shuffle_on: bool) -> None:
        """Accept Qobuz shuffle-mode commands for now."""
        self.provider.logger.debug(
            "Qobuz Connect shuffle mode ignored by prototype: %s", shuffle_on
        )

    async def set_autoplay_mode(self, autoplay_on: bool) -> None:
        """Accept Qobuz autoplay-mode commands for now."""
        self.provider.logger.debug(
            "Qobuz Connect autoplay mode ignored by prototype: %s", autoplay_on
        )

    async def load_track(self, queue_item_id: int, track_id: str) -> bool:
        """Load current Qobuz track metadata without starting playback."""
        track = QueueTrack(queue_item_id=queue_item_id, track_id=track_id)
        ma_track = await self._get_ma_track(track_id)
        track.metadata = {
            "title": ma_track.name,
            "artist": ma_track.artist_str,
            "album": ma_track.album.name if ma_track.album else "",
            "duration_ms": (ma_track.duration or 0) * 1000,
        }
        track.duration_ms = track.metadata["duration_ms"]
        self._current_track = track
        self._duration_ms = track.duration_ms
        self._set_position(0)
        await self._send_state_update()
        return True

    async def play(self, position_ms: int = 0) -> bool:
        """Start or resume MA native Qobuz playback."""
        player_id = self._require_target_player_id()
        if self._state == PlaybackState.PAUSED:
            await self.mass.player_queues.play(player_id)
            self._state = PlaybackState.PLAYING
            self._set_position(position_ms or self._position_value_ms)
            await self._send_state_update()
            return True

        if not self._current_track:
            track = await self.queue.get_current_track()
            if not track:
                track = await self.queue.advance_to_next()
            if not track:
                return False
            await self.load_track(track.queue_item_id, track.track_id)

        assert self._current_track is not None
        self._state = PlaybackState.LOADING
        await self._send_state_update()

        tracks = await self._build_ma_queue_tracks()
        current_ma_track = await self._get_ma_track(self._current_track.track_id)
        await self.mass.player_queues.play_media(
            queue_id=player_id,
            media=cast("Any", tracks or current_ma_track),
            option=QueueOption.REPLACE,
            start_item=current_ma_track,
        )
        # Apply the handoff position via play_index rather than seek(): seek()
        # requires queue.active=True, which is only flipped on by an async
        # player-state callback after the source switch completes — so it races
        # with the play_media that just kicked off the switch. play_index has no
        # such check and threads seek_position through the same start path.
        if (
            position_ms > 0
            and (queue := self.mass.player_queues.get(player_id)) is not None
            and queue.current_index is not None
        ):
            await self.mass.player_queues.play_index(
                player_id,
                queue.current_index,
                seek_position=position_ms // 1000,
            )
        self._state = PlaybackState.PLAYING
        self._set_position(position_ms)
        await self._send_state_update()
        return True

    async def pause(self) -> bool:
        """Pause MA queue playback."""
        player_id = self._require_target_player_id()
        if self._state == PlaybackState.PLAYING:
            await self.mass.player_queues.pause(player_id)
            self._state = PlaybackState.PAUSED
            self._set_position(self.current_position_ms)
            await self._send_state_update()
        return True

    async def stop_playback(self) -> None:
        """Stop MA queue playback."""
        player_id = self.provider.get_target_player_id()
        if player_id:
            with contextlib.suppress(Exception):
                await self.mass.player_queues.stop(player_id)
        self._state = PlaybackState.STOPPED
        self._set_position(0)
        await self._send_state_update()

    async def seek(self, position_ms: int) -> bool:
        """Seek MA native queue playback."""
        if not self._current_track:
            return False
        if self._state not in (PlaybackState.PLAYING, PlaybackState.PAUSED):
            self._set_position(position_ms)
            await self._send_state_update()
            return True
        player_id = self._require_target_player_id()
        await self.mass.player_queues.seek(player_id, position_ms // 1000)
        self._set_position(position_ms)
        await self._send_state_update()
        return True

    async def next_track(self) -> bool:
        """Skip to next MA queue track."""
        player_id = self._require_target_player_id()
        await self.mass.player_queues.next(player_id)
        track = await self.queue.advance_to_next()
        if track:
            self._current_track = track
            self._duration_ms = track.duration_ms
        self._set_position(0)
        await self._send_state_update()
        return True

    async def previous_track(self) -> bool:
        """Skip to previous MA queue track."""
        player_id = self._require_target_player_id()
        await self.mass.player_queues.previous(player_id)
        track = await self.queue.go_to_previous()
        if track:
            self._current_track = track
            self._duration_ms = track.duration_ms
        self._set_position(0)
        await self._send_state_update()
        return True

    async def get_buffer_status(self) -> BufferStatus:
        """Return buffer status for Qobuz state reporting."""
        return BufferStatus.OK

    async def _build_ma_queue_tracks(self) -> list[Track]:
        """Build MA Track list from current Qobuz Connect queue snapshot."""
        tracks: list[Track] = []
        async with self.queue._lock:
            qobuz_tracks = list(self.queue._tracks)
        for q_track in qobuz_tracks:
            tracks.append(await self._get_ma_track(q_track.track_id))
        return tracks

    async def _get_ma_track(self, track_id: str) -> Track:
        """Fetch a Qobuz track through the configured MA Qobuz provider."""
        return await self.provider.get_qobuz_provider().get_track(track_id)

    def _require_target_player_id(self) -> str:
        """Return target player id or raise."""
        player_id = self.provider.get_target_player_id()
        if not player_id:
            raise PlayerUnavailableError("No Music Assistant player available for Qobuz Connect")
        return player_id

    def _set_position(self, position_ms: int) -> None:
        """Update local timestamp-based position."""
        self._position_value_ms = max(0, position_ms)
        self._position_timestamp_ms = int(time.time() * 1000)

    async def _send_state_update(self) -> None:
        """Send Qobuz state update."""
        if self._state_reporter:
            await self._state_reporter.report_now()

    @property
    def current_position_ms(self) -> int:
        """Return current position in ms."""
        if self._state != PlaybackState.PLAYING:
            return self._position_value_ms
        return self._position_value_ms + int(time.time() * 1000) - self._position_timestamp_ms

    @property
    def current_track(self) -> QueueTrack | None:
        """Return current Qobuz queue track."""
        return self._current_track

    @property
    def duration_ms(self) -> int:
        """Return current track duration in ms."""
        return self._duration_ms

    @property
    def state(self) -> PlaybackState:
        """Return current playback state."""
        return self._state
