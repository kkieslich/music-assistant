"""
Qobuz Connect WebSocket transport.

Owns:
- The cloud WebSocket lifecycle: connect, AUTHENTICATE, SUBSCRIBE,
  send JOIN_SESSION, reconnect on disconnect (with exponential backoff),
  graceful close on shutdown.
- JWT token refresh: ``set_tokens()`` swaps the credentials and the
  ``token_version`` counter ticks so a stale reconnect attempt knows
  to wait for fresh tokens.
- Inbound frame loop: decode an outer envelope, unpack the inner
  ``QConnectBatch``, dispatch each message to the appropriate callback
  the provider registered at construction time.
- A small set of typed ``send_*`` helpers (renderer_state, volume,
  quality, queue_load_tracks, autoplay_load_tracks, player_state) that
  marshal via :mod:`.protocol` and write to the socket.

Exposes:
- ``QobuzConnectSession`` (callback-style interface).

Depends on:
- :mod:`.protocol` for encode/decode, :mod:`.models` for enums + DTOs.
- The ``websockets`` library (network transport).
- **No MA imports.** The session has no awareness of the player queue
  or any MA concept; everything domain-specific is routed via the
  provider-supplied callbacks (``on_set_state``, ``on_volume``,
  ``on_set_active``, ``on_queue_load_ack``, ...).

See :doc:`ARCHITECTURE` for the inbound dispatch table and the steady-state
message loop diagram.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import websockets
from websockets import ClientConnection

from .models import (
    BufferState,
    ConnectTokens,
    DeviceConfig,
    JWTConnectToken,
    LoopMode,
    PlayingState,
    QConnectMessageType,
    QueueError,
    QueueLoadAck,
    QueueStateSnapshot,
    QueueTracksAddedEvent,
    QueueVersion,
    SetStateEvent,
)
from .protocol import QobuzConnectCodec

LOGGER = logging.getLogger(__name__)

# Message types that Qobuz broadcasts to controllers about state changes
# on *other* renderers (or session-wide concerns). They reach us because of
# how subscription routing works in Qobuz Connect, but a renderer needs to
# take no action — they're explicitly acknowledged here so they don't fall
# through to the "Unhandled" warning. Numeric values are inlined because
# these types aren't in QConnectMessageType (Phase B Tier 3, see
# ARCHITECTURE.md).
_KNOWN_IGNORED_MESSAGE_TYPES: frozenset[int] = frozenset(
    {
        81,  # SRVR_CTRL_SESSION_STATE
        82,  # SRVR_CTRL_RENDERER_STATE_UPDATED
        83,  # SRVR_CTRL_ADD_RENDERER
        84,  # SRVR_CTRL_UPDATE_RENDERER
        85,  # SRVR_CTRL_REMOVE_RENDERER
        86,  # SRVR_CTRL_ACTIVE_RENDERER_CHANGED
        87,  # SRVR_CTRL_VOLUME_CHANGED
        97,  # SRVR_CTRL_LOOP_MODE_SET
        98,  # SRVR_CTRL_VOLUME_MUTED
        99,  # SRVR_CTRL_MAX_AUDIO_QUALITY_CHANGED
        100,  # SRVR_CTRL_FILE_AUDIO_QUALITY_CHANGED
    }
)

PING_INTERVAL = 10.0
PONG_TIMEOUT = 30.0
RECV_TIMEOUT = 1.0
TOKEN_REFRESH_BUFFER = 60
INITIAL_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0


class TokenRefreshRequired(Exception):
    """Raised when the websocket must wait for refreshed tokens."""


class QobuzServerDisconnect(Exception):
    """Raised when the Qobuz server asks this websocket to reconnect."""


@dataclass(slots=True, frozen=True)
class SessionCallbacks:
    """
    Typed bundle of every callback the provider supplies to the session.

    Bundling avoids the 15-argument constructor explosion that grew as
    Phase B added handlers for the queue-state / tracks-added / mode-set
    message types. Phase C will move these into an
    ``InboundDispatcher`` collaborator.
    """

    on_set_state: Callable[[SetStateEvent], Awaitable[None]]
    on_queue_load_ack: Callable[[QueueLoadAck], Awaitable[None]]
    on_queue_error: Callable[[QueueError], Awaitable[None]]
    on_queue_version: Callable[[QueueVersion], Awaitable[None]]
    on_queue_state: Callable[[QueueStateSnapshot], Awaitable[None]]
    on_queue_tracks_added: Callable[[QueueTracksAddedEvent], Awaitable[None]]
    on_volume: Callable[[int], Awaitable[None]]
    on_volume_delta: Callable[[int], Awaitable[None]]
    on_quality: Callable[[int], Awaitable[None]]
    on_loop_mode: Callable[[LoopMode], Awaitable[None]]
    on_shuffle_mode: Callable[[bool], Awaitable[None]]
    on_autoplay_mode: Callable[[bool], Awaitable[None]]
    on_state_request: Callable[[], Awaitable[None]]
    on_set_active: Callable[[bool], Awaitable[None]]


class QobuzConnectSession:
    """Own the Qobuz cloud websocket and typed protocol events."""

    def __init__(self, device: DeviceConfig, callbacks: SessionCallbacks) -> None:
        """Initialize session."""
        self.device = device
        self._device_uuid = _uuid_to_bytes(device.uuid)
        self._codec = QobuzConnectCodec(self._device_uuid)
        self._ws: ClientConnection | None = None
        self._ws_token: JWTConnectToken | None = None
        self._session_uuid: bytes | None = None
        self._token_update_event = asyncio.Event()
        self._token_version = 0
        self._should_run = False
        self._is_connected = False
        self._receive_task: asyncio.Task[None] | None = None
        self._pending_messages: list[bytes] = []
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._cb = callbacks

    @property
    def is_connected(self) -> bool:
        """Return whether websocket is connected."""
        return self._is_connected

    def set_tokens(self, tokens: ConnectTokens) -> None:
        """Set or refresh Qobuz cloud tokens."""
        previous_token = self._ws_token
        previous_session_uuid = self._session_uuid
        if tokens.ws_token:
            self._ws_token = tokens.ws_token
        self._session_uuid = _uuid_to_bytes(tokens.session_id)
        self._token_version += 1
        self._token_update_event.set()
        if (
            self._should_run
            and self._ws
            and (previous_token != self._ws_token or previous_session_uuid != self._session_uuid)
        ):
            asyncio.create_task(self._close_for_token_refresh())

    async def start(self) -> None:
        """Start websocket connection loop."""
        if not self._ws_token or not self._ws_token.is_valid():
            LOGGER.error("Cannot start Qobuz Connect session without websocket token")
            return
        if self._should_run:
            return
        self._should_run = True
        self._receive_task = asyncio.create_task(self._connection_loop())

    async def stop(self) -> None:
        """Stop websocket session."""
        self._should_run = False
        if self._ws:
            await self._ws.close()
        if self._receive_task:
            self._receive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._receive_task

    async def send_renderer_state(
        self,
        *,
        playing_state: PlayingState,
        buffer_state: BufferState,
        position_ms: int,
        position_timestamp_ms: int,
        duration_ms: int,
        queue_item_id: int,
        queue_version: QueueVersion,
    ) -> bool:
        """Report renderer state to Qobuz."""
        return await self.send_message(
            self._codec.encode_renderer_state(
                playing_state=playing_state,
                buffer_state=buffer_state,
                position_ms=position_ms,
                position_timestamp_ms=position_timestamp_ms,
                duration_ms=duration_ms,
                queue_item_id=queue_item_id,
                queue_version=queue_version,
            )
        )

    async def send_queue_load_tracks(
        self,
        *,
        action_uuid: bytes,
        track_id: str,
        queue_version: QueueVersion,
        qobuz_reference_id: int | None = None,
        queue_position: int = 0,
        autoplay_reset: bool = True,
        context_uuid: bytes | None = None,
        qweb_track_session: bool = False,
    ) -> bool:
        """Ask the Qobuz cloud queue to load a track selected in MA."""
        return await self.send_message(
            self._codec.encode_queue_load_tracks(
                action_uuid=action_uuid,
                track_id=track_id,
                queue_version=queue_version,
                qobuz_reference_id=qobuz_reference_id,
                queue_position=queue_position,
                autoplay_reset=autoplay_reset,
                context_uuid=context_uuid,
                qweb_track_session=qweb_track_session,
            )
        )

    async def send_autoplay_load_tracks(
        self,
        *,
        action_uuid: bytes,
        track_ids: list[int],
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
        autoplay_reset: bool = True,
        autoplay_loading: bool = False,
        prepend: bool = True,
        append: bool = False,
    ) -> bool:
        """Ask the Qobuz cloud to load explicit track ids through the autoplay path."""
        return await self.send_message(
            self._codec.encode_autoplay_load_tracks(
                action_uuid=action_uuid,
                track_ids=track_ids,
                queue_version=queue_version,
                context_uuid=context_uuid,
                autoplay_reset=autoplay_reset,
                autoplay_loading=autoplay_loading,
                prepend=prepend,
                append=append,
            )
        )

    async def send_player_state(
        self,
        *,
        playing_state: PlayingState,
        position_ms: int,
        queue_version: QueueVersion,
        queue_item_id: int,
    ) -> bool:
        """Send controller player-state command."""
        return await self.send_message(
            self._codec.encode_player_state(
                playing_state=playing_state,
                position_ms=position_ms,
                queue_version=queue_version,
                queue_item_id=queue_item_id,
            )
        )

    async def send_volume_changed(self, volume: int) -> bool:
        """Report renderer volume to Qobuz."""
        return await self.send_message(self._codec.encode_volume_changed(volume))

    async def send_volume_muted(self, muted: bool) -> bool:
        """Report renderer mute state to Qobuz."""
        return await self.send_message(self._codec.encode_volume_muted(muted))

    async def send_quality_reports(self, quality: int) -> None:
        """Report device/file/max quality to Qobuz."""
        await self.send_message(self._codec.encode_file_audio_quality_changed(quality))
        await self.send_message(self._codec.encode_device_audio_quality_changed(quality))
        await self.send_message(self._codec.encode_max_audio_quality_changed(quality))

    async def send_message(self, data: bytes) -> bool:
        """Send an encoded websocket frame, queuing if disconnected."""
        if self._ws and self._is_connected:
            try:
                await self._ws.send(data)
                return True
            except websockets.ConnectionClosed:
                LOGGER.debug("Qobuz websocket closed while sending; message queued")
            except Exception:
                LOGGER.exception("Failed to send Qobuz websocket message")
        self._pending_messages.append(data)
        return False

    async def _connection_loop(self) -> None:
        while self._should_run:
            should_backoff = True
            try:
                if not await self._wait_for_valid_token(TOKEN_REFRESH_BUFFER):
                    break
                assert self._ws_token is not None
                async with websockets.connect(
                    self._ws_token.endpoint,
                    ping_interval=PING_INTERVAL,
                    ping_timeout=PONG_TIMEOUT,
                ) as ws:
                    self._ws = ws
                    self._is_connected = False
                    await ws.send(self._codec.encode_authenticate(self._ws_token.jwt))
                    if self._session_uuid:
                        await ws.send(self._codec.encode_subscribe(self._session_uuid))
                        await ws.send(
                            self._codec.encode_join_session(
                                self._device_uuid,
                                self.device.name,
                                self._session_uuid,
                                self.device.max_quality,
                            )
                        )
                    self._is_connected = True
                    self._reconnect_delay = INITIAL_RECONNECT_DELAY
                    await self._flush_pending_messages()
                    await self._receive_loop()
            except TokenRefreshRequired:
                should_backoff = False
                self._is_connected = False
            except QobuzServerDisconnect:
                # Outer-envelope ``DISCONNECT`` (type 10) — the cloud closed
                # this WS, typically because another renderer claimed the
                # session or our JWT expired. The outer loop reconnects
                # after the backoff delay; the cloud will (re)send
                # AUTHENTICATE/SUBSCRIBE/JOIN_SESSION on the new socket.
                LOGGER.info("Qobuz Connect server closed the session — reconnecting")
                self._is_connected = False
            except asyncio.CancelledError:
                raise
            except websockets.ConnectionClosed as err:
                LOGGER.debug("Qobuz Connect websocket closed: %s", err)
                self._is_connected = False
            except Exception:
                LOGGER.exception("Qobuz Connect websocket error")
                self._is_connected = False
            finally:
                self._ws = None
                self._is_connected = False
            if self._should_run and should_backoff:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)

    async def _receive_loop(self) -> None:
        while self._should_run and self._ws:
            try:
                data = await asyncio.wait_for(self._ws.recv(), timeout=RECV_TIMEOUT)
                if isinstance(data, bytes):
                    await self._handle_message(data)
            except TimeoutError:
                if self._ws_token and _token_expiring(self._ws_token, TOKEN_REFRESH_BUFFER):
                    raise TokenRefreshRequired
            except websockets.ConnectionClosed:
                raise

    async def _handle_message(self, data: bytes) -> None:
        decoded = self._codec.decode_frame(data)
        if not decoded:
            return
        if decoded.msg_type.name == "PAYLOAD":
            await self._handle_payload(decoded.payload)
        elif decoded.msg_type.name == "ERROR":
            LOGGER.error("Qobuz websocket error %s: %s", decoded.error_code, decoded.error_message)
        elif decoded.msg_type.name == "DISCONNECT":
            raise QobuzServerDisconnect

    async def _handle_payload(self, payload: bytes | None) -> None:
        if not payload:
            return
        batch = self._codec.decode_qconnect_batch(payload)
        if not batch:
            return
        for msg in batch.messages:
            await self._dispatch_inner_message(msg)

    async def _dispatch_inner_message(self, msg: Any) -> None:  # noqa: PLR0915
        # Inherently statement-heavy: one branch per known inner message
        # type. Phase C of the qobuz_connect redesign replaces this with a
        # proper dispatcher table (see ARCHITECTURE.md).
        msg_type = msg.messageType
        if msg_type == QConnectMessageType.SRVR_RNDR_SET_STATE:
            if event := self._codec.parse_set_state(msg):
                LOGGER.debug(
                    "Qobuz SET_STATE state=%s pos=%s current=%s next=%s qv=%s",
                    event.playing_state,
                    event.position_ms,
                    _format_track_ref(event.current_item),
                    _format_track_ref(event.next_item),
                    event.queue_version,
                )
                await self._cb.on_set_state(event)
        elif msg_type == QConnectMessageType.SRVR_RNDR_SET_VOLUME:
            if msg.HasField("srvrRndrSetVolume"):
                vol = msg.srvrRndrSetVolume
                if vol.HasField("volume"):
                    await self._cb.on_volume(vol.volume)
                elif vol.HasField("volumeDelta"):
                    await self._cb.on_volume_delta(vol.volumeDelta)
        elif msg_type == QConnectMessageType.SRVR_RNDR_SET_MAX_AUDIO_QUALITY:
            if msg.HasField("srvrRndrSetMaxAudioQuality"):
                await self._cb.on_quality(msg.srvrRndrSetMaxAudioQuality.maxAudioQuality)
        elif msg_type == QConnectMessageType.SRVR_RNDR_SET_ACTIVE:
            if msg.HasField("srvrRndrSetActive"):
                active = bool(msg.srvrRndrSetActive.active)
                LOGGER.debug("Qobuz SET_ACTIVE active=%s", active)
                await self._cb.on_set_active(active)
        elif msg_type == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED:
            if ack := self._codec.parse_queue_load_ack(msg):
                LOGGER.debug(
                    "Qobuz queue-load ACK qv=%s tracks=%s",
                    ack.queue_version,
                    [_format_track_ref(track) for track in ack.tracks],
                )
                await self._cb.on_queue_load_ack(ack)
        elif msg_type == QConnectMessageType.SRVR_CTRL_AUTOPLAY_TRACKS_LOADED:
            if ack := self._codec.parse_autoplay_load_ack(msg):
                LOGGER.debug(
                    "Qobuz autoplay-load ACK qv=%s tracks=%s",
                    ack.queue_version,
                    [_format_track_ref(track) for track in ack.tracks],
                )
                await self._cb.on_queue_load_ack(ack)
        elif msg_type == QConnectMessageType.SRVR_CTRL_QUEUE_ERROR_MESSAGE:
            if error := self._codec.parse_queue_error(msg):
                await self._cb.on_queue_error(error)
        elif msg_type == QConnectMessageType.SRVR_CTRL_QUEUE_VERSION_CHANGED:
            if version := self._codec.parse_queue_version_changed(msg):
                await self._cb.on_queue_version(version)
        elif msg_type == QConnectMessageType.SRVR_CTRL_QUEUE_STATE:
            if snapshot := self._codec.parse_queue_state(msg):
                LOGGER.debug(
                    "Qobuz QUEUE_STATE qv=%s tracks=%d shuffle=%s autoplay=%s",
                    snapshot.queue_version,
                    len(snapshot.tracks),
                    snapshot.shuffle_mode,
                    snapshot.autoplay_mode,
                )
                await self._cb.on_queue_state(snapshot)
        elif msg_type == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_ADDED:
            if added := self._codec.parse_queue_tracks_added(msg):
                LOGGER.debug(
                    "Qobuz QUEUE_TRACKS_ADDED qv=%s tracks=%s",
                    added.queue_version,
                    [_format_track_ref(track) for track in added.tracks],
                )
                await self._cb.on_queue_tracks_added(added)
        elif msg_type == QConnectMessageType.SRVR_RNDR_SET_LOOP_MODE:
            if (mode := self._codec.parse_set_loop_mode(msg)) is not None:
                LOGGER.debug("Qobuz SET_LOOP_MODE mode=%s", mode)
                await self._cb.on_loop_mode(mode)
        elif msg_type == QConnectMessageType.SRVR_RNDR_SET_SHUFFLE_MODE:
            if (shuffle := self._codec.parse_set_shuffle_mode(msg)) is not None:
                LOGGER.debug("Qobuz SET_SHUFFLE_MODE on=%s", shuffle)
                await self._cb.on_shuffle_mode(shuffle)
        elif msg_type == QConnectMessageType.SRVR_RNDR_SET_AUTOPLAY_MODE:
            if (autoplay := self._codec.parse_set_autoplay_mode(msg)) is not None:
                LOGGER.debug("Qobuz SET_AUTOPLAY_MODE on=%s", autoplay)
                await self._cb.on_autoplay_mode(autoplay)
        elif msg_type == QConnectMessageType.CTRL_SRVR_ASK_FOR_RENDERER_STATE:
            LOGGER.debug("Qobuz requested renderer state")
            await self._cb.on_state_request()
        elif msg_type in _KNOWN_IGNORED_MESSAGE_TYPES:
            # Broadcasts about other renderers / session-wide state
            # the renderer takes no action on. See ARCHITECTURE.md
            # Tier-3 notes.
            LOGGER.debug("Qobuz broadcast ignored: type=%s", msg_type)
        else:
            LOGGER.debug("Unhandled Qobuz Connect message type: %s", msg_type)

    async def _flush_pending_messages(self) -> None:
        if not self._pending_messages or not self._ws:
            return
        pending = self._pending_messages
        self._pending_messages = []
        for index, data in enumerate(pending):
            try:
                await self._ws.send(data)
            except websockets.ConnectionClosed:
                LOGGER.debug("Qobuz websocket closed while flushing messages")
                self._pending_messages = pending[index:] + self._pending_messages
                break
            except Exception:
                LOGGER.exception("Failed to flush Qobuz websocket message")

    async def _wait_for_valid_token(self, buffer_s: int = 0) -> bool:
        while self._should_run:
            if (
                self._ws_token
                and self._ws_token.is_valid()
                and not _token_expiring(
                    self._ws_token,
                    buffer_s,
                )
            ):
                return True
            token_version = self._token_version
            self._token_update_event.clear()
            if self._token_version != token_version:
                continue
            await self._token_update_event.wait()
        return False

    async def _close_for_token_refresh(self) -> None:
        if self._ws:
            await self._ws.close()


def _token_expiring(token: JWTConnectToken, buffer_s: int) -> bool:
    return token.exp <= int(time.time()) + buffer_s


def _uuid_to_bytes(uuid_str: str) -> bytes:
    try:
        return uuid.UUID(uuid_str).bytes
    except ValueError:
        return hashlib.md5(uuid_str.encode(), usedforsecurity=False).digest()


def _format_track_ref(track_ref: object | None) -> str:
    if track_ref is None:
        return "-"
    return f"{getattr(track_ref, 'queue_item_id', '?')}:{getattr(track_ref, 'track_id', '?')}"
