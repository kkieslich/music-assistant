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
- :mod:`.protocol` for encode/decode, :mod:`.models` for enums + value types,
  :mod:`.sync_types` for the ``Event`` type of the ``submit`` callback.
- The ``websockets`` library (network transport).
- **No MA imports.** The session has no awareness of the player queue or any
  MA concept; the codec turns each inbound frame into a ``sync_types`` event
  and the dispatcher hands it to the provider-supplied ``submit`` callback
  (plus the ``on_set_active`` / ``on_quality`` provider hooks).

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

import websockets
from websockets import ClientConnection

from .models import (
    AudioQualityReport,
    BufferState,
    ConnectTokens,
    DeviceConfig,
    JWTConnectToken,
    LoopMode,
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from .protocol import QobuzConnectCodec
from .sync_types import Event

LOGGER = logging.getLogger(__name__)

PING_INTERVAL = 10.0
PONG_TIMEOUT = 30.0
RECV_TIMEOUT = 1.0
TOKEN_REFRESH_BUFFER = 60
# When we have no valid token and a self-refresh attempt fails, wait this long
# before retrying so a persistent failure (e.g. Qobuz logged out) doesn't spin.
TOKEN_REFRESH_RETRY_DELAY = 30.0
INITIAL_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0
REJOIN_MIN_INTERVAL = 5.0
# The cloud rejects frames whose envelope timestamp is older than ~2s
# ("Message too old", observed live 2026-07-08) and the rejection can drop
# the connection — queued frames past this age are discarded, not flushed.
PENDING_MAX_AGE = 2.0
# Hard cap on frames queued while disconnected. Only the last ~2s survive the
# reconnect flush anyway, so anything beyond a small window is pure memory
# growth during an extended outage (heartbeats queue one frame per interval).
MAX_PENDING_MESSAGES = 50
# Message-level errors that reject a single *report* on semantic grounds
# (stale current-track anchor, reporting while not the active renderer).
# These do NOT mean the cloud deregistered us — re-joining would just churn
# the session. Matched case-insensitively against the error message.
REPORT_SEMANTIC_ERRORS = (
    "current track not found",
    "non active renderer",
)


class TokenRefreshRequired(Exception):
    """Raised when the websocket must wait for refreshed tokens."""


class QobuzServerDisconnect(Exception):
    """Raised when the Qobuz server asks this websocket to reconnect."""


@dataclass(slots=True, frozen=True)
class SessionCallbacks:
    """
    The callbacks the provider supplies to the session's inbound dispatcher.

    Every inbound cloud message the codec turns into a :class:`.sync_types`
    event is fed to ``submit``; only the two provider-level hooks that do more
    than the reducer (activation volume/quality broadcast, quality-config
    persistence) and the disconnect lifecycle stay as dedicated callbacks.
    """

    submit: Callable[[Event], Awaitable[None]]
    on_set_active: Callable[[bool], Awaitable[None]]
    on_quality: Callable[[int], Awaitable[None]]
    # Fired whenever the connection loop tears down the websocket (both on
    # error and on a clean stop iteration); lets owners drop any state that
    # is only valid while connected.
    on_disconnected: Callable[[], Awaitable[None]] | None = None


class QobuzConnectSession:
    """Own the Qobuz cloud websocket and typed protocol events."""

    def __init__(
        self,
        device: DeviceConfig,
        callbacks: SessionCallbacks,
        token_refresher: Callable[[], Awaitable[JWTConnectToken | None]] | None = None,
    ) -> None:
        """
        Initialize session.

        The session always joins as a controller (the mode that matches the
        reference web client); the retired renderer role is gone.

        :param device: The advertised Qobuz Connect device config.
        :param callbacks: Inbound protocol event callbacks.
        :param token_refresher: Optional coroutine that mints a fresh websocket
            token independently of the app handshake. Called when the current
            token is missing or about to expire so the cloud session survives
            after the controlling app is closed.
        """
        self.device = device
        self._token_refresher = token_refresher
        self._device_uuid = _uuid_to_bytes(device.uuid)
        self._codec = QobuzConnectCodec(self._device_uuid)
        self._ws: ClientConnection | None = None
        self._ws_token: JWTConnectToken | None = None
        self._token_update_event = asyncio.Event()
        self._token_version = 0
        self._should_run = False
        self._is_connected = False
        self._receive_task: asyncio.Task[None] | None = None
        self._token_refresh_close_task: asyncio.Task[None] | None = None
        self._pending_messages: list[tuple[float, bytes]] = []
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._last_rejoin_monotonic: float = 0.0
        self._cb = callbacks
        # Local import: inbound_dispatcher imports from this module
        # (SessionCallbacks), so we defer the import to break the cycle.
        from .inbound_dispatcher import InboundDispatcher  # noqa: PLC0415

        self._dispatcher = InboundDispatcher(
            self._codec,
            callbacks,
            label="controller",
            # The cloud rejects frames from a deregistered renderer with
            # message-level errors (type 1), not outer ERROR frames — both
            # must funnel into the same rate-limited rejoin.
            on_error_message=self._maybe_rejoin_after_error,
        )

    @property
    def is_connected(self) -> bool:
        """Return whether websocket is connected."""
        return self._is_connected

    def set_tokens(self, tokens: ConnectTokens) -> None:
        """Set or refresh Qobuz cloud tokens."""
        previous_token = self._ws_token
        if tokens.ws_token:
            self._ws_token = tokens.ws_token
        self._token_version += 1
        self._token_update_event.set()
        if self._should_run and self._ws and previous_token != self._ws_token:
            # Cancel a still-pending close from a previous handshake before
            # dropping our only strong reference to it (asyncio tasks are
            # weak-ref'd; an orphaned task can be GC'd mid-close).
            if self._token_refresh_close_task and not self._token_refresh_close_task.done():
                self._token_refresh_close_task.cancel()
            self._token_refresh_close_task = asyncio.create_task(self._close_for_token_refresh())

    async def start(self) -> None:
        """Start websocket connection loop."""
        has_token = self._ws_token is not None and self._ws_token.is_valid()
        if not has_token and self._token_refresher is None:
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
        if self._token_refresh_close_task:
            self._token_refresh_close_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._token_refresh_close_task
            self._token_refresh_close_task = None

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
        track_ids: list[int] | None = None,
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
                track_ids=track_ids,
            )
        )

    async def send_clear_queue(self, *, queue_version: QueueVersion) -> bool:
        """Tell the Qobuz cloud to drop every queue item."""
        return await self.send_message(self._codec.encode_clear_queue(queue_version))

    async def send_queue_add_tracks(
        self,
        *,
        action_uuid: bytes,
        tracks: list[QueueTrackRef],
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
        autoplay_reset: bool = False,
    ) -> bool:
        """Append tracks to the Qobuz cloud queue (mirrors MA-side append)."""
        return await self.send_message(
            self._codec.encode_queue_add_tracks(
                action_uuid=action_uuid,
                tracks=tracks,
                queue_version=queue_version,
                context_uuid=context_uuid,
                autoplay_reset=autoplay_reset,
            )
        )

    async def send_queue_insert_tracks(
        self,
        *,
        action_uuid: bytes,
        tracks: list[QueueTrackRef],
        insert_after: int,
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
        autoplay_reset: bool = False,
    ) -> bool:
        """Insert tracks into the Qobuz cloud queue after ``insert_after``."""
        return await self.send_message(
            self._codec.encode_queue_insert_tracks(
                action_uuid=action_uuid,
                tracks=tracks,
                insert_after=insert_after,
                queue_version=queue_version,
                context_uuid=context_uuid,
                autoplay_reset=autoplay_reset,
            )
        )

    async def send_queue_remove_tracks(
        self,
        *,
        action_uuid: bytes,
        queue_item_ids: list[int],
        queue_version: QueueVersion,
        autoplay_reset: bool = False,
    ) -> bool:
        """Remove items from the Qobuz cloud queue by their ``queue_item_id``s."""
        return await self.send_message(
            self._codec.encode_queue_remove_tracks(
                action_uuid=action_uuid,
                queue_item_ids=queue_item_ids,
                queue_version=queue_version,
                autoplay_reset=autoplay_reset,
            )
        )

    async def send_queue_reorder_tracks(
        self,
        *,
        action_uuid: bytes,
        queue_item_ids: list[int],
        insert_after: int,
        queue_version: QueueVersion,
        autoplay_reset: bool = False,
    ) -> bool:
        """Move items in the Qobuz cloud queue to after ``insert_after``."""
        return await self.send_message(
            self._codec.encode_queue_reorder_tracks(
                action_uuid=action_uuid,
                queue_item_ids=queue_item_ids,
                insert_after=insert_after,
                queue_version=queue_version,
                autoplay_reset=autoplay_reset,
            )
        )

    async def send_ask_for_queue_state(
        self,
        *,
        queue_version: QueueVersion,
        queue_uuid: bytes,
    ) -> bool:
        """Request the full queue snapshot from the Qobuz cloud."""
        return await self.send_message(
            self._codec.encode_ask_for_queue_state(
                queue_version=queue_version,
                queue_uuid=queue_uuid,
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

    async def send_set_active_renderer(self, renderer_id: int) -> bool:
        """Route cloud playback to ``renderer_id`` (controller role)."""
        return await self.send_message(self._codec.encode_set_active_renderer(renderer_id))

    async def send_ctrl_set_volume(self, renderer_id: int, volume: int) -> bool:
        """Command absolute volume on a renderer (controller role)."""
        return await self.send_message(self._codec.encode_ctrl_set_volume(renderer_id, volume))

    async def send_ctrl_mute_volume(self, renderer_id: int, *, muted: bool) -> bool:
        """Command mute state on a renderer (controller role)."""
        return await self.send_message(
            self._codec.encode_ctrl_mute_volume(renderer_id, muted=muted)
        )

    async def send_ctrl_player_state(
        self,
        *,
        playing_state: PlayingState | None = None,
        position_ms: int | None = None,
        queue_version: QueueVersion | None = None,
        queue_item_id: int | None = None,
    ) -> bool:
        """Send a partial ``CTRL_SRVR_SET_PLAYER_STATE`` (controller role)."""
        return await self.send_message(
            self._codec.encode_ctrl_set_player_state(
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

    async def send_set_loop_mode(self, mode: LoopMode) -> bool:
        """Tell the Qobuz cloud to switch loop mode to ``mode``."""
        return await self.send_message(self._codec.encode_set_loop_mode(mode))

    async def send_set_shuffle_mode(
        self,
        *,
        shuffle_on: bool,
        queue_version: QueueVersion,
        current_queue_item_id: int,
        action_uuid: bytes,
    ) -> bool:
        """Tell the Qobuz cloud to enable/disable shuffle for the current queue."""
        return await self.send_message(
            self._codec.encode_set_shuffle_mode(
                shuffle_on=shuffle_on,
                queue_version=queue_version,
                current_queue_item_id=current_queue_item_id,
                action_uuid=action_uuid,
            )
        )

    async def send_file_quality_report(self, report: AudioQualityReport) -> bool:
        """Report the actual current-file format to Qobuz."""
        return await self.send_message(self._codec.encode_file_audio_quality_changed(report))

    async def send_device_quality_report(self, report: AudioQualityReport) -> bool:
        """Report a known renderer output format to Qobuz."""
        return await self.send_message(self._codec.encode_device_audio_quality_changed(report))

    async def send_max_quality_report(self, quality: int) -> bool:
        """Report the configured maximum stream quality to Qobuz."""
        return await self.send_message(self._codec.encode_max_audio_quality_changed(quality))

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
        self._pending_messages.append((time.monotonic(), data))
        if len(self._pending_messages) > MAX_PENDING_MESSAGES:
            del self._pending_messages[:-MAX_PENDING_MESSAGES]
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
                    await ws.send(self._codec.encode_subscribe(None))
                    await ws.send(
                        self._codec.encode_ctrl_join_session(
                            self._device_uuid,
                            self.device.name,
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
                if self._cb.on_disconnected is not None:
                    try:
                        await self._cb.on_disconnected()
                    except Exception:
                        LOGGER.debug("on_disconnected callback failed", exc_info=True)
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
            await self._maybe_rejoin_after_error(decoded.error_message or "")
        elif decoded.msg_type.name == "DISCONNECT":
            raise QobuzServerDisconnect

    async def _handle_payload(self, payload: bytes | None) -> None:
        if not payload:
            return
        batch = self._codec.decode_qconnect_batch(payload)
        if not batch:
            return
        for msg in batch.messages:
            await self._dispatcher.dispatch(msg)

    async def _flush_pending_messages(self) -> None:
        if not self._pending_messages or not self._ws:
            return
        pending = self._pending_messages
        self._pending_messages = []
        cutoff = time.monotonic() - PENDING_MAX_AGE
        fresh = [(ts, data) for ts, data in pending if ts >= cutoff]
        if dropped := len(pending) - len(fresh):
            LOGGER.debug("Dropped %d stale queued Qobuz frame(s) on reconnect", dropped)
        for index, (_ts, data) in enumerate(fresh):
            try:
                await self._ws.send(data)
            except websockets.ConnectionClosed:
                LOGGER.debug("Qobuz websocket closed while flushing messages")
                self._pending_messages = fresh[index:] + self._pending_messages
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
            # Snapshot + clear BEFORE the (possibly slow) refresh attempt so
            # a set_tokens() landing mid-refresh is never lost — it bumps the
            # version and re-sets the event, both of which we check below.
            token_version = self._token_version
            self._token_update_event.clear()
            # The app only hands us a short-lived token during the local
            # handshake. Once it expires we mint a fresh one ourselves via the
            # native Qobuz login instead of waiting for the app — otherwise the
            # cloud session dies as soon as the controlling app is closed and
            # can never be regained.
            if self._token_refresher is not None:
                refreshed = await self._token_refresher()
                # A minted token that is itself already inside the expiry
                # buffer (short-lived mint, clock skew) counts as a FAILED
                # refresh: adopting-and-retrying used to hot-loop createToken
                # with zero delay. Fall through to the bounded wait instead.
                if refreshed and refreshed.is_valid() and not _token_expiring(refreshed, buffer_s):
                    self._ws_token = refreshed
                    self._token_version += 1
                    continue
            if self._token_version != token_version:
                continue
            # Bound the wait so a failed self-refresh is retried, and so a new
            # app handshake (set_tokens) is still picked up promptly.
            try:
                await asyncio.wait_for(
                    self._token_update_event.wait(),
                    timeout=TOKEN_REFRESH_RETRY_DELAY,
                )
            except TimeoutError:
                continue
        return False

    async def _close_for_token_refresh(self) -> None:
        if self._ws:
            await self._ws.close()

    async def _maybe_rejoin_after_error(self, message: str = "") -> None:
        """
        Re-register with the cloud after an inbound ERROR frame.

        The cloud can silently deregister a device while its socket stays
        open — every state report is then answered with a message-level
        type-1 error. Re-sending the controller SUBSCRIBE + JOIN restores
        registration. Rate-limited so an error storm can't loop. Semantic
        per-report rejections (see ``REPORT_SEMANTIC_ERRORS``) are excluded —
        they don't indicate lost registration.
        """
        if not self._ws or not self._is_connected:
            return
        lowered = message.lower()
        if any(marker in lowered for marker in REPORT_SEMANTIC_ERRORS):
            LOGGER.debug("Skipping rejoin for semantic report error: %s", message)
            return
        now = time.monotonic()
        if now - self._last_rejoin_monotonic < REJOIN_MIN_INTERVAL:
            return
        self._last_rejoin_monotonic = now
        LOGGER.info("Qobuz Connect ERROR received — re-joining controller session")
        await self._ws.send(self._codec.encode_subscribe(None))
        await self._ws.send(
            self._codec.encode_ctrl_join_session(
                self._device_uuid,
                self.device.name,
                self.device.max_quality,
            )
        )


def _token_expiring(token: JWTConnectToken, buffer_s: int) -> bool:
    return token.exp <= int(time.time()) + buffer_s


def _uuid_to_bytes(uuid_str: str) -> bytes:
    try:
        return uuid.UUID(uuid_str).bytes
    except ValueError:
        return hashlib.md5(uuid_str.encode(), usedforsecurity=False).digest()
