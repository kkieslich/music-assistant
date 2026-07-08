"""
Outbound renderer-state reporting for the Qobuz Connect provider.

This is the "what does MA tell the Qobuz cloud right now" half of the
sync engine, lifted out of :mod:`.sync` so the engine itself can focus
on inbound reconciliation. After Phase C stage 6, every place that used
to call ``self.report_state`` / set the buffering reporter / drive the
heartbeat now goes through this class.

The reporter owns three concerns:

1. **Renderer state composition.** Reads ``QobuzMirror`` + the current
   MA queue, decides what wire-level position / timestamp / buffer
   state to send, and emits a single ``RNDR_SRVR_STATE_UPDATED`` frame
   via the session.
2. **Heartbeat task.** A 5-second loop that re-emits the canonical
   renderer state so the cloud doesn't time us out.
3. **Buffering reporter task.** A 1-second loop that fires while
   ``QobuzMirror.buffer_state == BUFFERING`` so the cloud sees the
   frozen anchor refresh quickly during track-load latency.

The engine retains thin ``report_state`` / ``_set_buffering`` /
``_set_buffer_ok`` methods that delegate here, preserving the public
API tests use.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import TYPE_CHECKING

from music_assistant_models.enums import PlaybackState as MAPlaybackState

from .models import BufferState, PlayingState

if TYPE_CHECKING:
    from .sync import QobuzConnectSyncEngine

LOGGER = logging.getLogger(__name__)

STATE_REPORT_INTERVAL_S = 5.0
BUFFERING_REPORT_INTERVAL_S = 1.0


class OutboundReporter:
    """Owns all renderer→cloud state-update emission."""

    __slots__ = ("_buffering_task", "_engine", "_heartbeat_task")

    def __init__(self, engine: QobuzConnectSyncEngine) -> None:
        """Bind the reporter to its host engine for state + bridge access."""
        self._engine = engine
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._buffering_task: asyncio.Task[None] | None = None

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Start the 5-second renderer-state heartbeat loop."""
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop the heartbeat + any in-flight buffering reporter."""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        self.cancel_buffering_reporter()

    # ---- buffer-state transitions --------------------------------------

    def set_buffering(self) -> None:
        """Mark transport as buffering and start the short-interval reporter."""
        self._engine.qobuz_state.buffer_state = BufferState.BUFFERING
        if self._buffering_task is None or self._buffering_task.done():
            self._buffering_task = asyncio.create_task(self._buffering_report_loop())

    def set_buffer_ok(self) -> None:
        """Mark transport as ready and cancel the buffering reporter."""
        self._engine.qobuz_state.buffer_state = BufferState.OK
        self.cancel_buffering_reporter()

    def cancel_buffering_reporter(self) -> None:
        """Cancel the buffering reporter — safe to call from inside the loop."""
        task = self._buffering_task
        if task is not None and not task.done():
            if asyncio.current_task() is not task:
                task.cancel()
        self._buffering_task = None

    # ---- canonical state report ----------------------------------------

    async def report_state(self, *, sync_from_ma: bool = True) -> None:
        """Compose and emit a single ``RNDR_SRVR_STATE_UPDATED`` frame."""
        engine = self._engine
        session = engine.bridge.session
        if session is None:
            return
        player_id = engine.bridge.target_player_id()
        queue = engine.bridge.get_queue(player_id) if player_id else None
        if sync_from_ma and queue is not None and not engine.command_handler.is_reconciling():
            await self._sync_mirror_from_ma_if_aligned(queue)
        current_item = engine.qobuz_state.current_item
        if current_item is None:
            return
        wire_position_ms, wire_timestamp_ms = self._wire_anchor()
        wire_buffer_state = self._wire_buffer_state()
        engine.bridge.logger.debug(
            "Qobuz report state=%s buffer=%s wire_buffer=%s pos=%sms (anchor ts=%s) "
            "item=%s:%s qv=%s.%s sync_from_ma=%s",
            engine.qobuz_state.playing_state,
            engine.qobuz_state.buffer_state,
            wire_buffer_state,
            wire_position_ms,
            wire_timestamp_ms,
            current_item.queue_item_id,
            current_item.track_id,
            engine.qobuz_state.queue_version.major,
            engine.qobuz_state.queue_version.minor,
            sync_from_ma,
        )
        await session.send_renderer_state(
            playing_state=engine.qobuz_state.playing_state,
            buffer_state=wire_buffer_state,
            position_ms=wire_position_ms,
            position_timestamp_ms=wire_timestamp_ms,
            duration_ms=engine.qobuz_state.duration_ms,
            queue_item_id=current_item.queue_item_id,
            queue_version=engine.qobuz_state.queue_version,
        )

    # ---- helpers (private to the reporter) -----------------------------

    async def _sync_mirror_from_ma_if_aligned(self, queue: object) -> None:
        """When MA and Qobuz agree on the current track, fold MA's position in."""
        engine = self._engine
        ma_track_id = (
            engine.bridge.qobuz_track_id_for(getattr(queue, "current_item", None))
            if getattr(queue, "current_item", None)
            else None
        )
        if (
            engine.qobuz_state.current_item
            and ma_track_id == engine.qobuz_state.current_item.track_id
        ):
            # Delegate to the engine for the actual position interpolation —
            # it owns the pending-position confirmation state machine.
            await engine._sync_mirror_from_ma_queue(queue)
        elif ma_track_id is None and getattr(queue, "state", None) in (
            MAPlaybackState.PLAYING,
            MAPlaybackState.PAUSED,
        ):
            engine.qobuz_state.playing_state = PlayingState.STOPPED

    def _wire_anchor(self) -> tuple[int, int]:
        """
        Return ``(position_ms, timestamp_ms)`` the Qobuz client should use.

        For PLAYING we ship the raw anchor pair so the client interpolates
        exactly once — sending an already-interpolated value with the
        original anchor would let the client interpolate on top of that,
        doubling the drift. For non-PLAYING + non-BUFFERING we ship a
        frozen snapshot (``timestamp = now``); for BUFFERING the same
        snapshot pattern applies (no interpolation while we wait).
        """
        state = self._engine.qobuz_state
        if state.buffer_state == BufferState.BUFFERING:
            return state.position_ms, int(time.time() * 1000)
        if state.playing_state == PlayingState.PLAYING:
            return state.position_ms, state.position_timestamp_ms or int(time.time() * 1000)
        return state.position_ms, int(time.time() * 1000)

    def _wire_buffer_state(self) -> BufferState:
        """Compute the buffer state exposed to the Qobuz client."""
        state = self._engine.qobuz_state
        if (
            state.playing_state == PlayingState.PLAYING
            and state.buffer_state == BufferState.BUFFERING
        ):
            return BufferState.BUFFERING
        return BufferState.OK

    # ---- background tasks ----------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Re-emit canonical renderer state every ``STATE_REPORT_INTERVAL_S``."""
        try:
            while True:
                await asyncio.sleep(STATE_REPORT_INTERVAL_S)
                # The cloud rejects renderer-state reports from a renderer it
                # doesn't consider active ("non active renderer", live
                # 2026-07-08) — stay silent until (re)activated.
                if not self._engine._is_active:
                    continue
                with contextlib.suppress(Exception):
                    await self.report_state()
        except asyncio.CancelledError:
            pass

    async def _buffering_report_loop(self) -> None:
        """Refresh the frozen anchor while we wait for MA audio to be ready."""
        try:
            while True:
                if self._engine.qobuz_state.buffer_state != BufferState.BUFFERING:
                    return
                await asyncio.sleep(BUFFERING_REPORT_INTERVAL_S)
                with contextlib.suppress(Exception):
                    await self.report_state()
        except asyncio.CancelledError:
            pass
