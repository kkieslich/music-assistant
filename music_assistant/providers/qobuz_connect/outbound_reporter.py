"""
Outbound renderer-state reporting for the Qobuz Connect provider.

This is the "what does MA tell the Qobuz cloud right now" half of the
sync layer. The reducer's ``ReportState`` effect and the two background
loops here drive it; it reads its state off a host that exposes a live
``QobuzMirror`` projection of the coordinator's ``CanonicalState`` (see
``_ReporterHost`` in ``__init__.py``), so it needs no engine of its own.

The reporter owns two concerns:

1. **Renderer state composition.** Reads the ``QobuzMirror`` projection,
   decides what wire-level position / timestamp / buffer state to send,
   and emits a single ``RNDR_SRVR_STATE_UPDATED`` frame via the session.
2. **Heartbeat task.** A 5-second loop that re-emits the canonical
   renderer state while active so the cloud doesn't time us out.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

from .models import BufferState, PlayingState

LOGGER = logging.getLogger(__name__)

STATE_REPORT_INTERVAL_S = 5.0


class OutboundReporter:
    """Owns all renderer→cloud state-update emission."""

    __slots__ = ("_engine", "_heartbeat_task")

    def __init__(self, engine: Any) -> None:
        """Bind the reporter to its host engine for state + bridge access."""
        self._engine = engine
        self._heartbeat_task: asyncio.Task[None] | None = None

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Start the 5-second renderer-state heartbeat loop."""
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop the heartbeat loop."""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

    # ---- canonical state report ----------------------------------------

    async def report_state(self) -> None:
        """Compose and emit a single ``RNDR_SRVR_STATE_UPDATED`` frame."""
        engine = self._engine
        session = engine.bridge.session
        if session is None:
            return
        current_item = engine.qobuz_state.current_item
        if current_item is None:
            return
        wire_position_ms, wire_timestamp_ms = self._wire_anchor()
        wire_buffer_state = self._wire_buffer_state()
        engine.bridge.logger.debug(
            "Qobuz report state=%s buffer=%s wire_buffer=%s pos=%sms (anchor ts=%s) "
            "item=%s:%s qv=%s.%s",
            engine.qobuz_state.playing_state,
            engine.qobuz_state.buffer_state,
            wire_buffer_state,
            wire_position_ms,
            wire_timestamp_ms,
            current_item.queue_item_id,
            current_item.track_id,
            engine.qobuz_state.queue_version.major,
            engine.qobuz_state.queue_version.minor,
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
