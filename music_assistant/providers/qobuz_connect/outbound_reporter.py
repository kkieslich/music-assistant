"""
Outbound renderer-state reporting for the Qobuz Connect provider.

This is the "what does MA tell the Qobuz cloud right now" half of the
sync layer. The reducer's ``ReportState`` effect and the heartbeat loop
here drive it; it reads the coordinator's live ``CanonicalState`` plus a
handful of explicit getters (current session, MA duration, active-ness),
so it needs no engine of its own.

The reporter owns two concerns:

1. **Renderer state composition.** Reads the live ``CanonicalState``,
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
from typing import TYPE_CHECKING

from .models import BufferState, PlayingState

if TYPE_CHECKING:
    from collections.abc import Callable

    from .models import QueueTrackRef
    from .session import QobuzConnectSession
    from .sync_types import CanonicalState

LOGGER = logging.getLogger(__name__)

STATE_REPORT_INTERVAL_S = 5.0


class OutboundReporter:
    """Owns all renderer→cloud state-update emission."""

    __slots__ = (
        "_active_getter",
        "_current_index_getter",
        "_duration_getter",
        "_heartbeat_task",
        "_logger",
        "_session_getter",
        "_state_getter",
    )

    def __init__(
        self,
        *,
        session_getter: Callable[[], QobuzConnectSession | None],
        state_getter: Callable[[], CanonicalState],
        duration_getter: Callable[[], int],
        active_getter: Callable[[], bool],
        logger: logging.Logger,
        current_index_getter: Callable[[], int | None] = lambda: None,
    ) -> None:
        """
        Bind the reporter to explicit getters for everything it reports.

        :param session_getter: Returns the current cloud session, or ``None`` when
            no socket is up (a reconnect replaces it, so this is read fresh each time).
        :param state_getter: Returns the coordinator's live ``CanonicalState``.
        :param duration_getter: Returns the current track duration in milliseconds.
        :param active_getter: Returns whether the heartbeat should report — active
            AND a target player exists.
        :param logger: Logger for report/heartbeat diagnostics.
        :param current_index_getter: Returns MA's current queue occurrence index.
        """
        self._session_getter = session_getter
        self._state_getter = state_getter
        self._duration_getter = duration_getter
        self._active_getter = active_getter
        self._current_index_getter = current_index_getter
        self._logger = logger
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
        session = self._session_getter()
        if session is None:
            return
        state = self._state_getter()
        current_item = self._current_item(state)
        if current_item is None:
            return
        wire_position_ms, wire_timestamp_ms = self._wire_anchor(state)
        wire_buffer_state = self._wire_buffer_state(state)
        self._logger.debug(
            "Qobuz report state=%s wire_buffer=%s pos=%sms (anchor ts=%s) item=%s:%s qv=%s.%s",
            state.playing,
            wire_buffer_state,
            wire_position_ms,
            wire_timestamp_ms,
            current_item.queue_item_id,
            current_item.track_id,
            state.cloud_version.major,
            state.cloud_version.minor,
        )
        await session.send_renderer_state(
            playing_state=state.playing,
            buffer_state=wire_buffer_state,
            position_ms=wire_position_ms,
            position_timestamp_ms=wire_timestamp_ms,
            duration_ms=self._duration_getter(),
            queue_item_id=current_item.queue_item_id,
            queue_version=state.cloud_version,
        )

    # ---- helpers (private to the reporter) -----------------------------

    def _current_item(self, state: CanonicalState) -> QueueTrackRef | None:
        """Resolve the canonical current track (by Qobuz id) to its queue-item ref."""
        if state.current_id is None:
            return None
        current_index = self._current_index_getter()
        if current_index is not None and 0 <= current_index < len(state.tracks):
            indexed = state.tracks[current_index]
            if indexed.track_id == str(state.current_id):
                return indexed
        return next((t for t in state.tracks if t.track_id == str(state.current_id)), None)

    def _wire_anchor(self, state: CanonicalState) -> tuple[int, int]:
        """
        Return ``(position_ms, timestamp_ms)`` the Qobuz client should use.

        While BUFFERING we ship a frozen snapshot (``timestamp = now``) so the
        client holds the position steady across MA's ~1s transition lag. For
        PLAYING (non-buffering) we ship the raw anchor pair so the client
        interpolates exactly once — sending an already-interpolated value with
        the original anchor would let the client interpolate on top of that,
        doubling the drift. For non-PLAYING we ship a frozen snapshot.
        """
        if state.buffer_state == BufferState.BUFFERING:
            return state.position_ms, int(time.time() * 1000)
        if state.playing == PlayingState.PLAYING:
            return state.position_ms, state.position_anchor_ms or int(time.time() * 1000)
        return state.position_ms, int(time.time() * 1000)

    def _wire_buffer_state(self, state: CanonicalState) -> BufferState:
        """
        Compute the buffer state exposed to the Qobuz client.

        Only surfaced while PLAYING: the reference web client reports
        BUFFERING in ``RNDR_SRVR_STATE_UPDATED`` while playing across a
        skip/seek transition (verified against the protocol captures), and OK
        otherwise (a paused renderer never reports BUFFERING).
        """
        if state.playing == PlayingState.PLAYING and state.buffer_state == BufferState.BUFFERING:
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
                if not self._active_getter():
                    continue
                with contextlib.suppress(Exception):
                    await self.report_state()
        except asyncio.CancelledError:
            pass
