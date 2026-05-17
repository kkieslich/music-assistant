"""
Seek + position-confirmation state machine for the sync engine.

Lifts out the eight methods that together implemented Qobuz's seek
semantics — paused-seek storage, playing-seek debouncing, and the
"hold Qobuz state frozen until MA confirms" position pipeline. Each is
small individually but coordinated; keeping them together (and away
from the rest of sync.py) makes the lifecycle of a single seek
readable at a glance.

The pipeline operates on three pieces of engine state, all owned by
the engine and consumed via property access here:

- ``engine.paused_seek``        — pending paused-seek scrub
- ``engine.playing_seek``       — debounced playing-seek
- ``engine.qobuz_position``     — Qobuz's reported position that MA
  must catch up to before we let the renderer state free-run

State is updated through dataclass replacement (see :mod:`.state`), so
mutation is always atomic across the fields of one logical concern.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING

from music_assistant_models.enums import PlaybackState as MAPlaybackState

from .state import PausedSeek, PendingPlayingSeek, PendingQobuzPosition, TrackRefKey

if TYPE_CHECKING:
    from .sync import QobuzConnectSyncEngine


SEEK_TOLERANCE_MS = 750
SEEK_DEBOUNCE_MS = 350
SEEK_CONFIRM_OVERSHOOT_MS = 5_000


class SeekPipeline:
    """Coordinates paused-seek, playing-seek-debounce and position confirmation."""

    __slots__ = ("_engine",)

    def __init__(self, engine: QobuzConnectSyncEngine) -> None:
        """Hold the engine for state + bridge access."""
        self._engine = engine

    # ---- paused-seek storage -------------------------------------------

    def set_paused_seek(self, position_ms: int) -> None:
        """Remember a paused-while-scrubbing position to apply on the next PLAYING."""
        self._engine.paused_seek = PausedSeek(position_ms=max(0, position_ms))

    def take_paused_seek(self) -> int | None:
        """Consume the pending paused-seek position, if any."""
        pending = self._engine.paused_seek
        if pending is None:
            return None
        self._engine.paused_seek = None
        return pending.position_ms

    # ---- pending-position (Qobuz wants MA at X before free-run) --------

    def set_pending_qobuz_position(self, position_ms: int, *, mark_issued: bool = True) -> None:
        """
        Remember the target position MA must confirm before Qobuz can free-run.

        :param position_ms: New target position from Qobuz, in milliseconds.
        :param mark_issued: ``True`` when this call accompanies an actual MA
            operation (``bridge.seek`` / ``bridge.play_index``) — sets
            ``issued_ms`` to ``position_ms`` and resets the timestamp.
            ``False`` when only the desired target has moved (a later Qobuz
            seek arrived while the prior MA operation is still in flight,
            or the seek-debounce path is reserving the target before its
            timer fires) — updates ``target_ms`` only so the confirmation
            check keeps comparing against what we actually told MA to do.
        """
        target = max(0, position_ms)
        pending = self._engine.qobuz_position
        if pending is None:
            self._engine.qobuz_position = PendingQobuzPosition(
                target_ms=target,
                issued_ms=target if mark_issued else None,
                timestamp_ms=int(time.time() * 1000),
            )
            return
        if mark_issued:
            pending.target_ms = target
            pending.issued_ms = target
            pending.timestamp_ms = int(time.time() * 1000)
            return
        pending.target_ms = target

    def pending_position_confirmed(self, ma_position_ms: int) -> bool:
        """Return whether MA has reached the position we last asked it to seek to."""
        pending = self._engine.qobuz_position
        if pending is None:
            return True
        if pending.issued_ms is None:
            # Target reserved but no MA seek issued yet — nothing for MA to
            # have reached. Hold the frozen Qobuz position until the
            # debounce path actually fires the MA seek.
            return False
        elapsed_ms = max(0, int(time.time() * 1000) - pending.timestamp_ms)
        lower_bound = max(0, pending.issued_ms - SEEK_TOLERANCE_MS)
        upper_bound = pending.issued_ms + elapsed_ms + SEEK_CONFIRM_OVERSHOOT_MS
        return lower_bound <= ma_position_ms <= upper_bound

    def has_deferred_target(self) -> bool:
        """Return whether a newer Qobuz seek target is waiting on the in-flight MA seek."""
        pending = self._engine.qobuz_position
        return (
            pending is not None
            and pending.issued_ms is not None
            and pending.target_ms != pending.issued_ms
        )

    def has_in_flight_seek(self) -> bool:
        """Return whether we have issued an MA seek that MA hasn't confirmed yet."""
        pending = self._engine.qobuz_position
        return pending is not None and pending.issued_ms is not None

    def clear_pending_position(self) -> None:
        """Clear a pending Qobuz seek/position confirmation."""
        self._engine.qobuz_position = None
        self._engine.playing_seek = None
        self.cancel_pending_seek()

    async def reissue_deferred_seek(self, player_id: str) -> bool:
        """
        Issue an MA seek for the deferred target after the prior one confirmed.

        Returns ``True`` when a fresh MA seek was issued, ``False`` when no
        deferred target was waiting (caller should clear the pending state
        in that case).
        """
        pending = self._engine.qobuz_position
        if pending is None or pending.issued_ms is None or pending.target_ms == pending.issued_ms:
            return False
        new_target = pending.target_ms
        self.set_pending_qobuz_position(new_target, mark_issued=True)
        await self._engine.bridge.seek(player_id, new_target // 1000)
        return True

    # ---- playing-seek (debounced) --------------------------------------

    async def seek_playing_if_needed(
        self,
        player_id: str,
        position_ms: int,
        generation: int | None = None,
    ) -> None:
        """Issue an MA seek if Qobuz's target differs from MA's current position."""
        engine = self._engine
        queue = engine.bridge.get_queue(player_id)
        if not queue or not queue.current_item or queue.state != MAPlaybackState.PLAYING:
            return
        if generation is not None and not engine._is_current_command(generation):
            return
        local_ms = int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
        if abs(local_ms - position_ms) < SEEK_TOLERANCE_MS:
            return
        # If a prior MA seek is still in flight and MA's reported position
        # is clearly *below* the in-flight target (= the seek hasn't
        # landed yet), don't fire another expensive MA seek on top. On
        # AirPlay/Snapcast each bridge.seek tears down and restarts the
        # renderer; stacking those during rapid scrubbing crashed the
        # audio bridge on Pi (see the May 2026 incident). Just record
        # the new desired target — the deferred reissue path in
        # :func:`sync._sync_mirror_from_ma_queue` sends a fresh seek
        # once MA confirms the in-flight one.
        #
        # We deliberately only defer on the "MA still buffering forward
        # to the prior target" case: backward-seek transients (MA still
        # reporting the old higher position) are rare scrubbing patterns,
        # and not deferring there means at most one redundant MA seek
        # rather than a stuck-deferred state if MA never drops back.
        pending = engine.qobuz_position
        if (
            self.has_in_flight_seek()
            and pending is not None
            and pending.issued_ms is not None
            and local_ms < pending.issued_ms - SEEK_TOLERANCE_MS
        ):
            self.set_pending_qobuz_position(position_ms, mark_issued=False)
            return
        self.set_pending_qobuz_position(position_ms)
        if generation is not None and not engine._is_current_command(generation):
            return
        await engine.bridge.seek(player_id, position_ms // 1000)

    def schedule_playing_seek(
        self,
        player_id: str,
        position_ms: int,
        generation: int | None = None,
    ) -> None:
        """Coalesce playing-seek commands before asking MA to restart the stream."""
        engine = self._engine
        current_ref = TrackRefKey.from_ref(engine.qobuz_state.current_item)
        engine.reporter.set_buffering()
        # Reserve the target *without* marking it as issued: the debounce
        # timer still has to fire before we call ``bridge.seek``. Marking
        # issued here would make ``seek_playing_if_needed`` think a prior
        # seek is in flight and defer instead of issuing the first one.
        self.set_pending_qobuz_position(position_ms, mark_issued=False)
        self.cancel_pending_seek()
        # ``generation or 0`` keeps the dataclass typed; ``None`` callers
        # come from pre-generation contexts that pre-date staleness checks.
        engine.playing_seek = PendingPlayingSeek(
            position_ms=position_ms,
            ref=current_ref,
            generation=generation if generation is not None else 0,
        )
        engine.playing_seek.task = asyncio.create_task(
            self._run_debounced_playing_seek(player_id, current_ref, generation)
        )

    async def _run_debounced_playing_seek(
        self,
        player_id: str,
        expected_ref: TrackRefKey | None,
        generation: int | None,
    ) -> None:
        """Run the latest playing seek after Qobuz scrub/echo traffic settles."""
        engine = self._engine
        try:
            await asyncio.sleep(SEEK_DEBOUNCE_MS / 1000)
            pending = engine.playing_seek
            if pending is None:
                return
            if generation is not None and not engine._is_current_command(generation):
                return
            if (generation if generation is not None else 0) != pending.generation:
                return
            if expected_ref != pending.ref:
                return
            if expected_ref != TrackRefKey.from_ref(engine.qobuz_state.current_item):
                return
            await self.seek_playing_if_needed(player_id, pending.position_ms, generation)
        except asyncio.CancelledError:
            pass
        finally:
            pending = engine.playing_seek
            if (
                pending is not None
                and asyncio.current_task() is pending.task
                and (
                    expected_ref == pending.ref
                    and (generation if generation is not None else 0) == pending.generation
                )
            ):
                engine.playing_seek = None

    def cancel_pending_seek(self) -> None:
        """Cancel a queued playing seek if it has not been sent to MA yet."""
        pending = self._engine.playing_seek
        if pending is not None and pending.task is not None and not pending.task.done():
            with contextlib.suppress(Exception):
                pending.task.cancel()
        self._engine.playing_seek = None
