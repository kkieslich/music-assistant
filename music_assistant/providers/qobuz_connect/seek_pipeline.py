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
    from .models import QueueTrackRef
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

    def set_paused_seek(self, position_ms: int, item: QueueTrackRef | None) -> None:
        """Remember a paused seek only for the Qobuz queue item it belongs to."""
        ref = TrackRefKey.from_ref(item)
        if ref is None:
            # No queue-item ref ⇒ can't match later. Drop rather than
            # store an ambiguous one.
            self._engine.paused_seek = None
            return
        self._engine.paused_seek = PausedSeek(position_ms=max(0, position_ms), ref=ref)

    def take_paused_seek(self, item: QueueTrackRef | None) -> int | None:
        """Consume a paused seek if it belongs to the item being played."""
        pending = self._engine.paused_seek
        if pending is None:
            return None
        ref = TrackRefKey.from_ref(item)
        if pending.ref != ref:
            self._engine.paused_seek = None
            return None
        self._engine.paused_seek = None
        return pending.position_ms

    # ---- pending-position (Qobuz wants MA at X before free-run) --------

    def set_pending_qobuz_position(
        self,
        *,
        position_ms: int,
        item: QueueTrackRef | None,
        source_ms: int | None,
    ) -> None:
        """Remember the target position MA must confirm before Qobuz can free-run."""
        self._engine.qobuz_position = PendingQobuzPosition(
            target_ms=max(0, position_ms),
            ref=TrackRefKey.from_ref(item),
            source_ms=source_ms if source_ms is not None else max(0, position_ms),
            timestamp_ms=int(time.time() * 1000),
        )

    def pending_position_confirmed(self, ma_position_ms: int) -> bool:
        """Return whether MA's position plausibly confirms the pending Qobuz target."""
        pending = self._engine.qobuz_position
        if pending is None:
            return True
        elapsed_ms = max(0, int(time.time() * 1000) - pending.timestamp_ms)
        lower_bound = max(0, pending.target_ms - SEEK_TOLERANCE_MS)
        upper_bound = pending.target_ms + elapsed_ms + SEEK_CONFIRM_OVERSHOOT_MS
        return lower_bound <= ma_position_ms <= upper_bound

    def clear_pending_position(self) -> None:
        """Clear a pending Qobuz seek/position confirmation."""
        self._engine.qobuz_position = None
        self._engine.playing_seek = None
        self.cancel_pending_seek()

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
        if abs(local_ms - position_ms) >= SEEK_TOLERANCE_MS:
            self.set_pending_qobuz_position(
                position_ms=position_ms,
                item=engine.qobuz_state.current_item,
                source_ms=local_ms,
            )
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
        self.set_pending_qobuz_position(
            position_ms=position_ms,
            item=engine.qobuz_state.current_item,
            source_ms=None,
        )
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
