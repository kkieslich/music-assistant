"""
Qobuz ``SRVR_RNDR_SET_STATE`` command handler.

This is the heart of the renderer: when the Qobuz app sends a state
command (play / pause / stop / seek / track-change), the handler
decides what MA-side work to do and orchestrates it. Lifted out of
sync.py so the engine itself is left with lifecycle + the small set
of cross-cutting helpers other collaborators need.

Responsibilities (all consolidated here):

- Mirror update (``_update_qobuz_mirror``).
- Immediate renderer-state report path for command events.
- Latest-only async reconciliation against MA: one ``_reconcile_task``
  at a time, guarded by ``_command_generation`` so a slower piece of
  reconcile work (track-metadata fetch, queue replacement) that gets
  overtaken by a newer ``SET_STATE`` command bails out cleanly.
  Note this is *not* defending against bursts of commands on the wire
  — the Qobuz controllers coalesce rapid skip/seek presses client-side
  before sending anything. The defense is for slow-MA-work-vs-new-event
  ordering only.
- Slow-path metadata update for non-command events (``_metadata_task``).
- Per-playing-state branches: ``_handle_qobuz_play`` /
  ``_handle_qobuz_pause`` / ``_handle_qobuz_stop`` /
  ``_handle_qobuz_position_only``.
- Replace MA's queue when Qobuz hands us a track MA doesn't have
  (``_replace_ma_queue_from_qobuz``).
- Prequeue the next track when Qobuz announces it
  (``_prequeue_next_item``).

The handler owns the two background ``asyncio.Task``s (reconcile +
metadata) and the prequeue-dedup ref. Everything else (mirror, seek
pipeline, metadata cache, queue loader, outbound reporter) is reached
via the engine.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.enums import QueueOption
from music_assistant_models.errors import PlayerUnavailableError
from music_assistant_models.queue_item import QueueItem

from .models import BufferState, PlayingState, SetStateEvent
from .seek_pipeline import SEEK_TOLERANCE_MS
from .state import TrackRefKey

if TYPE_CHECKING:
    from .models import QueueTrackRef
    from .sync import QobuzConnectSyncEngine


class CommandHandler:
    """Owns the ``SRVR_RNDR_SET_STATE`` reconciliation pipeline."""

    __slots__ = (
        "_engine",
        "_last_prequeued_next_ref",
        "_metadata_task",
        "_position_only_task",
        "_reconcile_task",
    )

    def __init__(self, engine: QobuzConnectSyncEngine) -> None:
        """Bind the handler to its host engine."""
        self._engine = engine
        self._reconcile_task: asyncio.Task[None] | None = None
        self._metadata_task: asyncio.Task[None] | None = None
        self._position_only_task: asyncio.Task[None] | None = None
        self._last_prequeued_next_ref: TrackRefKey | None = None

    # ---- public surface -------------------------------------------------

    async def handle_set_state(self, event: SetStateEvent) -> None:
        """Apply a full Qobuz SET_STATE event to MA."""
        engine = self._engine
        is_command_event = event.playing_state is not None
        is_position_only = not is_command_event and event.position_ms is not None
        should_report = is_command_event or is_position_only
        from .models import Origin  # noqa: PLC0415
        from .state import origin_scope  # noqa: PLC0415 — break import cycle

        async with origin_scope(engine, Origin.QOBUZ):
            if is_command_event:
                engine._command_generation += 1
            generation = engine._command_generation
            self._update_qobuz_mirror(event)
            if event.playing_state == PlayingState.PAUSED:
                engine.seek_pipeline.set_paused_seek(
                    max(0, event.position_ms)
                    if event.position_ms is not None
                    else engine.qobuz_state.position_ms,
                )
            elif is_position_only and engine.qobuz_state.playing_state == PlayingState.PAUSED:
                engine.seek_pipeline.set_paused_seek(max(0, cast("int", event.position_ms)))
            if should_report:
                self._prepare_immediate_command_report(event)
                await engine.report_state(sync_from_ma=False)
                if is_command_event:
                    self._schedule_reconcile(event, generation)
                else:
                    self._schedule_position_only(cast("int", event.position_ms), generation)
            else:
                self._schedule_metadata_update(event, generation)

    def is_reconciling(self) -> bool:
        """Return whether MA is still catching up to the latest Qobuz command."""
        return self._reconcile_task is not None and not self._reconcile_task.done()

    def reset_prequeue_dedup(self) -> None:
        """Clear the prequeue dedup ref — call after deactivation."""
        self._last_prequeued_next_ref = None

    def cancel_tasks(self) -> None:
        """Cancel the in-flight reconcile + metadata tasks (best-effort)."""
        if self._reconcile_task and not self._reconcile_task.done():
            self._reconcile_task.cancel()
        self._reconcile_task = None
        if self._metadata_task and not self._metadata_task.done():
            self._metadata_task.cancel()
        self._metadata_task = None
        if self._position_only_task and not self._position_only_task.done():
            self._position_only_task.cancel()
        self._position_only_task = None

    # ---- mirror + immediate-report ------------------------------------

    def _update_qobuz_mirror(self, event: SetStateEvent) -> None:
        engine = self._engine
        current_item_changed = False
        if event.queue_version:
            engine.qobuz_state.queue_version = event.queue_version
        if event.current_item:
            if not engine._same_queue_ref(engine.qobuz_state.current_item, event.current_item):
                current_item_changed = True
                engine.seek_pipeline.clear_pending_position()
                engine.paused_seek = None
            engine.qobuz_state.current_item = event.current_item
            if current_item_changed and event.position_ms is None:
                engine.qobuz_state.position_ms = 0
                engine.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        if event.next_item:
            engine.qobuz_state.next_item = event.next_item
        if (
            event.playing_state == PlayingState.PAUSED
            and event.position_ms is None
            and engine.qobuz_state.playing_state == PlayingState.PLAYING
        ):
            engine.qobuz_state.position_ms = engine._current_qobuz_position_ms()
            engine.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        if event.playing_state:
            engine.qobuz_state.playing_state = event.playing_state
            if event.playing_state == PlayingState.PLAYING:
                engine.reporter.set_buffering()
            else:
                engine.qobuz_state.buffer_state = BufferState.BUFFERING
        if event.position_ms is not None:
            engine.qobuz_state.position_ms = max(0, event.position_ms)
            engine.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
            if engine.qobuz_state.playing_state == PlayingState.PLAYING:
                engine.reporter.set_buffering()

    def _prepare_immediate_command_report(self, event: SetStateEvent) -> None:
        """Make the mirror reportable before slow MA queue/track operations run."""
        engine = self._engine
        if (
            engine.qobuz_state.current_item is None
            and event.playing_state == PlayingState.PLAYING
            and engine.qobuz_state.next_item is not None
        ):
            engine.qobuz_state.current_item = engine.qobuz_state.next_item
            engine.qobuz_state.next_item = None
        if (
            event.playing_state == PlayingState.PAUSED
            and event.position_ms is None
            and engine.qobuz_state.position_timestamp_ms == 0
        ):
            engine.qobuz_state.position_timestamp_ms = int(time.time() * 1000)

    # ---- reconcile / metadata tasks -----------------------------------

    def _schedule_reconcile(self, event: SetStateEvent, generation: int) -> None:
        """Schedule latest-only MA reconciliation for a Qobuz command."""
        if self._reconcile_task and not self._reconcile_task.done():
            self._reconcile_task.cancel()
        self._reconcile_task = asyncio.create_task(self._run_reconcile(event, generation))

    async def _run_reconcile(self, event: SetStateEvent, generation: int) -> None:
        """Apply the latest Qobuz command to MA after the immediate mirror update."""
        engine = self._engine
        try:
            if event.current_item and engine._is_current_command(generation, event.current_item):
                await engine.metadata.try_ensure_track_duration(event.current_item)
            if (
                engine._is_current_command(generation)
                and not engine._pending_queue_loads
                and event.next_item
            ):
                await self._prequeue_next_item(event.next_item, generation)

            if not engine._is_current_command(generation):
                return
            if event.playing_state == PlayingState.PLAYING:
                await self._handle_qobuz_play(event, generation)
            elif event.playing_state == PlayingState.PAUSED:
                await self._handle_qobuz_pause(event, generation)
            elif event.playing_state == PlayingState.STOPPED:
                await self._handle_qobuz_stop(generation)

            if engine._is_current_command(generation):
                await self._sync_latest_matching_ma_queue()
                await engine.report_state(sync_from_ma=False)
        except asyncio.CancelledError:
            raise
        except PlayerUnavailableError as err:
            if engine._is_current_command(generation):
                engine.bridge.logger.warning("Qobuz Connect target player unavailable: %s", err)
        except Exception:
            if engine._is_current_command(generation):
                engine.bridge.logger.exception("Failed to reconcile Qobuz Connect command")
        finally:
            if asyncio.current_task() is self._reconcile_task:
                self._reconcile_task = None

    def _schedule_position_only(self, position_ms: int, generation: int) -> None:
        """
        Schedule a lateral position-only seek without cancelling track work.

        A position-only Qobuz event ("the user moved the scrub bar") never
        changes which track is playing or whether it's playing — it only
        moves the play head. Folding it through ``_schedule_reconcile``
        would cancel any in-flight track-replace mid-way (between the
        ``stop_queue`` and the matching ``play_index``), leaving the
        renderer stopped. The mirror update + immediate renderer report
        have already happened in ``handle_set_state``; this task only
        carries out the MA-side seek work.
        """
        if self._position_only_task and not self._position_only_task.done():
            self._position_only_task.cancel()
        self._position_only_task = asyncio.create_task(
            self._run_position_only_seek(position_ms, generation)
        )

    async def _run_position_only_seek(self, position_ms: int, generation: int) -> None:
        """Execute a position-only seek; bails if a newer command supersedes."""
        engine = self._engine
        try:
            if not engine._is_current_command(generation):
                return
            await self._handle_qobuz_position_only(position_ms, generation)
            if engine._is_current_command(generation):
                await engine.report_state(sync_from_ma=False)
        except asyncio.CancelledError:
            raise
        except PlayerUnavailableError as err:
            if engine._is_current_command(generation):
                engine.bridge.logger.warning("Qobuz Connect target player unavailable: %s", err)
        except Exception:
            if engine._is_current_command(generation):
                engine.bridge.logger.exception("Failed to apply Qobuz Connect position-only seek")
        finally:
            if asyncio.current_task() is self._position_only_task:
                self._position_only_task = None

    def _schedule_metadata_update(self, event: SetStateEvent, generation: int) -> None:
        """Schedule slow metadata/prequeue work for non-command Qobuz updates."""
        if self._metadata_task and not self._metadata_task.done():
            self._metadata_task.cancel()
        self._metadata_task = asyncio.create_task(self._run_metadata_update(event, generation))

    async def _run_metadata_update(self, event: SetStateEvent, generation: int) -> None:
        """Resolve metadata and next queue items without blocking websocket receive."""
        engine = self._engine
        try:
            if event.current_item and engine._is_current_command(generation, event.current_item):
                await engine.metadata.try_ensure_track_duration(event.current_item)
            if (
                engine._is_current_command(generation)
                and not engine._pending_queue_loads
                and event.next_item
            ):
                await self._prequeue_next_item(event.next_item, generation)
        except asyncio.CancelledError:
            raise
        except Exception:
            if engine._is_current_command(generation):
                engine.bridge.logger.exception("Failed to process Qobuz Connect metadata update")
        finally:
            if asyncio.current_task() is self._metadata_task:
                self._metadata_task = None

    # ---- after-reconcile MA-sync --------------------------------------

    async def _sync_latest_matching_ma_queue(self) -> None:
        """Accept MA state after the latest reconcile has applied its command."""
        engine = self._engine
        player_id = engine.bridge.target_player_id()
        queue = engine.bridge.get_queue(player_id) if player_id else None
        current_item = engine.qobuz_state.current_item
        if not queue or not current_item or not getattr(queue, "current_item", None):
            return
        ma_track_id = engine.bridge.qobuz_track_id_for(queue.current_item)
        if ma_track_id == current_item.track_id:
            await engine._sync_mirror_from_ma_queue(queue)

    # ---- per-playing-state branches -----------------------------------

    async def _handle_qobuz_play(self, event: SetStateEvent, generation: int) -> None:
        engine = self._engine
        item = event.current_item or engine.qobuz_state.current_item
        if item is None and (event.next_item or engine.qobuz_state.next_item):
            item = event.next_item or engine.qobuz_state.next_item
            if item is None:
                return
            engine.qobuz_state.current_item = item
            engine.paused_seek = None
            if engine.qobuz_state.next_item == item:
                engine.qobuz_state.next_item = None
            engine.bridge.logger.debug(
                "Promoting Qobuz next item to current on PLAYING: %s:%s",
                item.queue_item_id,
                item.track_id,
            )
            await engine.metadata.try_ensure_track_duration(item)
            if not engine._is_current_command(generation, item):
                return
        if item is None:
            engine.bridge.logger.debug("Ignoring Qobuz PLAYING without current or next queue item")
            return
        if not engine._is_current_command(generation, item):
            return
        pending_paused_seek_ms = engine.seek_pipeline.take_paused_seek()
        start_position_ms = (
            pending_paused_seek_ms
            if pending_paused_seek_ms is not None
            else event.position_ms
            if event.position_ms is not None
            else engine.qobuz_state.position_ms
        )
        start_position_ms = max(0, start_position_ms or 0)
        engine.qobuz_state.position_ms = start_position_ms
        engine.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        engine.seek_pipeline.set_pending_qobuz_position(start_position_ms)

        player_id = engine._require_target_player_id()
        queue = engine.bridge.get_queue(player_id)
        current_ma_track_id = (
            engine.bridge.qobuz_track_id_for(queue.current_item)
            if queue and queue.current_item
            else None
        )
        if current_ma_track_id != item.track_id:
            await self._replace_ma_queue_from_qobuz(item, start_position_ms, generation)
            return

        if queue and queue.state == MAPlaybackState.PAUSED:
            if start_position_ms > 0 and queue.current_index is not None:
                if not engine._is_current_command(generation, item):
                    return
                await engine.bridge.play_index(
                    player_id,
                    queue.current_index,
                    seek_position=start_position_ms // 1000,
                )
            else:
                if not engine._is_current_command(generation, item):
                    return
                await engine.bridge.play(player_id)
        elif queue and queue.state == MAPlaybackState.PLAYING:
            await engine.seek_pipeline.seek_playing_if_needed(
                player_id, start_position_ms, generation
            )
        elif queue and queue.current_item:
            if start_position_ms > 0 and queue.current_index is not None:
                if not engine._is_current_command(generation, item):
                    return
                await engine.bridge.play_index(
                    player_id,
                    queue.current_index,
                    seek_position=start_position_ms // 1000,
                )
            else:
                if not engine._is_current_command(generation, item):
                    return
                await engine.bridge.play(player_id)

    async def _handle_qobuz_pause(self, event: SetStateEvent, generation: int) -> None:
        engine = self._engine
        engine.seek_pipeline.set_paused_seek(
            max(0, event.position_ms)
            if event.position_ms is not None
            else engine.qobuz_state.position_ms,
        )
        player_id = engine.bridge.target_player_id()
        queue = engine.bridge.get_queue(player_id) if player_id else None
        if queue and queue.state == MAPlaybackState.PLAYING:
            if not player_id:
                return
            if not engine._is_current_command(generation):
                return
            await engine.bridge.pause(player_id)

    async def _handle_qobuz_stop(self, generation: int) -> None:
        engine = self._engine
        player_id = engine.bridge.target_player_id()
        if player_id and engine._is_current_command(generation):
            with contextlib.suppress(Exception):
                await engine.bridge.stop_queue(player_id)

    async def _handle_qobuz_position_only(self, position_ms: int, generation: int) -> None:
        engine = self._engine
        if engine.qobuz_state.playing_state == PlayingState.PAUSED:
            engine.seek_pipeline.set_paused_seek(max(0, position_ms))
            return
        if engine.qobuz_state.playing_state == PlayingState.PLAYING:
            player_id = engine.bridge.target_player_id()
            if not player_id:
                return
            queue = engine.bridge.get_queue(player_id)
            local_ms = (
                int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
                if queue and queue.state == MAPlaybackState.PLAYING
                else None
            )
            if (
                engine.qobuz_position is None
                and local_ms is not None
                and abs(local_ms - position_ms) < SEEK_TOLERANCE_MS
            ):
                engine.reporter.set_buffer_ok()
                return
            engine.seek_pipeline.schedule_playing_seek(player_id, max(0, position_ms), generation)

    # ---- MA queue replacement / prequeue ------------------------------

    async def _replace_ma_queue_from_qobuz(
        self,
        current_item: QueueTrackRef,
        start_position_ms: int,
        generation: int,
    ) -> None:
        engine = self._engine
        player_id = engine._require_target_player_id()
        current_track = await engine.metadata.get_track_or_none(current_item.track_id)
        if not engine._is_current_command(generation, current_item):
            return
        if current_track is None:
            engine.bridge.logger.debug(
                "Ignoring Qobuz PLAYING for unresolved cloud track %s",
                current_item.track_id,
            )
            return
        tracks = [current_track]
        if engine.qobuz_state.next_item:
            with contextlib.suppress(Exception):
                tracks.append(
                    await engine.metadata.get_track(engine.qobuz_state.next_item.track_id)
                )
        if not engine._is_current_command(generation, current_item):
            return
        queue = engine.bridge.get_queue(player_id)
        if queue and queue.state != MAPlaybackState.IDLE:
            with contextlib.suppress(Exception):
                await engine.bridge.stop_queue(player_id)
        if not engine._is_current_command(generation, current_item):
            return
        queue_items = [QueueItem.from_media_item(player_id, track) for track in tracks]
        engine.bridge.clear_queue(player_id, skip_stop=True)
        if not engine._is_current_command(generation, current_item):
            return
        await engine.bridge.load_queue(
            player_id,
            queue_items=queue_items,
            keep_remaining=False,
            keep_played=False,
        )
        if not engine._is_current_command(generation, current_item):
            return
        # Re-read the mirror just before play_index: a position-only event
        # may have arrived during the (slow) stop+load and now lives on the
        # mirror. Folding it in here lets the new track start at the seeked
        # position directly, instead of starting at the original target and
        # then seeking again.
        final_start_ms = max(0, engine.qobuz_state.position_ms)
        engine.seek_pipeline.set_pending_qobuz_position(final_start_ms)
        await engine.bridge.play_index(
            player_id,
            0,
            seek_position=final_start_ms // 1000,
        )

    async def _prequeue_next_item(
        self,
        item: QueueTrackRef | None,
        generation: int | None = None,
    ) -> None:
        engine = self._engine
        if item is None:
            self._last_prequeued_next_ref = None
            return
        if generation is not None and not engine._is_current_command(generation):
            return
        current_item = engine.qobuz_state.current_item
        if current_item is None:
            return
        next_ref = TrackRefKey.from_ref(item)
        if next_ref == self._last_prequeued_next_ref:
            return
        player_id = engine.bridge.target_player_id()
        if not player_id:
            return
        queue = engine.bridge.get_queue(player_id) if player_id else None
        if not queue or not queue.current_item:
            return
        current_ma_track_id = engine.bridge.qobuz_track_id_for(queue.current_item)
        if current_ma_track_id != current_item.track_id:
            return
        ma_track = await engine.metadata.get_track_or_none(item.track_id)
        if ma_track is None:
            return
        if generation is not None and not engine._is_current_command(generation):
            return
        await engine.bridge.play_media(
            player_id,
            cast("Any", ma_track),
            option=QueueOption.REPLACE_NEXT,
        )
        self._last_prequeued_next_ref = next_ref
