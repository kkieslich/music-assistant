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
- Reconcile MA's queue toward ``qobuz_state.tracks`` whenever the
  cloud's view of the queue changes — chunked background ADDs for
  new items, surgical deletes for removed ones
  (``schedule_reconcile_ma_to_mirror`` /
  ``_run_reconcile_ma_to_mirror``).

The handler owns the background ``asyncio.Task``s (reconcile +
metadata + preload). Everything else (mirror, seek pipeline, metadata
cache, queue loader, outbound reporter) is reached via the engine.
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

# Background-preload tuning. 25 tracks per ``play_media(option=ADD)`` call
# is small enough to update MA's UI in visible steps and to give other
# tasks (a fresh SET_STATE, a seek) a generation-check opportunity between
# chunks; large enough that 600 tracks finish in ~24 cloud round-trips.
PRELOAD_CHUNK_SIZE = 25


class CommandHandler:
    """Owns the ``SRVR_RNDR_SET_STATE`` reconciliation pipeline."""

    __slots__ = (
        "_engine",
        "_last_reconciled_qv",
        "_metadata_task",
        "_position_only_task",
        "_preload_task",
        "_reconcile_task",
    )

    def __init__(self, engine: QobuzConnectSyncEngine) -> None:
        """Bind the handler to its host engine."""
        self._engine = engine
        self._reconcile_task: asyncio.Task[None] | None = None
        self._metadata_task: asyncio.Task[None] | None = None
        self._position_only_task: asyncio.Task[None] | None = None
        self._preload_task: asyncio.Task[None] | None = None
        # Queue-version we last reconciled MA toward. The cloud bumps its
        # ``queue_version`` on every mutation, so a delta naturally
        # invalidates this and the reconciler runs again on the new qv.
        self._last_reconciled_qv: tuple[int, int] | None = None

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
        """Return whether MA is still catching up to the latest Qobuz command.

        Covers both the per-SET_STATE reconcile task and the snapshot-driven
        MA-reconcile task (formerly the preload). Both can have MA's queue
        in a transient state diverged from the mirror; outbound MA→Qobuz
        differs must not run while either is in flight.
        """
        return (self._reconcile_task is not None and not self._reconcile_task.done()) or (
            self._preload_task is not None and not self._preload_task.done()
        )

    def reset_reconcile_dedup(self) -> None:
        """Clear the reconcile dedup ref — call after deactivation."""
        self._last_reconciled_qv = None

    def cancel_tasks(self) -> None:
        """Cancel the in-flight reconcile + metadata + preload tasks (best-effort)."""
        if self._reconcile_task and not self._reconcile_task.done():
            self._reconcile_task.cancel()
        self._reconcile_task = None
        if self._metadata_task and not self._metadata_task.done():
            self._metadata_task.cancel()
        self._metadata_task = None
        if self._position_only_task and not self._position_only_task.done():
            self._position_only_task.cancel()
        self._position_only_task = None
        if self._preload_task and not self._preload_task.done():
            self._preload_task.cancel()
        self._preload_task = None

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
        """Resolve metadata for the announced track so playback start is smooth.

        Metadata-only ``SET_STATE`` frames (no playing_state, no position) used
        to also prequeue ``event.next_item`` via ``play_media(REPLACE_NEXT)``,
        but the cloud now keeps MA's queue in sync via the ``QUEUE_TRACKS_*``
        delta path, so a separate prequeue is both redundant and destructive
        (REPLACE_NEXT wipes everything past current). This path is left as a
        thin track-duration prefetch.
        """
        engine = self._engine
        try:
            if event.current_item and engine._is_current_command(generation, event.current_item):
                await engine.metadata.try_ensure_track_duration(event.current_item)
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
            # Fast path: if the new current_item is already sitting in MA's
            # already-loaded queue (preload from this same playlist), just
            # ``play_index`` to it. Avoids the stop/clear/load/play_index
            # cycle that would otherwise wipe the preloaded queue and
            # force a 600-track refetch. Mirrors plex_connect's
            # ``handle_skip_to`` at ``plex_connect/player_remote.py:1187``.
            existing_idx = self._find_item_in_ma_queue(player_id, item)
            if existing_idx is not None:
                if not engine._is_current_command(generation, item):
                    return
                await engine.bridge.play_index(
                    player_id,
                    existing_idx,
                    seek_position=start_position_ms // 1000,
                )
                return
            await self._replace_ma_queue_from_qobuz(item, generation)
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

    def _find_item_in_ma_queue(self, player_id: str, item: QueueTrackRef) -> int | None:
        """Return the MA queue index for ``item`` if it is already loaded, else None.

        Lets ``_handle_qobuz_play`` short-circuit a full stop+clear+load
        cycle when the user is just skipping inside the same playlist that
        we've already preloaded — much like ``plex_connect``'s skip-to
        path at ``plex_connect/player_remote.py:1221``.
        """
        engine = self._engine
        for idx, queue_item in enumerate(engine.bridge.queue_items(player_id)):
            track_id = engine.bridge.qobuz_track_id_for(queue_item)
            if track_id == item.track_id:
                return idx
        return None

    async def _replace_ma_queue_from_qobuz(
        self,
        current_item: QueueTrackRef,
        generation: int,
    ) -> None:
        engine = self._engine
        player_id = engine._require_target_player_id()
        tracks, play_idx = await self._resolve_qobuz_queue_for_load(current_item, generation)
        if not engine._is_current_command(generation, current_item):
            return
        if not tracks:
            engine.bridge.logger.debug(
                "Ignoring Qobuz PLAYING for unresolved cloud track %s",
                current_item.track_id,
            )
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
            play_idx,
            seek_position=final_start_ms // 1000,
        )
        # We just blew away MA's queue with a fresh stop+clear+load. Any
        # previously-preloaded snapshot for the same ``queue_version`` is
        # gone too, so reset the dedup gate — otherwise a user skipping
        # within the same playlist (same qv, new current_item) would see
        # MA stuck at 2 items because ``schedule_reconcile_ma_to_mirror``
        # thinks it already preloaded this qv. The preload itself dedupes
        # via MA's existing queue_items, so re-running is safe and cheap
        # when nothing has actually been lost.
        self._last_reconciled_qv = None
        # Audio is now starting; if the cloud has already sent a snapshot
        # (QUEUE_STATE arrived before the replace finished), kick off the
        # background preload so MA's UI grows from current+next to the
        # full queue length without disrupting playback.
        if engine.qobuz_state.tracks and engine._is_current_command(generation, current_item):
            await self.schedule_reconcile_ma_to_mirror()

    async def _resolve_qobuz_queue_for_load(
        self,
        current_item: QueueTrackRef,
        generation: int,
    ) -> tuple[list[Any], int]:
        """Resolve current + next as MA tracks for the initial replace.

        Returns ``(tracks, play_index)``. Always current + next only — the
        full snapshot is loaded by the background preload in
        :meth:`_run_queue_preload` so first-audio doesn't have to wait for
        a 600-track metadata fetch. ``current_item`` must resolve.
        """
        engine = self._engine
        current_track = await engine.metadata.get_track_or_none(current_item.track_id)
        if not engine._is_current_command(generation, current_item):
            return [], 0
        if current_track is None:
            return [], 0
        tracks: list[Any] = [current_track]
        if engine.qobuz_state.next_item:
            with contextlib.suppress(Exception):
                tracks.append(
                    await engine.metadata.get_track(engine.qobuz_state.next_item.track_id)
                )
        return tracks, 0

    async def schedule_reconcile_ma_to_mirror(self) -> None:
        """Schedule a background pass that brings MA's queue in line with the mirror.

        Triggered after any cloud-side queue change has updated the mirror
        (full ``SRVR_CTRL_QUEUE_STATE`` snapshot or any ``SRVR_CTRL_QUEUE_*``
        delta). The reconciler removes MA items not in the mirror and
        chunk-adds mirror items not in MA — audio never stops because every
        mutation goes through non-disrupting ``delete_item`` /
        ``play_media(option=ADD)`` calls.

        Idempotent per ``qobuz_state.queue_version``. Cancellable via the
        ``_preload_task`` slot.
        """
        engine = self._engine
        current_item = engine.qobuz_state.current_item
        logger = engine.bridge.logger
        if current_item is None:
            return
        if engine.qobuz_state.playing_state != PlayingState.PLAYING:
            return
        snapshot_qv = (
            engine.qobuz_state.queue_version.major,
            engine.qobuz_state.queue_version.minor,
        )
        if snapshot_qv == self._last_reconciled_qv:
            return
        player_id = engine.bridge.target_player_id()
        if not player_id:
            return
        self._last_reconciled_qv = snapshot_qv
        generation = engine._command_generation
        logger.info(
            "Reconciling MA queue toward Qobuz mirror (%d tracks) at qv=%d.%d",
            len(engine.qobuz_state.tracks),
            snapshot_qv[0],
            snapshot_qv[1],
        )
        if self._preload_task and not self._preload_task.done():
            self._preload_task.cancel()
        self._preload_task = asyncio.create_task(
            self._run_ma_reconcile(player_id, current_item, generation)
        )

    async def _run_ma_reconcile(
        self,
        player_id: str,
        current_item: QueueTrackRef,
        generation: int,
    ) -> None:
        """Drive MA's queue toward the mirror without disrupting playback.

        Two passes:

        1. **Remove** MA items whose Qobuz ``track_id`` is no longer in
           ``qobuz_state.tracks`` (i.e. the cloud removed them). The
           currently-playing MA item is preserved even if the mirror says
           it's gone — that case is handled by a separate ``SET_STATE``.
        2. **Add** mirror items not yet present in MA, chunked via
           ``play_media(option=ADD)``. A ``_is_current_command`` check
           between chunks lets a fresh ``SET_STATE`` cancel us cleanly.
        """
        engine = self._engine
        logger = engine.bridge.logger
        # All MA mutations the reconciler triggers fire ``QUEUE_ITEMS_UPDATED``
        # events. Wrap the whole task body in ``Origin.QOBUZ`` so the outbound
        # MA→Qobuz differ recognizes those events as cloud-originated and skips
        # them. Without this, the differ would observe MA being "behind" the
        # mirror mid-reconcile and emit a wrong-direction
        # ``CTRL_SRVR_QUEUE_REMOVE_TRACKS`` with a stale version, which the
        # cloud rejects.
        from .models import Origin  # noqa: PLC0415
        from .state import origin_scope  # noqa: PLC0415 — break import cycle

        try:
            async with origin_scope(engine, Origin.QOBUZ):
                if not await self._reconcile_remove_stale(player_id, generation):
                    return
                await self._reconcile_add_missing(player_id, current_item, generation)
                await self._reconcile_add_history(player_id, current_item, generation)
                await self._reconcile_reorder(player_id, current_item, generation)
        except asyncio.CancelledError:
            raise
        except PlayerUnavailableError as err:
            if engine._is_current_command(generation):
                logger.warning("MA reconcile: target player unavailable: %s", err)
        except Exception:
            if engine._is_current_command(generation):
                logger.exception("MA reconcile failed")
        finally:
            if asyncio.current_task() is self._preload_task:
                self._preload_task = None

    async def _reconcile_remove_stale(self, player_id: str, generation: int) -> bool:
        """Drop MA items whose Qobuz track_id is no longer on the mirror.

        Returns False if a newer command superseded this run.
        """
        engine = self._engine
        logger = engine.bridge.logger
        snapshot_track_ids = {ref.track_id for ref in engine.qobuz_state.tracks}
        ma_items = list(engine.bridge.queue_items(player_id))
        queue = engine.bridge.get_queue(player_id)
        current_index = getattr(queue, "current_index", None) if queue else None
        for idx, item in enumerate(ma_items):
            if not engine._is_current_command(generation):
                logger.debug("MA reconcile bailing during remove: superseded")
                return False
            ma_track_id = engine.bridge.qobuz_track_id_for(item)
            if ma_track_id is None:
                continue
            if ma_track_id in snapshot_track_ids:
                continue
            if current_index is not None and idx == current_index:
                continue
            with contextlib.suppress(Exception):
                engine.bridge.delete_item(player_id, item.queue_item_id)
        return True

    async def _reconcile_add_missing(
        self,
        player_id: str,
        current_item: QueueTrackRef,
        generation: int,
    ) -> None:
        """Chunk-add mirror items past ``current_item`` not yet in MA's queue."""
        engine = self._engine
        logger = engine.bridge.logger
        if not engine.qobuz_state.tracks:
            return
        tracks_refs = list(engine.qobuz_state.tracks)
        current_key = TrackRefKey.from_ref(current_item)
        current_idx = next(
            (i for i, ref in enumerate(tracks_refs) if TrackRefKey.from_ref(ref) == current_key),
            None,
        )
        if current_idx is None:
            logger.debug("MA reconcile skipped: current_item %s not in snapshot", current_key)
            return
        remaining = tracks_refs[current_idx + 1 :]
        if not remaining:
            return
        existing_ids: set[str] = set()
        for item in engine.bridge.queue_items(player_id):
            track_id = engine.bridge.qobuz_track_id_for(item)
            if track_id:
                existing_ids.add(track_id)
        for chunk_start in range(0, len(remaining), PRELOAD_CHUNK_SIZE):
            if not engine._is_current_command(generation):
                logger.debug("MA reconcile bailing during add: superseded")
                return
            chunk_refs = remaining[chunk_start : chunk_start + PRELOAD_CHUNK_SIZE]
            pending_refs = [ref for ref in chunk_refs if ref.track_id not in existing_ids]
            if not pending_refs:
                continue
            resolved = await asyncio.gather(
                *[engine.metadata.get_track_or_none(ref.track_id) for ref in pending_refs],
            )
            if not engine._is_current_command(generation):
                return
            to_add: list[Any] = []
            for ref, ma_track in zip(pending_refs, resolved, strict=True):
                if ma_track is None:
                    continue
                to_add.append(ma_track)
                existing_ids.add(ref.track_id)
            if not to_add:
                continue
            await engine.bridge.play_media(
                player_id,
                cast("Any", to_add),
                option=QueueOption.ADD,
            )
            # Yield so other tasks (a new SET_STATE, a seek) can preempt
            # before we burn another chunk on the Qobuz API.
            await asyncio.sleep(0)

    async def _reconcile_reorder(
        self,
        player_id: str,
        current_item: QueueTrackRef,
        generation: int,
    ) -> None:
        """Align MA's full queue order with ``qobuz_state.tracks``.

        Handles all reorder shapes including items moving across the
        currently-playing position (Qobuz user dragging a previously-played
        track to be the next track, and the reverse). The currently-playing
        MA item is identified by ``queue_item_id`` and its new position
        becomes ``current_index`` after the rebuild.

        Strategy:

        1. Walk ``qobuz_state.tracks`` in order; for each ref, pop a
           matching MA item from a track-id-keyed pool. Items in MA but
           absent from the mirror (shouldn't happen post remove_stale)
           and non-Qobuz items end up at the tail.
        2. Find the playing anchor (by ``queue_item_id``) in the rebuilt
           list — that's the new ``current_index``.
        3. ``update_items`` + ``set_current_index``. ``update_items``
           internally re-enqueues the next track when
           ``index_in_buffer == current_index`` so MA's buffer pipeline
           reselects against the new order; no ``stop``/``play_index``
           needed.

        Inherent limitation (MA streaming architecture): if MA has already
        pre-buffered the *old* next track (``index_in_buffer > current_index``
        when the reorder lands), that buffered audio plays first; the new
        order takes effect on the track *after* the buffered one.
        """
        engine = self._engine
        logger = engine.bridge.logger
        if not engine._is_current_command(generation):
            return
        if not engine.qobuz_state.tracks:
            return

        mirror_tracks = list(engine.qobuz_state.tracks)
        current_key = TrackRefKey.from_ref(current_item)
        mirror_current_idx = next(
            (i for i, ref in enumerate(mirror_tracks) if TrackRefKey.from_ref(ref) == current_key),
            None,
        )
        if mirror_current_idx is None:
            # The cloud-side queue_item_id may have changed (autoplay-driven
            # requeue, etc.). Fall back to track_id-only match.
            mirror_current_idx = next(
                (i for i, ref in enumerate(mirror_tracks) if ref.track_id == current_item.track_id),
                None,
            )
        if mirror_current_idx is None:
            return

        ma_items = list(engine.bridge.queue_items(player_id))
        if not ma_items:
            return
        queue = engine.bridge.get_queue(player_id)
        current_index = getattr(queue, "current_index", None) if queue else None
        # Find the playing anchor by Qobuz's authoritative ``current_item.track_id``
        # rather than by MA's ``current_index`` integer. MA's current_index can
        # be stale after bootstrap (``play_index(0)`` runs on a 1-item queue,
        # then history is prepended via ``load(insert_at_index=0, …)`` without
        # touching ``current_index``) — using it directly would point at the
        # wrong item.
        playing_anchor = next(
            (
                item
                for item in ma_items
                if engine.bridge.qobuz_track_id_for(item) == current_item.track_id
            ),
            None,
        )
        if playing_anchor is None:
            return
        playing_qid = getattr(playing_anchor, "queue_item_id", None)
        if playing_qid is None:
            return

        # Pool MA items by Qobuz track_id. Non-Qobuz items stay at the tail.
        pool: dict[str, list[Any]] = {}
        non_qobuz: list[Any] = []
        for item in ma_items:
            tid = engine.bridge.qobuz_track_id_for(item)
            if tid is None:
                non_qobuz.append(item)
                continue
            pool.setdefault(tid, []).append(item)

        # Build the rebuilt order by walking mirror tracks. Items the
        # mirror references but MA doesn't have are simply skipped — they
        # would have been added by ``_reconcile_add_missing`` /
        # ``_reconcile_add_history`` in earlier passes.
        rebuilt: list[Any] = []
        for ref in mirror_tracks:
            items = pool.get(ref.track_id)
            if items:
                rebuilt.append(items.pop(0))
        leftover = [it for items in pool.values() for it in items]
        rebuilt.extend(leftover)
        rebuilt.extend(non_qobuz)

        # Identify where the playing anchor ended up so we can pin
        # ``current_index`` to it post-rebuild.
        new_current_index = next(
            (
                i
                for i, item in enumerate(rebuilt)
                if getattr(item, "queue_item_id", None) == playing_qid
            ),
            None,
        )
        if new_current_index is None:
            logger.debug(
                "MA reconcile: playing anchor %s vanished from rebuilt queue; skipping reorder",
                playing_qid,
            )
            return

        new_ids = [engine.bridge.qobuz_track_id_for(it) for it in rebuilt]
        old_ids = [engine.bridge.qobuz_track_id_for(it) for it in ma_items]
        order_changed = new_ids != old_ids
        current_index_changed = new_current_index != current_index
        if not order_changed and not current_index_changed:
            return

        logger.debug(
            "MA reconcile: aligning queue order to mirror (current_index %s → %d)",
            current_index,
            new_current_index,
        )
        if order_changed:
            engine.bridge.update_items(player_id, rebuilt)
        if current_index_changed:
            engine.bridge.set_current_index(player_id, new_current_index)

    async def _reconcile_add_history(
        self,
        player_id: str,
        current_item: QueueTrackRef,
        generation: int,
    ) -> None:
        """Insert mirror tracks *before* ``current_item`` at the front of MA's queue.

        Loads the "history" portion of the Qobuz playlist (tracks the user
        has already passed, but which Qobuz still keeps in the queue) so a
        skip-backward inside the same playlist resolves instantly from MA's
        loaded queue instead of triggering a fresh load. Each insert call
        uses ``insert_at_index=0`` with ``keep_played=True`` /
        ``keep_remaining=True``, which leaves the currently-playing item's
        audio stream untouched — MA's ``current_index`` shifts up
        internally as the front grows.
        """
        engine = self._engine
        logger = engine.bridge.logger
        if not engine.qobuz_state.tracks:
            return
        tracks_refs = list(engine.qobuz_state.tracks)
        current_key = TrackRefKey.from_ref(current_item)
        current_idx = next(
            (i for i, ref in enumerate(tracks_refs) if TrackRefKey.from_ref(ref) == current_key),
            None,
        )
        if current_idx is None or current_idx == 0:
            return
        history = tracks_refs[:current_idx]
        existing_ids: set[str] = set()
        for item in engine.bridge.queue_items(player_id):
            track_id = engine.bridge.qobuz_track_id_for(item)
            if track_id:
                existing_ids.add(track_id)
        # Iterate chunks back-to-front so the final MA order matches the
        # snapshot: each chunk goes in at index 0, so the *last* chunk
        # processed (= snapshot[0..]) ends up at the absolute front.
        for chunk_end in range(len(history), 0, -PRELOAD_CHUNK_SIZE):
            if not engine._is_current_command(generation):
                logger.debug("MA reconcile bailing during history-add: superseded")
                return
            chunk_start = max(0, chunk_end - PRELOAD_CHUNK_SIZE)
            chunk_refs = history[chunk_start:chunk_end]
            pending_refs = [ref for ref in chunk_refs if ref.track_id not in existing_ids]
            if not pending_refs:
                continue
            resolved = await asyncio.gather(
                *[engine.metadata.get_track_or_none(ref.track_id) for ref in pending_refs],
            )
            if not engine._is_current_command(generation):
                return
            queue_items: list[Any] = []
            for ref, ma_track in zip(pending_refs, resolved, strict=True):
                if ma_track is None:
                    continue
                queue_items.append(QueueItem.from_media_item(player_id, ma_track))
                existing_ids.add(ref.track_id)
            if not queue_items:
                continue
            await engine.bridge.insert_items(
                player_id,
                queue_items,
                insert_at_index=0,
                keep_played=True,
                keep_remaining=True,
            )
            await asyncio.sleep(0)
