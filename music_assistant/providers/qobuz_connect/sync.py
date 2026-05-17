"""Bidirectional Qobuz Connect <-> Music Assistant sync engine."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.enums import QueueOption
from music_assistant_models.errors import PlayerUnavailableError
from music_assistant_models.queue_item import QueueItem

from .models import (
    BufferState,
    Origin,
    PlayingState,
    QobuzMirror,
    QueueError,
    QueueLoadAck,
    QueueTrackRef,
    QueueVersion,
    SetStateEvent,
)

if TYPE_CHECKING:
    from music_assistant_models.event import MassEvent
    from music_assistant_models.media_items import Track

SEEK_TOLERANCE_MS = 750
SEEK_DEBOUNCE_MS = 350
SEEK_CONFIRM_OVERSHOOT_MS = 5_000
BUFFERING_REPORT_INTERVAL = 1.0
MA_QUEUE_LOAD_ACK_TIMEOUT = 3.0
STATE_REPORT_INTERVAL = 5.0


class QobuzConnectSyncEngine:
    """Single owner of MA/Qobuz playback and queue synchronization."""

    def __init__(self, provider: Any) -> None:
        """Initialize sync engine."""
        self.provider = provider
        self.mass = provider.mass
        self.qobuz_state = QobuzMirror()
        self.pending_paused_seek_ms: int | None = None
        self._pending_paused_seek_ref: str | None = None
        self.origin: Origin | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._last_prequeued_next_ref: str | None = None
        self._prequeued_qobuz_items: dict[str, list[int]] = {}
        self._synthetic_queue_item_id = 1_000_000
        self._pending_queue_loads: dict[
            bytes, asyncio.Future[QueueLoadAck | QueueError | None]
        ] = {}
        self._last_ma_origin_track_id: str | None = None
        self._pending_qobuz_position_ms: int | None = None
        self._pending_qobuz_position_ref: str | None = None
        self._pending_qobuz_position_source_ms: int | None = None
        self._pending_qobuz_position_timestamp_ms: int | None = None
        self._pending_seek_position_ms: int | None = None
        self._pending_seek_ref: str | None = None
        self._pending_seek_task: asyncio.Task[None] | None = None
        self._buffering_report_task: asyncio.Task[None] | None = None
        self._unresolvable_qobuz_track_ids: set[str] = set()

    async def start(self) -> None:
        """Start state heartbeat."""
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop state heartbeat."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        self._cancel_pending_seek()
        self._cancel_buffering_reporter()

    def reset_for_deactivation(self) -> None:
        """
        Clear sync state after the cloud told us we are no longer the active renderer.

        Keeps the heartbeat running so we can resume cleanly when the cloud
        reactivates us, but drops anything that was specific to the previous
        playback session.
        """
        self.qobuz_state = QobuzMirror()
        self.pending_paused_seek_ms = None
        self._pending_paused_seek_ref = None
        self._pending_qobuz_position_ms = None
        self._pending_qobuz_position_ref = None
        self._pending_qobuz_position_source_ms = None
        self._pending_qobuz_position_timestamp_ms = None
        self._pending_seek_position_ms = None
        self._pending_seek_ref = None
        self._cancel_pending_seek()
        self._cancel_buffering_reporter()
        self._prequeued_qobuz_items.clear()
        self._last_prequeued_next_ref = None
        self._last_ma_origin_track_id = None
        self._unresolvable_qobuz_track_ids.clear()

    async def handle_qobuz_set_state(self, event: SetStateEvent) -> None:
        """Apply a full Qobuz SET_STATE event to MA."""
        should_report = self._event_requires_renderer_report(event)
        self.origin = Origin.QOBUZ
        try:
            self._update_qobuz_mirror(event)
            if should_report:
                self._prepare_immediate_command_report(event)
                await self.report_state(sync_from_ma=False)
            if event.current_item:
                await self._try_ensure_qobuz_track_metadata(event.current_item)
            if not self._pending_queue_loads:
                await self._prequeue_next_item(event.next_item)

            if event.playing_state == PlayingState.PLAYING:
                await self._handle_qobuz_play(event)
            elif event.playing_state == PlayingState.PAUSED:
                await self._handle_qobuz_pause(event)
            elif event.playing_state == PlayingState.STOPPED:
                await self._handle_qobuz_stop()
            elif event.position_ms is not None:
                await self._handle_qobuz_position_only(event.position_ms)

            if should_report:
                await self.report_state(sync_from_ma=False)
        finally:
            self.origin = None

    async def handle_ma_queue_event(self, event: MassEvent) -> None:
        """React to MA queue updates that were not caused by Qobuz commands."""
        if self.origin == Origin.QOBUZ:
            return
        player_id = self.provider.get_target_player_id()
        if not player_id or event.object_id != player_id:
            return
        queue = event.data
        if not queue or not getattr(queue, "current_item", None):
            return
        if queue.state not in (MAPlaybackState.PLAYING, MAPlaybackState.PAUSED):
            return
        track_id = self.provider.get_qobuz_track_id_from_queue_item(queue.current_item)
        if not track_id:
            return

        if self.qobuz_state.next_item and track_id == self.qobuz_state.next_item.track_id:
            await self._sync_mirror_from_ma_queue(queue)
            await self.report_state(sync_from_ma=False)
            return

        if self.qobuz_state.current_item and self.qobuz_state.current_item.track_id == track_id:
            await self._sync_mirror_from_ma_queue(queue)
            await self.report_state()
            return

        if self._last_ma_origin_track_id == track_id:
            return
        self._last_ma_origin_track_id = track_id
        await self._send_ma_origin_queue_load(track_id, queue)

    async def handle_queue_load_ack(self, ack: QueueLoadAck) -> None:
        """Handle Qobuz queue-load acknowledgement."""
        self.origin = Origin.ACK
        try:
            self.qobuz_state.queue_version = ack.queue_version
            if ack.tracks:
                idx = min(ack.queue_position, len(ack.tracks) - 1)
                current_item = ack.tracks[idx]
                if await self._try_ensure_qobuz_track_metadata(current_item):
                    self.qobuz_state.current_item = current_item
            if future := self._pending_queue_loads.pop(ack.action_uuid, None):
                if not future.done():
                    future.set_result(ack)
            await self.report_state()
        finally:
            self.origin = None

    async def handle_queue_error(self, error: QueueError) -> None:
        """Handle Qobuz queue command error."""
        if error.queue_version:
            self.qobuz_state.queue_version = error.queue_version
        if future := self._pending_queue_loads.pop(error.action_uuid, None):
            if not future.done():
                future.set_result(error)
        self.provider.logger.warning(
            "Qobuz Connect queue command failed: %s %s (server queue version: %s.%s)",
            error.code,
            error.message,
            error.queue_version.major if error.queue_version else "?",
            error.queue_version.minor if error.queue_version else "?",
        )

    async def handle_queue_version(self, version: QueueVersion) -> None:
        """Remember Qobuz queue version changes."""
        self.qobuz_state.queue_version = version

    async def report_state(self, *, sync_from_ma: bool = True) -> None:
        """Report canonical renderer state to the Qobuz app."""
        session = self.provider.qobuz_session
        if not session:
            return
        player_id = self.provider.get_target_player_id()
        queue = self.mass.player_queues.get(player_id) if player_id else None
        if sync_from_ma and queue:
            ma_track_id = (
                self.provider.get_qobuz_track_id_from_queue_item(queue.current_item)
                if getattr(queue, "current_item", None)
                else None
            )
            if (
                self.qobuz_state.current_item
                and ma_track_id == self.qobuz_state.current_item.track_id
            ):
                await self._sync_mirror_from_ma_queue(queue)
            elif ma_track_id is None and queue.state in (
                MAPlaybackState.PLAYING,
                MAPlaybackState.PAUSED,
            ):
                self.qobuz_state.playing_state = PlayingState.STOPPED
        current_item = self.qobuz_state.current_item
        if current_item is None:
            return
        # The Qobuz client interpolates position itself from
        # (position_timestamp_ms, position_ms). For PLAYING we send the raw
        # anchor pair so the client interpolates exactly once — sending an
        # already-interpolated value with the original anchor would let the
        # client interpolate on top, doubling the drift. For non-PLAYING we
        # send a frozen snapshot (timestamp = now) since there is nothing to
        # interpolate forward.
        if self.qobuz_state.buffer_state == BufferState.BUFFERING:
            wire_position_ms = self.qobuz_state.position_ms
            wire_timestamp_ms = int(time.time() * 1000)
        elif self.qobuz_state.playing_state == PlayingState.PLAYING:
            wire_position_ms = self.qobuz_state.position_ms
            wire_timestamp_ms = self.qobuz_state.position_timestamp_ms or int(time.time() * 1000)
        else:
            wire_position_ms = self.qobuz_state.position_ms
            wire_timestamp_ms = int(time.time() * 1000)
        wire_buffer_state = self._wire_buffer_state()
        self.provider.logger.debug(
            "Qobuz report state=%s buffer=%s wire_buffer=%s pos=%sms (anchor ts=%s) item=%s:%s qv=%s.%s sync_from_ma=%s",
            self.qobuz_state.playing_state,
            self.qobuz_state.buffer_state,
            wire_buffer_state,
            wire_position_ms,
            wire_timestamp_ms,
            current_item.queue_item_id,
            current_item.track_id,
            self.qobuz_state.queue_version.major,
            self.qobuz_state.queue_version.minor,
            sync_from_ma,
        )
        await session.send_renderer_state(
            playing_state=self.qobuz_state.playing_state,
            buffer_state=wire_buffer_state,
            position_ms=wire_position_ms,
            position_timestamp_ms=wire_timestamp_ms,
            duration_ms=self.qobuz_state.duration_ms,
            queue_item_id=current_item.queue_item_id,
            queue_version=self.qobuz_state.queue_version,
        )

    async def set_volume(self, level: int) -> int:
        """Set MA player volume from Qobuz app."""
        player_id = self._require_target_player_id()
        volume = max(0, min(100, int(level)))
        await self.mass.players.cmd_volume_set(player_id, volume)
        session = self.provider.qobuz_session
        if session:
            await session.send_volume_changed(volume)
        return volume

    async def set_volume_delta(self, delta: int) -> int:
        """Adjust MA volume from Qobuz app."""
        player_id = self.provider.get_target_player_id()
        current = 0
        if player_id and (player := self.mass.players.get_player(player_id)):
            current = player.state.volume_level or 0
        return await self.set_volume(current + int(delta))

    def _event_requires_renderer_report(self, event: SetStateEvent) -> bool:
        """Return true when an inbound event represents a renderer command."""
        return event.playing_state is not None or event.position_ms is not None

    def _update_qobuz_mirror(self, event: SetStateEvent) -> None:
        current_item_changed = False
        if event.queue_version:
            self.qobuz_state.queue_version = event.queue_version
        if event.current_item:
            if not self._same_queue_ref(self.qobuz_state.current_item, event.current_item):
                current_item_changed = True
                self._clear_pending_position()
                if self._pending_paused_seek_ref != self._queue_ref_key(event.current_item):
                    self.pending_paused_seek_ms = None
                    self._pending_paused_seek_ref = None
            self.qobuz_state.current_item = event.current_item
            if current_item_changed and event.position_ms is None:
                self.qobuz_state.position_ms = 0
                self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        if event.next_item:
            self.qobuz_state.next_item = event.next_item
        if (
            event.playing_state == PlayingState.PAUSED
            and event.position_ms is None
            and self.qobuz_state.playing_state == PlayingState.PLAYING
        ):
            self.qobuz_state.position_ms = self._current_qobuz_position_ms()
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        if event.playing_state:
            self.qobuz_state.playing_state = event.playing_state
            if event.playing_state == PlayingState.PLAYING:
                self._set_buffering()
            else:
                self.qobuz_state.buffer_state = BufferState.BUFFERING
        if event.position_ms is not None:
            self.qobuz_state.position_ms = max(0, event.position_ms)
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
            if self.qobuz_state.playing_state == PlayingState.PLAYING:
                self._set_buffering()

    def _prepare_immediate_command_report(self, event: SetStateEvent) -> None:
        """Make the mirror reportable before slow MA queue/track operations run."""
        if (
            self.qobuz_state.current_item is None
            and event.playing_state == PlayingState.PLAYING
            and self.qobuz_state.next_item is not None
        ):
            self.qobuz_state.current_item = self.qobuz_state.next_item
            self.qobuz_state.next_item = None
        if (
            event.playing_state == PlayingState.PAUSED
            and event.position_ms is None
            and self.qobuz_state.position_timestamp_ms == 0
        ):
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)

    async def _handle_qobuz_play(self, event: SetStateEvent) -> None:
        item = event.current_item or self.qobuz_state.current_item
        if item is None and (event.next_item or self.qobuz_state.next_item):
            item = event.next_item or self.qobuz_state.next_item
            if item is None:
                return
            self.qobuz_state.current_item = item
            self.pending_paused_seek_ms = None
            self._pending_paused_seek_ref = None
            if self.qobuz_state.next_item == item:
                self.qobuz_state.next_item = None
            self.provider.logger.debug(
                "Promoting Qobuz next item to current on PLAYING: %s:%s",
                item.queue_item_id,
                item.track_id,
            )
            await self._try_ensure_qobuz_track_metadata(item)
        if item is None:
            self.provider.logger.debug("Ignoring Qobuz PLAYING without current or next queue item")
            return
        pending_paused_seek_ms = self._take_pending_paused_seek(item)
        start_position_ms = (
            pending_paused_seek_ms
            if pending_paused_seek_ms is not None
            else event.position_ms
            if event.position_ms is not None
            else self.qobuz_state.position_ms
        )
        if start_position_ms is not None:
            self.qobuz_state.position_ms = max(0, start_position_ms)
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
            self._set_pending_qobuz_position(
                position_ms=start_position_ms,
                item=item,
                source_ms=0,
            )

        player_id = self._require_target_player_id()
        queue = self.mass.player_queues.get(player_id)
        current_ma_track_id = (
            self.provider.get_qobuz_track_id_from_queue_item(queue.current_item)
            if queue and queue.current_item
            else None
        )
        if current_ma_track_id != item.track_id:
            await self._replace_ma_queue_from_qobuz(item, start_position_ms)
            return

        if queue and queue.state == MAPlaybackState.PAUSED:
            if start_position_ms > 0 and queue.current_index is not None:
                await self.mass.player_queues.play_index(
                    player_id,
                    queue.current_index,
                    seek_position=start_position_ms // 1000,
                )
            else:
                await self.mass.player_queues.play(player_id)
        elif queue and queue.state == MAPlaybackState.PLAYING:
            await self._seek_playing_if_needed(player_id, start_position_ms)
        elif queue and queue.current_item:
            if start_position_ms > 0 and queue.current_index is not None:
                await self.mass.player_queues.play_index(
                    player_id,
                    queue.current_index,
                    seek_position=start_position_ms // 1000,
                )
            else:
                await self.mass.player_queues.play(player_id)

    async def _handle_qobuz_pause(self, event: SetStateEvent) -> None:
        self._set_pending_paused_seek(
            (
                max(0, event.position_ms)
                if event.position_ms is not None
                else self.qobuz_state.position_ms
            ),
            self.qobuz_state.current_item,
        )
        player_id = self.provider.get_target_player_id()
        queue = self.mass.player_queues.get(player_id) if player_id else None
        if queue and queue.state == MAPlaybackState.PLAYING:
            if not player_id:
                return
            await self.mass.player_queues.pause(player_id)

    async def _handle_qobuz_stop(self) -> None:
        player_id = self.provider.get_target_player_id()
        if player_id:
            with contextlib.suppress(Exception):
                await self.mass.player_queues.stop(player_id)

    async def _handle_qobuz_position_only(self, position_ms: int) -> None:
        if self.qobuz_state.playing_state == PlayingState.PAUSED:
            self._set_pending_paused_seek(max(0, position_ms), self.qobuz_state.current_item)
            return
        if self.qobuz_state.playing_state == PlayingState.PLAYING:
            player_id = self.provider.get_target_player_id()
            if player_id:
                queue = self.mass.player_queues.get(player_id)
                local_ms = (
                    int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
                    if queue and queue.state == MAPlaybackState.PLAYING
                    else None
                )
                if (
                    self._pending_qobuz_position_ms is None
                    and local_ms is not None
                    and abs(local_ms - position_ms) < SEEK_TOLERANCE_MS
                ):
                    self._set_buffer_ok()
                    return
                self._schedule_playing_seek(player_id, max(0, position_ms))

    async def _replace_ma_queue_from_qobuz(
        self,
        current_item: QueueTrackRef,
        start_position_ms: int,
    ) -> None:
        player_id = self._require_target_player_id()
        current_track = await self._get_ma_track_or_none(current_item.track_id)
        if current_track is None:
            self.provider.logger.debug(
                "Ignoring Qobuz PLAYING for unresolved cloud track %s",
                current_item.track_id,
            )
            return
        tracks = [current_track]
        if self.qobuz_state.next_item:
            with contextlib.suppress(Exception):
                tracks.append(await self._get_ma_track(self.qobuz_state.next_item.track_id))
        queue = self.mass.player_queues.get(player_id)
        if queue and queue.state != MAPlaybackState.IDLE:
            with contextlib.suppress(Exception):
                await self.mass.player_queues.stop(player_id)
        queue_items = [QueueItem.from_media_item(player_id, track) for track in tracks]
        self.mass.player_queues.clear(player_id, skip_stop=True)
        await self.mass.player_queues.load(
            player_id,
            queue_items=queue_items,
            keep_remaining=False,
            keep_played=False,
        )
        self._set_pending_qobuz_position(
            position_ms=start_position_ms,
            item=current_item,
            source_ms=0,
        )
        await self.mass.player_queues.play_index(
            player_id,
            0,
            seek_position=start_position_ms // 1000,
        )

    async def _seek_playing_if_needed(self, player_id: str, position_ms: int) -> None:
        queue = self.mass.player_queues.get(player_id)
        if not queue or not queue.current_item or queue.state != MAPlaybackState.PLAYING:
            return
        local_ms = int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
        if abs(local_ms - position_ms) >= SEEK_TOLERANCE_MS:
            self._set_pending_qobuz_position(
                position_ms=position_ms,
                item=self.qobuz_state.current_item,
                source_ms=local_ms,
            )
            await self.mass.player_queues.seek(player_id, position_ms // 1000)

    def _schedule_playing_seek(self, player_id: str, position_ms: int) -> None:
        """Coalesce playing seek commands before asking MA to restart the stream."""
        current_ref = self._queue_ref_key(self.qobuz_state.current_item)
        self._set_buffering()
        self._set_pending_qobuz_position(
            position_ms=position_ms,
            item=self.qobuz_state.current_item,
            source_ms=None,
        )
        self._pending_seek_position_ms = position_ms
        self._pending_seek_ref = current_ref
        self._cancel_pending_seek()
        self._pending_seek_task = asyncio.create_task(
            self._run_debounced_playing_seek(player_id, current_ref)
        )

    async def _run_debounced_playing_seek(self, player_id: str, expected_ref: str | None) -> None:
        """Run the latest playing seek after Qobuz scrub/echo traffic settles."""
        try:
            await asyncio.sleep(SEEK_DEBOUNCE_MS / 1000)
            position_ms = self._pending_seek_position_ms
            if position_ms is None:
                return
            if expected_ref != self._pending_seek_ref:
                return
            if expected_ref != self._queue_ref_key(self.qobuz_state.current_item):
                return
            await self._seek_playing_if_needed(player_id, position_ms)
        except asyncio.CancelledError:
            pass
        finally:
            if asyncio.current_task() is self._pending_seek_task:
                self._pending_seek_task = None
                if expected_ref == self._pending_seek_ref:
                    self._pending_seek_position_ms = None
                    self._pending_seek_ref = None

    async def _prequeue_next_item(self, item: QueueTrackRef | None) -> None:
        if item is None:
            self._last_prequeued_next_ref = None
            return
        current_item = self.qobuz_state.current_item
        if current_item is None:
            return
        next_ref = f"{item.queue_item_id}:{item.track_id}"
        if next_ref == self._last_prequeued_next_ref:
            return
        player_id = self.provider.get_target_player_id()
        if not player_id:
            return
        queue = self.mass.player_queues.get(player_id) if player_id else None
        if not queue or not queue.current_item:
            return
        current_ma_track_id = self.provider.get_qobuz_track_id_from_queue_item(queue.current_item)
        if current_ma_track_id != current_item.track_id:
            return
        ma_track = await self._get_ma_track_or_none(item.track_id)
        if ma_track is None:
            return
        await self.mass.player_queues.play_media(
            queue_id=player_id,
            media=cast("Any", ma_track),
            option=QueueOption.REPLACE_NEXT,
        )
        self._last_prequeued_next_ref = next_ref
        self._prequeued_qobuz_items.setdefault(item.track_id, []).append(item.queue_item_id)

    async def _send_ma_origin_queue_load(self, track_id: str, queue: Any) -> None:
        session = self.provider.qobuz_session
        if not session:
            return
        queue_version = QueueVersion(
            self.qobuz_state.queue_version.major,
            self.qobuz_state.queue_version.minor,
        )
        action_uuid = uuid.uuid4().bytes
        future: asyncio.Future[QueueLoadAck | QueueError | None] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending_queue_loads[action_uuid] = future
        if self._try_parse_qobuz_id(track_id) is None:
            self._pending_queue_loads.pop(action_uuid, None)
            self._last_ma_origin_track_id = None
            self.provider.logger.debug(
                "MA-to-Qobuz queue load unsupported for track %s: no numeric Qobuz track id",
                track_id,
            )
            return
        self.provider.logger.debug(
            "Trying MA-to-Qobuz QWeb-style queue load for track %s with queue version %s.%s",
            track_id,
            queue_version.major,
            queue_version.minor,
        )
        await session.send_queue_load_tracks(
            action_uuid=action_uuid,
            track_id=track_id,
            queue_version=queue_version,
            context_uuid=self._context_uuid_for_ma_origin_load(),
            qweb_track_session=True,
        )
        try:
            result = await asyncio.wait_for(future, timeout=MA_QUEUE_LOAD_ACK_TIMEOUT)
        except TimeoutError:
            self._pending_queue_loads.pop(action_uuid, None)
            self._last_ma_origin_track_id = None
            self.provider.logger.warning(
                "MA-to-Qobuz queue load unsupported by current session for track %s",
                track_id,
            )
            result = None
        if isinstance(result, QueueLoadAck):
            self.qobuz_state.playing_state = (
                PlayingState.PLAYING
                if queue.state == MAPlaybackState.PLAYING
                else PlayingState.PAUSED
            )
            self._set_buffer_ok()
            await self.report_state()
        elif isinstance(result, QueueError):
            if result.message != "Le tableau d'octets doit avoir une longueur de 16":
                self._last_ma_origin_track_id = None

    async def _qobuz_queue_item_id_for_track(self, track_id: str) -> int:
        if queue_item_ids := self._prequeued_qobuz_items.get(track_id):
            queue_item_id = queue_item_ids.pop(0)
            if not queue_item_ids:
                self._prequeued_qobuz_items.pop(track_id, None)
            return queue_item_id
        self._synthetic_queue_item_id += 1
        return self._synthetic_queue_item_id

    async def _qobuz_queue_load_context(self, track_id: str) -> tuple[int, int] | None:
        """Resolve Qobuz queue load context reference and item position for a track."""
        try:
            track = await self._get_ma_track(track_id)
            album = getattr(track, "album", None)
            album_id = getattr(album, "item_id", None)
            if not (numeric_album_id := self._try_parse_qobuz_id(album_id)):
                return None
            album_tracks = await self.provider.get_qobuz_provider().get_album_tracks(str(album_id))
            for index, album_track in enumerate(album_tracks):
                if str(album_track.item_id) == track_id:
                    return numeric_album_id, index
            position = max(0, int(getattr(track, "track_number", 1) or 1) - 1)
            return numeric_album_id, position
        except Exception:
            self.provider.logger.debug(
                "Could not resolve Qobuz album context for MA-origin track %s",
                track_id,
                exc_info=True,
            )
            return None

    @staticmethod
    def _try_parse_qobuz_id(value: Any) -> int | None:
        """Return an integer Qobuz id when the protocol can represent the value."""
        if value is None:
            return None
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    def _context_uuid_for_ma_origin_load(self) -> bytes:
        """Return a valid context UUID for controller queue-load commands."""
        current_item = self.qobuz_state.current_item
        if current_item and current_item.context_uuid and len(current_item.context_uuid) == 16:
            return current_item.context_uuid
        next_item = self.qobuz_state.next_item
        if next_item and next_item.context_uuid and len(next_item.context_uuid) == 16:
            return next_item.context_uuid
        return uuid.uuid4().bytes

    async def _sync_mirror_from_ma_queue(self, queue: Any) -> None:
        ma_track_id = self.provider.get_qobuz_track_id_from_queue_item(queue.current_item)
        if (
            ma_track_id
            and self.qobuz_state.next_item
            and ma_track_id == self.qobuz_state.next_item.track_id
            and (
                self.qobuz_state.current_item is None
                or ma_track_id != self.qobuz_state.current_item.track_id
            )
        ):
            self.provider.logger.debug(
                "Promoting Qobuz next item to current after MA advanced: %s:%s",
                self.qobuz_state.next_item.queue_item_id,
                self.qobuz_state.next_item.track_id,
            )
            self.qobuz_state.current_item = self.qobuz_state.next_item
            self.qobuz_state.next_item = None
            self._last_prequeued_next_ref = None
            self.pending_paused_seek_ms = None
            self._pending_paused_seek_ref = None
            self._clear_pending_position()
            self.qobuz_state.position_ms = 0
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        ma_playing_state = self._playing_state_from_ma_queue(queue)
        target_state = self.qobuz_state.playing_state
        is_confirmed = self._ma_state_confirms_qobuz_target(ma_playing_state, target_state)
        if self.qobuz_state.buffer_state == BufferState.BUFFERING and not is_confirmed:
            self.provider.logger.debug(
                "Keeping Qobuz command state %s while MA is still %s",
                target_state,
                ma_playing_state,
            )
            return
        if target_state == PlayingState.PAUSED and ma_playing_state == PlayingState.STOPPED:
            self._set_buffer_ok()
            self._clear_pending_position()
            return
        ma_position_ms = int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
        if self._pending_qobuz_position_ms is not None:
            if self._pending_qobuz_position_ref != self._queue_ref_key(
                self.qobuz_state.current_item
            ):
                self._clear_pending_position()
            elif not self._pending_position_confirmed(ma_position_ms):
                self.provider.logger.debug(
                    "Holding Qobuz position at %sms until MA reaches it; MA currently %sms",
                    self._pending_qobuz_position_ms,
                    ma_position_ms,
                )
                return
            else:
                self._clear_pending_position()
        self._set_buffer_ok()
        self.qobuz_state.playing_state = ma_playing_state
        self.qobuz_state.position_ms = ma_position_ms
        self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)

    def _clear_pending_position(self) -> None:
        """Clear a pending Qobuz seek/position confirmation."""
        self._pending_qobuz_position_ms = None
        self._pending_qobuz_position_ref = None
        self._pending_qobuz_position_source_ms = None
        self._pending_qobuz_position_timestamp_ms = None
        self._pending_seek_position_ms = None
        self._pending_seek_ref = None
        self._cancel_pending_seek()

    def _set_pending_paused_seek(
        self,
        position_ms: int,
        item: QueueTrackRef | None,
    ) -> None:
        """Remember a paused seek only for the Qobuz queue item it belongs to."""
        self.pending_paused_seek_ms = max(0, position_ms)
        self._pending_paused_seek_ref = self._queue_ref_key(item)

    def _take_pending_paused_seek(self, item: QueueTrackRef | None) -> int | None:
        """Consume a paused seek if it belongs to the item being played."""
        if self.pending_paused_seek_ms is None:
            return None
        if self._pending_paused_seek_ref != self._queue_ref_key(item):
            self.pending_paused_seek_ms = None
            self._pending_paused_seek_ref = None
            return None
        position_ms = self.pending_paused_seek_ms
        self.pending_paused_seek_ms = None
        self._pending_paused_seek_ref = None
        return position_ms

    def _set_pending_qobuz_position(
        self,
        *,
        position_ms: int,
        item: QueueTrackRef | None,
        source_ms: int | None,
    ) -> None:
        """Remember the target position MA must confirm before Qobuz can free-run."""
        self._pending_qobuz_position_ms = max(0, position_ms)
        self._pending_qobuz_position_ref = self._queue_ref_key(item)
        self._pending_qobuz_position_source_ms = source_ms
        self._pending_qobuz_position_timestamp_ms = int(time.time() * 1000)

    def _pending_position_confirmed(self, ma_position_ms: int) -> bool:
        """Return whether MA's position plausibly confirms the pending Qobuz target."""
        target_ms = self._pending_qobuz_position_ms
        if target_ms is None:
            return True
        source_ms = self._pending_qobuz_position_source_ms
        elapsed_ms = 0
        if self._pending_qobuz_position_timestamp_ms is not None:
            elapsed_ms = max(
                0,
                int(time.time() * 1000) - self._pending_qobuz_position_timestamp_ms,
            )
        lower_bound = max(0, target_ms - SEEK_TOLERANCE_MS)
        upper_bound = target_ms + elapsed_ms + SEEK_CONFIRM_OVERSHOOT_MS
        if source_ms is not None and source_ms < target_ms:
            return ma_position_ms >= lower_bound and ma_position_ms <= upper_bound
        return lower_bound <= ma_position_ms <= upper_bound

    def _cancel_pending_seek(self) -> None:
        """Cancel a queued playing seek if it has not been sent to MA yet."""
        if self._pending_seek_task and not self._pending_seek_task.done():
            self._pending_seek_task.cancel()
        self._pending_seek_task = None

    def _set_buffering(self) -> None:
        """Mark transport as buffering and refresh clients while MA catches up."""
        self.qobuz_state.buffer_state = BufferState.BUFFERING
        self._ensure_buffering_reporter()

    def _set_buffer_ok(self) -> None:
        """Mark transport as ready and stop the temporary buffering reporter."""
        self.qobuz_state.buffer_state = BufferState.OK
        self._cancel_buffering_reporter()

    def _ensure_buffering_reporter(self) -> None:
        """Start a short-interval reporter while Qobuz is waiting on MA audio readiness."""
        if self._buffering_report_task and not self._buffering_report_task.done():
            return
        self._buffering_report_task = asyncio.create_task(self._buffering_report_loop())

    def _cancel_buffering_reporter(self) -> None:
        """Cancel the short-interval buffering reporter."""
        if self._buffering_report_task and not self._buffering_report_task.done():
            if asyncio.current_task() is not self._buffering_report_task:
                self._buffering_report_task.cancel()
        self._buffering_report_task = None

    async def _buffering_report_loop(self) -> None:
        """Refresh Qobuz with a frozen position while MA prepares audio."""
        try:
            while True:
                if self.qobuz_state.buffer_state != BufferState.BUFFERING:
                    return
                await asyncio.sleep(BUFFERING_REPORT_INTERVAL)
                with contextlib.suppress(Exception):
                    await self.report_state()
        except asyncio.CancelledError:
            pass

    def _wire_buffer_state(self) -> BufferState:
        """Return the buffer state we expose to Qobuz clients."""
        if (
            self.qobuz_state.playing_state == PlayingState.PLAYING
            and self.qobuz_state.buffer_state == BufferState.BUFFERING
        ):
            return BufferState.BUFFERING
        return BufferState.OK

    @staticmethod
    def _queue_ref_key(item: QueueTrackRef | None) -> str | None:
        """Return a stable queue-item key for pending seek ownership."""
        if item is None:
            return None
        return f"{item.queue_item_id}:{item.track_id}"

    def _same_queue_ref(self, first: QueueTrackRef | None, second: QueueTrackRef | None) -> bool:
        """Return whether two queue refs point at the same Qobuz queue item."""
        return self._queue_ref_key(first) == self._queue_ref_key(second)

    @staticmethod
    def _playing_state_from_ma_queue(queue: Any) -> PlayingState:
        """Map MA queue state to Qobuz playing state."""
        if queue.state == MAPlaybackState.PLAYING:
            return PlayingState.PLAYING
        if queue.state == MAPlaybackState.PAUSED:
            return PlayingState.PAUSED
        return PlayingState.STOPPED

    @staticmethod
    def _ma_state_confirms_qobuz_target(
        ma_playing_state: PlayingState, target_state: PlayingState
    ) -> bool:
        """Return whether MA has caught up with the latest Qobuz command."""
        return ma_playing_state == target_state or (
            target_state == PlayingState.PAUSED and ma_playing_state == PlayingState.STOPPED
        )

    async def _ensure_qobuz_track_metadata(self, item: QueueTrackRef) -> None:
        ma_track = await self._get_ma_track(item.track_id)
        self.qobuz_state.duration_ms = (ma_track.duration or 0) * 1000

    async def _try_ensure_qobuz_track_metadata(self, item: QueueTrackRef) -> bool:
        if item.track_id in self._unresolvable_qobuz_track_ids:
            self.qobuz_state.duration_ms = 0
            return False
        try:
            await self._ensure_qobuz_track_metadata(item)
            return True
        except Exception:
            self._unresolvable_qobuz_track_ids.add(item.track_id)
            self.qobuz_state.duration_ms = 0
            self.provider.logger.warning(
                "Ignoring unresolved Qobuz Connect cloud track %s",
                item.track_id,
            )
            return False

    async def _get_ma_track(self, track_id: str) -> Track:
        return cast("Track", await self.provider.get_qobuz_provider().get_track(track_id))

    async def _get_ma_track_or_none(self, track_id: str) -> Track | None:
        if track_id in self._unresolvable_qobuz_track_ids:
            return None
        try:
            return await self._get_ma_track(track_id)
        except Exception:
            self._unresolvable_qobuz_track_ids.add(track_id)
            self.provider.logger.warning(
                "Ignoring unresolved Qobuz Connect cloud track %s", track_id
            )
            return None

    def _current_qobuz_position_ms(self) -> int:
        if self.qobuz_state.playing_state != PlayingState.PLAYING:
            return self.qobuz_state.position_ms
        return (
            self.qobuz_state.position_ms
            + int(time.time() * 1000)
            - self.qobuz_state.position_timestamp_ms
        )

    def _require_target_player_id(self) -> str:
        player_id = self.provider.get_target_player_id()
        if not player_id:
            raise PlayerUnavailableError("No Music Assistant player available for Qobuz Connect")
        return cast("str", player_id)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(STATE_REPORT_INTERVAL)
            with contextlib.suppress(Exception):
                await self.report_state()
