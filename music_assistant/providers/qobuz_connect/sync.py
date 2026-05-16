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

from .models import (
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

    async def handle_qobuz_set_state(self, event: SetStateEvent) -> None:
        """Apply a full Qobuz SET_STATE event to MA."""
        should_report = self._event_requires_renderer_report(event)
        self.origin = Origin.QOBUZ
        try:
            self._update_qobuz_mirror(event)
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
        position_ms = self._current_qobuz_position_ms()
        self.provider.logger.debug(
            "Qobuz report state=%s pos=%sms item=%s:%s qv=%s.%s sync_from_ma=%s",
            self.qobuz_state.playing_state,
            position_ms,
            current_item.queue_item_id,
            current_item.track_id,
            self.qobuz_state.queue_version.major,
            self.qobuz_state.queue_version.minor,
            sync_from_ma,
        )
        await session.send_renderer_state(
            playing_state=self.qobuz_state.playing_state,
            position_ms=position_ms,
            position_timestamp_ms=(
                self.qobuz_state.position_timestamp_ms or int(time.time() * 1000)
            ),
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
        if event.queue_version:
            self.qobuz_state.queue_version = event.queue_version
        if event.current_item:
            self.qobuz_state.current_item = event.current_item
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
        if event.position_ms is not None:
            self.qobuz_state.position_ms = max(0, event.position_ms)
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)

    async def _handle_qobuz_play(self, event: SetStateEvent) -> None:
        item = event.current_item or self.qobuz_state.current_item
        if item is None and (event.next_item or self.qobuz_state.next_item):
            item = event.next_item or self.qobuz_state.next_item
            if item is None:
                return
            self.qobuz_state.current_item = item
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
        start_position_ms = (
            self.pending_paused_seek_ms
            if self.pending_paused_seek_ms is not None
            else event.position_ms
            if event.position_ms is not None
            else self.qobuz_state.position_ms
        )
        self.pending_paused_seek_ms = None
        if start_position_ms is not None:
            self.qobuz_state.position_ms = max(0, start_position_ms)
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
            self._pending_qobuz_position_ms = start_position_ms

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
        self.pending_paused_seek_ms = (
            max(0, event.position_ms)
            if event.position_ms is not None
            else self.qobuz_state.position_ms
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
            self.pending_paused_seek_ms = max(0, position_ms)
            return
        if self.qobuz_state.playing_state == PlayingState.PLAYING:
            player_id = self.provider.get_target_player_id()
            if player_id:
                self._pending_qobuz_position_ms = max(0, position_ms)
                await self._seek_playing_if_needed(player_id, position_ms)

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
        await self.mass.player_queues.play_media(
            queue_id=player_id,
            media=cast("Any", tracks),
            option=QueueOption.REPLACE,
            start_item=tracks[0],
        )
        if start_position_ms > 0:
            self._pending_qobuz_position_ms = start_position_ms
            queue = self.mass.player_queues.get(player_id)
            if queue and queue.current_index is not None:
                await self.mass.player_queues.play_index(
                    player_id,
                    queue.current_index,
                    seek_position=start_position_ms // 1000,
                )

    async def _seek_playing_if_needed(self, player_id: str, position_ms: int) -> None:
        queue = self.mass.player_queues.get(player_id)
        if not queue or not queue.current_item or queue.state != MAPlaybackState.PLAYING:
            return
        local_ms = int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
        if abs(local_ms - position_ms) >= SEEK_TOLERANCE_MS:
            self._pending_qobuz_position_ms = position_ms
            await self.mass.player_queues.seek(player_id, position_ms // 1000)

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
        if queue.state == MAPlaybackState.PLAYING:
            self.qobuz_state.playing_state = PlayingState.PLAYING
        elif queue.state == MAPlaybackState.PAUSED:
            self.qobuz_state.playing_state = PlayingState.PAUSED
        else:
            self.qobuz_state.playing_state = PlayingState.STOPPED
        ma_position_ms = int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
        if self._pending_qobuz_position_ms is not None:
            if abs(ma_position_ms - self._pending_qobuz_position_ms) >= SEEK_TOLERANCE_MS:
                self.provider.logger.debug(
                    "Holding Qobuz position at %sms until MA reaches it; MA currently %sms",
                    self._pending_qobuz_position_ms,
                    ma_position_ms,
                )
                return
            self._pending_qobuz_position_ms = None
        self.qobuz_state.position_ms = ma_position_ms
        self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)

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
