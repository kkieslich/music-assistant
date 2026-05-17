"""
Qobuz Connect sync engine — facade + cross-cutting state.

After Phase C the engine is a thin facade that wires up six
collaborators and holds the small set of state they need to share.
Everything domain-specific lives in its own module:

- :mod:`.command_handler` — ``SRVR_RNDR_SET_STATE`` reconciliation
  pipeline (mirror update + reconcile + per-playing-state branches +
  MA-queue replacement + prequeue).
- :mod:`.outbound_reporter` — renderer→cloud emission (heartbeat,
  buffering reporter, the canonical ``RNDR_SRVR_STATE_UPDATED`` frame).
- :mod:`.seek_pipeline` — paused-seek storage, playing-seek
  debouncing, position-confirmation.
- :mod:`.metadata_resolver` — MA track-metadata lookups + fail-cache.
- :mod:`.queue_loader` — MA→Qobuz queue-load round-trip.
- :mod:`.ma_bridge` — the single seam to the MA world (provider +
  ``mass.player_queues.*`` / ``mass.players.*``).

What stays on the engine:

- ``QobuzMirror`` canonical state snapshot (``self.qobuz_state``).
- The pending-action dataclass fields (``paused_seek``, ``playing_seek``,
  ``qobuz_position``) — these are touched by ~3 collaborators each so
  they're easiest to keep here as plain attributes.
- ``_pending_queue_loads`` futures map (used by command_handler +
  queue_loader + the queue-ack inbound handler — central enough to
  stay here).
- ``_command_generation`` (stale-reconcile guard — monotonic counter
  bumped on every renderer command; in-flight reconcile work that
  awaits slow MA operations bails when this no longer matches the
  generation it started at) and ``_last_ma_origin_track_id``
  (handle_ma_queue_event guard).
- ``origin`` (in-flight-change source marker, set via
  :func:`.state.origin_scope`).
- The Phase B mirror-update handlers (``handle_queue_state`` /
  ``handle_queue_tracks_*`` / ``handle_loop_mode`` / etc.) — they're
  small one-liner mirror updates; extracting them would only add
  indirection.
- The MA-event entry point (``handle_ma_queue_event`` +
  ``_sync_mirror_from_ma_queue``).
- Volume command surface (``set_volume`` / ``set_volume_delta``).
- Cross-cutting helpers used by multiple collaborators:
  ``_is_current_command``, ``_same_queue_ref``,
  ``_current_qobuz_position_ms``, ``_require_target_player_id``,
  ``_playing_state_from_ma_queue``, ``_ma_state_confirms_qobuz_target``.

Exposes ``QobuzConnectSyncEngine``.

See :doc:`ARCHITECTURE` for the end-to-end flow, the inbound/outbound
message tables and the glossary.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.errors import PlayerUnavailableError

from .command_handler import CommandHandler
from .ma_bridge import MABridge
from .metadata_resolver import MetadataResolver
from .models import (
    BufferState,
    LoopMode,
    Origin,
    PlayingState,
    QobuzMirror,
    QueueClearedEvent,
    QueueError,
    QueueLoadAck,
    QueueStateSnapshot,
    QueueTrackRef,
    QueueTracksAddedEvent,
    QueueTracksInsertedEvent,
    QueueTracksRemovedEvent,
    QueueTracksReorderedEvent,
    QueueVersion,
    SessionStateEvent,
    SetStateEvent,
)
from .outbound_reporter import OutboundReporter
from .queue_loader import QueueLoader
from .seek_pipeline import SeekPipeline
from .state import (
    PausedSeek,
    PendingPlayingSeek,
    PendingQobuzPosition,
    TrackRefKey,
    origin_scope,
)

if TYPE_CHECKING:
    from music_assistant_models.event import MassEvent

MA_QUEUE_LOAD_ACK_TIMEOUT = 3.0


class QobuzConnectSyncEngine:
    """Single owner of MA/Qobuz playback and queue synchronization."""

    def __init__(self, provider: Any) -> None:
        """Initialize sync engine."""
        self.provider = provider
        # Single seam for the MA world — provider accessors plus the
        # ``mass.player_queues.*`` / ``mass.players.*`` operations.
        # Direct ``self.mass``/``self.provider`` use has been retired
        # from this file in favour of the bridge.
        self.bridge = MABridge(provider)
        # All renderer→cloud emission (heartbeat, buffering reporter,
        # the canonical ``RNDR_SRVR_STATE_UPDATED`` frame) lives here.
        self.reporter = OutboundReporter(self)
        # Seek + position-confirmation state machine.
        self.seek_pipeline = SeekPipeline(self)
        # MA-track-metadata lookups + fail-cache.
        self.metadata = MetadataResolver(self)
        # MA→Qobuz queue-load round-trip.
        self.queue_loader = QueueLoader(self)
        # Qobuz SET_STATE command handler (mirror update + reconcile +
        # per-playing-state branches + MA-queue replacement + prequeue).
        self.command_handler = CommandHandler(self)
        self.qobuz_state = QobuzMirror()
        # Pending-action ephemeral state, grouped into typed dataclasses
        # (see state.py). Each ``None`` means "no pending action of this
        # kind"; replaces a flat soup of 10+ scattered fields.
        self.paused_seek: PausedSeek | None = None
        self.playing_seek: PendingPlayingSeek | None = None
        self.qobuz_position: PendingQobuzPosition | None = None
        self.origin: Origin | None = None
        self._pending_queue_loads: dict[
            bytes, asyncio.Future[QueueLoadAck | QueueError | None]
        ] = {}
        self._last_ma_origin_track_id: str | None = None
        self._command_generation = 0
        # Tracks whether the Qobuz cloud has us as the active renderer. False
        # between SRVR_RNDR_SET_ACTIVE(false) and the next ...(true). While
        # inactive we silence outbound MA-origin echoes so we don't push
        # queue loads at a session the cloud no longer expects us to drive.
        # Defaults to True: the heartbeat starts on provider load before any
        # SET_ACTIVE arrives, and pre-Qobuz MA playback should still be able
        # to register with the cloud once a session opens.
        self._is_active = True
        # Whether we've already fired ``CTRL_SRVR_ASK_FOR_QUEUE_STATE`` for
        # the currently-active Qobuz session. The cloud only emits
        # ``SRVR_CTRL_QUEUE_STATE`` in response to that explicit ask — and,
        # unlike the reference Web Client (a controller+renderer combo),
        # we as a renderer-only never receive ``SRVR_CTRL_SESSION_STATE``
        # to trigger from. So instead we fire the ask the first time *any*
        # inbound message lands a non-empty ``queue_version`` on the mirror.
        # Reset on deactivation / reactivation so reconnects re-ask.
        self._asked_queue_state = False

    @property
    def pending_paused_seek_ms(self) -> int | None:
        """Position of the pending paused-seek, if any. Read-only convenience."""
        return self.paused_seek.position_ms if self.paused_seek else None

    async def start(self) -> None:
        """Start the outbound heartbeat."""
        await self.reporter.start()

    async def stop(self) -> None:
        """Stop heartbeats + cancel any in-flight async work."""
        await self.reporter.stop()
        self.command_handler.cancel_tasks()
        self.seek_pipeline.cancel_pending_seek()

    async def report_state(self, *, sync_from_ma: bool = True) -> None:
        """Report canonical renderer state to the Qobuz app (delegates)."""
        await self.reporter.report_state(sync_from_ma=sync_from_ma)

    def reset_for_deactivation(self) -> None:
        """
        Clear sync state after the cloud told us we are no longer the active renderer.

        Keeps the heartbeat running so we can resume cleanly when the cloud
        reactivates us, but drops anything that was specific to the previous
        playback session. Use :meth:`release_target_player` instead from the
        provider's deactivation handler — that method also stops and clears
        the underlying MA queue.
        """
        self._is_active = False
        self.qobuz_state = QobuzMirror()
        self.paused_seek = None
        self.qobuz_position = None
        self.playing_seek = None
        self._command_generation += 1
        self.command_handler.cancel_tasks()
        self.command_handler.reset_reconcile_dedup()
        self.seek_pipeline.cancel_pending_seek()
        self.reporter.cancel_buffering_reporter()
        self._last_ma_origin_track_id = None
        self._asked_queue_state = False
        self.metadata.clear_unresolvable_cache()

    def set_active(self, *, active: bool) -> None:
        """Toggle the engine's "Qobuz cloud says we're the active renderer" flag."""
        self._is_active = active
        if active:
            # Fresh activation — next inbound queueVersion should re-ask the
            # cloud for the full snapshot, even if the underlying session
            # hasn't reconnected.
            self._asked_queue_state = False

    async def release_target_player(self) -> None:
        """
        Hand the target player back to the user — stop playback + clear queue.

        Sequence matters here:
        1. Reset our mirror + cancel async tasks *first* so any heartbeat or
           in-flight reconcile racing during the stop/clear can't push a
           stale "still playing" frame back to the Qobuz cloud (observed in
           the May 2026 log as two extra ``Qobuz report state=2`` lines
           emitted between the deactivation log and the actual stop).
        2. Stop the MA queue so the audio stream tears down cleanly.
        3. Clear the MA queue so the next time the user opens MA the old
           Qobuz Connect tracks aren't sitting there orphaned.
        """
        player_id = self.bridge.target_player_id()
        self.reset_for_deactivation()
        if not player_id:
            return
        with contextlib.suppress(Exception):
            await self.bridge.stop_queue(player_id)
        with contextlib.suppress(Exception):
            self.bridge.clear_queue(player_id, skip_stop=True)

    async def handle_qobuz_set_state(self, event: SetStateEvent) -> None:
        """Apply a full Qobuz SET_STATE event to MA (delegates)."""
        await self.command_handler.handle_set_state(event)
        # Renderer-role caveat: we authenticate with a device-session JWT and
        # the cloud doesn't push us ``SRVR_CTRL_SESSION_STATE`` like it does
        # to the controller-role Web Client. So the SET_STATE queueVersion
        # is the first place we learn the right value to ask for queue
        # state with.
        await self.maybe_ask_for_queue_state()

    async def handle_ma_queue_event(self, event: MassEvent) -> None:
        """React to MA queue updates that were not caused by Qobuz commands."""
        if self.origin == Origin.QOBUZ:
            return
        if not self._is_active:
            # The cloud told us we're no longer the active renderer. The
            # post-deactivation ``stop`` and ``clear`` themselves emit MA
            # queue events that would otherwise echo back to the cloud as
            # MA-origin queue loads, undoing the handoff.
            return
        player_id = self.bridge.target_player_id()
        if not player_id or event.object_id != player_id:
            return
        queue = event.data
        if not queue or not getattr(queue, "current_item", None):
            return
        if queue.state not in (MAPlaybackState.PLAYING, MAPlaybackState.PAUSED):
            return
        track_id = self.bridge.qobuz_track_id_for(queue.current_item)
        if not track_id:
            return
        if self.command_handler.is_reconciling():
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
        await self.queue_loader.send_ma_origin_load(track_id, queue)

    async def handle_queue_load_ack(self, ack: QueueLoadAck) -> None:
        """Handle Qobuz queue-load acknowledgement."""
        async with origin_scope(self, Origin.ACK):
            self.qobuz_state.queue_version = ack.queue_version
            if ack.tracks:
                idx = min(ack.queue_position, len(ack.tracks) - 1)
                current_item = ack.tracks[idx]
                if await self.metadata.try_ensure_track_duration(current_item):
                    self.qobuz_state.current_item = current_item
            if future := self._pending_queue_loads.pop(ack.action_uuid, None):
                if not future.done():
                    future.set_result(ack)
            await self.report_state()

    async def handle_queue_error(self, error: QueueError) -> None:
        """Handle Qobuz queue command error."""
        if error.queue_version:
            self.qobuz_state.queue_version = error.queue_version
        if future := self._pending_queue_loads.pop(error.action_uuid, None):
            if not future.done():
                future.set_result(error)
        self.bridge.logger.warning(
            "Qobuz Connect queue command failed: %s %s (server queue version: %s.%s)",
            error.code,
            error.message,
            error.queue_version.major if error.queue_version else "?",
            error.queue_version.minor if error.queue_version else "?",
        )

    async def handle_queue_version(self, version: QueueVersion) -> None:
        """Remember Qobuz queue version changes."""
        self.qobuz_state.queue_version = version
        await self.maybe_ask_for_queue_state()

    async def handle_session_state(self, event: SessionStateEvent) -> None:
        """Apply a ``SRVR_CTRL_SESSION_STATE`` notification to the mirror + ask.

        The cloud emits ``SRVR_CTRL_QUEUE_STATE`` in response to an explicit
        ``CTRL_SRVR_ASK_FOR_QUEUE_STATE``. The Web Client captures show
        ``SESSION_STATE`` as the natural trigger, but the cloud only pushes
        it to clients in the controller role (user-login JWT). Renderers
        like us (device-session JWT from ``/connect``) don't receive it,
        so :meth:`maybe_ask_for_queue_state` is also wired into
        ``handle_qobuz_set_state`` / ``handle_queue_version`` —
        ``_asked_queue_state`` coalesces all entry points into one ask.
        """
        self.qobuz_state.queue_version = event.queue_version
        await self.maybe_ask_for_queue_state()

    async def maybe_ask_for_queue_state(self) -> None:
        """Send ``CTRL_SRVR_ASK_FOR_QUEUE_STATE`` if we haven't already this session."""
        if self._asked_queue_state:
            return
        # QobuzMirror's default factory returns QueueVersion(0, 0); skip
        # asking until the cloud has actually told us a real version.
        version = self.qobuz_state.queue_version
        if version.major == 0 and version.minor == 0:
            return
        session = self.bridge.session
        if session is None:
            return
        import uuid as _uuid  # noqa: PLC0415 — defer the import; only used here

        self._asked_queue_state = True
        self.bridge.logger.debug(
            "Asking Qobuz cloud for full queue snapshot at qv=%s.%s",
            self.qobuz_state.queue_version.major,
            self.qobuz_state.queue_version.minor,
        )
        await session.send_ask_for_queue_state(
            queue_version=self.qobuz_state.queue_version,
            queue_uuid=_uuid.uuid4().bytes,
        )

    async def handle_queue_state(self, snapshot: QueueStateSnapshot) -> None:
        """Apply a full ``SRVR_CTRL_QUEUE_STATE`` snapshot to the mirror + MA.

        The snapshot normally lands *after* an earlier ``SET_STATE`` reconcile
        has already filled MA's queue with just current+next (because the
        snapshot is the cloud's async response to our ask). Once the full
        list is on the mirror, fire-and-forget the chunked background
        preload that extends MA's queue without disrupting playback.
        """
        self.qobuz_state.queue_version = snapshot.queue_version
        self.qobuz_state.tracks = list(snapshot.tracks)
        self.qobuz_state.shuffle_mode = snapshot.shuffle_mode
        self.qobuz_state.autoplay_mode = snapshot.autoplay_mode
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_added(self, event: QueueTracksAddedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_ADDED`` delta, then reconcile MA."""
        self.qobuz_state.queue_version = event.queue_version
        self.qobuz_state.tracks.extend(event.tracks)
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_inserted(self, event: QueueTracksInsertedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_INSERTED`` delta, then reconcile MA."""
        self.qobuz_state.queue_version = event.queue_version
        insert_index = max(0, min(event.insert_after, len(self.qobuz_state.tracks)))
        self.qobuz_state.tracks[insert_index:insert_index] = event.tracks
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_removed(self, event: QueueTracksRemovedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_REMOVED`` delta, then reconcile MA."""
        self.qobuz_state.queue_version = event.queue_version
        removed_ids = set(event.queue_item_ids)
        self.qobuz_state.tracks = [
            track for track in self.qobuz_state.tracks if track.queue_item_id not in removed_ids
        ]
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_reordered(self, event: QueueTracksReorderedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_REORDERED`` delta, then reconcile MA.

        Note: the reconciler currently only adds missing items and removes
        stale ones — pure-reorder events (same set, different order) leave
        MA's queue order untouched in Pass 2a. Full position alignment lands
        with the positional-insert pass.
        """
        self.qobuz_state.queue_version = event.queue_version
        ids_to_move = list(event.queue_item_ids)
        id_to_track = {track.queue_item_id: track for track in self.qobuz_state.tracks}
        moving = [id_to_track[item_id] for item_id in ids_to_move if item_id in id_to_track]
        remaining = [
            track
            for track in self.qobuz_state.tracks
            if track.queue_item_id not in set(ids_to_move)
        ]
        target = max(0, min(event.insert_after, len(remaining)))
        self.qobuz_state.tracks = remaining[:target] + moving + remaining[target:]
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_cleared(self, _event: QueueClearedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_CLEARED`` notification, then reconcile MA.

        Empties the mirror; the reconciler then removes every MA item that
        isn't the currently-playing one. Audio continues uninterrupted.
        """
        self.qobuz_state.queue_version = _event.queue_version
        self.qobuz_state.tracks = []
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_loop_mode(self, mode: LoopMode) -> None:
        """Record a renderer ``SET_LOOP_MODE`` command in the mirror."""
        self.qobuz_state.loop_mode = mode

    async def handle_shuffle_mode(self, shuffle_on: bool) -> None:
        """Record a renderer ``SET_SHUFFLE_MODE`` command in the mirror."""
        self.qobuz_state.shuffle_mode = shuffle_on

    async def handle_autoplay_mode(self, autoplay_on: bool) -> None:
        """Record a renderer ``SET_AUTOPLAY_MODE`` command in the mirror."""
        self.qobuz_state.autoplay_mode = autoplay_on

    async def set_volume(self, level: int) -> int:
        """Set MA player volume from Qobuz app."""
        player_id = self._require_target_player_id()
        volume = max(0, min(100, int(level)))
        await self.bridge.cmd_volume_set(player_id, volume)
        session = self.bridge.session
        if session:
            await session.send_volume_changed(volume)
        return volume

    async def set_volume_delta(self, delta: int) -> int:
        """Adjust MA volume from Qobuz app."""
        player_id = self.bridge.target_player_id()
        current = 0
        if player_id and (player := self.bridge.get_player(player_id)):
            current = player.state.volume_level or 0
        return await self.set_volume(current + int(delta))

    async def _sync_mirror_from_ma_queue(self, queue: Any) -> None:
        ma_track_id = self.bridge.qobuz_track_id_for(queue.current_item)
        if (
            ma_track_id
            and self.qobuz_state.next_item
            and ma_track_id == self.qobuz_state.next_item.track_id
            and (
                self.qobuz_state.current_item is None
                or ma_track_id != self.qobuz_state.current_item.track_id
            )
        ):
            self.bridge.logger.debug(
                "Promoting Qobuz next item to current after MA advanced: %s:%s",
                self.qobuz_state.next_item.queue_item_id,
                self.qobuz_state.next_item.track_id,
            )
            self.qobuz_state.current_item = self.qobuz_state.next_item
            self.qobuz_state.next_item = None
            self.command_handler.reset_reconcile_dedup()
            self.paused_seek = None
            self.seek_pipeline.clear_pending_position()
            self.qobuz_state.position_ms = 0
            self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)
        ma_playing_state = self._playing_state_from_ma_queue(queue)
        target_state = self.qobuz_state.playing_state
        is_confirmed = self._ma_state_confirms_qobuz_target(ma_playing_state, target_state)
        if self.qobuz_state.buffer_state == BufferState.BUFFERING and not is_confirmed:
            self.bridge.logger.debug(
                "Keeping Qobuz command state %s while MA is still %s",
                target_state,
                ma_playing_state,
            )
            return
        if target_state == PlayingState.PAUSED and ma_playing_state == PlayingState.STOPPED:
            self._set_buffer_ok()
            self.seek_pipeline.clear_pending_position()
            return
        ma_position_ms = int(getattr(queue, "corrected_elapsed_time", 0) * 1000)
        if self.qobuz_position is not None:
            if not self.seek_pipeline.pending_position_confirmed(ma_position_ms):
                self.bridge.logger.debug(
                    "Holding Qobuz target=%sms issued=%sms until MA reaches it; MA at %sms",
                    self.qobuz_position.target_ms,
                    self.qobuz_position.issued_ms,
                    ma_position_ms,
                )
                return
            # MA caught up to the position we last asked it to seek to. If a
            # newer Qobuz seek arrived while that one was in flight, send a
            # fresh MA seek for it now — rather than letting the rapid seeks
            # all queue MA-side at once.
            player_id = self.bridge.target_player_id()
            if player_id and self.seek_pipeline.has_deferred_target():
                if await self.seek_pipeline.reissue_deferred_seek(player_id):
                    return
            self.seek_pipeline.clear_pending_position()
        self._set_buffer_ok()
        self.qobuz_state.playing_state = ma_playing_state
        self.qobuz_state.position_ms = ma_position_ms
        self.qobuz_state.position_timestamp_ms = int(time.time() * 1000)

    def _is_current_command(
        self,
        generation: int,
        item: QueueTrackRef | None = None,
    ) -> bool:
        """Return whether a slow operation still belongs to the newest Qobuz command."""
        if generation != self._command_generation:
            return False
        return item is None or TrackRefKey.from_ref(item) == TrackRefKey.from_ref(
            self.qobuz_state.current_item
        )

    def _set_buffering(self) -> None:
        """Delegate to ``self.reporter.set_buffering`` — kept as a sync helper."""
        self.reporter.set_buffering()

    def _set_buffer_ok(self) -> None:
        """Delegate to ``self.reporter.set_buffer_ok`` — kept as a sync helper."""
        self.reporter.set_buffer_ok()

    @staticmethod
    def _same_queue_ref(first: QueueTrackRef | None, second: QueueTrackRef | None) -> bool:
        """Return whether two queue refs point at the same Qobuz queue item."""
        return TrackRefKey.from_ref(first) == TrackRefKey.from_ref(second)

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

    def _current_qobuz_position_ms(self) -> int:
        if self.qobuz_state.playing_state != PlayingState.PLAYING:
            return self.qobuz_state.position_ms
        return (
            self.qobuz_state.position_ms
            + int(time.time() * 1000)
            - self.qobuz_state.position_timestamp_ms
        )

    def _require_target_player_id(self) -> str:
        player_id = self.bridge.target_player_id()
        if not player_id:
            raise PlayerUnavailableError("No Music Assistant player available for Qobuz Connect")
        return player_id
