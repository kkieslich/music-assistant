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
import uuid
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
    OutboundActionKind,
    OutboundActionMeta,
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

# Outbound action ledger TTL. Entries register a queue mutation we *sent* to
# the Qobuz cloud; the cloud echoes the mutation back as ``SRVR_CTRL_QUEUE_*``
# with the same ``action_uuid``. If we never see the echo within this window
# (cloud silently dropped it, lost connection, etc.) we drop the ledger entry
# so it can't leak.
OUTBOUND_ACTION_TTL = 10.0

# Translate MA's str-enum ``RepeatMode`` (off / one / all) to the Qobuz
# int-enum ``LoopMode`` (OFF=1 / REPEAT_ONE=2 / REPEAT_ALL=3). Used by the
# MA→cloud loop-mode emit path.
_MA_REPEAT_TO_LOOP: dict[str, LoopMode] = {
    "off": LoopMode.OFF,
    "one": LoopMode.REPEAT_ONE,
    "all": LoopMode.REPEAT_ALL,
}

# Reverse map for cloud→MA loop-mode application. Keyed on ``LoopMode``;
# value is the matching ``RepeatMode`` value as a string so we don't need
# to import MA's enum into the type signature.
_LOOP_TO_MA_REPEAT: dict[LoopMode, str] = {
    LoopMode.OFF: "off",
    LoopMode.REPEAT_ONE: "one",
    LoopMode.REPEAT_ALL: "all",
}


def _detect_single_item_move(
    mirror_order: list[str], ma_order: list[str]
) -> tuple[int, int] | None:
    """
    Detect a single-item drag-reorder between two equal-set lists.

    Returns ``(src_idx, dst_idx)`` where ``src_idx`` is the item's position
    in ``mirror_order`` and ``dst_idx`` is its new position in ``ma_order``,
    or ``None`` if the diff isn't a single contiguous item move. Forward
    (dst > src) and backward (dst < src) moves both supported.

    The shape of a forward single-item move (mirror → ma) is:

    - For ``i < src``: ``ma[i] == mirror[i]``
    - For ``src <= i < dst``: ``ma[i] == mirror[i + 1]`` (items shifted left)
    - ``ma[dst] == mirror[src]``
    - For ``i > dst``: ``ma[i] == mirror[i]``

    Backward move is the mirror image. Any other diff (two items moved,
    overlapping moves, etc.) returns ``None`` so the caller falls back to
    the "complex change" warning.
    """
    if mirror_order == ma_order or len(mirror_order) != len(ma_order):
        return None
    first_div = next(
        (i for i, (m, a) in enumerate(zip(mirror_order, ma_order, strict=True)) if m != a),
        None,
    )
    if first_div is None:
        return None
    last_div = next(
        (i for i in range(len(mirror_order) - 1, -1, -1) if mirror_order[i] != ma_order[i]),
        None,
    )
    if last_div is None or last_div == first_div:
        return None
    # Forward move: mirror[first_div] is the moved item; in ma it lands at last_div.
    if ma_order[last_div] == mirror_order[first_div] and all(
        ma_order[i] == mirror_order[i + 1] for i in range(first_div, last_div)
    ):
        return first_div, last_div
    # Backward move: mirror[last_div] is the moved item; in ma it lands at first_div.
    if ma_order[first_div] == mirror_order[last_div] and all(
        ma_order[i] == mirror_order[i - 1] for i in range(first_div + 1, last_div + 1)
    ):
        return last_div, first_div
    return None


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
        # Ledger of queue-mutation commands *we* sent to the Qobuz cloud.
        # Used by the inbound dispatcher to short-circuit the
        # ``SRVR_CTRL_QUEUE_*`` echo: when an inbound delta carries an
        # ``action_uuid`` in this dict, MA's queue already reflects the
        # change (we originated it) and the reconciler must not re-apply.
        # Entries self-expire after ``OUTBOUND_ACTION_TTL`` seconds.
        self._outbound_action_uuids: dict[bytes, OutboundActionMeta] = {}
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
        # the currently-active Qobuz session, keyed by the ``queue_version``
        # we asked for last. The cloud only emits ``SRVR_CTRL_QUEUE_STATE``
        # in response to that explicit ask — and every queue mutation in
        # the Qobuz app bumps ``queue_version``, so we need to re-ask each
        # time to keep the mirror's track order current. ``None`` means
        # "haven't asked yet"; reset on deactivation / reactivation so
        # reconnects re-ask.
        self._last_asked_qv: tuple[int, int] | None = None

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
        self._last_asked_qv = None
        self.metadata.clear_unresolvable_cache()

    def set_active(self, *, active: bool) -> None:
        """Toggle the engine's "Qobuz cloud says we're the active renderer" flag."""
        self._is_active = active
        if active:
            # Fresh activation — next inbound queueVersion should re-ask the
            # cloud for the full snapshot, even if the underlying session
            # hasn't reconnected.
            self._last_asked_qv = None

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

    def register_outbound_action(
        self, kind: OutboundActionKind, queue_version: QueueVersion
    ) -> bytes:
        """Mint and register an ``action_uuid`` for a MA→cloud queue mutation."""
        action_uuid = uuid.uuid4().bytes
        self._outbound_action_uuids[action_uuid] = OutboundActionMeta(
            kind=kind,
            queue_version=queue_version,
            expires_at=time.monotonic() + OUTBOUND_ACTION_TTL,
        )
        self._gc_outbound_actions()
        return action_uuid

    def consume_outbound_action(self, action_uuid: bytes) -> OutboundActionMeta | None:
        """Return-and-pop the ledger entry for ``action_uuid``, or ``None`` if not ours."""
        meta = self._outbound_action_uuids.pop(action_uuid, None)
        if meta is None:
            return None
        if time.monotonic() > meta.expires_at:
            return None
        return meta

    def _gc_outbound_actions(self) -> None:
        """Drop ledger entries past their TTL — bounded leak guard."""
        now = time.monotonic()
        expired = [
            uid for uid, meta in self._outbound_action_uuids.items() if meta.expires_at < now
        ]
        for uid in expired:
            self._outbound_action_uuids.pop(uid, None)

    async def handle_ma_queue_items_updated(self, event: MassEvent) -> None:
        """
        Propagate user-driven MA queue edits back to the Qobuz cloud.

        Diffs MA's queue items against the mirror's view and emits the
        minimum ``CTRL_SRVR_QUEUE_*`` message(s) needed to bring the cloud
        in line. Skipped while origin == QOBUZ (we're applying inbound
        deltas) or the session is inactive.

        Pass 2c supports: CLEAR, REMOVE, ADD-at-end. Pure-reorder and
        middle-insert detection are best-effort and may fall back to
        re-asking for queue state.
        """
        if self.origin in (Origin.QOBUZ, Origin.ACK):
            return
        if not self._is_active:
            return
        if self.command_handler.is_reconciling():
            return
        player_id = self.bridge.target_player_id()
        if not player_id or event.object_id != player_id:
            return
        session = self.bridge.session
        if session is None:
            return

        ma_items = list(self.bridge.queue_items(player_id))
        ma_track_ids = [self.bridge.qobuz_track_id_for(it) for it in ma_items]
        # Drop non-Qobuz items entirely — they live in MA's queue but never
        # reach the cloud.
        ma_qobuz_ids = [tid for tid in ma_track_ids if tid]
        # Filter mirror to only items that *could* be in MA. The metadata
        # resolver marks track_ids it can't fetch (404 / region-locked / etc.)
        # as unresolvable; the reconciler drops those, so MA's queue is
        # genuinely shorter than mirror. Without this filter the differ would
        # see those gaps as user-removes and emit ``REMOVE_TRACKS`` for
        # tracks the user never actually touched.
        unresolvable = self.metadata.unresolvable_track_ids
        mirror_track_ids = [
            ref.track_id for ref in self.qobuz_state.tracks if ref.track_id not in unresolvable
        ]
        mirror_qid_by_track_id: dict[str, list[int]] = {}
        for ref in self.qobuz_state.tracks:
            if ref.track_id in unresolvable:
                continue
            mirror_qid_by_track_id.setdefault(ref.track_id, []).append(ref.queue_item_id)

        ma_set = set(ma_qobuz_ids)
        mirror_set = set(mirror_track_ids)

        if ma_qobuz_ids == mirror_track_ids:
            return  # no diff

        if not ma_qobuz_ids and mirror_track_ids:
            await self._emit_clear_queue(session)
            return

        removed_track_ids = mirror_set - ma_set
        added_track_ids = ma_set - mirror_set

        if removed_track_ids and not added_track_ids:
            await self._emit_remove_tracks(session, removed_track_ids, mirror_qid_by_track_id)
            return

        if added_track_ids and not removed_track_ids:
            # Detect pure-append (MA = mirror + new tail) vs middle-insert.
            append_only = ma_qobuz_ids[: len(mirror_track_ids)] == mirror_track_ids
            if append_only:
                new_tail = ma_qobuz_ids[len(mirror_track_ids) :]
                await self._emit_add_tracks(session, new_tail)
                return
            self.bridge.logger.debug(
                "MA queue diff: middle-insert not yet supported by outbound differ; "
                "skipping cloud emit (mirror=%s, ma=%s)",
                mirror_track_ids,
                ma_qobuz_ids,
            )
            return

        # Same set, different order → user reordered tracks in MA's UI. Try
        # to detect a single contiguous item move (the common case for
        # drag-and-drop) and emit one ``CTRL_SRVR_QUEUE_REORDER_TRACKS``.
        if not removed_track_ids and not added_track_ids:
            # The mirror may legitimately carry more entries than MA does
            # — Qobuz playlists sometimes contain track_ids the metadata
            # resolver can't fetch (404 / region-locked / etc.). The
            # reconciler drops those, so MA's queue is shorter than mirror
            # by the unresolvable count. For reorder detection, filter
            # mirror down to the subsequence MA actually has (by track_id,
            # accounting for duplicates) so the two lists have the same
            # length and a single-item move stays detectable.
            from collections import Counter  # noqa: PLC0415

            ma_remaining = Counter(ma_qobuz_ids)
            mirror_in_ma_order: list[str] = []
            for tid in mirror_track_ids:
                if ma_remaining.get(tid, 0) > 0:
                    mirror_in_ma_order.append(tid)
                    ma_remaining[tid] -= 1
            move = _detect_single_item_move(mirror_in_ma_order, ma_qobuz_ids)
            if move is not None:
                src_idx, dst_idx = move
                moved_track_id = mirror_in_ma_order[src_idx]
                qids = mirror_qid_by_track_id.get(moved_track_id, [])
                if len(qids) != 1:
                    # Duplicate track_ids — ambiguous which instance moved.
                    self.bridge.logger.debug(
                        "MA queue diff: reorder of duplicate track_id %s ambiguous; "
                        "skipping cloud emit",
                        moved_track_id,
                    )
                    return
                await self._emit_reorder_tracks(session, [qids[0]], dst_idx)
                return
            self.bridge.logger.debug(
                "MA queue diff: complex reorder not yet supported by outbound differ; "
                "skipping cloud emit (mirror=%s, ma=%s)",
                mirror_track_ids,
                ma_qobuz_ids,
            )
            return

        # Mixed change (simultaneous add+remove). Best-effort: skip and let
        # the next snapshot resync.
        self.bridge.logger.debug(
            "MA queue diff: simultaneous add+remove not yet supported by outbound differ; "
            "skipping cloud emit (mirror=%s, ma=%s)",
            mirror_track_ids,
            ma_qobuz_ids,
        )

    async def _emit_clear_queue(self, session: Any) -> None:
        """
        Send ``CTRL_SRVR_CLEAR_QUEUE`` and update the mirror optimistically.

        ``queue_version`` is sent as the *current* value — the cloud increments
        and broadcasts the new version itself via ``SRVR_CTRL_QUEUE_VERSION_CHANGED``.
        Sending a pre-bumped version is rejected with
        ``ERROR_QUEUE_REMOVE_TRACKS Queue version mismatch`` (the cloud expects
        the request to assert the version it's operating on, not the
        post-mutation version).
        """
        current_version = self.qobuz_state.queue_version
        self.register_outbound_action(OutboundActionKind.CLEAR, current_version)
        self.qobuz_state.tracks = []
        await session.send_clear_queue(queue_version=current_version)

    async def _emit_remove_tracks(
        self,
        session: Any,
        removed_track_ids: set[str],
        mirror_qid_by_track_id: dict[str, list[int]],
    ) -> None:
        """Send ``CTRL_SRVR_QUEUE_REMOVE_TRACKS`` with the current ``queue_version``."""
        queue_item_ids: list[int] = []
        for tid in removed_track_ids:
            qids = mirror_qid_by_track_id.get(tid, [])
            queue_item_ids.extend(qids)
        if not queue_item_ids:
            return
        current_version = self.qobuz_state.queue_version
        action_uuid = self.register_outbound_action(OutboundActionKind.REMOVE, current_version)
        removed_ids_set = set(queue_item_ids)
        self.qobuz_state.tracks = [
            ref for ref in self.qobuz_state.tracks if ref.queue_item_id not in removed_ids_set
        ]
        await session.send_queue_remove_tracks(
            action_uuid=action_uuid,
            queue_item_ids=queue_item_ids,
            queue_version=current_version,
        )

    async def _emit_reorder_tracks(
        self,
        session: Any,
        queue_item_ids: list[int],
        insert_after: int,
    ) -> None:
        """
        Send ``CTRL_SRVR_QUEUE_REORDER_TRACKS`` with the current ``queue_version``.

        Optimistically applies the move to the mirror so the next reconcile
        pass is a no-op. Mirrors :meth:`handle_queue_tracks_reordered`'s
        layout: items in ``queue_item_ids`` are removed from
        ``mirror.tracks`` then re-inserted as a contiguous block at index
        ``insert_after`` of the remaining list.
        """
        current_version = self.qobuz_state.queue_version
        action_uuid = self.register_outbound_action(OutboundActionKind.REORDER, current_version)
        ids_to_move = set(queue_item_ids)
        id_to_track = {track.queue_item_id: track for track in self.qobuz_state.tracks}
        moving = [id_to_track[qid] for qid in queue_item_ids if qid in id_to_track]
        remaining = [ref for ref in self.qobuz_state.tracks if ref.queue_item_id not in ids_to_move]
        target = max(0, min(insert_after, len(remaining)))
        self.qobuz_state.tracks = remaining[:target] + moving + remaining[target:]
        await session.send_queue_reorder_tracks(
            action_uuid=action_uuid,
            queue_item_ids=queue_item_ids,
            insert_after=insert_after,
            queue_version=current_version,
        )

    async def _emit_add_tracks(self, session: Any, new_track_ids: list[str]) -> None:
        """Send ``CTRL_SRVR_QUEUE_ADD_TRACKS`` with the current ``queue_version``."""
        if not new_track_ids:
            return
        current_version = self.qobuz_state.queue_version
        action_uuid = self.register_outbound_action(OutboundActionKind.ADD, current_version)
        # The cloud will assign real queue_item_ids; we register placeholders
        # on the mirror so the next reconcile pass doesn't re-add them. The
        # echoed ``SRVR_CTRL_QUEUE_TRACKS_ADDED`` will overwrite these with
        # the cloud-assigned ids when it lands.
        placeholder_refs = [QueueTrackRef(queue_item_id=0, track_id=tid) for tid in new_track_ids]
        self.qobuz_state.tracks.extend(placeholder_refs)
        await session.send_queue_add_tracks(
            action_uuid=action_uuid,
            tracks=placeholder_refs,
            queue_version=current_version,
        )

    async def _maybe_emit_modes_to_cloud(self, queue: Any) -> None:
        """
        Propagate MA-side loop + shuffle changes to the Qobuz cloud.

        The cloud is the authority on these flags via
        ``SRVR_RNDR_SET_LOOP_MODE`` / ``SRVR_RNDR_SET_SHUFFLE_MODE`` — until
        now MA-side toggles never reached the app. Compares MA's current
        ``repeat_mode`` / ``shuffle_enabled`` against the mirror; emits one
        ``CTRL_SRVR_SET_LOOP_MODE`` / ``CTRL_SRVR_SET_SHUFFLE_MODE`` per
        change, then optimistically updates the mirror so the next event
        with the same value is a no-op.

        The MA→cloud direction for both modes is best-effort: the cloud
        echoes back via ``SRVR_CTRL_LOOP_MODE_SET`` / ``..._SHUFFLE_MODE_SET``
        which we currently ignore (they're in the dispatcher's known-ignored
        set, mostly because they're broadcast for *other* renderers and our
        own echo is redundant once the mirror is updated optimistically).
        """
        session = self.bridge.session
        if session is None:
            return
        # Map MA's str-enum ``RepeatMode`` to the cloud's int-enum
        # ``LoopMode``. Fall through unmapped values without emitting.
        ma_repeat = getattr(queue, "repeat_mode", None)
        if ma_repeat is not None:
            mapped = _MA_REPEAT_TO_LOOP.get(getattr(ma_repeat, "value", ma_repeat))
            if mapped is not None and mapped != self.qobuz_state.loop_mode:
                self.qobuz_state.loop_mode = mapped
                await session.send_set_loop_mode(mapped)
        ma_shuffle = getattr(queue, "shuffle_enabled", None)
        if ma_shuffle is not None and bool(ma_shuffle) != self.qobuz_state.shuffle_mode:
            new_shuffle = bool(ma_shuffle)
            self.qobuz_state.shuffle_mode = new_shuffle
            current_qid = (
                self.qobuz_state.current_item.queue_item_id if self.qobuz_state.current_item else 0
            )
            action_uuid = self.register_outbound_action(
                OutboundActionKind.SHUFFLE, self.qobuz_state.queue_version
            )
            await session.send_set_shuffle_mode(
                shuffle_on=new_shuffle,
                queue_version=self.qobuz_state.queue_version,
                current_queue_item_id=current_qid,
                action_uuid=action_uuid,
            )

    async def handle_ma_queue_event(self, event: MassEvent) -> None:
        """React to MA queue updates that were not caused by Qobuz commands."""
        # Diagnostic trace: log every entry + which branch we took. Lets us
        # see in a live MA log whether a natural track advance reaches us
        # at all, which guard short-circuits it, and what current/next the
        # mirror held at decision time. Remove once the natural-advance
        # path is fully understood.
        logger = self.bridge.logger
        if self.origin == Origin.QOBUZ:
            logger.debug("ma_queue_event: skip — origin=QOBUZ")
            return
        if not self._is_active:
            logger.debug("ma_queue_event: skip — not active")
            return
        player_id = self.bridge.target_player_id()
        if not player_id or event.object_id != player_id:
            logger.debug(
                "ma_queue_event: skip — object_id mismatch (event=%s target=%s)",
                event.object_id,
                player_id,
            )
            return
        queue = event.data
        if not queue:
            logger.debug("ma_queue_event: skip — no queue data")
            return
        await self._maybe_emit_modes_to_cloud(queue)
        if not getattr(queue, "current_item", None):
            logger.debug("ma_queue_event: skip — queue.current_item is None")
            return
        if queue.state not in (MAPlaybackState.PLAYING, MAPlaybackState.PAUSED):
            logger.debug(
                "ma_queue_event: skip — queue.state=%s (not PLAYING/PAUSED)",
                queue.state,
            )
            return
        track_id = self.bridge.qobuz_track_id_for(queue.current_item)
        if not track_id:
            logger.debug("ma_queue_event: skip — no qobuz track_id for current item")
            return
        if self.command_handler.is_reconciling():
            logger.debug("ma_queue_event: skip — reconciling (ma_track_id=%s)", track_id)
            return

        cur_id = self.qobuz_state.current_item.track_id if self.qobuz_state.current_item else None
        next_id = self.qobuz_state.next_item.track_id if self.qobuz_state.next_item else None
        logger.debug(
            "ma_queue_event: ma_track=%s mirror_current=%s mirror_next=%s "
            "qobuz_position=%s last_ma_origin=%s",
            track_id,
            cur_id,
            next_id,
            self.qobuz_position.target_ms if self.qobuz_position else None,
            self._last_ma_origin_track_id,
        )

        if self.qobuz_state.next_item and track_id == self.qobuz_state.next_item.track_id:
            logger.debug("ma_queue_event: branch=promote-next (track matches mirror.next)")
            await self._sync_mirror_from_ma_queue(queue)
            await self.report_state(sync_from_ma=False)
            return

        if self.qobuz_state.current_item and self.qobuz_state.current_item.track_id == track_id:
            logger.debug("ma_queue_event: branch=same-current (track matches mirror.current)")
            await self._sync_mirror_from_ma_queue(queue)
            await self.report_state()
            return

        if self._last_ma_origin_track_id == track_id:
            logger.debug("ma_queue_event: skip — already sent ma-origin load for %s", track_id)
            return
        logger.debug(
            "ma_queue_event: branch=ma-origin-load (no match — track diverged from mirror)"
        )
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
        """
        Handle Qobuz queue command error and resync if it was one of ours.

        The cloud rejects ``CTRL_SRVR_QUEUE_*`` commands when their
        ``queue_version`` is stale — typically because a natural track
        advance bumped the cloud's version between the moment we computed
        ours and the moment the message arrived. Our mirror is then
        optimistically updated but the cloud is not, so MA's local state
        will silently revert on the next inbound snapshot.

        Recovery: drop the ledger entry, absorb the cloud's reported version,
        and ask for a fresh snapshot. The inbound reconciler will align MA
        back to the cloud's authoritative state. The user's reorder is lost
        on the rejected attempt — they can retry; subsequent attempts will
        carry the newer version and succeed.
        """
        was_our_action = self.consume_outbound_action(error.action_uuid) is not None
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
        if was_our_action:
            # Re-ask for the cloud's current snapshot so the inbound
            # reconciler can drag MA's optimistically-updated queue back
            # in line with the cloud's authoritative view.
            await self.maybe_ask_for_queue_state()

    async def handle_queue_version(self, version: QueueVersion) -> None:
        """Remember Qobuz queue version changes."""
        self.qobuz_state.queue_version = version
        await self.maybe_ask_for_queue_state()

    async def handle_session_state(self, event: SessionStateEvent) -> None:
        """
        Apply a ``SRVR_CTRL_SESSION_STATE`` notification to the mirror + ask.

        The cloud emits ``SRVR_CTRL_QUEUE_STATE`` in response to an explicit
        ``CTRL_SRVR_ASK_FOR_QUEUE_STATE``. The Web Client captures show
        ``SESSION_STATE`` as the natural trigger, but the cloud only pushes
        it to clients in the controller role (user-login JWT). Renderers
        like us (device-session JWT from ``/connect``) don't receive it,
        so :meth:`maybe_ask_for_queue_state` is also wired into
        ``handle_qobuz_set_state`` / ``handle_queue_version`` —
        ``_last_asked_qv`` coalesces all entry points and re-asks on qv bumps.
        """
        self.qobuz_state.queue_version = event.queue_version
        await self.maybe_ask_for_queue_state()

    async def maybe_ask_for_queue_state(self) -> None:
        """
        Send ``CTRL_SRVR_ASK_FOR_QUEUE_STATE`` whenever the cloud advances ``queue_version``.

        The cloud only emits ``SRVR_CTRL_QUEUE_STATE`` in response to an
        explicit ask. Every queue mutation in the Qobuz app bumps
        ``queue_version`` — without re-asking, the mirror keeps the stale
        track list and the reconciler has nothing new to apply. Tracking
        the last-asked version (rather than a single boolean) makes us
        re-fetch the snapshot on every cloud-side mutation.
        """
        # QobuzMirror's default factory returns QueueVersion(0, 0); skip
        # asking until the cloud has actually told us a real version.
        version = self.qobuz_state.queue_version
        if version.major == 0 and version.minor == 0:
            return
        current_qv = (version.major, version.minor)
        if current_qv == self._last_asked_qv:
            return
        session = self.bridge.session
        if session is None:
            return
        import uuid as _uuid  # noqa: PLC0415 — defer the import; only used here

        self._last_asked_qv = current_qv
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
        """Apply a full ``SRVR_CTRL_QUEUE_STATE`` snapshot to the mirror + MA."""
        self.qobuz_state.queue_version = snapshot.queue_version
        self.qobuz_state.tracks = list(snapshot.tracks)
        self.qobuz_state.shuffle_mode = snapshot.shuffle_mode
        self.qobuz_state.autoplay_mode = snapshot.autoplay_mode
        # Mirror MA's shuffle flag to the snapshot. Qobuz sometimes conveys
        # shuffle changes via the snapshot alone (no preceding
        # ``SRVR_RNDR_SET_SHUFFLE_MODE`` — observed for shuffle-OFF
        # toggles), so without this MA's UI flag goes stale. The reconciler
        # then rebuilds queue order to match the mirror, so we only need to
        # flip the flag here.
        player_id = self.bridge.target_player_id()
        if player_id is not None:
            self.bridge.set_shuffle_flag(player_id, snapshot.shuffle_mode)
        # Tracks just mutated — release the dedup gate so any prior
        # materialize (e.g. one that ran during the stale-tracks gap
        # between SET_STATE and this snapshot) doesn't silently swallow
        # this reconcile.
        self.command_handler.reset_reconcile_dedup()
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_added(self, event: QueueTracksAddedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_ADDED`` delta, then reconcile MA."""
        echo = self.consume_outbound_action(event.action_uuid)
        if echo is not None:
            # Our own ADD echoed back — patch the cloud-assigned queue_item_ids
            # onto the placeholder mirror entries we registered when sending.
            self.qobuz_state.queue_version = event.queue_version
            self._absorb_cloud_qids_for_self_add(event.tracks)
            return
        self.qobuz_state.queue_version = event.queue_version
        self.qobuz_state.tracks.extend(event.tracks)
        self.command_handler.reset_reconcile_dedup()
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_inserted(self, event: QueueTracksInsertedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_INSERTED`` delta, then reconcile MA."""
        echo = self.consume_outbound_action(event.action_uuid)
        if echo is not None:
            self.qobuz_state.queue_version = event.queue_version
            return
        self.qobuz_state.queue_version = event.queue_version
        insert_index = max(0, min(event.insert_after, len(self.qobuz_state.tracks)))
        self.qobuz_state.tracks[insert_index:insert_index] = event.tracks
        self.command_handler.reset_reconcile_dedup()
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_tracks_removed(self, event: QueueTracksRemovedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_REMOVED`` delta, then reconcile MA."""
        echo = self.consume_outbound_action(event.action_uuid)
        if echo is not None:
            # We originated this remove — mirror already updated optimistically.
            self.qobuz_state.queue_version = event.queue_version
            return
        self.qobuz_state.queue_version = event.queue_version
        removed_ids = set(event.queue_item_ids)
        self.qobuz_state.tracks = [
            track for track in self.qobuz_state.tracks if track.queue_item_id not in removed_ids
        ]
        self.command_handler.reset_reconcile_dedup()
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    def _absorb_cloud_qids_for_self_add(self, cloud_tracks: list[QueueTrackRef]) -> None:
        """
        Patch cloud-assigned queue_item_ids onto our placeholder mirror entries.

        When we sent ``CTRL_SRVR_QUEUE_ADD_TRACKS`` we registered placeholder
        ``QueueTrackRef(queue_item_id=0, track_id=X)`` entries on the mirror
        because the cloud is authoritative for queue_item_ids. The echo
        carries those assigned ids — patch them in-place by track_id.
        """
        for cloud_ref in cloud_tracks:
            for mirror_ref in self.qobuz_state.tracks:
                if mirror_ref.queue_item_id == 0 and mirror_ref.track_id == cloud_ref.track_id:
                    mirror_ref.queue_item_id = cloud_ref.queue_item_id
                    break

    async def handle_queue_tracks_reordered(self, event: QueueTracksReorderedEvent) -> None:
        """Apply a ``SRVR_CTRL_QUEUE_TRACKS_REORDERED`` delta, then reconcile MA."""
        echo = self.consume_outbound_action(event.action_uuid)
        if echo is not None:
            # We originated this reorder — mirror was already updated
            # optimistically when we emitted. Just absorb the cloud-assigned
            # new version and skip the reconciler.
            self.qobuz_state.queue_version = event.queue_version
            return
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
        self.command_handler.reset_reconcile_dedup()
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_queue_cleared(self, _event: QueueClearedEvent) -> None:
        """
        Apply a ``SRVR_CTRL_QUEUE_CLEARED`` notification, then reconcile MA.

        Empties the mirror; the reconciler then removes every MA item that
        isn't the currently-playing one. Audio continues uninterrupted.
        """
        echo = self.consume_outbound_action(_event.action_uuid)
        self.qobuz_state.queue_version = _event.queue_version
        self.qobuz_state.tracks = []
        if echo is not None:
            return  # MA already cleared; reconciler would be a no-op
        self.command_handler.reset_reconcile_dedup()
        await self.command_handler.schedule_reconcile_ma_to_mirror()

    async def handle_loop_mode(self, mode: LoopMode) -> None:
        """
        Apply a renderer ``SET_LOOP_MODE`` command to mirror + MA queue.

        Wraps the MA mutation in ``Origin.QOBUZ`` so the resulting
        ``QUEUE_UPDATED`` event doesn't echo back through
        ``_maybe_emit_modes_to_cloud`` as an MA-originated change. The
        mirror is also updated optimistically — by the time MA's event
        fires, both sides match and the differ would be a no-op anyway,
        but the origin guard belt-and-braces the case where MA's signal
        races our mirror update.
        """
        self.qobuz_state.loop_mode = mode
        repeat = _LOOP_TO_MA_REPEAT.get(mode)
        player_id = self.bridge.target_player_id()
        if repeat is None or player_id is None:
            return
        async with origin_scope(self, Origin.QOBUZ):
            self.bridge.set_repeat(player_id, repeat)

    async def handle_shuffle_mode(self, shuffle_on: bool) -> None:
        """Apply a renderer ``SET_SHUFFLE_MODE`` command to mirror + MA queue."""
        self.qobuz_state.shuffle_mode = shuffle_on
        player_id = self.bridge.target_player_id()
        if player_id is None:
            return
        async with origin_scope(self, Origin.QOBUZ):
            await self.bridge.set_shuffle(player_id, shuffle_on)

    async def handle_autoplay_mode(self, autoplay_on: bool) -> None:
        """Record a renderer ``SET_AUTOPLAY_MODE`` command in the mirror."""
        self.qobuz_state.autoplay_mode = autoplay_on

    async def set_volume(self, level: int) -> int:
        """Set MA player volume from Qobuz app."""
        player_id = self._require_target_player_id()
        volume = max(0, min(100, int(level)))
        player = self.bridge.get_player(player_id)
        ptype = getattr(getattr(player, "state", None), "type", None)
        before = getattr(player, "group_volume", None)
        await self.bridge.cmd_volume_set(player_id, volume)
        after = getattr(self.bridge.get_player(player_id), "group_volume", None)
        self.bridge.logger.debug(
            "Qobuz->MA set_volume: recv=%s applied=%s player=%s type=%s group_volume %s->%s",
            level,
            volume,
            player_id,
            ptype,
            before,
            after,
        )
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
            # Refresh the duration we report to the cloud. Without this we
            # keep reporting the old track's duration after a natural
            # advance, and the Qobuz app caps the scrub slider at the
            # previous track's length. ``QueueItem.duration`` is in
            # seconds; the heartbeat ships ms. A missing/zero duration
            # leaves the field at 0 (Qobuz handles unknown duration).
            ma_duration_s = getattr(queue.current_item, "duration", None)
            self.qobuz_state.duration_ms = int(ma_duration_s * 1000) if ma_duration_s else 0
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
