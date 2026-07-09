"""
Pure Qobuz Connect sync reducer.

``reduce(state, event)`` is the single owner of MA↔Qobuz sync decisions.
It is pure: no I/O, no MA imports, no websocket. The cloud ``queue_version``
is the logical clock; MA-origin edits are optimistic proposals that become
canonical truth only when the cloud echoes them with a version bump.
"""

from __future__ import annotations

import dataclasses

from .models import PlayingState, QueueTrackRef, QueueVersion
from .sync_types import (
    CanonicalState,
    CloudActiveRendererChanged,
    CloudAddRenderer,
    CloudAutoplaySet,
    CloudAutoplayTracksLoaded,
    CloudCleared,
    CloudLoadAck,
    CloudLoopSet,
    CloudMute,
    CloudQuality,
    CloudQueueError,
    CloudRemoveRenderer,
    CloudRendererStateUpdated,
    CloudSessionState,
    CloudSetActive,
    CloudSetState,
    CloudShuffleSet,
    CloudSnapshot,
    CloudStateRequest,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudVersionChanged,
    CloudVolume,
    CloudVolumeDelta,
    Connected,
    Disconnected,
    Effect,
    Event,
    MaModesChanged,
    MaPause,
    MaPlayTrack,
    MaQueueChanged,
    MaReleasePlayer,
    MaResume,
    MaResyncQueue,
    MaSeek,
    MaSetLoop,
    MaSetShuffleFlag,
    MaSetVolume,
    MaTransportChanged,
    MaVolumeChanged,
    Proposal,
    ProposalKind,
    ProposalTimeout,
    PushAdd,
    PushAutoplay,
    PushClear,
    PushLoad,
    PushLoop,
    PushMute,
    PushPlayerState,
    PushReorder,
    PushVolume,
    ReduceResult,
    ReportState,
)

# Events handled by the transport/session lane. CloudLoadAck also appears in
# _LIST_CONFIRM above; it lands here only when it isn't the echo of one of
# our own pending proposals (e.g. a queue load initiated elsewhere).
_TRANSPORT_INBOUND = (
    CloudSetState,
    CloudRendererStateUpdated,
    CloudStateRequest,
    CloudSetActive,
    CloudSessionState,
    CloudActiveRendererChanged,
    CloudAddRenderer,
    CloudRemoveRenderer,
    MaTransportChanged,
    Connected,
    Disconnected,
    CloudLoadAck,
)

# Position drift beyond this is treated as an explicit seek rather than a
# heartbeat/position-only update.
_SEEK_THRESHOLD_MS = 1500

# Events handled by the modes lane (loop/autoplay/shuffle flags).
_MODES_INBOUND = (
    CloudLoopSet,
    CloudShuffleSet,
    CloudAutoplaySet,
    MaModesChanged,
)

# Side-channel events (volume/mute/quality): ungated, fire-and-forget, no
# canonical state to track since CanonicalState carries no volume/mute/quality
# fields.
_SIDE_INBOUND = (
    CloudVolume,
    CloudVolumeDelta,
    CloudMute,
    CloudQuality,
    MaVolumeChanged,
)

_LIST_INBOUND = (
    CloudSnapshot,
    CloudVersionChanged,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudCleared,
    CloudAutoplayTracksLoaded,
)

# Cloud events that can be the echo of one of our own pending proposals.
_LIST_CONFIRM = (
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudCleared,
    CloudLoadAck,
)

ConfirmEvent = (
    CloudTracksAdded
    | CloudTracksInserted
    | CloudTracksRemoved
    | CloudTracksReordered
    | CloudCleared
    | CloudLoadAck
)


def reduce(state: CanonicalState, event: Event) -> ReduceResult:
    """Compute the next canonical state and the effects an event produces."""
    version = getattr(event, "version", None)
    if version is not None and _version_le(version, state.cloud_version):
        return ReduceResult(state, ())
    if isinstance(event, MaQueueChanged):
        return _reduce_ma_queue_changed(state, event)
    if isinstance(event, ProposalTimeout):
        return _reduce_proposal_timeout(state, event)
    if isinstance(event, CloudQueueError):
        proposal = _matching_proposal(state, event.action_uuid)
        if proposal is None:
            return ReduceResult(state, ())
        return _reject_proposal(state, event, proposal)
    if isinstance(event, _LIST_CONFIRM):
        proposal = _matching_proposal(state, event.action_uuid)
        if proposal is not None:
            return _confirm_proposal(state, event, proposal)
        # No matching proposal: a concurrent remote change, not our own echo.
        # Fall through to the ordinary app-origin inbound handling below.
    if isinstance(event, _LIST_INBOUND):
        return _reduce_list_inbound(state, event)
    if isinstance(event, _TRANSPORT_INBOUND):
        return _reduce_transport(state, event)
    if isinstance(event, _MODES_INBOUND):
        return _reduce_modes(state, event)
    if isinstance(event, _SIDE_INBOUND):
        return _reduce_side(state, event)
    # Defensive fallback: the lanes above exhaustively cover the Event union,
    # so mypy proves this unreachable. Kept so an unhandled event no-ops
    # rather than crashing; if the union later grows a case this misses, the
    # ignore becomes unused and mypy flags the gap.
    return ReduceResult(state, ())  # type: ignore[unreachable]


def _reduce_ma_queue_changed(state: CanonicalState, event: MaQueueChanged) -> ReduceResult:
    """Turn an MA-origin queue edit into a pending proposal + a cloud push effect."""
    proposal = _diff_ma_list(state, event)
    if proposal is None:
        return ReduceResult(state, ())
    new = dataclasses.replace(state, pending=(*state.pending, proposal))
    return ReduceResult(new, (_push_for(proposal),))


def _reduce_proposal_timeout(state: CanonicalState, event: ProposalTimeout) -> ReduceResult:
    """Drop a proposal that never got a cloud echo and converge MA to canonical truth."""
    proposal = _matching_proposal(state, event.action_uuid)
    if proposal is None:
        return ReduceResult(state, ())
    pending = tuple(p for p in state.pending if p is not proposal)
    new = dataclasses.replace(state, pending=pending)
    return _with_resync(new)


def _reduce_list_inbound(state: CanonicalState, event: Event) -> ReduceResult:
    if isinstance(event, CloudSnapshot):
        tracks = tuple(event.tracks)
        current_id = _current_from_pointer(tracks, event.track_index)
        new = dataclasses.replace(
            state,
            cloud_version=event.version,
            tracks=tracks,
            autoplay_tracks=tuple(event.autoplay_tracks),
            current_id=current_id,
        )
        return _with_resync(new)
    if isinstance(event, CloudVersionChanged):
        return ReduceResult(dataclasses.replace(state, cloud_version=event.version), ())
    if isinstance(event, CloudCleared):
        new = dataclasses.replace(state, cloud_version=event.version, tracks=(), current_id=None)
        return _with_resync(new)
    if isinstance(event, CloudTracksAdded):
        tracks = state.tracks + tuple(event.tracks)
        return _with_resync(dataclasses.replace(state, cloud_version=event.version, tracks=tracks))
    if isinstance(event, CloudTracksInserted):
        idx = max(0, min(event.insert_after, len(state.tracks)))
        tracks = state.tracks[:idx] + tuple(event.tracks) + state.tracks[idx:]
        return _with_resync(dataclasses.replace(state, cloud_version=event.version, tracks=tracks))
    if isinstance(event, CloudTracksRemoved):
        removed = set(event.queue_item_ids)
        tracks = tuple(t for t in state.tracks if t.queue_item_id not in removed)
        # Current-change on removal only updates the anchor here; MaPlayTrack
        # (restarting audio) is wired in the transport lane (Task 4).
        current_id = _successor_if_removed(state, removed)
        return _with_resync(
            dataclasses.replace(
                state, cloud_version=event.version, tracks=tracks, current_id=current_id
            ),
        )
    if isinstance(event, CloudTracksReordered):
        tracks = _reorder(state.tracks, event.queue_item_ids, event.insert_after)
        return _with_resync(dataclasses.replace(state, cloud_version=event.version, tracks=tracks))
    if isinstance(event, CloudAutoplayTracksLoaded):
        return _with_resync(
            dataclasses.replace(
                state, cloud_version=event.version, autoplay_tracks=tuple(event.tracks)
            ),
        )
    return ReduceResult(state, ())


def _reduce_transport(state: CanonicalState, event: Event) -> ReduceResult:
    """Route a transport/session-lane event to its handler."""
    if isinstance(event, CloudSetActive):
        return _takeover(state) if event.active else _deactivate(state)
    if isinstance(event, CloudSetState):
        return _apply_transport(
            state,
            playing=event.playing,
            position_ms=event.position_ms,
            current_id=event.current_ref.queue_item_id if event.current_ref else None,
        )
    if isinstance(event, CloudRendererStateUpdated):
        # Another renderer's live state feeds canonical truth only while we
        # aren't active; once we're active it's our own echo or irrelevant.
        if state.active:
            return ReduceResult(state, ())
        current_id = state.current_id
        if event.current_index is not None and state.tracks:
            idx = max(0, min(event.current_index, len(state.tracks) - 1))
            current_id = state.tracks[idx].queue_item_id
        new = dataclasses.replace(
            state,
            current_id=current_id,
            playing=event.playing or state.playing,
            position_ms=event.position_ms if event.position_ms is not None else state.position_ms,
        )
        return ReduceResult(new, ())
    if isinstance(event, CloudStateRequest):
        return ReduceResult(state, (ReportState(),))
    if isinstance(event, MaTransportChanged):
        return _ma_transport(state, event)
    if isinstance(event, CloudSessionState):
        current_id = _current_from_pointer(state.tracks, event.track_index)
        new = dataclasses.replace(state, cloud_version=event.version, current_id=current_id)
        return ReduceResult(new, ())
    if isinstance(event, CloudActiveRendererChanged):
        return ReduceResult(dataclasses.replace(state, active_rid=event.renderer_id), ())
    if isinstance(event, CloudAddRenderer):
        # Own-renderer matching against device_uuid needs uuid-comparison
        # context the pure reducer doesn't hold; the coordinator resolves
        # own-ness and drives own_rid through the events the reducer does
        # understand (e.g. CloudActiveRendererChanged).
        return ReduceResult(state, ())
    if isinstance(event, CloudRemoveRenderer):
        return _remove_renderer(state, event)
    if isinstance(event, Connected):
        return ReduceResult(state, ())
    if isinstance(event, Disconnected):
        return ReduceResult(dataclasses.replace(state, active=False, pending=()), ())
    if isinstance(event, CloudLoadAck):
        current_id = _current_from_load_ack(event)
        new = dataclasses.replace(state, cloud_version=event.version, current_id=current_id)
        return ReduceResult(new, (ReportState(),))
    return ReduceResult(state, ())


def _reduce_modes(state: CanonicalState, event: Event) -> ReduceResult:
    """Route a modes-lane event (loop/autoplay/shuffle) to its handler."""
    if isinstance(event, CloudLoopSet):
        new = dataclasses.replace(state, loop=event.loop)
        return ReduceResult(new, (MaSetLoop(event.loop),))
    if isinstance(event, CloudAutoplaySet):
        # No MaSetAutoplay effect exists: autoplay is a cloud-side flag only.
        return ReduceResult(dataclasses.replace(state, autoplay=event.autoplay), ())
    if isinstance(event, CloudShuffleSet):
        # The reorder itself rides on the list lane's snapshot/reorder events;
        # this only flips MA's shuffle flag.
        return ReduceResult(state, (MaSetShuffleFlag(event.shuffle),))
    if isinstance(event, MaModesChanged):
        return _ma_modes_changed(state, event)
    return ReduceResult(state, ())


def _reduce_side(state: CanonicalState, event: Event) -> ReduceResult:
    """
    Route a side-channel event (volume/mute/quality) to its handler.

    Side channels are ungated and fire-and-forget: CanonicalState carries no
    volume/mute/quality fields, so these never touch canonical truth.
    """
    if isinstance(event, CloudVolume):
        return ReduceResult(state, (MaSetVolume(event.volume),))
    if isinstance(event, CloudVolumeDelta | CloudMute | CloudQuality):
        # No MA effect exists for a relative delta, mute, or quality change.
        return ReduceResult(state, ())
    if isinstance(event, MaVolumeChanged):
        return ReduceResult(state, (PushVolume(event.volume), PushMute(event.muted)))
    return ReduceResult(state, ())


def _ma_modes_changed(state: CanonicalState, event: MaModesChanged) -> ReduceResult:
    """Diff MA's reported loop/autoplay against canonical; push only what changed."""
    new = state
    effects: list[Effect] = []
    if event.loop != state.loop:
        new = dataclasses.replace(new, loop=event.loop)
        effects.append(PushLoop(event.loop))
    if event.autoplay != state.autoplay:
        new = dataclasses.replace(new, autoplay=event.autoplay)
        effects.append(PushAutoplay(event.autoplay))
    return ReduceResult(new, tuple(effects))


def _apply_transport(
    state: CanonicalState,
    *,
    playing: PlayingState | None,
    position_ms: int | None,
    current_id: int | None,
) -> ReduceResult:
    """
    Apply a cloud transport command under the audio-never-interrupted rule.

    Exactly one branch fires, in strict priority order: a changed current
    track (the only audio-restarting case), else a play/pause toggle, else
    an explicit seek, else a bare position/heartbeat update with no effect.

    :param state: Canonical state before the transport command.
    :param playing: Reported playing state, or None if not carried.
    :param position_ms: Reported position, or None if not carried.
    :param current_id: Reported current queue-item id, or None if not carried.
    """
    if current_id is not None and current_id != state.current_id:
        new = dataclasses.replace(
            state,
            current_id=current_id,
            playing=playing or state.playing,
            position_ms=position_ms or 0,
        )
        return ReduceResult(new, (MaPlayTrack(track_id=current_id, position_ms=position_ms or 0),))
    if playing is not None and playing != state.playing:
        new = dataclasses.replace(state, playing=playing)
        effect = MaPause() if playing is PlayingState.PAUSED else MaResume()
        return ReduceResult(new, (effect,))
    if position_ms is not None and abs(position_ms - state.position_ms) > _SEEK_THRESHOLD_MS:
        new = dataclasses.replace(state, position_ms=position_ms)
        return ReduceResult(new, (MaSeek(position_ms),))
    # Position/heartbeat only: no MA effect, ever.
    return ReduceResult(
        dataclasses.replace(
            state, position_ms=position_ms if position_ms is not None else state.position_ms
        ),
        (),
    )


def _takeover(state: CanonicalState) -> ReduceResult:
    """Activate this renderer and adopt canonical current, playing it once if it's live."""
    active = dataclasses.replace(state, active=True)
    if state.current_id is not None and state.playing is PlayingState.PLAYING:
        return ReduceResult(
            active, (MaPlayTrack(track_id=state.current_id, position_ms=state.position_ms),)
        )
    return ReduceResult(active, (ReportState(),))


def _deactivate(state: CanonicalState) -> ReduceResult:
    """Give up the renderer role and release the MA player."""
    return ReduceResult(dataclasses.replace(state, active=False), (MaReleasePlayer(),))


def _ma_transport(state: CanonicalState, event: MaTransportChanged) -> ReduceResult:
    """Fold MA's own transport report into canonical state and push it to cloud."""
    current_id = event.current_track_id if event.current_track_id is not None else state.current_id
    new = dataclasses.replace(
        state, playing=event.playing, current_id=current_id, position_ms=event.position_ms
    )
    effect = PushPlayerState(
        playing=event.playing,
        position_ms=event.position_ms,
        queue_version=state.cloud_version,
        queue_item_id=current_id,
    )
    return ReduceResult(new, (effect,))


def _remove_renderer(state: CanonicalState, event: CloudRemoveRenderer) -> ReduceResult:
    """Clear own/active renderer bookkeeping when the matching renderer leaves."""
    active_rid = None if state.active_rid == event.renderer_id else state.active_rid
    own_rid = None if state.own_rid == event.renderer_id else state.own_rid
    if active_rid == state.active_rid and own_rid == state.own_rid:
        return ReduceResult(state, ())
    new = dataclasses.replace(state, active_rid=active_rid, own_rid=own_rid)
    return ReduceResult(new, ())


def _current_from_load_ack(event: CloudLoadAck) -> int | None:
    """Resolve the new current queue-item id from a load ack's clamped position."""
    if not event.tracks:
        return None
    idx = max(0, min(event.queue_position, len(event.tracks) - 1))
    return event.tracks[idx].queue_item_id


def _with_resync(new: CanonicalState) -> ReduceResult:
    """Emit MaResyncQueue only when active; list-lane never restarts audio."""
    if not new.active:
        return ReduceResult(new, ())
    effects: tuple[Effect, ...] = (
        MaResyncQueue(
            track_ids=tuple(t.queue_item_id for t in new.tracks), current_track_id=new.current_id
        ),
    )
    return ReduceResult(new, effects)


def _current_from_pointer(tracks: tuple[QueueTrackRef, ...], track_index: int) -> int | None:
    if not tracks:
        return None
    idx = max(0, min(track_index - 1, len(tracks) - 1))
    return tracks[idx].queue_item_id


def _successor_if_removed(state: CanonicalState, removed: set[int]) -> int | None:
    if state.current_id not in removed:
        return state.current_id
    old_ids = [t.queue_item_id for t in state.tracks]
    if state.current_id not in old_ids:
        return None
    start = old_ids.index(state.current_id)
    for qid in old_ids[start + 1 :]:
        if qid not in removed:
            return qid
    return None


def _reorder(
    tracks: tuple[QueueTrackRef, ...], moving_ids: tuple[int, ...], insert_after: int
) -> tuple[QueueTrackRef, ...]:
    moving_set = set(moving_ids)
    by_id = {t.queue_item_id: t for t in tracks}
    moving = [by_id[i] for i in moving_ids if i in by_id]
    remaining = [t for t in tracks if t.queue_item_id not in moving_set]
    target = max(0, min(insert_after, len(remaining)))
    return tuple(remaining[:target] + moving + remaining[target:])


def _version_le(a: QueueVersion, b: QueueVersion) -> bool:
    return (a.major, a.minor) <= (b.major, b.minor)


def _matching_proposal(state: CanonicalState, action_uuid: bytes) -> Proposal | None:
    """Find the pending proposal a cloud event is an echo of, if any."""
    for p in state.pending:
        if p.action_uuid == action_uuid:
            return p
    return None


def _confirm_proposal(
    state: CanonicalState, event: ConfirmEvent, proposal: Proposal
) -> ReduceResult:
    """Fold a confirmed proposal into truth; MA already reflects this state."""
    # Trust the proposal's resolved target rather than re-deriving the delta
    # from the echo event — this stays correct across every proposal kind.
    # Where the echo itself carries real track ids (adds/inserts/load-acks
    # assign them cloud-side), prefer those over the placeholder fallback.
    echoed_track_ids = {t.queue_item_id: t.track_id for t in getattr(event, "tracks", ())}
    tracks = tuple(
        QueueTrackRef(queue_item_id=i, track_id=echoed_track_ids.get(i, _track_id_for(state, i)))
        for i in proposal.target_track_ids
    )
    pending = tuple(p for p in state.pending if p is not proposal)
    new = dataclasses.replace(
        state,
        cloud_version=event.version,
        tracks=tracks,
        current_id=proposal.current_track_id,
        pending=pending,
    )
    return ReduceResult(new, ())  # MA already has it


def _reject_proposal(
    state: CanonicalState, event: CloudQueueError, proposal: Proposal
) -> ReduceResult:
    """Rebase a rejected proposal once against the newer version, else converge MA."""
    absorbed = dataclasses.replace(state, cloud_version=event.version)
    if proposal.retries_left > 0:
        rebased = dataclasses.replace(
            proposal,
            base_version=event.version,
            retries_left=proposal.retries_left - 1,
            target_track_ids=_rebase_target(absorbed, proposal),
        )
        pending = tuple(rebased if p is proposal else p for p in absorbed.pending)
        new = dataclasses.replace(absorbed, pending=pending)
        return ReduceResult(new, (_push_for(rebased),))
    pending = tuple(p for p in absorbed.pending if p is not proposal)
    new = dataclasses.replace(absorbed, pending=pending)
    return _with_resync(new)  # converge MA to cloud truth


def _rebase_target(state: CanonicalState, proposal: Proposal) -> tuple[int, ...]:
    """
    Recompute a rejected proposal's target against current canonical tracks.

    Only ADD's target is unambiguous to re-derive after canonical drift
    (current canonical + whichever proposed ids aren't already canonical,
    in their proposed order). LOAD/CLEAR/REORDER targets are kept as-is:
    their intended delta can't be reconstructed from target_track_ids alone
    once canonical has moved.
    """
    if proposal.kind is not ProposalKind.ADD:
        return proposal.target_track_ids
    canonical_ids = tuple(t.queue_item_id for t in state.tracks)
    tail = tuple(i for i in proposal.target_track_ids if i not in canonical_ids)
    return canonical_ids + tail


def _diff_ma_list(state: CanonicalState, event: MaQueueChanged) -> Proposal | None:
    """
    Detect an MA-origin structural change and turn it into a proposal.

    Canonical ``tracks`` is filtered to ``event.resolvable`` before
    comparison: MA legitimately cannot materialize region-locked/404 ids, so
    a resolvable-filtered subsequence is not a user-driven removal. A pure
    current-pointer move (list unchanged) is not a list change at all — it is
    left for the transport lane, which is what prevents the 238-track
    re-push bug (a natural track-advance being misread as a fresh queue
    load). Any pending proposal that already targets this exact list is
    suppressed so we never re-push our own unconfirmed optimistic edit.
    """
    canonical_ids = tuple(
        t.queue_item_id for t in state.tracks if t.queue_item_id in event.resolvable
    )
    if canonical_ids == event.track_ids:
        return None
    if any(p.target_track_ids == event.track_ids for p in state.pending):
        return None
    if not event.track_ids:
        kind = ProposalKind.CLEAR
    elif canonical_ids and event.track_ids[: len(canonical_ids)] == canonical_ids:
        kind = ProposalKind.ADD
    elif set(event.track_ids) == set(canonical_ids):
        kind = ProposalKind.REORDER
    else:
        kind = ProposalKind.LOAD
    return Proposal(
        action_uuid=event.action_uuid,
        base_version=state.cloud_version,
        kind=kind,
        target_track_ids=event.track_ids,
        current_track_id=event.current_track_id,
    )


def _push_for(proposal: Proposal) -> Effect:
    """Map a pending proposal to the cloud push command that realizes it."""
    if proposal.kind is ProposalKind.CLEAR:
        return PushClear(action_uuid=proposal.action_uuid, base_version=proposal.base_version)
    if proposal.kind is ProposalKind.ADD:
        # NOTE: pushes the full resolved target as the "add" payload; Task 6's
        # richer diff supplies the true appended-only delta.
        return PushAdd(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            track_ids=proposal.target_track_ids,
        )
    if proposal.kind is ProposalKind.LOAD:
        current_index = (
            proposal.target_track_ids.index(proposal.current_track_id)
            if proposal.current_track_id in proposal.target_track_ids
            else 0
        )
        return PushLoad(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            track_ids=proposal.target_track_ids,
            current_index=current_index,
            context_uuid=b"\x00" * 16,
        )
    if proposal.kind is ProposalKind.REORDER:
        # Encode any permutation as "move every item, in the target order,
        # to the front" — mirrors _reorder()'s semantics exactly since the
        # remaining list is empty once every id is moved.
        return PushReorder(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            queue_item_ids=proposal.target_track_ids,
            insert_after=0,
        )
    raise NotImplementedError(f"push mapping for {proposal.kind} lands in a later task")


def _track_id_for(state: CanonicalState, queue_item_id: int) -> str:
    """Look up a queue item's provider track id from truth, else a placeholder."""
    for t in state.tracks:
        if t.queue_item_id == queue_item_id:
            return t.track_id
    return str(queue_item_id)
