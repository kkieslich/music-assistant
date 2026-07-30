"""
Pure Qobuz Connect sync reducer.

``reduce(state, event)`` is the single owner of MA↔Qobuz sync decisions.
It is pure: no I/O, no MA imports, no websocket. The cloud ``queue_version``
is the logical clock; MA-origin edits are optimistic proposals that become
canonical truth only when the cloud echoes them with a version bump.
"""

from __future__ import annotations

import dataclasses

from .models import BufferState, PlayingState, QueueTrackRef, QueueVersion
from .sync_types import (
    AskSnapshot,
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
    Disconnected,
    Effect,
    Event,
    MaAdjustVolume,
    MaModesChanged,
    MaPause,
    MaPlayTrack,
    MaQueueChanged,
    MaReleasePlayer,
    MaResume,
    MaResyncQueue,
    MaSeek,
    MaSetLoop,
    MaSetMuted,
    MaSetShuffleFlag,
    MaSetVolume,
    MaTransportChanged,
    MaVolumeChanged,
    Proposal,
    ProposalKind,
    ProposalTimeout,
    PushClear,
    PushInsert,
    PushLoad,
    PushLoop,
    PushMute,
    PushRemove,
    PushReorder,
    PushSetActive,
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
    Disconnected,
    CloudLoadAck,
)

# Position drift beyond this is treated as an explicit seek rather than a
# heartbeat/position-only update.
_SEEK_THRESHOLD_MS = 1500

# After a cloud-commanded seek/track-change, MA's reported position is
# considered caught up once it lands within this of the live commanded
# position.
_POSITION_CONVERGE_MS = 2000

# MA can briefly report a new queue item near zero before its corrected
# elapsed time falls back to the previous stream's position. Keep the
# settling guard through that transient handover.
_POSITION_SETTLE_GRACE_MS = 1000

# Hard cap on how long the transport lane holds the commanded target while
# MA catches up. Bounds suppression so a genuinely divergent MA can never
# freeze the reported position indefinitely.
#
# follow-up: now that BUFFERING is reported on the wire (Task 4), the app
# freezes its own interpolation during a transition, so this settling window
# may be shrinkable once BUFFERING is validated live. Do not shrink it here
# until then — the settling guard still protects canonical state from
# adopting MA's stale positions independently of what the app displays.
_POSITION_SETTLE_TIMEOUT_MS = 5000

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

# ---- version gate: per-lane opt-in --------------------------------------
#
# Only the event types listed below are dropped when their queue_version is
# stale. This is opt-in by construction: a future event type that happens to
# carry a `version` field is NOT gated unless it is explicitly added here, so
# the next transport/control event is safe by default.

# A CloudSnapshot is the authoritative full-queue answer to our own
# AskSnapshot, so an *equal* version still applies (CloudSessionState /
# CloudVersionChanged pre-advance cloud_version to the version the snapshot
# then reports); reject only if the snapshot is *strictly* older.
_GATED_LT = (CloudSnapshot,)

# The queue-list lane plus session-state: an event at a version <= the one we
# already hold is a stale replay and must be dropped. CloudSessionState stays
# gated here — the reconnect path still works because Disconnected resets
# cloud_version, so a recreated session's lower version is never stale-gated.
_GATED_LE = (
    CloudVersionChanged,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudCleared,
    CloudAutoplayTracksLoaded,
    CloudLoadAck,
    CloudSessionState,
)

# Deliberately absent from both tuples, even though each carries a `version`:
#
# - CloudQueueError: a rejection must always reach its matching proposal (the
#   coordinator falls back to version=cloud_version when the wire error carries
#   none, which `<=` would always swallow).
# - CloudSetState: the live "which track is playing / play / pause / skip"
#   command. Its queue_version can lag our cloud_version (the app advances the
#   queue while we hold an older snapshot), but the transport intent is always
#   current and orthogonal to queue-list staleness. Gating it froze MA on the
#   wrong track and made phone skips no-ops (live 2026-07-09). It is idempotent
#   (``_apply_transport`` only acts on a real change) and also re-asks for a
#   fresh snapshot when its version advances.


def reduce(state: CanonicalState, event: Event) -> ReduceResult:
    """Compute the next canonical state and the effects an event produces."""
    if isinstance(event, _GATED_LT):
        if _version_lt(event.version, state.cloud_version):
            return ReduceResult(state, ())
    elif isinstance(event, _GATED_LE):
        if _version_le(event.version, state.cloud_version):
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
    # Translate against the PRE-append state: the proposal's own tracks
    # aren't in canonical yet, which is exactly why an ADD's tail comes out
    # as only the newly appended ids.
    return ReduceResult(new, (_emit_push(state, proposal),))


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
        # When we are the ACTIVE renderer, WE own "current" — the cloud follows
        # our reported playback. A snapshot pulled after a queue edit (reorder)
        # carries the cloud's own trackIndex, which lags/shifts relative to what
        # we are actually playing; adopting it flipped canonical current to a
        # track we were not playing, then diverged the app and MA on the next
        # skip (live 2026-07-10 bidirectional-edit drift). Keep our current as
        # long as it is still in the queue; only fall back to the snapshot
        # pointer when inactive or when our track is gone.
        keep_current = (
            state.active
            and state.current_id is not None
            and any(_safe_qid(t) == state.current_id for t in tracks)
        )
        current_id = (
            state.current_id if keep_current else _current_from_pointer(tracks, event.track_index)
        )
        # We now hold this version's queue: record it as asked-for so an
        # immediately-following notification at the same version doesn't
        # trigger a redundant re-ask.
        new = dataclasses.replace(
            state,
            cloud_version=event.version,
            tracks=tracks,
            autoplay_tracks=tuple(event.autoplay_tracks),
            current_id=current_id,
            last_asked_version=event.version,
        )
        return _with_resync(new)
    if isinstance(event, CloudVersionChanged):
        new = dataclasses.replace(state, cloud_version=event.version)
        asked, ask_effects = _maybe_ask_snapshot(new, event.version)
        return ReduceResult(asked, ask_effects)
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
        if event.active:
            return ReduceResult(state, ()) if state.active else _takeover(state, event.now_ms)
        return _deactivate(state)
    if isinstance(event, CloudSetState):
        return _reduce_set_state(state, event)
    if isinstance(event, CloudRendererStateUpdated):
        # Another renderer's live state feeds canonical truth only while we
        # aren't active; once we're active it's our own echo or irrelevant.
        if state.active:
            return ReduceResult(state, ())
        current_id = state.current_id
        if event.current_index is not None and state.tracks:
            idx = max(0, min(event.current_index, len(state.tracks) - 1))
            current_id = _qid(state.tracks[idx])
        new = dataclasses.replace(
            state,
            current_id=current_id,
            playing=event.playing or state.playing,
            position_ms=event.position_ms if event.position_ms is not None else state.position_ms,
            position_anchor_ms=event.now_ms,
        )
        return ReduceResult(new, ())
    if isinstance(event, CloudStateRequest):
        return ReduceResult(state, (ReportState(),))
    if isinstance(event, MaTransportChanged):
        return _ma_transport(state, event)
    if isinstance(event, CloudSessionState):
        current_id = _current_from_pointer(state.tracks, event.track_index)
        new = dataclasses.replace(state, cloud_version=event.version, current_id=current_id)
        # The primary handoff path: the cloud only pushes the queue snapshot
        # in response to an explicit ask, so connecting mid-session must ask.
        asked, ask_effects = _maybe_ask_snapshot(new, event.version)
        return ReduceResult(asked, ask_effects)
    if isinstance(event, CloudActiveRendererChanged):
        return _active_renderer_changed(state, event)
    if isinstance(event, CloudAddRenderer):
        # Own-renderer matching against device_uuid needs uuid-comparison
        # context the pure reducer doesn't hold; the coordinator resolves
        # own-ness and carries the verdict on event.is_own.
        if not event.is_own or state.own_rid == event.renderer_id:
            return ReduceResult(state, ())
        return ReduceResult(dataclasses.replace(state, own_rid=event.renderer_id), ())
    if isinstance(event, CloudRemoveRenderer):
        return _remove_renderer(state, event)
    if isinstance(event, Disconnected):
        # Reset the ask-dedup so a reconnect re-seeds a fresh snapshot rather
        # than trusting a possibly-stale last_asked_version. cloud_version is
        # reset too: queue_version numbering is session-scoped, so a recreated
        # cloud session can legitimately restart LOWER — holding the old
        # version would stale-gate every post-reconnect event and leave the
        # provider deaf until restart.
        new = dataclasses.replace(
            state,
            active=False,
            activation_requested=False,
            release_pending=False,
            own_rid=None,
            active_rid=None,
            pending=(),
            last_asked_version=QueueVersion(),
            cloud_version=QueueVersion(),
        )
        return ReduceResult(new, ())
    if isinstance(event, CloudLoadAck):
        return _reduce_load_ack(state, event)
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
    if isinstance(event, CloudVolumeDelta):
        return ReduceResult(state, (MaAdjustVolume(event.delta),))
    if isinstance(event, CloudMute):
        return ReduceResult(state, (MaSetMuted(event.muted),))
    if isinstance(event, CloudQuality):
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
        # Track MA's autoplay flag in canonical, but there is no cloud push:
        # the Qobuz session protocol exposes no renderer->cloud autoplay verb.
        new = dataclasses.replace(new, autoplay=event.autoplay)
    return ReduceResult(new, tuple(effects))


def _reduce_set_state(state: CanonicalState, event: CloudSetState) -> ReduceResult:
    """Apply a CloudSetState transport command, then ask for a snapshot if it carries a version."""
    result = _apply_transport(
        state,
        now_ms=event.now_ms,
        playing=event.playing,
        position_ms=event.position_ms,
        current_id=_qid(event.current_ref) if event.current_ref else None,
    )
    if event.version is None:
        return result
    # An app play with a version we don't yet hold must trigger a snapshot
    # ask, deduped against redundant asks at the same version.
    asked, ask_effects = _maybe_ask_snapshot(result.state, event.version)
    return ReduceResult(asked, result.effects + ask_effects)


def _report_on_buffer_change(
    old: BufferState, new: BufferState, effects: tuple[Effect, ...]
) -> tuple[Effect, ...]:
    """
    Append a ``ReportState`` when ``buffer_state`` transitions.

    A BUFFERING/OK transition must reach the app immediately (freeze/unfreeze
    its position interpolation) rather than waiting for the next heartbeat.
    """
    return (*effects, ReportState()) if old != new else effects


def _apply_transport(
    state: CanonicalState,
    *,
    now_ms: int,
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
    :param now_ms: The event's timestamp, stamped as the anchor whenever
        position_ms is (re)captured so the reporter can interpolate.
    :param playing: Reported playing state, or None if not carried.
    :param position_ms: Reported position, or None if not carried.
    :param current_id: Reported current Qobuz track id, or None if not carried.
    """
    if current_id is not None and current_id != state.current_id:
        new = dataclasses.replace(
            state,
            current_id=current_id,
            playing=playing or state.playing,
            position_ms=position_ms or 0,
            position_anchor_ms=now_ms,
            settling_position=True,
            buffer_state=BufferState.BUFFERING,
        )
        effects = _report_on_buffer_change(
            state.buffer_state,
            new.buffer_state,
            (MaPlayTrack(track_id=current_id, position_ms=position_ms or 0),),
        )
        return ReduceResult(new, effects)
    if playing is not None and playing != state.playing:
        # Capture the live interpolated position when toggling play/pause. During
        # PLAYING the reporter interpolates position from (position_ms, anchor),
        # so position_ms is a stale base; if we pause without capturing the
        # elapsed time, the frozen report snaps the slider back to that base
        # (live 2026-07-11 "slider moves back"). Freeze at base + elapsed and
        # re-anchor; on resume this simply continues from the paused position.
        live_position_ms = state.position_ms
        if state.playing is PlayingState.PLAYING:
            live_position_ms = state.position_ms + max(0, now_ms - state.position_anchor_ms)
        # A resume restarts the flow stream, so MA's position lags on the way
        # back to PLAYING (same as a seek/track-change); settle it and mark
        # BUFFERING. A pause has no such lag — its position was just frozen
        # above and buffer clears back to OK.
        resuming = playing is PlayingState.PLAYING
        buffer_state = BufferState.BUFFERING if resuming else BufferState.OK
        new = dataclasses.replace(
            state,
            playing=playing,
            position_ms=live_position_ms,
            position_anchor_ms=now_ms,
            settling_position=resuming,
            buffer_state=buffer_state,
        )
        effect = MaPause() if playing is PlayingState.PAUSED else MaResume()
        effects = _report_on_buffer_change(state.buffer_state, buffer_state, (effect,))
        return ReduceResult(new, effects)
    if position_ms is not None and abs(position_ms - state.position_ms) > _SEEK_THRESHOLD_MS:
        new = dataclasses.replace(
            state,
            position_ms=position_ms,
            position_anchor_ms=now_ms,
            settling_position=True,
            buffer_state=BufferState.BUFFERING,
        )
        effects = _report_on_buffer_change(
            state.buffer_state, new.buffer_state, (MaSeek(position_ms),)
        )
        return ReduceResult(new, effects)
    # Position/heartbeat only: no MA effect, ever. Only re-anchor when a
    # position was actually reported — a bare heartbeat with no position
    # carries no fresh interpolation point.
    if position_ms is not None:
        return ReduceResult(
            dataclasses.replace(state, position_ms=position_ms, position_anchor_ms=now_ms), ()
        )
    return ReduceResult(state, ())


def _takeover(state: CanonicalState, now_ms: int) -> ReduceResult:
    """Activate this renderer and adopt canonical current, playing it once if it's live."""
    active = dataclasses.replace(
        state,
        active=True,
        activation_requested=False,
        release_pending=False,
    )
    if state.current_id is not None and state.playing is PlayingState.PLAYING:
        # A handoff from phone/web hands us a position anchored at
        # position_anchor_ms; while PLAYING it has advanced since. Resume at the
        # LIVE position (base + elapsed) or the takeover lands several seconds
        # behind. Mirrors _apply_transport's pause-branch interpolation.
        live_position_ms = state.position_ms + max(0, now_ms - state.position_anchor_ms)
        # Resync first so MA's queue is populated (a prior deactivate may have
        # released/cleared it) before the play fast-paths off that queue.
        resync = MaResyncQueue(
            track_ids=tuple(qid for t in state.tracks if (qid := _safe_qid(t)) is not None),
            current_track_id=state.current_id,
        )
        play = MaPlayTrack(track_id=state.current_id, position_ms=live_position_ms)
        # Playing the adopted current restarts MA audio, so its position lags;
        # settle and mark BUFFERING until MA reports the handed-over position.
        # Re-anchor the stored position to the same live value.
        new = dataclasses.replace(
            active,
            position_ms=live_position_ms,
            position_anchor_ms=now_ms,
            settling_position=True,
            buffer_state=BufferState.BUFFERING,
        )
        effects = _report_on_buffer_change(state.buffer_state, new.buffer_state, (resync, play))
        return ReduceResult(new, effects)
    return ReduceResult(active, (ReportState(),))


def _deactivate(state: CanonicalState) -> ReduceResult:
    """Give up the renderer role and release the MA player."""
    if not state.active:
        return ReduceResult(state, ())
    return ReduceResult(
        dataclasses.replace(
            state,
            active=False,
            activation_requested=False,
            release_pending=True,
        ),
        (MaReleasePlayer(),),
    )


def _ma_transport(state: CanonicalState, event: MaTransportChanged) -> ReduceResult:
    """Fold MA's own transport into canonical and REPORT it as a renderer (not a command)."""
    if event.current_item_unmappable:
        if not state.active:
            return ReduceResult(state, ())
        return ReduceResult(
            dataclasses.replace(
                state,
                current_id=None,
                playing=PlayingState.STOPPED,
                position_ms=0,
                position_anchor_ms=event.now_ms,
                settling_position=False,
                buffer_state=BufferState.OK,
                active=False,
                activation_requested=False,
                release_pending=True,
            ),
            (MaReleasePlayer(),),
        )
    playing = event.playing
    position_ms = event.position_ms
    position_anchor_ms = event.now_ms
    settling = False
    # Pause and resume both stop/restart the underlying MA flow stream, so the
    # MA player briefly transitions to IDLE (-> STOPPED) around a pause, a
    # resume, and a track change. The renderer's LOGICAL state across those is
    # still PAUSED or PLAYING — it is never "STOPPED" while it holds a current
    # track. Reporting the transient STOPPED (with the stopped stream's overshot
    # position) made the app flap play->pause->play and jump the slider by a few
    # seconds (live 2026-07-11). While active and holding a current track, a
    # STOPPED transition is a transport-transition side-effect: keep the current
    # intent and freeze the position. A genuine stop (empty queue, current_id
    # None) still reports STOPPED.
    if (
        state.active
        and event.playing is PlayingState.STOPPED
        and state.playing in (PlayingState.PAUSED, PlayingState.PLAYING)
        and state.current_id is not None
    ):
        playing = state.playing
        position_ms = state.position_ms
        position_anchor_ms = state.position_anchor_ms
        # A transient stop is part of the very transition we're settling; keep
        # settling so the stale PLAYING report that follows is still guarded.
        settling = state.settling_position
    # Settling guard: right after a cloud-commanded seek/track-change, MA's
    # corrected_elapsed_time still reflects the pre-transition track/position
    # for ~1s. Adopting it dragged the app's slider backward (seek) or showed
    # the new track at the old position (skip) — the self-healing desync the
    # user hit on 2026-07-11. Hold the commanded target until MA converges to
    # it (or the settle window expires as a safety cap).
    elif state.settling_position and state.active and event.playing is PlayingState.PLAYING:
        settle_elapsed_ms = max(0, event.now_ms - state.position_anchor_ms)
        expected_position_ms = state.position_ms + settle_elapsed_ms
        converged = (
            settle_elapsed_ms >= _POSITION_SETTLE_GRACE_MS
            and event.current_track_id == state.current_id
            and abs(event.position_ms - expected_position_ms) <= _POSITION_CONVERGE_MS
        )
        timed_out = settle_elapsed_ms > _POSITION_SETTLE_TIMEOUT_MS
        if not converged and not timed_out:
            return ReduceResult(state, (ReportState(),))
    current_id = event.current_track_id if event.current_track_id is not None else state.current_id
    release_pending = state.release_pending
    if not state.active and event.playing is PlayingState.STOPPED:
        release_pending = False
    # Buffer tracks settling: BUFFERING while a transition is still settling
    # (transient stop mid-transition), back to OK once MA has converged/timed
    # out. This ReduceResult already emits ReportState, so the transition
    # reaches the app immediately.
    new = dataclasses.replace(
        state,
        playing=playing,
        current_id=current_id,
        position_ms=position_ms,
        position_anchor_ms=position_anchor_ms,
        settling_position=settling,
        buffer_state=BufferState.BUFFERING if settling else BufferState.OK,
        release_pending=release_pending,
    )
    # MA is the RENDERER: it reports its live state via rndrSrvrStateUpdated
    # (the ReportState effect), NOT ctrlSrvrSetPlayerState. The latter is a
    # CONTROLLER command that tells the cloud "make this the current track" —
    # sending it for MA's own playback made MA fight the app for control and
    # created a command-echo loop that ping-ponged between two tracks (live
    # 2026-07-09). Controllers/the app follow the renderer's reported current,
    # so a user skip inside MA still propagates via the report. Our own report
    # echoes back as srvrCtrlRendererStateUpdated(ownId) and is ignored.
    if not state.active:
        if (
            playing is PlayingState.PLAYING
            and state.own_rid is not None
            and state.active_rid != state.own_rid
            and not state.activation_requested
            and not state.release_pending
        ):
            return ReduceResult(
                dataclasses.replace(new, activation_requested=True),
                (PushSetActive(),),
            )
        return ReduceResult(new, ())
    return ReduceResult(new, (ReportState(),))


def _active_renderer_changed(
    state: CanonicalState, event: CloudActiveRendererChanged
) -> ReduceResult:
    """Confirm or revoke this renderer's ownership of the cloud session."""
    new = dataclasses.replace(state, active_rid=event.renderer_id)
    is_own = state.own_rid is not None and event.renderer_id == state.own_rid
    if is_own:
        return ReduceResult(
            dataclasses.replace(
                new,
                active=True,
                activation_requested=False,
                release_pending=False,
            ),
            (ReportState(),),
        )
    if state.active:
        return ReduceResult(
            dataclasses.replace(
                new,
                active=False,
                activation_requested=False,
                release_pending=True,
            ),
            (MaReleasePlayer(),),
        )
    return ReduceResult(new, ())


def _remove_renderer(state: CanonicalState, event: CloudRemoveRenderer) -> ReduceResult:
    """Clear own/active renderer bookkeeping when the matching renderer leaves."""
    active_rid = None if state.active_rid == event.renderer_id else state.active_rid
    own_rid = None if state.own_rid == event.renderer_id else state.own_rid
    if active_rid == state.active_rid and own_rid == state.own_rid:
        return ReduceResult(state, ())
    new = dataclasses.replace(state, active_rid=active_rid, own_rid=own_rid)
    return ReduceResult(new, ())


def _reduce_load_ack(state: CanonicalState, event: CloudLoadAck) -> ReduceResult:
    """
    Adopt an app-initiated queue load (SRVR_CTRL_QUEUE_TRACKS_LOADED).

    The cloud broadcasts this — carrying the FULL new track list — whenever
    anyone (the app, another client) loads content. A Qobuz web client adopts
    that queue; so must we, or MA keeps playing off the stale connect-time
    snapshot while the app has moved on (the live 2026-07-09 wrong-track /
    ignored-skip bug). Autoplay continuation tracks arrive separately as
    CloudAutoplayTracksLoaded and are appended, not replaced.
    """
    tracks = tuple(event.tracks)
    if not tracks:
        return ReduceResult(dataclasses.replace(state, cloud_version=event.version), ())
    idx = max(0, min(event.queue_position, len(tracks) - 1))
    new_current = _safe_qid(tracks[idx])
    new = dataclasses.replace(
        state, cloud_version=event.version, tracks=tracks, current_id=new_current
    )
    result = _with_resync(new)
    # A new queue load while we are the active, playing renderer must switch
    # playback to the loaded queue's current track. A real web renderer
    # auto-plays on SRVR_CTRL_QUEUE_TRACKS_LOADED — verified in the
    # ref_new_album capture: the active renderer emits rndrSrvrStateUpdated
    # PLAYING at the new queueVersion the instant the load arrives, with no
    # separate SET_STATE. Resyncing the list alone left MA streaming the
    # previous album while the app had moved on (live 2026-07-09
    # "MA showed a broken state after I played another song"). Gated on a
    # real current-track change so a shuffle reorder (same current, new
    # order) never restarts audio.
    if (
        new.active
        and state.playing is PlayingState.PLAYING
        and new_current is not None
        and new_current != state.current_id
    ):
        play = MaPlayTrack(track_id=new_current, position_ms=0)
        # The loaded track starts at 0 and MA's position lags the restart;
        # anchor position to 0, settle, and mark BUFFERING so the app doesn't
        # briefly show the new track at the old track's position. _with_resync
        # reads only tracks/current/active, so result.effects is unchanged.
        settling = dataclasses.replace(
            new,
            position_ms=0,
            position_anchor_ms=event.now_ms,
            settling_position=True,
            buffer_state=BufferState.BUFFERING,
        )
        effects = _report_on_buffer_change(
            state.buffer_state, settling.buffer_state, (*result.effects, play)
        )
        return ReduceResult(settling, effects)
    return result


def _maybe_ask_snapshot(
    state: CanonicalState, version: QueueVersion
) -> tuple[CanonicalState, tuple[Effect, ...]]:
    """
    Ask the cloud for a full queue snapshot of ``version`` if we haven't already.

    The cloud only sends SRVR_CTRL_QUEUE_STATE in response to an explicit
    ask; it never pushes it unsolicited. Deduped against
    ``last_asked_version`` so redundant notifications at the same version —
    including the echo of our own confirmed proposals — don't trigger a
    repeat ask.

    :param state: Canonical state to check and, if asking, update.
    :param version: The cloud queue_version to request a snapshot for.
    """
    if version == QueueVersion():
        return state, ()
    if (version.major, version.minor) == (
        state.last_asked_version.major,
        state.last_asked_version.minor,
    ):
        return state, ()
    new = dataclasses.replace(state, last_asked_version=version)
    return new, (AskSnapshot(version=version),)


def _with_resync(new: CanonicalState) -> ReduceResult:
    """Emit MaResyncQueue only when active; list-lane never restarts audio."""
    if not new.active:
        return ReduceResult(new, ())
    effects: tuple[Effect, ...] = (
        MaResyncQueue(
            track_ids=tuple(
                qid
                for track in (*new.tracks, *new.autoplay_tracks)
                if (qid := _safe_qid(track)) is not None
            ),
            current_track_id=new.current_id,
        ),
    )
    return ReduceResult(new, effects)


def _current_from_pointer(tracks: tuple[QueueTrackRef, ...], track_index: int) -> int | None:
    """Resolve the current Qobuz track id from a one-indexed pointer into ``tracks``."""
    if not tracks:
        return None
    idx = max(0, min(track_index - 1, len(tracks) - 1))
    return _qid(tracks[idx])


def _successor_if_removed(state: CanonicalState, removed: set[int]) -> int | None:
    """
    Resolve the new current Qobuz track id after a queue_item_id-keyed removal.

    ``state.current_id`` lives in Qobuz-id space while ``removed`` is a set of
    cloud queue_item_ids, so the current track's ref is located first and its
    queue_item_id is what's actually tested against ``removed``.
    """
    if state.current_id is None:
        return None
    current_ref = next((t for t in state.tracks if _safe_qid(t) == state.current_id), None)
    if current_ref is None or current_ref.queue_item_id not in removed:
        return state.current_id
    start = state.tracks.index(current_ref)
    for t in state.tracks[start + 1 :]:
        if t.queue_item_id in removed:
            continue
        qid = _safe_qid(t)
        if qid is not None:
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


def _version_lt(a: QueueVersion, b: QueueVersion) -> bool:
    return (a.major, a.minor) < (b.major, b.minor)


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
    # Trust the proposal's resolved target (Qobuz ids) rather than
    # re-deriving the delta from the echo event — this stays correct across
    # every proposal kind. Where the echo itself carries real refs (adds/
    # inserts/load-acks assign real queue_item_ids cloud-side), prefer those
    # over the existing-canonical fallback, and finally a placeholder ref for
    # a Qobuz id neither source has yet resolved a queue_item_id for. Refs
    # are pooled per qid and consumed once per occurrence, so a queue holding
    # the same track twice keeps two DISTINCT queue_item_ids instead of
    # collapsing onto the first match (which broke later slot-keyed
    # reorder/remove translation for duplicates).
    pools: dict[int, list[QueueTrackRef]] = {}
    for ref in getattr(event, "tracks", ()):
        if (qid := _safe_qid(ref)) is not None:
            pools.setdefault(qid, []).append(ref)
    for ref in state.tracks:
        if (qid := _safe_qid(ref)) is None:
            continue
        pool = pools.setdefault(qid, [])
        if all(pooled.queue_item_id != ref.queue_item_id for pooled in pool):
            pool.append(ref)
    tracks = tuple(
        found.pop(0)
        if (found := pools.get(qid))
        else QueueTrackRef(queue_item_id=0, track_id=str(qid))
        for qid in proposal.target_track_ids
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
        rebased = _rebase_proposal(absorbed, proposal, event.version)
        pending = tuple(rebased if p is proposal else p for p in absorbed.pending)
        new = dataclasses.replace(absorbed, pending=pending)
        # Translate against the post-absorb canonical (current truth), so a
        # retry's push reflects whatever drifted in underneath us.
        return ReduceResult(new, (_emit_push(absorbed, rebased),))
    pending = tuple(p for p in absorbed.pending if p is not proposal)
    new = dataclasses.replace(absorbed, pending=pending)
    return _with_resync(new)  # converge MA to cloud truth


def _rebase_proposal(state: CanonicalState, proposal: Proposal, version: QueueVersion) -> Proposal:
    """Rebuild a retry from its full intended target against current truth."""
    canonical_ids = tuple(qid for track in state.tracks if (qid := _safe_qid(track)) is not None)
    target_ids = proposal.target_track_ids
    kind = proposal.kind
    payload = proposal.push_payload_ids

    if kind is ProposalKind.LOAD:
        payload = target_ids
    elif kind is ProposalKind.ADD:
        if target_ids[: len(canonical_ids)] == canonical_ids:
            payload = target_ids[len(canonical_ids) :]
        else:
            kind = ProposalKind.LOAD
            payload = target_ids
    elif kind is ProposalKind.INSERT:
        inserted = _consume_subsequence(target_ids, canonical_ids)
        if inserted is None:
            kind = ProposalKind.LOAD
            payload = target_ids
        else:
            payload = inserted
    elif kind is ProposalKind.REMOVE:
        removed = _consume_subsequence(canonical_ids, target_ids)
        if removed is None:
            kind = ProposalKind.LOAD
            payload = target_ids
        else:
            payload = removed
    elif kind is ProposalKind.REORDER and sorted(target_ids) != sorted(canonical_ids):
        kind = ProposalKind.LOAD
        payload = target_ids

    return dataclasses.replace(
        proposal,
        base_version=version,
        kind=kind,
        retries_left=proposal.retries_left - 1,
        push_payload_ids=payload,
    )


_SUBSEQ_DONE = object()


def _consume_subsequence(
    source: tuple[int, ...], target: tuple[int, ...]
) -> tuple[int, ...] | None:
    """
    Return source occurrences left after consuming ``target`` left-to-right.

    ``target`` must be an order-preserving, multiset-aware subsequence of
    ``source`` (a pure removal, no reorder), else ``None``. Duplicates
    are consumed first-match, so dropping one of two identical ids yields
    exactly one removed id.
    """
    removed: list[int] = []
    remaining = iter(target)
    expected: object = next(remaining, _SUBSEQ_DONE)
    for qid in source:
        if qid == expected:
            expected = next(remaining, _SUBSEQ_DONE)
        else:
            removed.append(qid)
    if expected is not _SUBSEQ_DONE:
        return None
    return tuple(removed)


def _match_occurrences(
    refs: tuple[QueueTrackRef, ...], ids: tuple[int, ...]
) -> tuple[QueueTrackRef, ...] | None:
    """Match Qobuz ids to distinct canonical refs in left-to-right order."""
    remaining = list(refs)
    matched: list[QueueTrackRef] = []
    for qid in ids:
        index = next(
            (idx for idx, ref in enumerate(remaining) if _safe_qid(ref) == qid),
            None,
        )
        if index is None:
            return None
        matched.append(remaining.pop(index))
    return tuple(matched)


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
        qid for t in state.tracks if (qid := _safe_qid(t)) is not None and qid in event.resolvable
    )
    if canonical_ids == event.track_ids:
        return None
    if any(p.target_track_ids == event.track_ids for p in state.pending):
        return None
    if not event.track_ids:
        kind = ProposalKind.CLEAR
        push_payload_ids: tuple[int, ...] = ()
    elif canonical_ids and event.track_ids[: len(canonical_ids)] == canonical_ids:
        kind = ProposalKind.ADD
        # Positional tail against the resolvable-filtered canonical prefix —
        # preserves duplicate ids, unlike a set-membership filter would.
        push_payload_ids = event.track_ids[len(canonical_ids) :]
    elif (removed := _consume_subsequence(canonical_ids, event.track_ids)) is not None:
        # A pure removal: the new list is an order-preserving subsequence of
        # canonical. Carry the REMOVED qids in push_payload_ids (translated to
        # cloud slot ids at emit time); target_track_ids stays the survivor
        # list that the CloudTracksRemoved echo folds into canonical.
        kind = ProposalKind.REMOVE
        push_payload_ids = removed
    elif sorted(event.track_ids) == sorted(canonical_ids):
        kind = ProposalKind.REORDER
        push_payload_ids = ()  # translated to slot ids at emit time.
    else:
        kind = ProposalKind.LOAD
        push_payload_ids = event.track_ids
    return Proposal(
        action_uuid=event.action_uuid,
        base_version=state.cloud_version,
        kind=kind,
        target_track_ids=event.track_ids,
        current_track_id=event.current_track_id,
        push_payload_ids=push_payload_ids,
        context_uuid=event.context_uuid,
    )


def _emit_push(state: CanonicalState, proposal: Proposal) -> Effect:
    """
    Translate a proposal into the cloud push command that realizes it.

    All Qobuz-id -> cloud-slot translation happens here against ``state``,
    so the same logic produces a correct push both for a proposal's initial
    emission and for a retry after a version-stale rejection — the only
    difference between the two call sites is which state (pre-edit
    canonical vs. post-absorb canonical) they translate against.

    :param state: Canonical state to translate ids against.
    :param proposal: The proposal being realized as a cloud push.
    """
    if proposal.kind is ProposalKind.CLEAR:
        return PushClear(action_uuid=proposal.action_uuid, base_version=proposal.base_version)
    if proposal.kind is ProposalKind.LOAD:
        current_index = (
            proposal.target_track_ids.index(proposal.current_track_id)
            if proposal.current_track_id in proposal.target_track_ids
            else 0
        )
        return PushLoad(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            track_ids=proposal.push_payload_ids,
            current_index=current_index,
            context_uuid=proposal.context_uuid,
        )
    if proposal.kind is ProposalKind.ADD:
        # CTRL_SRVR_QUEUE_ADD_TRACKS identifies tracks by a controller-session
        # slot (66 in the reference capture), not by catalog Qobuz id. MA has
        # no such web-session slot: sending QueueTrackRef(trackId=...) is
        # silently ignored by the real cloud. A full load speaks catalog ids,
        # preserves duplicate occurrences, and keeps playback on the same
        # current index.
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
            context_uuid=proposal.context_uuid,
        )
    if proposal.kind is ProposalKind.INSERT:
        # No diff path produces INSERT proposals yet; implemented
        # defensively so _emit_push stays total over ProposalKind.
        return PushInsert(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            track_ids=proposal.push_payload_ids,
            insert_after=0,
            context_uuid=proposal.context_uuid,
        )
    if proposal.kind is ProposalKind.REMOVE:
        # push_payload_ids carries the REMOVED Qobuz ids (canonical minus the
        # surviving target, computed at diff time). Translate those — NOT the
        # survivors in target_track_ids — to cloud queue_item_ids; the wire
        # remove command speaks slot ids. target_track_ids stays the survivor
        # list that _confirm_proposal folds into canonical on the echo.
        matched = _match_occurrences(state.tracks, proposal.push_payload_ids) or ()
        return PushRemove(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            queue_item_ids=tuple(ref.queue_item_id for ref in matched),
        )
    if proposal.kind is ProposalKind.REORDER:
        # The wire command speaks cloud queue_item_ids (slots), not Qobuz
        # track ids — translate the reordered Qobuz ids via the canonical
        # correspondence against ``state``. A REORDER is same-set by
        # construction, so every target id is present in canonical and the
        # translation is total.
        matched = _match_occurrences(state.tracks, proposal.target_track_ids) or ()
        return PushReorder(
            action_uuid=proposal.action_uuid,
            base_version=proposal.base_version,
            queue_item_ids=tuple(ref.queue_item_id for ref in matched),
            insert_after=0,
        )
    raise NotImplementedError(f"push mapping for {proposal.kind} lands in a later task")


def _qid(ref: QueueTrackRef) -> int:
    """Qobuz track id (int) for a queue ref — the reducer's track identity."""
    return int(ref.track_id)


def _safe_qid(ref: QueueTrackRef) -> int | None:
    """Qobuz track id for a queue ref, or None if ``track_id`` isn't numeric."""
    try:
        return int(ref.track_id)
    except ValueError:
        return None
