"""Golden tests for optimistic proposals: emit, confirm, reject+rebase."""

from __future__ import annotations

import dataclasses

from music_assistant.providers.qobuz_connect.models import QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudCleared,
    CloudQueueError,
    CloudTracksAdded,
    CloudTracksRemoved,
    MaQueueChanged,
    MaResyncQueue,
    Proposal,
    ProposalKind,
    ProposalTimeout,
    PushAdd,
    PushInsert,
    PushLoad,
    PushRemove,
    PushReorder,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    # queue_item_id = small cloud slot; track_id = distinct large Qobuz id.
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(900000 + i)) for i in ids)


def _state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )


def test_ma_append_emits_proposal_without_mutating_truth() -> None:
    """An MA-origin append pushes to cloud and records a pending proposal; truth is untouched."""
    state = _state()
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert tuple(t.queue_item_id for t in result.state.tracks) == (0, 1)  # truth unchanged
    assert len(result.state.pending) == 1
    assert result.state.pending[0].action_uuid == b"\xaa" * 16
    assert any(isinstance(e, PushAdd) for e in result.effects)


def test_own_echo_confirms_and_does_not_resync_ma() -> None:
    """The cloud echo of our own proposal folds into truth with no MA apply."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    r2 = reduce(
        r1.state,
        CloudTracksAdded(
            now_ms=2, version=QueueVersion(6, 1), action_uuid=b"\xaa" * 16, tracks=_refs(2)
        ),
    )
    assert tuple(t.queue_item_id for t in r2.state.tracks) == (0, 1, 2)
    assert r2.state.pending == ()
    assert not any(isinstance(e, MaResyncQueue) for e in r2.effects)


def test_reject_rebases_once_then_converges() -> None:
    """A version-stale rejection rebases the proposal once, then converges to cloud."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    # cloud advanced underneath us -> reject at a newer version
    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    assert r2.state.cloud_version == QueueVersion(7, 1)
    assert len(r2.state.pending) == 1  # rebased, still pending
    assert r2.state.pending[0].retries_left == 0
    assert any(isinstance(e, PushAdd) for e in r2.effects)  # re-pushed
    # second rejection -> give up, converge MA
    r3 = reduce(
        r2.state,
        CloudQueueError(
            now_ms=3,
            version=QueueVersion(8, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    assert r3.state.pending == ()
    assert any(isinstance(e, MaResyncQueue) for e in r3.effects)


def test_reject_at_equal_version_still_rebases() -> None:
    """A rejection carrying the coordinator's version-less fallback (== cloud_version) still rebases."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    # No version on the wire error -> coordinator falls back to cloud_version,
    # which is exactly equal to state.cloud_version.
    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=r1.state.cloud_version,
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    assert len(r2.state.pending) == 1  # rebased, still pending
    assert r2.state.pending[0].retries_left == 0
    assert any(isinstance(e, PushAdd) for e in r2.effects)  # re-pushed


def test_proposal_timeout_drops_and_converges() -> None:
    """A proposal that never gets a cloud echo is dropped; MA converges to canonical."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    r2 = reduce(r1.state, ProposalTimeout(now_ms=9999, action_uuid=b"\xaa" * 16))
    assert r2.state.pending == ()
    assert any(isinstance(e, MaResyncQueue) for e in r2.effects)


def test_confirm_add_uses_real_item_ids_from_echo() -> None:
    """Confirming an ADD folds the echo's real queue_item_id onto the appended Qobuz id."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    r2 = reduce(
        r1.state,
        CloudTracksAdded(
            now_ms=2,
            version=QueueVersion(6, 1),
            action_uuid=b"\xaa" * 16,
            tracks=(QueueTrackRef(queue_item_id=2, track_id="900002"),),
        ),
    )
    confirmed = r2.state.tracks[-1]
    assert confirmed.queue_item_id == 2
    assert confirmed.track_id == "900002"


def test_reject_retry_add_repushes_tail_not_full() -> None:
    """A version-stale rejection of an ADD re-pushes only the tail, not the full target list."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    pushes = [e for e in r2.effects if isinstance(e, PushAdd)]
    assert len(pushes) == 1
    assert pushes[0].track_ids == (900002,)
    assert r2.state.pending[0].retries_left == 0


def test_reject_retry_reorder_translates_to_slot_ids() -> None:
    """A version-stale rejection of a REORDER re-pushes with translated cloud slot ids."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=900000, active=True
    )
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xbb" * 16,
            track_ids=(900002, 900000, 900001),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xbb" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    pushes = [e for e in r2.effects if isinstance(e, PushReorder)]
    assert len(pushes) == 1
    assert pushes[0].queue_item_ids == (2, 0, 1)
    assert pushes[0].insert_after == 0


def test_confirm_with_duplicate_track_keeps_distinct_queue_item_ids() -> None:
    """
    Confirming a proposal whose target repeats a track must not collapse slots.

    A queue can legitimately hold the same Qobuz track twice; the cloud
    assigns each occurrence its own queue_item_id. Folding the echo through a
    plain qid->ref map gave every occurrence the FIRST matching ref, so
    canonical ended up with duplicate queue_item_ids and later slot-keyed
    commands (reorder/remove) targeted the wrong occurrence.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0),
        current_id=900000,
        active=True,
        pending=(
            Proposal(
                action_uuid=b"\x07" * 16,
                base_version=QueueVersion(5, 1),
                kind=ProposalKind.ADD,
                # User re-added the already-queued track: same qid twice.
                target_track_ids=(900000, 900000),
                current_track_id=900000,
                push_payload_ids=(900000,),
            ),
        ),
    )
    echo = CloudTracksAdded(
        now_ms=1,
        version=QueueVersion(6, 1),
        action_uuid=b"\x07" * 16,
        # Cloud assigned slot 5 to the appended duplicate.
        tracks=(QueueTrackRef(queue_item_id=5, track_id="900000"),),
    )
    result = reduce(state, echo)
    assert result.state.pending == ()
    item_ids = sorted(t.queue_item_id for t in result.state.tracks)
    assert item_ids == [0, 5], f"occurrences must keep distinct slots, got {item_ids}"


def _remove_state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900000,
        active=True,
    )


def test_remove_confirms_on_cloud_tracks_removed_echo() -> None:
    """A REMOVE proposal folds into truth on the CloudTracksRemoved echo, no MA resync."""
    r1 = reduce(
        _remove_state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert r1.state.pending[0].kind is ProposalKind.REMOVE

    r2 = reduce(
        r1.state,
        CloudTracksRemoved(
            now_ms=2,
            version=QueueVersion(6, 1),
            action_uuid=b"\xaa" * 16,
            queue_item_ids=(1,),
        ),
    )
    assert r2.state.pending == ()
    assert tuple(t.queue_item_id for t in r2.state.tracks) == (0, 2)
    assert tuple(t.track_id for t in r2.state.tracks) == ("900000", "900002")
    assert not any(isinstance(e, MaResyncQueue) for e in r2.effects)


def test_remove_reject_rebases_once_then_converges() -> None:
    """A version-stale rejection of a REMOVE rebases once (target kept), then converges MA."""
    r1 = reduce(
        _remove_state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    assert len(r2.state.pending) == 1  # rebased, still pending
    assert r2.state.pending[0].kind is ProposalKind.REMOVE
    assert r2.state.pending[0].target_track_ids == (900000, 900002)  # kept as-is
    assert any(isinstance(e, PushRemove) for e in r2.effects)  # re-pushed

    r3 = reduce(
        r2.state,
        CloudQueueError(
            now_ms=3,
            version=QueueVersion(8, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )
    assert r3.state.pending == ()
    assert any(isinstance(e, MaResyncQueue) for e in r3.effects)


def test_load_retry_preserves_full_target_and_valid_context() -> None:
    """A rejected LOAD must never retry as the empty request seen in the recorder."""
    context_uuid = b"\xcc" * 16
    r1 = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            context_uuid=context_uuid,
            track_ids=(20, 21),
            current_track_id=20,
            resolvable=frozenset({20, 21, 900000, 900001}),
        ),
    )

    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )

    pushes = [effect for effect in r2.effects if isinstance(effect, PushLoad)]
    assert len(pushes) == 1
    assert pushes[0].track_ids == (20, 21)
    assert pushes[0].context_uuid == context_uuid
    assert len(pushes[0].action_uuid) == 16
    assert any(pushes[0].action_uuid)


def test_clear_echo_then_load_rejection_retries_original_tracks() -> None:
    """Reproduce the recorder sequence without degrading the retry to an empty LOAD."""
    clear = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\x01" * 16,
            context_uuid=b"\x02" * 16,
            track_ids=(),
            current_track_id=None,
            resolvable=frozenset({900000, 900001}),
        ),
    )
    cleared = reduce(
        clear.state,
        CloudCleared(
            now_ms=2,
            version=QueueVersion(6, 1),
            action_uuid=b"\x01" * 16,
        ),
    )
    load = reduce(
        cleared.state,
        MaQueueChanged(
            now_ms=3,
            action_uuid=b"\x03" * 16,
            context_uuid=b"\x04" * 16,
            track_ids=(20, 21),
            current_track_id=20,
            resolvable=frozenset({20, 21}),
        ),
    )

    retried = reduce(
        load.state,
        CloudQueueError(
            now_ms=4,
            version=QueueVersion(7, 1),
            action_uuid=b"\x03" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )

    pushes = [effect for effect in retried.effects if isinstance(effect, PushLoad)]
    assert len(pushes) == 1
    assert pushes[0].track_ids == (20, 21)


def test_remove_retry_preserves_removed_occurrence() -> None:
    """A rejected REMOVE must still target the removed cloud slot."""
    r1 = reduce(
        _remove_state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            context_uuid=b"\xcc" * 16,
            track_ids=(900000, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )

    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )

    pushes = [effect for effect in r2.effects if isinstance(effect, PushRemove)]
    assert len(pushes) == 1
    assert pushes[0].queue_item_ids == (1,)


def test_add_duplicate_retry_preserves_one_appended_occurrence() -> None:
    """Rebasing an ADD uses occurrence counts, not set membership."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0),
        current_id=900000,
        active=True,
    )
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            context_uuid=b"\xcc" * 16,
            track_ids=(900000, 900000),
            current_track_id=900000,
            resolvable=frozenset({900000}),
        ),
    )

    r2 = reduce(
        r1.state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )

    pushes = [effect for effect in r2.effects if isinstance(effect, PushAdd)]
    assert len(pushes) == 1
    assert pushes[0].track_ids == (900000,)


def test_insert_retry_preserves_insert_payload() -> None:
    """A defensively supported INSERT proposal must retain its intended IDs."""
    proposal = Proposal(
        action_uuid=b"\xaa" * 16,
        context_uuid=b"\xcc" * 16,
        base_version=QueueVersion(5, 1),
        kind=ProposalKind.INSERT,
        target_track_ids=(900000, 900002, 900001),
        current_track_id=900000,
        push_payload_ids=(900002,),
    )
    state = dataclasses.replace(_state(), pending=(proposal,))

    result = reduce(
        state,
        CloudQueueError(
            now_ms=2,
            version=QueueVersion(7, 1),
            action_uuid=b"\xaa" * 16,
            code="1",
            message="Queue version mismatch",
        ),
    )

    pushes = [effect for effect in result.effects if isinstance(effect, PushInsert)]
    assert len(pushes) == 1
    assert pushes[0].track_ids == (900002,)
