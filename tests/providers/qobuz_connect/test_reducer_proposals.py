"""Golden tests for optimistic proposals: emit, confirm, reject+rebase."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudQueueError,
    CloudTracksAdded,
    MaQueueChanged,
    MaResyncQueue,
    Proposal,
    ProposalKind,
    ProposalTimeout,
    PushAdd,
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
