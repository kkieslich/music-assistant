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
    ProposalTimeout,
    PushAdd,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in ids)


def _state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=0, active=True
    )


def test_ma_append_emits_proposal_without_mutating_truth() -> None:
    """An MA-origin append pushes to cloud and records a pending proposal; truth is untouched."""
    state = _state()
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(0, 1, 2),
            current_track_id=0,
            resolvable=frozenset({0, 1, 2}),
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
            track_ids=(0, 1, 2),
            current_track_id=0,
            resolvable=frozenset({0, 1, 2}),
        ),
    )
    r2 = reduce(
        r1.state,
        CloudTracksAdded(
            now_ms=2,
            version=QueueVersion(6, 1),
            action_uuid=b"\xaa" * 16,
            tracks=_refs(2),
            after_index=2,
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
            track_ids=(0, 1, 2),
            current_track_id=0,
            resolvable=frozenset({0, 1, 2}),
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


def test_proposal_timeout_drops_and_converges() -> None:
    """A proposal that never gets a cloud echo is dropped; MA converges to canonical."""
    state = _state()
    r1 = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(0, 1, 2),
            current_track_id=0,
            resolvable=frozenset({0, 1, 2}),
        ),
    )
    r2 = reduce(r1.state, ProposalTimeout(now_ms=9999, action_uuid=b"\xaa" * 16))
    assert r2.state.pending == ()
    assert any(isinstance(e, MaResyncQueue) for e in r2.effects)
