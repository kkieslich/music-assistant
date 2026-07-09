"""Golden tests for structural MA-queue diffing (no intent guessing)."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    MaQueueChanged,
    ProposalKind,
    PushAdd,
    PushReorder,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in ids)


def _state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=0, active=True
    )


def test_pointer_only_move_makes_no_list_proposal() -> None:
    """A natural advance (same list, new current) must NOT produce a list proposal."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(0, 1, 2),
            current_track_id=1,
            resolvable=frozenset({0, 1, 2}),
        ),
    )
    assert result.state.pending == ()  # the 238-track re-push bug: impossible


def test_reorder_detected_as_reorder_not_load() -> None:
    """Same set, different order -> REORDER proposal."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(2, 0, 1),
            current_track_id=0,
            resolvable=frozenset({0, 1, 2}),
        ),
    )
    assert len(result.state.pending) == 1
    assert result.state.pending[0].kind is ProposalKind.REORDER


def test_unresolvable_subsequence_is_not_a_removal() -> None:
    """MA missing region-locked ids (not in resolvable) is not read as user-removes."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=0, active=True
    )
    # MA only has 0 and 2 resolvable; 1 is unresolvable -> no proposal
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(0, 2),
            current_track_id=0,
            resolvable=frozenset({0, 2}),
        ),
    )
    assert result.state.pending == ()


def test_genuine_new_load_detected() -> None:
    """A wholly different list -> LOAD proposal."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(9, 8, 7),
            current_track_id=9,
            resolvable=frozenset({9, 8, 7}),
        ),
    )
    assert result.state.pending[0].kind is ProposalKind.LOAD


def test_append_pushes_only_the_appended_tail() -> None:
    """
    ADD proposal targets the full list, but the PushAdd payload is tail-only.

    The cloud's add command APPENDS its payload to the existing cloud queue,
    so pushing the full resolved list would duplicate the tracks the cloud
    already has (the latent bug this test pins).
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=0, active=True
    )
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
    assert len(result.state.pending) == 1
    proposal = result.state.pending[0]
    assert proposal.kind is ProposalKind.ADD
    assert proposal.target_track_ids == (0, 1, 2)
    assert len(result.effects) == 1
    push = result.effects[0]
    assert isinstance(push, PushAdd)
    assert push.track_ids == (2,)


def test_empty_ma_list_with_nonempty_canonical_is_clear() -> None:
    """MA reports an empty queue while canonical has tracks -> CLEAR proposal."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(),
            current_track_id=None,
            # resolvable must cover canonical's ids so the empty MA list isn't
            # itself filtered away to "no change" (see _diff_ma_list docstring).
            resolvable=frozenset({0, 1, 2}),
        ),
    )
    assert len(result.state.pending) == 1
    assert result.state.pending[0].kind is ProposalKind.CLEAR


def test_pending_add_suppresses_repeated_identical_ma_event() -> None:
    """Re-feeding the same MA event that produced a pending ADD must not re-propose."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=0, active=True
    )
    event = MaQueueChanged(
        now_ms=1,
        action_uuid=b"\xaa" * 16,
        track_ids=(0, 1, 2),
        current_track_id=0,
        resolvable=frozenset({0, 1, 2}),
    )
    first = reduce(state, event)
    assert len(first.state.pending) == 1

    second = reduce(first.state, event)
    assert len(second.state.pending) == 1
    assert second.effects == ()


def test_reorder_pushes_full_target_order_with_insert_after_zero() -> None:
    """REORDER's PushReorder encodes the permutation as move-to-front."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(2, 0, 1),
            current_track_id=0,
            resolvable=frozenset({0, 1, 2}),
        ),
    )
    assert len(result.effects) == 1
    push = result.effects[0]
    assert isinstance(push, PushReorder)
    assert push.queue_item_ids == (2, 0, 1)
    assert push.insert_after == 0
