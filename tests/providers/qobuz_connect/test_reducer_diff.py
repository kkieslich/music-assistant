"""Golden tests for structural MA-queue diffing (no intent guessing)."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    MaQueueChanged,
    ProposalKind,
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
