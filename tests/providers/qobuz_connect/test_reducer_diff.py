"""Golden tests for structural MA-queue diffing (no intent guessing)."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    MaQueueChanged,
    ProposalKind,
    PushLoad,
    PushRemove,
    PushReorder,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    # queue_item_id = small cloud slot; track_id = distinct large Qobuz id.
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(900000 + i)) for i in ids)


def _state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=900000, active=True
    )


def test_pointer_only_move_makes_no_list_proposal() -> None:
    """A natural advance (same list, new current) must NOT produce a list proposal."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900002),
            current_track_id=900001,
            resolvable=frozenset({900000, 900001, 900002}),
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
            track_ids=(900002, 900000, 900001),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert len(result.state.pending) == 1
    assert result.state.pending[0].kind is ProposalKind.REORDER


def test_unresolvable_subsequence_is_not_a_removal() -> None:
    """MA missing region-locked ids (not in resolvable) is not read as user-removes."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=900000, active=True
    )
    # MA only has 900000 and 900002 resolvable; 900001 is unresolvable -> no proposal
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900002}),
        ),
    )
    assert result.state.pending == ()


def test_removal_detected_as_remove_not_load() -> None:
    """A pure removal (subsequence of canonical) -> REMOVE proposal + PushRemove of the slot."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002),  # dropped the middle track
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert len(result.state.pending) == 1
    proposal = result.state.pending[0]
    assert proposal.kind is ProposalKind.REMOVE
    # target stays the survivor list; the removed qid rides push_payload_ids.
    assert proposal.target_track_ids == (900000, 900002)
    assert proposal.push_payload_ids == (900001,)
    push = result.effects[0]
    assert isinstance(push, PushRemove)
    # 900001 lives in cloud slot 1 -> that's the queue_item_id removed.
    assert push.queue_item_ids == (1,)


def test_removal_of_duplicate_removes_only_one_occurrence() -> None:
    """Dropping one of two identical tracks removes a single slot, not both."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(
            QueueTrackRef(queue_item_id=0, track_id="900000"),
            QueueTrackRef(queue_item_id=1, track_id="900000"),
            QueueTrackRef(queue_item_id=2, track_id="900002"),
        ),
        current_id=900000,
        active=True,
    )
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002),  # one of the two 900000s is gone
            current_track_id=900000,
            resolvable=frozenset({900000, 900002}),
        ),
    )
    assert len(result.state.pending) == 1
    proposal = result.state.pending[0]
    assert proposal.kind is ProposalKind.REMOVE
    assert proposal.push_payload_ids == (900000,)  # exactly one occurrence
    push = result.effects[0]
    assert isinstance(push, PushRemove)
    assert push.queue_item_ids == (0,)  # first matching slot


def test_removing_two_duplicate_occurrences_uses_two_distinct_slots() -> None:
    """Occurrence translation must never reuse the first matching cloud slot."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(
            QueueTrackRef(queue_item_id=10, track_id="900000"),
            QueueTrackRef(queue_item_id=11, track_id="900000"),
            QueueTrackRef(queue_item_id=12, track_id="900002"),
        ),
        current_id=900002,
        active=True,
    )
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900002,),
            current_track_id=900002,
            resolvable=frozenset({900000, 900002}),
        ),
    )

    push = result.effects[0]
    assert isinstance(push, PushRemove)
    assert push.queue_item_ids == (10, 11)


def test_removal_plus_reorder_falls_back_to_load() -> None:
    """A removal combined with a reorder is not a clean subsequence -> LOAD."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900002, 900000),  # dropped 900001 AND reordered
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert len(result.state.pending) == 1
    assert result.state.pending[0].kind is ProposalKind.LOAD


def test_changed_duplicate_multiplicity_falls_back_to_load() -> None:
    """Equal sets with unequal occurrence counts are not a valid REORDER."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(
            QueueTrackRef(queue_item_id=10, track_id="900000"),
            QueueTrackRef(queue_item_id=11, track_id="900000"),
            QueueTrackRef(queue_item_id=12, track_id="900002"),
        ),
        current_id=900000,
        active=True,
    )
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002, 900002),
            current_track_id=900000,
            resolvable=frozenset({900000, 900002}),
        ),
    )

    assert result.state.pending[0].kind is ProposalKind.LOAD


def test_genuine_new_load_detected() -> None:
    """A wholly different list -> LOAD proposal."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900009, 900008, 900007),
            current_track_id=900009,
            resolvable=frozenset({900009, 900008, 900007}),
        ),
    )
    assert result.state.pending[0].kind is ProposalKind.LOAD


def test_append_uses_full_load_with_current_index() -> None:
    """
    ADD proposal uses a full load because the cloud add verb needs a web-session slot.

    MA only has Qobuz catalog ids, which the real cloud silently ignores in
    CTRL_SRVR_QUEUE_ADD_TRACKS. A full load accepts those ids directly.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
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
    assert len(result.state.pending) == 1
    proposal = result.state.pending[0]
    assert proposal.kind is ProposalKind.ADD
    assert proposal.target_track_ids == (900000, 900001, 900002)
    assert len(result.effects) == 1
    push = result.effects[0]
    assert isinstance(push, PushLoad)
    assert push.track_ids == (900000, 900001, 900002)
    assert push.current_index == 0


def test_add_tail_preserves_duplicate_track() -> None:
    """
    A duplicate re-add of a track already in canonical must not be dropped.

    Queue multiplicity (the same Qobuz track appearing twice) is a real,
    supported case; the ADD tail must be computed positionally, not by
    set-membership, or a duplicate re-add silently becomes a no-op push.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001, 900000),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001}),
        ),
    )
    assert len(result.state.pending) == 1
    proposal = result.state.pending[0]
    assert proposal.kind is ProposalKind.ADD
    assert proposal.target_track_ids == (900000, 900001, 900000)
    assert len(result.effects) == 1
    push = result.effects[0]
    assert isinstance(push, PushLoad)
    assert push.track_ids == (900000, 900001, 900000)


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
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert len(result.state.pending) == 1
    assert result.state.pending[0].kind is ProposalKind.CLEAR


def test_pending_add_suppresses_repeated_identical_ma_event() -> None:
    """Re-feeding the same MA event that produced a pending ADD must not re-propose."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    event = MaQueueChanged(
        now_ms=1,
        action_uuid=b"\xaa" * 16,
        track_ids=(900000, 900001, 900002),
        current_track_id=900000,
        resolvable=frozenset({900000, 900001, 900002}),
    )
    first = reduce(state, event)
    assert len(first.state.pending) == 1

    second = reduce(first.state, event)
    assert len(second.state.pending) == 1
    assert second.effects == ()


def test_reorder_pushes_full_target_order_with_insert_after_zero() -> None:
    """REORDER's PushReorder encodes the permutation as move-to-front, in slot ids."""
    result = reduce(
        _state(),
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900002, 900000, 900001),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001, 900002}),
        ),
    )
    assert len(result.effects) == 1
    push = result.effects[0]
    assert isinstance(push, PushReorder)
    # target order is Qobuz ids (900002, 900000, 900001) -> translated to
    # their cloud slot ids (2, 0, 1) via the canonical correspondence.
    assert push.queue_item_ids == (2, 0, 1)
    assert push.insert_after == 0


def test_reorder_with_duplicates_consumes_each_cloud_slot_once() -> None:
    """Repeated Qobuz IDs retain distinct occurrence identity in a REORDER."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(
            QueueTrackRef(queue_item_id=10, track_id="900000"),
            QueueTrackRef(queue_item_id=11, track_id="900000"),
            QueueTrackRef(queue_item_id=12, track_id="900002"),
        ),
        current_id=900000,
        active=True,
    )
    result = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900002, 900000),
            current_track_id=900000,
            resolvable=frozenset({900000, 900002}),
        ),
    )

    push = result.effects[0]
    assert isinstance(push, PushReorder)
    assert push.queue_item_ids == (10, 12, 11)
    assert len(set(push.queue_item_ids)) == 3


def test_diff_matches_by_qobuz_id_not_slot() -> None:
    """
    Diffing compares Qobuz track ids, not cloud slot ids.

    An MA list reporting the same Qobuz ids in the same order (regardless of
    what cloud slot ids they happen to occupy) must be a no-op; a reorder of
    those same Qobuz ids must be detected as REORDER and translated back to
    slot ids for the wire command.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    same = reduce(
        state,
        MaQueueChanged(
            now_ms=1,
            action_uuid=b"\xaa" * 16,
            track_ids=(900000, 900001),
            current_track_id=900000,
            resolvable=frozenset({900000, 900001}),
        ),
    )
    assert same.state.pending == ()  # equal in Qobuz-id space -> no proposal

    reordered = reduce(
        state,
        MaQueueChanged(
            now_ms=2,
            action_uuid=b"\xbb" * 16,
            track_ids=(900001, 900000),
            current_track_id=900001,
            resolvable=frozenset({900000, 900001}),
        ),
    )
    assert len(reordered.state.pending) == 1
    assert reordered.state.pending[0].kind is ProposalKind.REORDER
    assert len(reordered.effects) == 1
    push = reordered.effects[0]
    assert isinstance(push, PushReorder)
    assert push.queue_item_ids == (1, 0)
    assert push.insert_after == 0
