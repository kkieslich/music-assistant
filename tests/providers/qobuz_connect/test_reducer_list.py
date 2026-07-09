# tests/providers/qobuz_connect/test_reducer_list.py
"""Golden tests for the reducer list lane (inbound, app-origin)."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudCleared,
    CloudSnapshot,
    CloudTracksAdded,
    CloudTracksRemoved,
    CloudVersionChanged,
    MaResyncQueue,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    # queue_item_id = small cloud slot; track_id = distinct large Qobuz id.
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(900000 + i)) for i in ids)


def test_snapshot_sets_truth_and_derives_current_from_pointer() -> None:
    """A snapshot replaces tracks and maps trackIndex (next pointer) to current-1."""
    state = CanonicalState(active=True)
    result = reduce(
        state,
        CloudSnapshot(
            now_ms=1000,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1, 2, 3),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=3,
        ),
    )
    assert result.state.cloud_version == QueueVersion(5, 1)
    assert len(result.state.tracks) == 4
    # trackIndex 3 -> current index 2 -> Qobuz id of slot 2 (900002)
    assert result.state.current_id == 900002
    assert any(isinstance(e, MaResyncQueue) for e in result.effects)


def test_snapshot_current_is_qobuz_id() -> None:
    """A snapshot's current_id is the pointed track's Qobuz id, not its cloud slot id."""
    result = reduce(
        CanonicalState(active=True),
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1, 2, 3),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=3,
        ),
    )
    assert result.state.current_id == 900002  # Qobuz id of slot index 2


def test_snapshot_while_inactive_emits_no_ma_effect() -> None:
    """An inactive renderer absorbs truth silently (no MA apply)."""
    result = reduce(
        CanonicalState(active=False),
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        ),
    )
    assert result.state.tracks == _refs(0, 1)
    assert result.effects == ()


def test_stale_version_is_ignored() -> None:
    """An event at or below cloud_version is a duplicate echo — ignored."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), active=True)
    result = reduce(state, CloudVersionChanged(now_ms=1, version=QueueVersion(5, 1)))
    assert result.state is state
    assert result.effects == ()


def test_app_origin_add_appends_and_resyncs() -> None:
    """An app-origin add (no matching proposal) appends and resyncs MA."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), active=True)
    result = reduce(
        state,
        CloudTracksAdded(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\x09" * 16,
            tracks=_refs(2),
            after_index=2,
        ),
    )
    assert result.state.cloud_version == QueueVersion(6, 1)
    assert tuple(t.queue_item_id for t in result.state.tracks) == (0, 1, 2)
    assert any(isinstance(e, MaResyncQueue) for e in result.effects)


def test_app_origin_remove_of_current_picks_successor() -> None:
    """Removing the current track advances current_id to the successor's Qobuz id."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=900001, active=True
    )
    result = reduce(
        state,
        CloudTracksRemoved(
            now_ms=1, version=QueueVersion(6, 1), action_uuid=b"\x09" * 16, queue_item_ids=(1,)
        ),
    )
    assert tuple(t.queue_item_id for t in result.state.tracks) == (0, 2)
    assert result.state.current_id == 900002


def test_remove_current_with_no_successor_clears_current() -> None:
    """Removing the current track with nothing after it clears current_id (no wrap to top)."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1, 2), current_id=900002, active=True
    )
    result = reduce(
        state,
        CloudTracksRemoved(
            now_ms=1, version=QueueVersion(6, 1), action_uuid=b"\x09" * 16, queue_item_ids=(2,)
        ),
    )
    assert tuple(t.queue_item_id for t in result.state.tracks) == (0, 1)
    assert result.state.current_id is None


def test_cleared_empties_tracks_and_current() -> None:
    """A cloud clear empties the list and drops the current anchor."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    result = reduce(
        state, CloudCleared(now_ms=1, version=QueueVersion(6, 1), action_uuid=b"\x09" * 16)
    )
    assert result.state.tracks == ()
    assert result.state.current_id is None
