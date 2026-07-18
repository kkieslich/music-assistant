# tests/providers/qobuz_connect/test_reducer_list.py
"""Golden tests for the reducer list lane (inbound, app-origin)."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.reducer import _GATED_LE, _GATED_LT, reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    AskSnapshot,
    CanonicalState,
    CloudAutoplayTracksLoaded,
    CloudCleared,
    CloudLoadAck,
    CloudSessionState,
    CloudSetState,
    CloudSnapshot,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
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


def test_non_gated_versioned_event_passes_through_at_stale_version() -> None:
    """A versioned event outside the gated tuples (CloudSetState) is never stale-gated."""
    state = CanonicalState(cloud_version=QueueVersion(9, 1), tracks=_refs(0, 1), active=True)
    # A CloudSetState carrying an *older* queue_version must still be applied:
    # transport intent is orthogonal to queue-list staleness.
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=QueueVersion(5, 1),
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=_refs(1)[0],
        ),
    )
    assert result.state is not state or result.effects != ()


def test_gated_tuples_cover_exactly_the_list_lane_and_session_state() -> None:
    """The version gate is opt-in: the gated tuples are exactly list-lane + session-state."""
    assert set(_GATED_LT) == {CloudSnapshot}
    assert set(_GATED_LE) == {
        CloudVersionChanged,
        CloudTracksAdded,
        CloudTracksInserted,
        CloudTracksRemoved,
        CloudTracksReordered,
        CloudCleared,
        CloudAutoplayTracksLoaded,
        CloudLoadAck,
        CloudSessionState,
    }


def test_app_origin_add_appends_and_resyncs() -> None:
    """An app-origin add (no matching proposal) appends and resyncs MA."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), active=True)
    result = reduce(
        state,
        CloudTracksAdded(
            now_ms=1, version=QueueVersion(6, 1), action_uuid=b"\x09" * 16, tracks=_refs(2)
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


def test_version_changed_asks_once() -> None:
    """Two CloudVersionChanged events at the same version only ask for a snapshot once."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1), active=True)
    r1 = reduce(state, CloudVersionChanged(now_ms=1, version=QueueVersion(6, 1)))
    asks1 = [e for e in r1.effects if isinstance(e, AskSnapshot)]
    assert asks1 == [AskSnapshot(version=QueueVersion(6, 1))]
    assert r1.state.last_asked_version == QueueVersion(6, 1)

    # Re-delivering the same version is stale relative to the now-bumped
    # cloud_version, so it's gated out entirely -> no second ask.
    r2 = reduce(r1.state, CloudVersionChanged(now_ms=2, version=QueueVersion(6, 1)))
    assert r2.state is r1.state
    assert not any(isinstance(e, AskSnapshot) for e in r2.effects)


def test_snapshot_arrival_sets_last_asked() -> None:
    """A CloudSnapshot's arrival records last_asked_version so a same-version re-ask is skipped."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1), active=True)
    r1 = reduce(
        state,
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(6, 1),
            tracks=_refs(0, 1),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        ),
    )
    assert r1.state.last_asked_version == QueueVersion(6, 1)

    r2 = reduce(r1.state, CloudVersionChanged(now_ms=2, version=QueueVersion(6, 1)))
    assert r2.state is r1.state  # stale version (== cloud_version) is ignored entirely
    assert not any(isinstance(e, AskSnapshot) for e in r2.effects)


def test_snapshot_applies_at_equal_version_after_session_state() -> None:
    """
    A snapshot at the version CloudSessionState just advanced to must still apply.

    CloudSessionState advances cloud_version to the session's version, then the
    QUEUE_STATE snapshot arrives at that SAME version. It must still apply (the
    authoritative answer to our ask) — otherwise the queue never populates and an
    app->MA handoff shows nothing. Rejected only if strictly older.
    """
    state = CanonicalState()
    after_session = reduce(
        state, CloudSessionState(now_ms=1, version=QueueVersion(46, 1), track_index=2)
    ).state
    assert after_session.cloud_version == QueueVersion(46, 1)  # version pre-advanced

    result = reduce(
        after_session,
        CloudSnapshot(
            now_ms=2,
            version=QueueVersion(46, 1),
            tracks=_refs(0, 1, 2, 3),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=2,
        ),
    )
    assert len(result.state.tracks) == 4  # snapshot applied, not dropped as stale
    # trackIndex 2 -> current index 1 -> that track's Qobuz id (900000 + 1)
    assert result.state.current_id == 900001


def test_strictly_older_snapshot_is_still_dropped() -> None:
    """A snapshot strictly older than what we hold is still rejected."""
    state = CanonicalState(cloud_version=QueueVersion(50, 1), tracks=_refs(0, 1), active=True)
    result = reduce(
        state,
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(48, 1),
            tracks=_refs(9),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        ),
    )
    assert result.state is state  # dropped, older
