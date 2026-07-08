"""Tests for the sync_types data model."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudSnapshot,
    MaPlayTrack,
    Proposal,
    ProposalKind,
    ReduceResult,
)


def test_canonical_state_defaults() -> None:
    """A fresh CanonicalState is empty, stopped, and inactive."""
    state = CanonicalState()
    assert state.cloud_version == QueueVersion(0, 0)
    assert state.tracks == ()
    assert state.current_id is None
    assert state.playing is PlayingState.STOPPED
    assert state.active is False
    assert state.pending == ()


def test_events_and_effects_are_frozen_values() -> None:
    """Events/effects are frozen dataclasses usable as immutable values."""
    ev = CloudSnapshot(
        now_ms=1000,
        version=QueueVersion(5, 1),
        tracks=(QueueTrackRef(queue_item_id=1, track_id="a"),),
        autoplay_tracks=(),
        shuffle=False,
        autoplay=False,
        track_index=0,
    )
    assert ev.version == QueueVersion(5, 1)
    eff = MaPlayTrack(track_id=1, position_ms=0)
    assert eff.track_id == 1


def test_reduce_result_holds_state_and_effects() -> None:
    """ReduceResult bundles a new state and a tuple of effects."""
    proposal = Proposal(
        action_uuid=b"\x01" * 16,
        base_version=QueueVersion(5, 1),
        kind=ProposalKind.ADD,
        target_track_ids=(1, 2),
        current_track_id=1,
    )
    state = CanonicalState(pending=(proposal,))
    result = ReduceResult(state=state, effects=(MaPlayTrack(1, 0),))
    assert result.state.pending[0].kind is ProposalKind.ADD
    assert len(result.effects) == 1
