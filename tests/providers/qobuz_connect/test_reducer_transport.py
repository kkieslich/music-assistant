"""Golden tests for the transport lane and the audio-never-interrupted rule."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudRendererStateUpdated,
    CloudSetActive,
    CloudSetState,
    MaPause,
    MaPlayTrack,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in ids)


def _playing_state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=0,
        playing=PlayingState.PLAYING,
        active=True,
    )


def test_setstate_same_current_emits_no_audio_effect() -> None:
    """A SET_STATE echo for the track we already play must NOT restart audio."""
    state = _playing_state()
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=1000,
            current_ref=_refs(0)[0],
            next_ref=None,
        ),
    )
    assert not any(isinstance(e, MaPlayTrack) for e in result.effects)


def test_setstate_new_current_plays_that_track() -> None:
    """A SET_STATE whose current changed emits exactly one MaPlayTrack."""
    state = _playing_state()
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=_refs(2)[0],
            next_ref=None,
        ),
    )
    plays = [e for e in result.effects if isinstance(e, MaPlayTrack)]
    assert len(plays) == 1
    assert plays[0].track_id == 2
    assert result.state.current_id == 2


def test_setstate_pause_toggle_does_not_restart() -> None:
    """A play->pause command emits MaPause, never MaPlayTrack."""
    state = _playing_state()
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=None,
            playing=PlayingState.PAUSED,
            position_ms=None,
            current_ref=_refs(0)[0],
            next_ref=None,
        ),
    )
    assert any(isinstance(e, MaPause) for e in result.effects)
    assert not any(isinstance(e, MaPlayTrack) for e in result.effects)


def test_setactive_takeover_plays_current_when_playing() -> None:
    """SET_ACTIVE(true) adopts canonical current and plays it once."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=1,
        playing=PlayingState.PLAYING,
        active=False,
    )
    result = reduce(state, CloudSetActive(now_ms=1, active=True))
    assert result.state.active is True
    plays = [e for e in result.effects if isinstance(e, MaPlayTrack)]
    assert len(plays) == 1
    assert plays[0].track_id == 1


def test_setactive_takeover_paused_does_not_play() -> None:
    """SET_ACTIVE(true) on a paused session announces but does not auto-play."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=0,
        playing=PlayingState.PAUSED,
        active=False,
    )
    result = reduce(state, CloudSetActive(now_ms=1, active=True))
    assert not any(isinstance(e, MaPlayTrack) for e in result.effects)


def test_renderer_state_updated_while_inactive_folds_current_index() -> None:
    """Another renderer's broadcast advances canonical current_id via current_index (no -1)."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=0,
        playing=PlayingState.PLAYING,
        active=False,
    )
    result = reduce(
        state,
        CloudRendererStateUpdated(
            now_ms=1, renderer_id=9, playing=PlayingState.PLAYING, position_ms=1000, current_index=2
        ),
    )
    assert result.state.current_id == 2  # tracks[2].queue_item_id, no -1
    assert result.effects == ()  # not the active renderer -> no MA effect


def test_heartbeat_position_zero_is_preserved() -> None:
    """A position-only update to 0 (rewind) is not swallowed as 'absent'."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=0,
        playing=PlayingState.PLAYING,
        position_ms=500,
        active=True,
    )
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=_refs(0)[0],
            next_ref=None,
        ),
    )
    assert result.state.position_ms == 0
