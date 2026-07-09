"""Golden tests for the transport lane and the audio-never-interrupted rule."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    AskSnapshot,
    CanonicalState,
    CloudAddRenderer,
    CloudLoadAck,
    CloudRendererStateUpdated,
    CloudSessionState,
    CloudSetActive,
    CloudSetState,
    Disconnected,
    MaPause,
    MaPlayTrack,
    MaResyncQueue,
    ReportState,
)


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    # queue_item_id = small cloud slot; track_id = distinct large Qobuz id.
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(900000 + i)) for i in ids)


def _playing_state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900000,
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
    assert plays[0].track_id == 900002
    assert result.state.current_id == 900002


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
    """SET_ACTIVE(true) adopts canonical current, resyncs the queue, then plays it once."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900001,
        playing=PlayingState.PLAYING,
        active=False,
    )
    result = reduce(state, CloudSetActive(now_ms=1, active=True))
    assert result.state.active is True
    assert len(result.effects) == 2
    resync, play = result.effects
    assert isinstance(resync, MaResyncQueue)
    assert resync.track_ids == (900000, 900001, 900002)
    assert resync.current_track_id == 900001
    assert isinstance(play, MaPlayTrack)
    assert play.track_id == 900001


def test_setactive_takeover_paused_does_not_play() -> None:
    """SET_ACTIVE(true) on a paused session announces but does not auto-play."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
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
        current_id=900000,
        playing=PlayingState.PLAYING,
        active=False,
    )
    result = reduce(
        state,
        CloudRendererStateUpdated(
            now_ms=1, renderer_id=9, playing=PlayingState.PLAYING, position_ms=1000, current_index=2
        ),
    )
    assert result.state.current_id == 900002  # Qobuz id of tracks[2], no -1
    assert result.effects == ()  # not the active renderer -> no MA effect


def test_heartbeat_position_zero_is_preserved() -> None:
    """A position-only update to 0 (rewind) is not swallowed as 'absent'."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
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


def test_add_renderer_own_sets_own_rid() -> None:
    """CloudAddRenderer with is_own=True adopts the renderer id as own_rid."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1))
    result = reduce(
        state,
        CloudAddRenderer(now_ms=1, renderer_id=7, device_uuid=b"\x01" * 16, is_own=True),
    )
    assert result.state.own_rid == 7
    assert result.effects == ()


def test_add_renderer_not_own_leaves_own_rid_untouched() -> None:
    """CloudAddRenderer with is_own=False (another renderer) is a no-op."""
    state = CanonicalState(cloud_version=QueueVersion(5, 1), own_rid=3)
    result = reduce(
        state,
        CloudAddRenderer(now_ms=1, renderer_id=7, device_uuid=b"\x02" * 16, is_own=False),
    )
    assert result.state.own_rid == 3


def test_setstate_new_current_plays_qobuz_id() -> None:
    """CloudSetState's current_ref plays and stores the Qobuz id, not the cloud slot id."""
    state = _playing_state()
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=QueueTrackRef(queue_item_id=2, track_id="900002"),
            next_ref=None,
        ),
    )
    plays = [e for e in result.effects if isinstance(e, MaPlayTrack)]
    assert len(plays) == 1
    assert plays[0].track_id == 900002
    assert result.state.current_id == 900002


def test_transport_sets_position_anchor() -> None:
    """A CloudSetState carrying a position stamps position_anchor_ms from now_ms."""
    state = _playing_state()
    result = reduce(
        state,
        CloudSetState(
            now_ms=12345,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=2000,
            current_ref=_refs(0)[0],
            next_ref=None,
        ),
    )
    assert result.state.position_anchor_ms == 12345


def test_session_state_asks_for_snapshot() -> None:
    """CloudSessionState on a fresh state emits AskSnapshot and records last_asked_version."""
    state = CanonicalState()
    result = reduce(state, CloudSessionState(now_ms=1, version=QueueVersion(30, 1), track_index=1))
    asks = [e for e in result.effects if isinstance(e, AskSnapshot)]
    assert asks == [AskSnapshot(version=QueueVersion(30, 1))]
    assert result.state.last_asked_version == QueueVersion(30, 1)


def test_disconnect_resets_ask() -> None:
    """Disconnected resets last_asked_version so the next CloudSessionState re-asks."""
    state = CanonicalState(last_asked_version=QueueVersion(30, 1))
    result = reduce(state, Disconnected(now_ms=1))
    assert result.state.last_asked_version == QueueVersion(0, 0)

    result2 = reduce(
        result.state, CloudSessionState(now_ms=2, version=QueueVersion(30, 1), track_index=1)
    )
    asks = [e for e in result2.effects if isinstance(e, AskSnapshot)]
    assert asks == [AskSnapshot(version=QueueVersion(30, 1))]


def test_load_ack_ignores_current_not_in_canonical() -> None:
    """A load ack whose current track isn't in canonical leaves current_id untouched."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=(QueueTrackRef(queue_item_id=0, track_id="999999"),),
            queue_position=0,
        ),
    )
    assert result.state.current_id == 900000
    assert result.state.cloud_version == QueueVersion(6, 1)
    assert any(isinstance(e, ReportState) for e in result.effects)


def test_load_ack_adopts_current_in_canonical() -> None:
    """A load ack whose current track IS in canonical adopts it as current_id."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=_refs(0, 1),
            queue_position=1,
        ),
    )
    assert result.state.current_id == 900001
    assert result.state.cloud_version == QueueVersion(6, 1)
    assert any(isinstance(e, ReportState) for e in result.effects)


def test_setstate_applies_even_when_version_is_stale() -> None:
    """
    A transport command switches the current track even if its version lags cloud_version.

    The app advances the queue (higher cloud_version) while we still hold an older
    snapshot; its SET_STATE 'now playing track X' commands and skips carry the older
    queue_version. Gating them froze MA on the wrong track and made phone skips
    no-ops (live 2026-07-09). Transport is a live command, exempt from the gate.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(48, 5),
        current_id=223528143,
        playing=PlayingState.PLAYING,
        active=True,
        tracks=(QueueTrackRef(queue_item_id=1, track_id="223528143"),),
    )
    result = reduce(
        state,
        CloudSetState(
            now_ms=1,
            version=QueueVersion(48, 3),  # older than cloud_version 48.5
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=QueueTrackRef(queue_item_id=1, track_id="402969598"),
            next_ref=None,
        ),
    )
    plays = [e for e in result.effects if isinstance(e, MaPlayTrack)]
    assert len(plays) == 1
    assert plays[0].track_id == 402969598
    assert result.state.current_id == 402969598
    # and it re-asks for the queue it doesn't hold yet
    assert any(isinstance(e, AskSnapshot) for e in result.effects)
