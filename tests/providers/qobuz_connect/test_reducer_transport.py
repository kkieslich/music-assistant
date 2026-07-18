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
    CloudSnapshot,
    Disconnected,
    MaPause,
    MaPlayTrack,
    MaResyncQueue,
    MaTransportChanged,
)


def _snapshot(tracks: tuple[QueueTrackRef, ...], track_index: int) -> CloudSnapshot:
    return CloudSnapshot(
        now_ms=1,
        version=QueueVersion(6, 1),
        tracks=tracks,
        autoplay_tracks=(),
        shuffle=False,
        autoplay=False,
        track_index=track_index,
    )


def test_active_snapshot_keeps_current_track() -> None:
    """
    While MA is the active renderer, a pulled snapshot must not move current.

    The active renderer OWNS 'current' — the cloud follows its report. A
    snapshot pulled after a queue reorder carries a stale/shifted trackIndex;
    honouring it flipped canonical current to a track MA was not playing,
    which then diverged the app and MA on the next skip (live 2026-07-10
    bidirectional-edit drift). Current stays put as long as it is still in the
    queue.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900000,
        playing=PlayingState.PLAYING,
        active=True,
    )
    # Snapshot pointer resolves to a DIFFERENT track (index 3 -> 1-indexed ->
    # tracks[2] == 900002), but 900000 is still present in the queue.
    result = reduce(state, _snapshot(_refs(0, 1, 2), track_index=3))
    assert result.state.current_id == 900000  # kept, not overridden to 900002
    assert tuple(t.queue_item_id for t in result.state.tracks) == (0, 1, 2)


def test_inactive_snapshot_adopts_pointer() -> None:
    """When NOT the active renderer, MA follows the snapshot's current pointer."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900000,
        active=False,
    )
    result = reduce(state, _snapshot(_refs(0, 1, 2), track_index=3))
    assert result.state.current_id == 900002  # index 3 -> tracks[2]


def test_active_snapshot_adopts_pointer_when_current_gone() -> None:
    """If MA's current track is no longer in the queue, adopt the snapshot pointer."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900000,
        playing=PlayingState.PLAYING,
        active=True,
    )
    # New queue does not contain 900000 anymore -> fall back to the pointer.
    result = reduce(state, _snapshot(_refs(7, 8, 9), track_index=1))
    assert result.state.current_id == 900007  # index 1 -> tracks[0]


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


def test_app_load_ack_adopts_the_new_queue() -> None:
    """
    An app-initiated queue load replaces canonical tracks (MA follows like a web client).

    SRVR_CTRL_QUEUE_TRACKS_LOADED broadcasts the full new track list whenever anyone
    loads content; MA must adopt it rather than stay on its stale snapshot (the
    live 2026-07-09 wrong-track bug).
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
        active=True,
    )
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=_refs(7, 8, 9),
            queue_position=1,
        ),
    )
    assert tuple(t.queue_item_id for t in result.state.tracks) == (7, 8, 9)
    assert result.state.current_id == 900008  # queue_position 1 -> tracks[1] Qobuz id
    assert result.state.cloud_version == QueueVersion(6, 1)
    assert any(isinstance(e, MaResyncQueue) for e in result.effects)


def test_load_ack_while_playing_plays_new_current() -> None:
    """
    A new queue load on an active, playing renderer switches playback.

    A real web renderer auto-plays the newly loaded queue's current track on
    SRVR_CTRL_QUEUE_TRACKS_LOADED (ref_new_album capture). Resyncing the list
    alone left MA streaming the previous album (live 2026-07-09
    "MA showed a broken state after I played another song").
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
        playing=PlayingState.PLAYING,
        active=True,
    )
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=_refs(7, 8, 9),
            queue_position=0,
        ),
    )
    plays = [e for e in result.effects if isinstance(e, MaPlayTrack)]
    assert len(plays) == 1
    assert plays[0].track_id == 900007  # tracks[0] Qobuz id of the new queue
    assert result.state.current_id == 900007
    assert any(isinstance(e, MaResyncQueue) for e in result.effects)
    # The new track restarts audio at 0; canonical anchors there and settles so
    # MA's lagging position can't briefly show the new track at the old spot.
    assert result.state.position_ms == 0
    assert result.state.settling_position is True


def test_load_ack_same_current_does_not_restart() -> None:
    """A reorder (shuffle) keeps the current track — audio must not restart."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1, 2),
        current_id=900000,
        playing=PlayingState.PLAYING,
        active=True,
    )
    # New order, but the current track (queue_position -> id 900000) is unchanged.
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=_refs(0, 2, 1),
            queue_position=0,
        ),
    )
    assert not any(isinstance(e, MaPlayTrack) for e in result.effects)


def test_load_ack_while_inactive_does_not_play() -> None:
    """A queue load while we are not the active renderer never starts audio."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
        playing=PlayingState.PLAYING,
        active=False,
    )
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=_refs(7, 8),
            queue_position=0,
        ),
    )
    assert not any(isinstance(e, MaPlayTrack) for e in result.effects)


def test_pause_captures_live_position() -> None:
    """
    Pausing freezes the position at base+elapsed, not the stale anchor base.

    During PLAYING the reporter interpolates position from (position_ms, anchor).
    Pausing must capture that live position, or the app's slider snaps back to
    the base (live 2026-07-11 "slider moves back the wrong 2-3s").
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
        playing=PlayingState.PLAYING,
        position_ms=1000,
        position_anchor_ms=1000,
        active=True,
    )
    result = reduce(
        state,
        CloudSetState(
            now_ms=11000,
            version=None,
            playing=PlayingState.PAUSED,
            position_ms=None,
            current_ref=None,
            next_ref=None,
        ),
    )
    assert result.state.playing is PlayingState.PAUSED
    assert result.state.position_ms == 11000  # 1000 base + 10000ms elapsed while playing


def test_paused_renderer_keeps_paused_when_player_idles() -> None:
    """
    Keep PAUSED (frozen position) when the idling player emits STOPPED.

    Pausing stops the flow stream, so the MA player idles (STOPPED) a moment
    after the user pauses — but the renderer's logical state is PAUSED.
    Reporting STOPPED with the stream's overshot position made the app flap
    play->pause and jump the slider (live 2026-07-11). While paused, a STOPPED
    transition from the idling player must keep PAUSED and freeze the position.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
        playing=PlayingState.PAUSED,
        position_ms=76000,
        position_anchor_ms=1000,
        active=True,
    )
    result = reduce(
        state,
        MaTransportChanged(
            now_ms=2000, playing=PlayingState.STOPPED, current_track_id=None, position_ms=80565
        ),
    )
    assert result.state.playing is PlayingState.PAUSED  # not overridden to STOPPED
    assert result.state.position_ms == 76000  # frozen at the pause point, not 80565


def test_playing_renderer_transient_stop_keeps_playing() -> None:
    """
    A transient STOPPED while PLAYING with a current track keeps PLAYING.

    Resume and track changes briefly idle the flow stream; reporting the
    transient STOPPED made the app bounce play->pause->play on resume (live
    2026-07-11). With a current track held, keep PLAYING and freeze position.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=_refs(0, 1),
        current_id=900000,
        playing=PlayingState.PLAYING,
        position_ms=1000,
        active=True,
    )
    result = reduce(
        state,
        MaTransportChanged(
            now_ms=2000, playing=PlayingState.STOPPED, current_track_id=None, position_ms=2000
        ),
    )
    assert result.state.playing is PlayingState.PLAYING
    assert result.state.position_ms == 1000


def test_genuine_stop_on_empty_queue_reports_stopped() -> None:
    """With no current track (empty queue), a STOPPED transition is a real stop."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(),
        current_id=None,
        playing=PlayingState.PLAYING,
        active=True,
    )
    result = reduce(
        state,
        MaTransportChanged(
            now_ms=2000, playing=PlayingState.STOPPED, current_track_id=None, position_ms=0
        ),
    )
    assert result.state.playing is PlayingState.STOPPED


def test_empty_load_ack_keeps_tracks() -> None:
    """A load ack with no tracks just bumps the version (doesn't wipe the queue)."""
    state = CanonicalState(
        cloud_version=QueueVersion(5, 1), tracks=_refs(0, 1), current_id=900000, active=True
    )
    result = reduce(
        state,
        CloudLoadAck(
            now_ms=1,
            version=QueueVersion(6, 1),
            action_uuid=b"\xcc" * 16,
            tracks=(),
            queue_position=0,
        ),
    )
    assert tuple(t.queue_item_id for t in result.state.tracks) == (0, 1)
    assert result.state.cloud_version == QueueVersion(6, 1)


def test_seek_ignores_stale_ma_position_until_converged() -> None:
    """
    After a cloud seek, MA's lagging position must not drag the slider back.

    ``corrected_elapsed_time`` keeps reporting the pre-seek position (still
    counting up from the old anchor) for ~1s until MA's seeked stream reports
    back. Adopting it snapped the app's slider backward while MA had actually
    seeked (live 2026-07-11 "the slider jumps back but MA actually seeked").
    The commanded target holds until MA's report converges to it.
    """
    r1 = reduce(
        _playing_state(),
        CloudSetState(
            now_ms=1000,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=60000,
            current_ref=_refs(0)[0],
            next_ref=None,
        ),
    )
    assert r1.state.position_ms == 60000  # commanded seek target

    # MA still reports the OLD position (~30s) on the SAME track: stale lag.
    r2 = reduce(
        r1.state,
        MaTransportChanged(
            now_ms=1500, playing=PlayingState.PLAYING, current_track_id=900000, position_ms=30000
        ),
    )
    assert r2.state.position_ms == 60000  # held, not dragged back to 30000

    # MA's seeked stream now reports ~60s: converged -> adopt MA as truth.
    r3 = reduce(
        r2.state,
        MaTransportChanged(
            now_ms=2500, playing=PlayingState.PLAYING, current_track_id=900000, position_ms=60050
        ),
    )
    assert r3.state.position_ms == 60050


def test_skip_ignores_stale_ma_position_until_converged() -> None:
    """
    After a cloud skip, MA's stale elapsed must not show the new track at the old position.

    Skipping to a later track resets audio to 0, but MA's ``elapsed_time``
    still reflects the previous track (~50s, not reset until the new stream
    reports). Adopting it showed the new track at a wrong, too-high position
    (live 2026-07-11 "new track played but app showed the wrong position, after
    some commands it worked again"). The commanded track+0 hold until MA
    catches up.
    """
    r1 = reduce(
        _playing_state(),
        CloudSetState(
            now_ms=1000,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=_refs(2)[0],
            next_ref=None,
        ),
    )
    assert r1.state.current_id == 900002
    assert r1.state.position_ms == 0

    # MA still reports the OLD track at its overshot position: stale lag.
    r2 = reduce(
        r1.state,
        MaTransportChanged(
            now_ms=1400, playing=PlayingState.PLAYING, current_track_id=900000, position_ms=50000
        ),
    )
    assert r2.state.current_id == 900002  # not flipped back to the old track
    assert r2.state.position_ms == 0  # not dragged to 50000

    # MA's new stream now reports near 0 on the new track: converged.
    r3 = reduce(
        r2.state,
        MaTransportChanged(
            now_ms=2400, playing=PlayingState.PLAYING, current_track_id=900002, position_ms=200
        ),
    )
    assert r3.state.current_id == 900002
    assert r3.state.position_ms == 200


def test_ma_transport_adopts_position_when_not_settling() -> None:
    """
    Absent a just-issued cloud command, MA is the source of truth for position.

    Natural progression and MA-origin seeks flow straight through — the
    settling guard must only suppress the lag window right after a cloud
    seek/track-change, never ordinary MA reports.
    """
    result = reduce(
        _playing_state(),
        MaTransportChanged(
            now_ms=5000, playing=PlayingState.PLAYING, current_track_id=900000, position_ms=45000
        ),
    )
    assert result.state.position_ms == 45000


def test_settling_times_out_and_adopts_ma() -> None:
    """
    If MA never converges, adopt its report after the settle timeout (bounded suppression).

    The suppression window is capped so a genuinely divergent MA (e.g. a track
    shorter than the seek target) can't freeze the reported position forever.
    """
    r1 = reduce(
        _playing_state(),
        CloudSetState(
            now_ms=1000,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=60000,
            current_ref=_refs(0)[0],
            next_ref=None,
        ),
    )
    # Well past the settle timeout, still reporting a non-converging position.
    result = reduce(
        r1.state,
        MaTransportChanged(
            now_ms=1000 + 5001,
            playing=PlayingState.PLAYING,
            current_track_id=900000,
            position_ms=30000,
        ),
    )
    assert result.state.position_ms == 30000  # adopted after timeout


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


def test_disconnect_resets_cloud_version_so_a_session_reset_is_not_stale() -> None:
    """
    A reconnect must accept a session whose queue_version restarted lower.

    queue_version.major is a small session-scoped counter (21/60/84 in the
    captures); when the cloud recreates the session it can hand out a LOWER
    major than the one we last held. Keeping the old cloud_version across a
    disconnect made every post-reconnect event "stale" — the provider went
    permanently deaf until restart.
    """
    state = CanonicalState(
        cloud_version=QueueVersion(84, 1),
        tracks=_refs(0, 1),
        last_asked_version=QueueVersion(84, 1),
    )
    dropped = reduce(state, Disconnected(now_ms=1))
    assert dropped.state.cloud_version == QueueVersion(0, 0)

    rejoined = reduce(
        dropped.state, CloudSessionState(now_ms=2, version=QueueVersion(2, 1), track_index=1)
    )
    asks = [e for e in rejoined.effects if isinstance(e, AskSnapshot)]
    assert asks == [AskSnapshot(version=QueueVersion(2, 1))]
    assert rejoined.state.cloud_version == QueueVersion(2, 1)
