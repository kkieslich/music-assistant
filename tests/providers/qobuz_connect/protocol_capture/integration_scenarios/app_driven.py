"""
App-driven scenarios: the web client (phone) controls, MA renders.

Grounded against real reference captures where behaviour is non-obvious:
a newly-active renderer immediately continues playing the current track;
a new queue load auto-plays the loaded current. Every scenario that expects
playback also asserts real audio on the BlackHole loopback.
"""

from __future__ import annotations

from itertools import pairwise

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    ALBUM_URL,
    IntegrationSession,
    ScenarioResult,
)
from tests.providers.qobuz_connect.protocol_capture.ma_probe import ProbeEvents

TRACK0 = 1065476
ALBUM_A_IDS = set(range(1065476, 1065492))  # Daft Punk — Discovery (ALBUM_URL)
ALBUM_B_URL = "https://play.qobuz.com/album/0060694932902"  # Eminem — The Eminem Show
ALBUM_B_IDS = set(range(3879017, 3879037))


async def scenario_handoff_fresh(session: IntegrationSession) -> ScenarioResult:
    """Handoff from a fresh track-0 state: MA becomes active, plays track 0, makes sound."""
    result = ScenarioResult(scenario="handoff_fresh")
    await session.reset_to_clean_state()
    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    ev = session.wait_for_stream(cursor, timeout=25.0)
    result.check(
        "MA became active",
        any(r.event == "CloudSetActive" and r.active for r in ev.reduces),
    )
    result.check(
        "MA streamed track 0",
        bool(ev.streams) and ev.streams[-1].track_id == TRACK0,
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio is actually playing")
    return result


async def scenario_handoff_midtrack(session: IntegrationSession) -> ScenarioResult:
    """
    Mid-track handoff must keep the CURRENT track (regression for the live bug).

    Play track 0, seek to ~50%, then hand off. MA must play track 0 (not the
    NEXT track) and make sound. This reproduces the reported bug where MA
    started from the next track.
    """
    result = ScenarioResult(scenario="handoff_midtrack")
    await session.reset_to_clean_state()
    await session.q.seek_to_fraction(0.5)
    await session.q.page.wait_for_timeout(3000)
    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    ev = session.wait_for_stream(cursor, timeout=25.0)
    result.check(
        "MA streamed the SAME track (track 0), not the next",
        bool(ev.streams) and ev.streams[-1].track_id == TRACK0,
        detail=f"streams={[s.track_id for s in ev.streams]} (expected {TRACK0})",
    )
    session.assert_sound(result, "audio is actually playing after mid-track handoff")
    return result


async def scenario_handoff_paused(session: IntegrationSession) -> ScenarioResult:
    """Handoff while paused: MA adopts current and stays silent; then play → sound."""
    result = ScenarioResult(scenario="handoff_paused")
    await session.reset_to_clean_state()
    await session.q.pause()
    await session.q.page.wait_for_timeout(2000)
    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    session.ma.wait_for_event(
        cursor, lambda ev: any(r.event == "CloudSetActive" for r in ev.reduces), timeout=20.0
    )
    session.assert_silence(result, "MA is silent while handed off paused")
    cursor2 = session.ma.cursor()
    await session.q.resume()
    session.wait_for_stream(cursor2, timeout=20.0)
    session.assert_sound(result, "audio plays after resume")
    return result


async def scenario_skip_next(session: IntegrationSession) -> ScenarioResult:
    """Advance MA to the next track on a web-client skip, with sound and no play storm."""
    result = ScenarioResult(scenario="skip_next")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    cursor = session.ma.cursor()
    await session.q.skip_next()
    ev = session.wait_for_stream(cursor, timeout=20.0)
    result.check(
        "MA advanced off track 0",
        bool(ev.streams)
        and ev.streams[-1].track_id != TRACK0
        and ev.streams[-1].track_id in ALBUM_A_IDS,
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    result.check("no play storm", len([e for e in ev.effects() if e == "MaPlayTrack"]) <= 3)
    session.assert_sound(result, "audio plays after skip")
    return result


async def scenario_skip_prev(session: IntegrationSession) -> ScenarioResult:
    """Return MA to the earlier track on a web-client previous (after one skip), with sound."""
    result = ScenarioResult(scenario="skip_prev")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    await session.q.skip_next()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    cursor = session.ma.cursor()
    await session.q.skip_previous()
    ev = session.wait_for_stream(cursor, timeout=20.0)
    result.check(
        "MA moved to a track after previous",
        bool(ev.streams) and ev.streams[-1].track_id in ALBUM_A_IDS,
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio plays after previous")
    return result


async def scenario_play_new_album(session: IntegrationSession) -> ScenarioResult:
    """
    Load a different album from the app after handoff — plays it WITH SOUND.

    Regression baseline for the reported "MA shows playing but no audio" case:
    MA must adopt the new cloud queue, stream a new-album track, and actually
    produce sound.
    """
    result = ScenarioResult(scenario="play_new_album")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    cursor = session.ma.cursor()
    await session.q.play_album_by_url(ALBUM_B_URL)
    ev = session.ma.wait_for_event(
        cursor, lambda e: any(s.track_id in ALBUM_B_IDS for s in e.streams), timeout=30.0
    )
    result.check(
        "MA streamed a new-album track",
        any(s.track_id in ALBUM_B_IDS for s in ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio actually plays for the new album")
    return result


async def scenario_play_new_track(session: IntegrationSession) -> ScenarioResult:
    """Play a specific track of another album from the app; MA plays it with sound."""
    result = ScenarioResult(scenario="play_new_track")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    cursor = session.ma.cursor()
    await session.q.play_track_by_index_on_album(ALBUM_B_URL, 3)
    ev = session.ma.wait_for_event(
        cursor, lambda e: any(s.track_id in ALBUM_B_IDS for s in e.streams), timeout=30.0
    )
    result.check(
        "MA streamed the chosen new track",
        any(s.track_id in ALBUM_B_IDS for s in ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio plays for the new track")
    return result


async def scenario_seek_scrub(session: IntegrationSession) -> ScenarioResult:
    """Keep audio flowing on an app seek without restarting the track."""
    result = ScenarioResult(scenario="seek_scrub")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    cursor = session.ma.cursor()
    await session.q.seek_to_fraction(0.7)
    session.ma.wait_for_event(
        cursor, lambda e: any(r.event == "CloudSetState" for r in e.reduces), timeout=15.0
    )
    ev = session.observe(cursor)
    result.check(
        "seek did not restart playback (<=1 MaPlayTrack)",
        len([e for e in ev.effects() if e == "MaPlayTrack"]) <= 1,
        detail=f"effects={ev.effects()}",
    )
    session.assert_sound(result, "audio continues after seek")
    return result


async def scenario_skip_to_later_position(session: IntegrationSession) -> ScenarioResult:
    """
    Skipping to a later queue track reports the NEW track at a low position, not the old one.

    Reproduces the live 2026-07-11 desync: after ~6s of track 0, jump to a much
    later track. MA's ``corrected_elapsed_time`` still reflects track 0's ~6s
    until the new stream reports, so an unguarded renderer reported the new
    track at that stale position (the app showed the wrong playing position,
    self-healing "after some commands"). MA's reports for the skipped-to track
    must start low and never jump backward.
    """
    result = ScenarioResult(scenario="skip_to_later_position")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    await session.q.page.wait_for_timeout(6000)  # let track 0 play so its position is high
    cursor = session.ma.cursor()
    await session.q.play_track_by_index_on_album(ALBUM_URL, 5)  # jump to a much later track
    ev = session.ma.wait_for_event(
        cursor,
        lambda e: any(r.track_id in ALBUM_A_IDS and r.track_id != TRACK0 for r in e.reports),
        timeout=30.0,
    )
    new_reports = [r for r in ev.reports if r.track_id in ALBUM_A_IDS and r.track_id != TRACK0]
    new_id = new_reports[-1].track_id
    positions = [r.position_ms for r in ev.reports if r.track_id == new_id]
    result.check(
        "new track's first reported position is low (not the old track's ~6s)",
        bool(positions) and positions[0] < 20000,
        detail=f"positions={positions}",
    )
    backward = [(a, b) for a, b in pairwise(positions) if b + 3000 < a]
    result.check(
        "new track position never jumps backward",
        not backward,
        detail=f"positions={positions} backward={backward}",
    )
    session.assert_sound(result, "audio plays on the skipped-to track")
    return result


async def scenario_seek_no_backward_jump(session: IntegrationSession) -> ScenarioResult:
    """
    Report the position moving forward on a forward app seek — never snapping back.

    Reproduces the live 2026-07-11 seek desync: after a seek, MA's
    ``corrected_elapsed_time`` keeps reporting the pre-seek position (counting
    up from the old anchor) until the seeked stream reports, so an unguarded
    renderer snapped the app's slider back even though MA had seeked. The
    reported position must rise toward the seek target and never drop back.
    """
    result = ScenarioResult(scenario="seek_no_backward_jump")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    await session.q.page.wait_for_timeout(4000)  # play ~4s so the pre-seek position is small
    cursor = session.ma.cursor()
    await session.q.seek_to_fraction(0.7)  # seek far forward
    session.ma.wait_for_event(cursor, lambda e: any(r.state == 2 for r in e.reports), timeout=20.0)
    await session.q.page.wait_for_timeout(4000)
    positions = [
        r.position_ms
        for r in session.observe(cursor).reports
        if r.state == 2 and r.track_id == TRACK0
    ]
    result.check(
        "seek moved the reported position forward (no snap-back to ~4s)",
        bool(positions) and max(positions) > 60000,
        detail=f"positions={positions}",
    )
    backward = [(a, b) for a, b in pairwise(positions) if b + 3000 < a]
    result.check(
        "reported position never jumps backward after seek",
        not backward,
        detail=f"positions={positions} backward={backward}",
    )
    session.assert_sound(result, "audio continues after seek")
    return result


async def scenario_queue_add(session: IntegrationSession) -> ScenarioResult:
    """Adding a track to the queue in the app is reflected on MA without a restart."""
    result = ScenarioResult(scenario="queue_add")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    settle = session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    settle_last = settle.last_reduce()
    base_tracks = settle_last.tracks if settle_last else 0
    cursor = session.ma.cursor()
    await session.q.play_album_by_url(ALBUM_B_URL)  # open album B page
    await session.q.add_track_to_queue_on_open_album(1)

    def _grew(events: ProbeEvents) -> bool:
        last = events.last_reduce()
        return last is not None and last.tracks != base_tracks

    ev = session.ma.wait_for_event(cursor, _grew, timeout=20.0)
    last = ev.last_reduce()
    result.check(
        "MA queue changed size",
        bool(last and last.tracks != base_tracks),
        detail=f"tracks={last.tracks if last else '?'} (was {base_tracks})",
    )
    session.assert_sound(result, "audio continues after queue add")
    return result


async def scenario_queue_reorder(session: IntegrationSession) -> ScenarioResult:
    """Reordering the queue in the app must not restart MA audio."""
    result = ScenarioResult(scenario="queue_reorder")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    cursor = session.ma.cursor()
    await session.q.reorder_current_forward(1)
    await session.q.page.wait_for_timeout(5000)
    ev = session.observe(cursor)
    result.check(
        "reorder did not restart the current track",
        len([e for e in ev.effects() if e == "MaPlayTrack"]) == 0,
        detail=f"effects={ev.effects()}",
    )
    session.assert_sound(result, "audio continues after reorder")
    return result


async def scenario_pause_resume(session: IntegrationSession) -> ScenarioResult:
    """Pause silences MA output; resume brings sound back."""
    result = ScenarioResult(scenario="pause_resume")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    session.assert_sound(result, "sound before pause")
    await session.q.pause()
    session.assert_silence(result, "silent after pause")
    await session.q.resume()
    session.assert_sound(result, "sound after resume")
    return result


async def scenario_pause_resume_reporting(session: IntegrationSession) -> ScenarioResult:
    """
    Pause/resume report a clean PAUSED/PLAYING state with a stable position.

    Guards the live 2026-07-11 report bugs: pausing must report PAUSED (never a
    STOPPED that jumps the slider), and resume must not flap play->pause->play.
    Uses MA's renderer-state reports (state 1=STOPPED, 2=PLAYING, 3=PAUSED).
    """
    result = ScenarioResult(scenario="pause_resume_reporting")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)
    await session.q.page.wait_for_timeout(7000)  # play ~7s so the position is clearly non-zero

    cursor = session.ma.cursor()
    await session.q.pause()
    await session.q.page.wait_for_timeout(4000)
    reports = session.observe(cursor).reports
    states = [r.state for r in reports]
    result.check(
        "pause reports PAUSED (3), never STOPPED (1)",
        3 in states and 1 not in states,
        detail=f"report states={states}",
    )
    paused_pos = [r.position_ms for r in reports if r.state == 3]
    result.check(
        "paused position is non-zero and frozen (no snap-back)",
        bool(paused_pos) and min(paused_pos) > 3000 and (max(paused_pos) - min(paused_pos)) < 500,
        detail=f"paused positions={paused_pos}",
    )

    cursor = session.ma.cursor()
    await session.q.resume()
    await session.q.page.wait_for_timeout(4000)
    states2 = [r.state for r in session.observe(cursor).reports]
    result.check(
        "resume reports PLAYING (2), no STOPPED (1) bounce",
        2 in states2 and 1 not in states2,
        detail=f"report states={states2}",
    )
    return result


SCENARIOS = {
    "handoff_fresh": scenario_handoff_fresh,
    "handoff_midtrack": scenario_handoff_midtrack,
    "handoff_paused": scenario_handoff_paused,
    "skip_next": scenario_skip_next,
    "skip_prev": scenario_skip_prev,
    "play_new_album": scenario_play_new_album,
    "play_new_track": scenario_play_new_track,
    "seek_scrub": scenario_seek_scrub,
    "skip_to_later_position": scenario_skip_to_later_position,
    "seek_no_backward_jump": scenario_seek_no_backward_jump,
    "queue_add": scenario_queue_add,
    "queue_reorder": scenario_queue_reorder,
    "pause_resume": scenario_pause_resume,
    "pause_resume_reporting": scenario_pause_resume_reporting,
}
