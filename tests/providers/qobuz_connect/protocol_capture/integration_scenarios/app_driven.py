"""
App-driven scenarios: the web client (phone) controls, MA renders.

Grounded against real reference captures where behaviour is non-obvious:
a newly-active renderer immediately continues playing the current track;
a new queue load auto-plays the loaded current. Every scenario that expects
playback also asserts real audio on the BlackHole loopback.
"""

from __future__ import annotations

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    IntegrationSession,
    ScenarioResult,
)

TRACK0 = 1065476
ALBUM_A_IDS = set(range(1065476, 1065492))  # Daft Punk — Discovery
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
        bool(ev.streams) and ev.streams[-1].track_id != TRACK0 and ev.streams[-1].track_id in ALBUM_A_IDS,
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


SCENARIOS = {
    "handoff_fresh": scenario_handoff_fresh,
    "handoff_midtrack": scenario_handoff_midtrack,
    "handoff_paused": scenario_handoff_paused,
    "skip_next": scenario_skip_next,
    "skip_prev": scenario_skip_prev,
}
