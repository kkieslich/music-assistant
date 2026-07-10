"""
Multi-action session scenarios: long, chained interactions without resets.

Single-action scenarios each start from a clean reset, so they cannot catch
state that drifts across a longer session. These scenarios perform many
interactions back-to-back and, after every step, assert BOTH that real audio
is playing AND that the Qobuz app and MA agree on the current track. This is
where "silent after a skip" and "app shows a different track than MA" surface.
"""

from __future__ import annotations

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    IntegrationSession,
    ScenarioResult,
)

ALBUM_B_URL = "https://play.qobuz.com/album/0060694932902"  # Eminem — The Eminem Show


async def scenario_marathon_app(session: IntegrationSession) -> ScenarioResult:
    """
    Run a long controller-driven session and check sound + sync each step.

    Handoff, many skips, new album, reorder, seek — asserting real audio and
    app/MA track agreement after every step. Reproduces cumulative drift and
    the "silent after a skip" report that a single skip-from-clean never
    triggers.
    """
    result = ScenarioResult(scenario="marathon_app")
    await session.reset_to_clean_state()

    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    session.wait_for_playing(cursor, timeout=25.0)
    await session.assert_playing_and_synced(result, "handoff")

    for i in range(3):
        cursor = session.ma.cursor()
        await session.q.skip_next()
        session.wait_for_playing(cursor, timeout=20.0)
        await session.assert_playing_and_synced(result, f"skip#{i + 1}")

    cursor = session.ma.cursor()
    await session.q.play_album_by_url(ALBUM_B_URL)
    session.wait_for_playing(cursor, timeout=30.0)
    await session.assert_playing_and_synced(result, "new_album")

    cursor = session.ma.cursor()
    await session.q.reorder_current_forward(1)
    await session.q.page.wait_for_timeout(4000)
    await session.assert_playing_and_synced(result, "reorder_app")

    for i in range(2):
        cursor = session.ma.cursor()
        await session.q.skip_next()
        session.wait_for_playing(cursor, timeout=20.0)
        await session.assert_playing_and_synced(result, f"skip_after_reorder#{i + 1}")

    cursor = session.ma.cursor()
    await session.q.seek_to_fraction(0.6)
    await session.q.page.wait_for_timeout(3000)
    await session.assert_playing_and_synced(result, "seek")

    cursor = session.ma.cursor()
    await session.q.skip_next()
    session.wait_for_playing(cursor, timeout=20.0)
    await session.assert_playing_and_synced(result, "final_skip")
    return result


async def scenario_skip_storm(session: IntegrationSession) -> ScenarioResult:
    """
    Stress the transport lane with rapid repeated skips after handoff.

    Looks for a skip that lands silent or drifts the app/MA current apart.
    """
    result = ScenarioResult(scenario="skip_storm")
    await session.reset_to_clean_state()
    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    session.wait_for_playing(cursor, timeout=25.0)
    await session.assert_playing_and_synced(result, "handoff")

    for i in range(6):
        cursor = session.ma.cursor()
        await session.q.skip_next()
        session.wait_for_playing(cursor, timeout=20.0)
        await session.assert_playing_and_synced(result, f"skip#{i + 1}")
    return result


SCENARIOS = {
    "marathon_app": scenario_marathon_app,
    "skip_storm": scenario_skip_storm,
}
