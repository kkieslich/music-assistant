"""
Modes / volume scenarios (App -> MA).

Grounded facts (from the ref_modes capture): loop/shuffle mode changes are
controller-managed and never reach the renderer, so MA must NOT restart audio
on a mode toggle; volume DOES reach the renderer (``srvrRndrSetVolume`` ->
``CloudVolume`` -> ``MaSetVolume``).

Mute is intentionally omitted: while a Connect renderer is active, the Qobuz
web "Mute"-labelled button toggles autoplay (not mute) and never silences the
renderer, so it is not drivable as a controller->renderer mute (see
``QobuzPage.toggle_mute``). Autoplay-from-app is likewise omitted — no stable
web control exists.
"""

from __future__ import annotations

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    IntegrationSession,
    ScenarioResult,
)


async def _handoff_and_settle(session: IntegrationSession) -> None:
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_stream(session.ma.cursor(), timeout=20.0)


async def scenario_shuffle_toggle(session: IntegrationSession) -> ScenarioResult:
    """Toggle shuffle on the controller without restarting MA audio."""
    result = ScenarioResult(scenario="shuffle_toggle")
    await _handoff_and_settle(session)
    cursor = session.ma.cursor()
    await session.q.toggle_shuffle()
    await session.q.page.wait_for_timeout(4000)
    ev = session.observe(cursor)
    result.check(
        "shuffle did not restart audio",
        len([e for e in ev.effects() if e == "MaPlayTrack"]) == 0,
        detail=f"effects={ev.effects()}",
    )
    session.assert_sound(result, "audio continues through shuffle toggle")
    return result


async def scenario_repeat_toggle(session: IntegrationSession) -> ScenarioResult:
    """Toggle repeat on the controller without restarting MA audio."""
    result = ScenarioResult(scenario="repeat_toggle")
    await _handoff_and_settle(session)
    cursor = session.ma.cursor()
    await session.q.toggle_repeat()
    await session.q.page.wait_for_timeout(4000)
    ev = session.observe(cursor)
    result.check(
        "repeat did not restart audio",
        len([e for e in ev.effects() if e == "MaPlayTrack"]) == 0,
        detail=f"effects={ev.effects()}",
    )
    session.assert_sound(result, "audio continues through repeat toggle")
    return result


async def scenario_volume(session: IntegrationSession) -> ScenarioResult:
    """Apply a web-client volume change on MA (MaSetVolume) without silencing audio."""
    result = ScenarioResult(scenario="volume")
    await _handoff_and_settle(session)
    got = False
    for attempt in range(3):
        cursor = session.ma.cursor()
        await session.q.set_volume_percent(30 + attempt * 10)
        ev = session.ma.wait_for_event(cursor, lambda e: "MaSetVolume" in e.effects(), timeout=12.0)
        if "MaSetVolume" in ev.effects():
            got = True
            break
    result.check("MA applied a volume command (MaSetVolume)", got)
    session.assert_sound(result, "audio still playing at reduced volume")
    return result


SCENARIOS = {
    "shuffle_toggle": scenario_shuffle_toggle,
    "repeat_toggle": scenario_repeat_toggle,
    "volume": scenario_volume,
}
