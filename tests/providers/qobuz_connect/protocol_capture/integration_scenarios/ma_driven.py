"""
MA-driven scenarios: MA initiates playback/edits; the app follows. Token-gated.

These issue authenticated commands to the MA WebSocket API, so they need an MA
access token (``MA_TOKEN`` env var or ``.auth/ma_token`` from ``mint_ma_token``).
Without a token each scenario records a clean SKIP.
"""

from __future__ import annotations

from collections import Counter

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    IntegrationSession,
    ScenarioResult,
    ma_play_media,
    ma_player_command,
)
from tests.providers.qobuz_connect.protocol_capture.ma_probe import ProbeEvents

ALBUM_B_FIRST_TRACK_ID = "3879017"
ALBUM_B_URI = "qobuz://album/0060694932902"
TRACK_B_URI = "qobuz://track/3879019"  # a track within album B
TRACK_B_ID = "3879019"


async def _reset_ma_inactive(session: IntegrationSession) -> None:
    await session.reset_to_clean_state()
    await session.q.select_local_output()
    await session.q.page.wait_for_timeout(3000)


async def scenario_initiate_album_from_ma(session: IntegrationSession) -> ScenarioResult:
    """Start a Qobuz album on MA: it plays with sound and claims the cloud renderer role."""
    result = ScenarioResult(scenario="initiate_album_from_ma")
    await _reset_ma_inactive(session)
    cursor = session.ma.cursor()
    err = await ma_play_media(ALBUM_B_URI)
    if err == "no-token":
        result.skip("no MA token; run mint_ma_token")
        return result
    result.check("MA accepted play_media", err is None, detail=f"error={err}")

    def playback_started(event: ProbeEvents) -> bool:
        trace = event.last_reduce()
        return bool(event.streams) and (
            "PushSetActive" in event.effects() or bool(trace and trace.active)
        )

    ev = session.ma.wait_for_event(
        cursor,
        playback_started,
        timeout=25.0,
    )
    result.check(
        "MA streamed the album",
        any(str(stream.track_id) == ALBUM_B_FIRST_TRACK_ID for stream in ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    last = ev.last_reduce()
    result.check(
        "MA claimed active renderer role",
        "PushSetActive" in ev.effects() or bool(last and last.active),
        detail=f"effects={sorted(set(ev.effects()))}",
    )
    session.assert_sound(result, "audio plays for MA-initiated album")
    return result


async def scenario_initiate_track_from_ma(session: IntegrationSession) -> ScenarioResult:
    """Start a specific Qobuz track on MA: it plays that track with sound."""
    result = ScenarioResult(scenario="initiate_track_from_ma")
    await _reset_ma_inactive(session)
    cursor = session.ma.cursor()
    err = await ma_play_media(TRACK_B_URI)
    if err == "no-token":
        result.skip("no MA token; run mint_ma_token")
        return result
    result.check("MA accepted play_media", err is None, detail=f"error={err}")
    ev = session.ma.wait_for_event(cursor, lambda e: bool(e.streams), timeout=25.0)
    result.check(
        "MA streamed the requested track",
        any(str(stream.track_id) == TRACK_B_ID for stream in ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio plays for MA-initiated track")
    await session.assert_in_sync(result, "both apps and MA show requested track")
    result.check(
        "requested track remained current",
        session.ma_current_track_id() == TRACK_B_ID,
        detail=f"requested={TRACK_B_ID} current={session.ma_current_track_id()}",
    )
    return result


async def scenario_ma_skip(session: IntegrationSession) -> ScenarioResult:
    """Advance MA on an MA-side skip, producing sound."""
    result = ScenarioResult(scenario="ma_skip")
    await _reset_ma_inactive(session)
    cursor = session.ma.cursor()
    if await ma_play_media(ALBUM_B_URI) == "no-token":
        result.skip("no MA token")
        return result
    session.wait_for_stream(cursor, timeout=25.0)
    cursor = session.ma.cursor()
    await ma_player_command("players/cmd/next")
    ev = session.wait_for_stream(cursor, timeout=20.0)
    result.check(
        "MA advanced after MA-side skip",
        bool(ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio plays after MA-side skip")
    return result


async def scenario_ma_pause(session: IntegrationSession) -> ScenarioResult:
    """Silence output on an MA-side pause; resume sound on an MA-side play."""
    result = ScenarioResult(scenario="ma_pause")
    await _reset_ma_inactive(session)
    cursor = session.ma.cursor()
    if await ma_play_media(ALBUM_B_URI) == "no-token":
        result.skip("no MA token")
        return result
    session.wait_for_stream(cursor, timeout=25.0)
    session.assert_sound(result, "sound before MA pause")
    await ma_player_command("players/cmd/pause")
    session.assert_silence(result, "silent after MA pause")
    await ma_player_command("players/cmd/play")
    session.assert_sound(result, "sound after MA play")
    return result


async def scenario_ma_queue_edit(session: IntegrationSession) -> ScenarioResult:
    """Enqueuing a track on MA adds that exact occurrence to both cloud clients."""
    result = ScenarioResult(scenario="ma_queue_edit")
    await _reset_ma_inactive(session)
    cursor = session.ma.cursor()
    if await ma_play_media(ALBUM_B_URI) == "no-token":
        result.skip("no MA token")
        return result
    session.wait_for_stream(cursor, timeout=25.0)

    before_a: tuple[str, ...] = ()
    before_b: tuple[str, ...] = ()
    for _ in range(20):
        before_a = session.client.recorder.cloud_queue_track_ids()
        before_b = (
            session.observer.recorder.cloud_queue_track_ids()
            if session.observer is not None
            else ()
        )
        if (
            before_a == before_b
            and len(before_a) == 20
            and before_a[0] == ALBUM_B_FIRST_TRACK_ID
            and TRACK_B_ID in before_a
        ):
            break
        await session.q.page.wait_for_timeout(500)
    result.check(
        "both clients start from the same cloud queue",
        before_a == before_b
        and len(before_a) == 20
        and before_a[0] == ALBUM_B_FIRST_TRACK_ID
        and TRACK_B_ID in before_a,
        detail=f"a={before_a} b={before_b}",
    )
    await ma_play_media(TRACK_B_URI, option="add")
    converged = False
    after_a: tuple[str, ...] = ()
    after_b: tuple[str, ...] = ()
    for _ in range(20):
        await session.q.page.wait_for_timeout(1000)
        after_a = session.client.recorder.cloud_queue_track_ids()
        after_b = (
            session.observer.recorder.cloud_queue_track_ids()
            if session.observer is not None
            else ()
        )
        added = Counter(after_a) - Counter(before_a)
        if after_a == after_b and added == Counter({TRACK_B_ID: 1}):
            converged = True
            break
    result.check(
        "both clients received exactly the requested added occurrence",
        converged,
        detail=f"before={before_a} after_a={after_a} after_b={after_b}",
    )
    return result


SCENARIOS = {
    "initiate_album_from_ma": scenario_initiate_album_from_ma,
    "initiate_track_from_ma": scenario_initiate_track_from_ma,
    "ma_skip": scenario_ma_skip,
    "ma_pause": scenario_ma_pause,
    "ma_queue_edit": scenario_ma_queue_edit,
}
