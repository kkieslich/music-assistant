"""
MA-driven scenarios: MA initiates playback/edits; the app follows. Token-gated.

These issue authenticated commands to the MA WebSocket API, so they need an MA
access token (``MA_TOKEN`` env var or ``.auth/ma_token`` from ``mint_ma_token``).
Without a token each scenario records a clean SKIP.
"""

from __future__ import annotations

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    IntegrationSession,
    ScenarioResult,
    ma_play_media,
    ma_player_command,
)
from tests.providers.qobuz_connect.protocol_capture.ma_probe import ProbeEvents

ALBUM_B_IDS = set(range(3879017, 3879037))
ALBUM_B_URI = "qobuz://album/0060694932902"
TRACK_B_URI = "qobuz://track/3879019"  # a track within album B


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
        result.check("skipped (no MA token; run mint_ma_token)", True)
        return result
    result.check("MA accepted play_media", err is None, detail=f"error={err}")
    ev = session.ma.wait_for_event(cursor, lambda e: bool(e.streams), timeout=25.0)
    result.check(
        "MA streamed the album",
        bool(ev.streams) and ev.streams[-1].track_id in ALBUM_B_IDS,
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
        result.check("skipped (no MA token; run mint_ma_token)", True)
        return result
    result.check("MA accepted play_media", err is None, detail=f"error={err}")
    ev = session.ma.wait_for_event(cursor, lambda e: bool(e.streams), timeout=25.0)
    result.check(
        "MA streamed a track from album B",
        bool(ev.streams) and ev.streams[-1].track_id in ALBUM_B_IDS,
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    session.assert_sound(result, "audio plays for MA-initiated track")
    return result


async def scenario_ma_skip(session: IntegrationSession) -> ScenarioResult:
    """Advance MA on an MA-side skip, producing sound."""
    result = ScenarioResult(scenario="ma_skip")
    await _reset_ma_inactive(session)
    if await ma_play_media(ALBUM_B_URI) == "no-token":
        result.check("skipped (no MA token)", True)
        return result
    session.wait_for_stream(session.ma.cursor(), timeout=25.0)
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
    if await ma_play_media(ALBUM_B_URI) == "no-token":
        result.check("skipped (no MA token)", True)
        return result
    session.wait_for_stream(session.ma.cursor(), timeout=25.0)
    session.assert_sound(result, "sound before MA pause")
    await ma_player_command("players/cmd/pause")
    session.assert_silence(result, "silent after MA pause")
    await ma_player_command("players/cmd/play")
    session.assert_sound(result, "sound after MA play")
    return result


async def scenario_ma_queue_edit(session: IntegrationSession) -> ScenarioResult:
    """Enqueuing a track on MA grows the cloud queue (proposal path)."""
    result = ScenarioResult(scenario="ma_queue_edit")
    await _reset_ma_inactive(session)
    if await ma_play_media(ALBUM_B_URI) == "no-token":
        result.check("skipped (no MA token)", True)
        return result
    settle = session.wait_for_stream(session.ma.cursor(), timeout=25.0)
    settle_last = settle.last_reduce()
    base_tracks = settle_last.tracks if settle_last else 0
    cursor = session.ma.cursor()
    await ma_play_media(TRACK_B_URI, option="add")

    def _grew(events: ProbeEvents) -> bool:
        last = events.last_reduce()
        return last is not None and last.tracks != base_tracks

    ev = session.ma.wait_for_event(cursor, _grew, timeout=20.0)
    last = ev.last_reduce()
    result.check(
        "MA queue changed after MA-side enqueue",
        bool(last and last.tracks != base_tracks),
        detail=f"tracks={last.tracks if last else '?'} (was {base_tracks})",
    )
    return result


SCENARIOS = {
    "initiate_album_from_ma": scenario_initiate_album_from_ma,
    "initiate_track_from_ma": scenario_initiate_track_from_ma,
    "ma_skip": scenario_ma_skip,
    "ma_pause": scenario_ma_pause,
    "ma_queue_edit": scenario_ma_queue_edit,
}
