"""Live quality scenarios against the real Qobuz cloud and managed MA."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec
from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    IntegrationSession,
    ScenarioResult,
    ma_query,
)

QUALITY_TRACK_ID = "270789542"
QUALITY_ALBUM_URL = "https://play.qobuz.com/album/ie31cw0smabrc"


async def scenario_actual_quality(session: IntegrationSession) -> ScenarioResult:
    """Verify a 24/44.1 file remains distinct from the configured 24/192 ceiling."""
    result = ScenarioResult(scenario="actual_quality")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    session.wait_for_playing(session.ma.cursor(), timeout=25.0)

    cursor = session.ma.cursor()
    await session.q.play_track_by_index_on_album(QUALITY_ALBUM_URL, 8)
    events = session.ma.wait_for_event(
        cursor,
        lambda observed: any(
            report.kind == "file" and report.sample_rate == 44_100 and report.bit_depth == 24
            for report in observed.qualities
        ),
        timeout=30.0,
    )
    exact_reports = [
        report
        for report in events.qualities
        if report.kind == "file" and report.sample_rate == 44_100 and report.bit_depth == 24
    ]
    result.check(
        "MA reported the actual 24-bit/44.1-kHz file format",
        bool(exact_reports),
        detail=f"quality_reports={events.qualities!r}",
    )

    quality_a = ""
    quality_b = ""
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        quality_a = await session.q.displayed_audio_quality()
        quality_b = await session.q2.displayed_audio_quality() if session.q2 is not None else ""
        if _shows_24_44_1(quality_a) and _shows_24_44_1(quality_b):
            break
        await session.q.page.wait_for_timeout(500)
    result.check(
        "both Qobuz clients display 24-bit/44.1-kHz",
        _shows_24_44_1(quality_a) and _shows_24_44_1(quality_b),
        detail=f"client_a={quality_a!r} client_b={quality_b!r}",
    )
    await session.assert_in_sync(result, "both clients and MA show the quality test track")
    result.check(
        "quality test track is exact",
        session.ma_current_track_id() == QUALITY_TRACK_ID,
        detail=f"expected={QUALITY_TRACK_ID} actual={session.ma_current_track_id()}",
    )
    session.assert_sound(result, "quality test track produces BlackHole audio")
    maximums = [
        report for report in session.ma.events_since(0).qualities if report.kind == "maximum"
    ]
    result.check(
        "configured maximum remains separately reported as 24/192",
        any(report.quality == 27 for report in maximums),
        detail=f"maximum_reports={maximums!r}",
    )
    return result


async def scenario_quality_change(session: IntegrationSession) -> ScenarioResult:
    """Verify an app quality change reaches both Connect and native Qobuz configs."""
    result = ScenarioResult(scenario="quality_change")
    await session.reset_to_clean_state()
    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    session.wait_for_playing(cursor, timeout=25.0)
    session.assert_sound(result, "audio continues during quality changes")
    await session.assert_in_sync(result, "both clients and MA agree before quality changes")
    renderer_id = session.client.recorder.renderer_id(session.connect_target)
    result.check(
        "cloud advertised the exact managed renderer",
        renderer_id is not None,
        detail=f"target={session.connect_target!r} renderer_id={renderer_id!r}",
    )
    if renderer_id is None:
        return result
    codec = QobuzConnectCodec(uuid.uuid4().bytes)
    await session.q.send_qconnect_frame(codec.encode_ctrl_set_max_quality(renderer_id, 6))

    connect_quality, native_quality = await _wait_for_provider_qualities("6")
    result.check(
        "CD quality persisted to Connect and native Qobuz providers",
        connect_quality == native_quality == "6",
        detail=f"connect={connect_quality!r} native={native_quality!r}",
    )
    session.assert_sound(result, "audio continues after applying CD quality")
    await session.assert_in_sync(result, "both clients and MA agree after applying CD quality")

    await session.q.send_qconnect_frame(codec.encode_ctrl_set_max_quality(renderer_id, 27))
    connect_quality, native_quality = await _wait_for_provider_qualities("27")
    result.check(
        "24/192 quality restored in both providers",
        connect_quality == native_quality == "27",
        detail=f"connect={connect_quality!r} native={native_quality!r}",
    )
    session.assert_sound(result, "audio continues after restoring 24/192 quality")
    await session.assert_in_sync(result, "both clients and MA agree after restoring 24/192")
    return result


def _shows_24_44_1(text: str) -> bool:
    normalized = text.lower().replace(",", ".")
    return "24-bit" in normalized and "44.1 khz" in normalized


async def _provider_quality(domain: str, key: str) -> object:
    providers = await ma_query(
        "config/providers",
        {"provider_domain": domain, "include_values": True},
    )
    if not isinstance(providers, list) or len(providers) != 1:
        return None
    provider = providers[0]
    if not isinstance(provider, dict):
        return None
    values = provider.get("values")
    if not isinstance(values, dict):
        return None
    entry: Any = values.get(key)
    return entry.get("value") if isinstance(entry, dict) else entry


async def _wait_for_provider_qualities(expected: str) -> tuple[object, object]:
    """Poll both provider configs until they converge or the cloud command times out."""
    connect_quality: object = None
    native_quality: object = None
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        connect_quality = await _provider_quality("qobuz_connect", "max_quality")
        native_quality = await _provider_quality("qobuz", "quality")
        if connect_quality == native_quality == expected:
            break
        await asyncio.sleep(0.5)
    return connect_quality, native_quality


SCENARIOS = {
    "actual_quality": scenario_actual_quality,
    "quality_change": scenario_quality_change,
}
