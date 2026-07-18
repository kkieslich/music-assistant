"""Flight recorder: ring capture, incident dumps, rolling persistence."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import music_assistant.providers.qobuz_connect.flight_recorder as fr_module
from music_assistant.providers.qobuz_connect.flight_recorder import FlightRecorder
from music_assistant.providers.qobuz_connect.models import PlayingState, QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import CanonicalState, CloudSetActive


def _state() -> CanonicalState:
    return CanonicalState(
        cloud_version=QueueVersion(3, 1),
        tracks=(QueueTrackRef(queue_item_id=1, track_id="101"),),
        current_id=101,
        playing=PlayingState.PLAYING,
        active=True,
    )


async def _drain(recorder: FlightRecorder) -> None:
    while recorder._dump_tasks:
        await asyncio.gather(*recorder._dump_tasks, return_exceptions=True)


async def test_record_reduce_captures_event_effects_and_state(tmp_path: Path) -> None:
    """Reduce records land in the ring with event name, state digest and counters."""
    recorder = FlightRecorder(tmp_path)
    result = reduce(_state(), CloudSetActive(now_ms=0, active=True))
    recorder.record_reduce(CloudSetActive(now_ms=0, active=True), result)
    entry = recorder._ring[-1]
    assert entry["kind"] == "reduce"
    assert entry["event"]["_"] == "CloudSetActive"
    assert entry["state"]["active"] is True
    assert entry["state"]["cloud_v"] == "3.1"
    assert recorder._counters["event:CloudSetActive"] == 1


async def test_error_log_record_writes_incident_file(tmp_path: Path) -> None:
    """An ERROR log record triggers an incident dump including the state snapshot."""
    recorder = FlightRecorder(tmp_path, state_getter=_state)
    await recorder.start()
    try:
        logging.getLogger("music_assistant.providers.qobuz_connect.coordinator").error(
            "Qobuz Connect effect MaPlayTrack failed"
        )
        await _drain(recorder)
        incidents = list((tmp_path / "incidents").glob("incident-*-error-log.json"))
        assert len(incidents) == 1
        doc = json.loads(incidents[0].read_text())
        assert doc["state"]["current_id"] == 101
        assert doc["state"]["track_ids"] == ["101"]
        assert any(e["kind"] == "log" and e["level"] == "ERROR" for e in doc["entries"])
    finally:
        await recorder.stop()


async def test_error_incidents_are_debounced(tmp_path: Path) -> None:
    """Back-to-back errors produce one incident file but two ring entries."""
    recorder = FlightRecorder(tmp_path)
    await recorder.start()
    try:
        logger = logging.getLogger("music_assistant.providers.qobuz_connect.session")
        logger.error("boom one")
        logger.error("boom two")
        await _drain(recorder)
        assert len(list((tmp_path / "incidents").glob("incident-*.json"))) == 1
        # Both records still land in the ring even though only one dump ran.
        assert recorder._counters["log:ERROR"] == 2
    finally:
        await recorder.stop()


async def test_stop_writes_final_rolling_dump(tmp_path: Path) -> None:
    """stop() persists a final rolling dump and detaches the log tap."""
    recorder = FlightRecorder(tmp_path, state_getter=_state)
    await recorder.start()
    logging.getLogger("music_assistant.providers.qobuz_connect.session").warning(
        "reconnecting after error"
    )
    await recorder.stop()
    doc = json.loads((tmp_path / "rolling.json").read_text())
    assert doc["counters"]["log:WARNING"] == 1
    assert any("reconnecting" in e.get("msg", "") for e in doc["entries"])
    # Handler must be detached after stop — no further capture.
    logging.getLogger("music_assistant.providers.qobuz_connect.session").warning("late")
    assert recorder._counters["log:WARNING"] == 1


async def test_incident_pruning_keeps_newest(tmp_path: Path, monkeypatch: object) -> None:
    """Incident files beyond MAX_INCIDENTS are pruned oldest-first."""
    monkeypatch.setattr(fr_module, "MAX_INCIDENTS", 3)  # type: ignore[attr-defined]
    recorder = FlightRecorder(tmp_path)
    await recorder.start()
    try:
        incidents_dir = tmp_path / "incidents"
        for i in range(5):
            (incidents_dir / f"incident-2026010{i}T000000Z-old.json").write_text("{}")
        recorder.incident("manual")
        await _drain(recorder)
        remaining = sorted(p.name for p in incidents_dir.glob("incident-*.json"))
        assert len(remaining) == 3
        assert any(name.endswith("-manual.json") for name in remaining)
    finally:
        await recorder.stop()


async def test_ring_is_bounded(tmp_path: Path) -> None:
    """The ring never grows past RING_SIZE entries."""
    recorder = FlightRecorder(tmp_path)
    for i in range(fr_module.RING_SIZE + 500):
        recorder.record("tick", n=i)
    assert len(recorder._ring) == fr_module.RING_SIZE
    assert recorder._ring[-1]["n"] == fr_module.RING_SIZE + 499
