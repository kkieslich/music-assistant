"""
Replay recorded reference-client captures through the real inbound pipeline.

Every incoming binary frame in ``protocol_capture/.runs/*.json`` is decoded
by the real ``QobuzConnectCodec``, routed by the real ``InboundDispatcher``
into a real ``QobuzConnectCoordinator``/``reduce()`` (with fakes only at the
session/MA boundary). This connects the capture harness's ground truth to
the offline test suite: any codec/dispatch/reducer change that breaks on
real recorded traffic fails here, without Playwright or network.

Skipped when no local ``.runs`` captures exist (they contain account tokens
and are not committed).
"""

from __future__ import annotations

import contextlib
import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.inbound_dispatcher import InboundDispatcher
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec

RUNS_DIR = Path(__file__).parent / "protocol_capture" / ".runs"
CAPTURES = sorted(RUNS_DIR.glob("*__client_*.json")) if RUNS_DIR.is_dir() else []
# Excludes the http side-captures which share the directory.
CAPTURES = [c for c in CAPTURES if not c.name.endswith("_http.json")]


class _NullRunner:
    """Swallows effects; replay only exercises intake + reduction."""

    def __init__(self) -> None:
        self.count = 0

    async def run(self, effect: Any) -> None:
        self.count += 1


class _NullBridge:
    """No MA present during replay: every lookup returns empty/None."""

    def queue_items(self, player_id: str) -> list[Any]:
        return []

    def qobuz_track_id_for(self, item: Any) -> str | None:
        return None

    def get_queue(self, player_id: str) -> Any:
        return None

    def get_player(self, player_id: str) -> Any:
        return None


def _frames(capture: Path) -> list[bytes]:
    doc = json.loads(capture.read_text())
    out: list[bytes] = []
    for message in doc.get("messages", []):
        if message.get("eventType") != "binary" or message.get("direction") != "incoming":
            continue
        data = message.get("data")
        if not isinstance(data, dict):
            continue
        with contextlib.suppress(KeyError, ValueError):
            out.append(bytes(data[str(k)] for k in range(len(data))))
    return out


@pytest.mark.skipif(not CAPTURES, reason="no local protocol_capture/.runs captures")
@pytest.mark.parametrize("capture", CAPTURES, ids=lambda c: c.stem)
async def test_replay_capture_through_real_pipeline(capture: Path) -> None:
    """No recorded cloud frame may raise anywhere in the inbound pipeline."""
    codec = QobuzConnectCodec(uuid.uuid4().bytes)
    coordinator = QobuzConnectCoordinator(
        runner=_NullRunner(),  # type: ignore[arg-type]
        bridge=_NullBridge(),  # type: ignore[arg-type]
        device_uuid=uuid.uuid4().bytes,
    )
    dispatcher = InboundDispatcher(codec, coordinator.build_session_callbacks())
    dispatched = 0
    for index, raw in enumerate(_frames(capture)):
        outer = codec.decode_frame(raw)
        payload = getattr(outer, "payload", None)
        if not payload:
            continue
        # Non-QConnect payloads (auth acks etc.) are not replayable.
        batch = None
        with contextlib.suppress(Exception):
            batch = codec.decode_qconnect_batch(payload)
        if batch is None:
            continue
        for msg in batch.messages:
            try:
                await dispatcher.dispatch(msg)
            except Exception as err:
                pytest.fail(
                    f"{capture.name}: frame {index} type={msg.messageType} "
                    f"killed the pipeline: {err!r}"
                )
            dispatched += 1
    state = coordinator.state
    # Structural sanity after replaying the whole session.
    assert state.position_ms >= 0
    real_ids = [t.queue_item_id for t in state.tracks if t.queue_item_id != 0]
    assert len(real_ids) == len(set(real_ids)), "duplicate cloud queue_item_ids in canonical"
    assert dispatched > 0, f"{capture.name}: no dispatchable frames decoded"
