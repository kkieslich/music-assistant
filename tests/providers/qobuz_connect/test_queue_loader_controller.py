"""Tests for the controller-based MA-origin queue load."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any, cast

from music_assistant_models.enums import PlaybackState as MAPlaybackState

from music_assistant.providers.qobuz_connect.models import (
    OutboundActionKind,
    QobuzMirror,
    QueueLoadAck,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.queue_loader import QueueLoader


class FakeBridge:
    """Minimal bridge stub exposing the attributes ``queue_loader.py`` consumes."""

    def __init__(self, items: list[Any]) -> None:
        """Hold the fixed queue items list."""
        self.logger = logging.getLogger("test")
        self.session: Any = None
        self._items = items

    def target_player_id(self) -> str | None:
        """Return a fixed player id."""
        return "player1"

    def queue_items(self, _player_id: str) -> list[Any]:
        """Return the fixed queue items."""
        return self._items

    def qobuz_track_id_for(self, item: Any) -> str | None:
        """Return the fake item's Qobuz id."""
        return cast("str | None", item.qobuz_id)


class FakeController:
    """Acks every load immediately with cloud-assigned queue item ids."""

    def __init__(self, engine_ref: dict[str, Any]) -> None:
        """Hold a back-reference to the engine so acks can resolve its futures."""
        self.is_connected = True
        self.session = object()
        self.calls: list[tuple[str, Any]] = []
        self._engine_ref = engine_ref

    async def activate_self(self) -> bool:
        """Record the activation call."""
        self.calls.append(("activate_self", None))
        return True

    async def load_queue(self, **kwargs: Any) -> bool:
        """Record the load call and immediately resolve the pending ack future."""
        self.calls.append(("load_queue", kwargs))
        engine = self._engine_ref["engine"]
        ack = QueueLoadAck(
            action_uuid=kwargs["action_uuid"],
            queue_version=QueueVersion(22, 1),
            tracks=[
                QueueTrackRef(queue_item_id=100 + i, track_id=str(tid))
                for i, tid in enumerate(kwargs["track_ids"])
            ],
        )
        engine._pending_queue_loads[kwargs["action_uuid"]].set_result(ack)
        return True

    async def play_item(self, queue_version: QueueVersion, queue_item_id: int) -> bool:
        """Record the play_item call."""
        self.calls.append(("play_item", queue_item_id))
        return True


class FakeReporter:
    """Stub reporter recording nothing beyond the call itself."""

    def set_buffer_ok(self) -> None:
        """No-op."""
        return


class FakeEngine:
    """Self-contained fake exposing exactly the attributes ``queue_loader.py`` consumes."""

    def __init__(self, items: list[Any], controller: Any) -> None:
        """Wire up the fake bridge/provider/reporter for a single test."""
        self.bridge = FakeBridge(items)
        self.provider = SimpleNamespace(controller=controller)
        self.qobuz_state = QobuzMirror(queue_version=QueueVersion(21, 1))
        self.reporter = FakeReporter()
        self._pending_queue_loads: dict[bytes, asyncio.Future[Any]] = {}
        self._is_active = True
        self._last_ma_origin_track_id: str | None = None
        self.reported = 0

    def register_outbound_action(self, kind: OutboundActionKind, _qv: QueueVersion) -> bytes:
        """Return a fixed action_uuid, asserting the LOAD kind is used."""
        assert kind == OutboundActionKind.LOAD
        return b"\x0a" * 16

    async def report_state(self) -> None:
        """Count report_state calls."""
        self.reported += 1


def _item(qobuz_id: str | None) -> Any:
    return SimpleNamespace(qobuz_id=qobuz_id)


def _queue_playing() -> Any:
    return SimpleNamespace(state=MAPlaybackState.PLAYING)


async def test_ma_origin_load_sends_full_queue_via_controller() -> None:
    """All numeric ids are packed; play_item targets the current item's cloud id."""
    ref: dict[str, Any] = {}
    controller = FakeController(ref)
    engine = FakeEngine([_item("111"), _item("222"), _item("333")], controller)
    ref["engine"] = engine
    await QueueLoader(cast("Any", engine)).send_ma_origin_load("222", _queue_playing())
    load_calls = [c for c in controller.calls if c[0] == "load_queue"]
    assert load_calls
    assert load_calls[0][1]["track_ids"] == [111, 222, 333]
    # The cloud rejects loads whose contextUuid is not exactly 16 bytes.
    assert len(load_calls[0][1]["context_uuid"]) == 16
    play_calls = [c for c in controller.calls if c[0] == "play_item"]
    assert play_calls == [("play_item", 101)]  # index 1 -> cloud id 100+1
    assert engine.reported == 1


async def test_ma_origin_load_activates_self_when_inactive() -> None:
    """An inactive engine reactivates via the controller before loading."""
    ref: dict[str, Any] = {}
    controller = FakeController(ref)
    engine = FakeEngine([_item("222")], controller)
    engine._is_active = False
    ref["engine"] = engine
    await QueueLoader(cast("Any", engine)).send_ma_origin_load("222", _queue_playing())
    assert controller.calls[0] == ("activate_self", None)


async def test_ma_origin_load_falls_back_when_controller_disabled() -> None:
    """No controller (disabled via config) -> the legacy qweb-style load path is used."""

    class FakeSession:
        """Records legacy qweb-style load calls."""

        def __init__(self) -> None:
            """Start with an empty call log."""
            self.calls: list[dict[str, Any]] = []

        async def send_queue_load_tracks(self, **kwargs: Any) -> bool:
            """Record the call."""
            self.calls.append(kwargs)
            return True

    engine = FakeEngine([_item("222")], controller=None)
    session = FakeSession()
    engine.bridge.session = session

    async def _resolve_soon() -> None:
        await asyncio.sleep(0.05)
        for future in engine._pending_queue_loads.values():
            if not future.done():
                future.set_result(None)

    task = asyncio.get_running_loop().create_task(_resolve_soon())
    await QueueLoader(cast("Any", engine)).send_ma_origin_load("222", _queue_playing())
    await task
    assert session.calls
    assert session.calls[0]["qweb_track_session"] is True


async def test_ma_origin_load_skips_and_warns_once_when_controller_disconnected(
    caplog: Any,
) -> None:
    """Controller enabled but not connected -> skip (no legacy fallback), warn once."""
    ref: dict[str, Any] = {}
    controller = FakeController(ref)
    controller.is_connected = False
    engine = FakeEngine([_item("222")], controller)
    ref["engine"] = engine
    engine._last_ma_origin_track_id = "222"

    class FakeSession:
        """Fails the test if the legacy path is used."""

        async def send_queue_load_tracks(self, **kwargs: Any) -> bool:
            """Record the call so we can assert it was never made."""
            raise AssertionError("legacy load path must not be used while reconnecting")

    engine.bridge.session = FakeSession()
    loader = QueueLoader(cast("Any", engine))

    with caplog.at_level(logging.WARNING, logger="test"):
        await loader.send_ma_origin_load("222", _queue_playing())
        await loader.send_ma_origin_load("222", _queue_playing())

    assert controller.calls == []
    assert engine._last_ma_origin_track_id is None
    warnings = [  # type: ignore[unreachable]
        r for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "controller connection unavailable" in warnings[0].message


async def test_ma_origin_load_skips_items_without_numeric_ids() -> None:
    """Radio items are dropped; a non-numeric CURRENT track skips the load."""
    ref: dict[str, Any] = {}
    controller = FakeController(ref)
    engine = FakeEngine([_item("111"), _item("radio-abc"), _item("333")], controller)
    ref["engine"] = engine
    await QueueLoader(cast("Any", engine)).send_ma_origin_load("333", _queue_playing())
    load_calls = [c for c in controller.calls if c[0] == "load_queue"]
    assert load_calls[0][1]["track_ids"] == [111, 333]

    controller.calls.clear()
    await QueueLoader(cast("Any", engine)).send_ma_origin_load("radio-abc", _queue_playing())
    assert controller.calls == []
