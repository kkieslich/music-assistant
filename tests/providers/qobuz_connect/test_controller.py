"""Tests for the QobuzConnectController state machine and verb guards."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from music_assistant.providers.qobuz_connect.controller import QobuzConnectController
from music_assistant.providers.qobuz_connect.models import (
    DeviceConfig,
    QueueVersion,
    RendererRecord,
)

DEVICE_UUID = uuid.UUID("11111111-2222-3333-4444-555555555555").bytes


class FakeSession:
    """Records controller verb sends."""

    def __init__(self) -> None:
        """Initialize a connected fake session with an empty call log."""
        self.is_connected = True
        self.calls: list[tuple[str, Any]] = []

    async def send_set_active_renderer(self, renderer_id: int) -> bool:
        """Record a SET_ACTIVE_RENDERER send."""
        self.calls.append(("set_active_renderer", renderer_id))
        return True

    async def send_queue_load_tracks(self, **kwargs: Any) -> bool:
        """Record a CTRL_SRVR_QUEUE_LOAD_TRACKS send."""
        self.calls.append(("queue_load_tracks", kwargs))
        return True

    async def send_ctrl_player_state(self, **kwargs: Any) -> bool:
        """Record a controller SetPlayerState send."""
        self.calls.append(("ctrl_player_state", kwargs))
        return True

    async def send_ctrl_set_volume(self, renderer_id: int, volume: int) -> bool:
        """Record a controller SET_VOLUME send."""
        self.calls.append(("ctrl_set_volume", (renderer_id, volume)))
        return True


def _controller() -> tuple[QobuzConnectController, FakeSession]:
    device = DeviceConfig(
        name="MA", uuid=str(uuid.uuid4()), http_port=8695, bind_address="0.0.0.0", max_quality=27
    )

    async def _refresher() -> None:
        return None

    controller = QobuzConnectController(device, DEVICE_UUID, _refresher, logging.getLogger("test"))
    fake = FakeSession()
    controller._session = fake  # type: ignore[assignment]
    return controller, fake


async def test_add_renderer_with_own_uuid_sets_own_id() -> None:
    """A renderer record with our device uuid sets own_renderer_id."""
    controller, _ = _controller()
    await controller._on_add_renderer(RendererRecord(3, DEVICE_UUID, "Local Dev"))
    await controller._on_add_renderer(RendererRecord(9, b"\x02" * 16, "Other"))
    assert controller.own_renderer_id == 3
    assert controller.is_connected is True


async def test_remove_renderer_clears_own_id() -> None:
    """Removing our renderer clears own_renderer_id and disconnects."""
    controller, _ = _controller()
    await controller._on_add_renderer(RendererRecord(3, DEVICE_UUID, "Local Dev"))
    await controller._on_remove_renderer(3)
    assert controller.own_renderer_id is None
    assert controller.is_connected is False


async def test_activate_self_sends_set_active_and_skips_when_active() -> None:
    """activate_self sends SET_ACTIVE_RENDERER once and no-ops when already active."""
    controller, fake = _controller()
    await controller._on_add_renderer(RendererRecord(3, DEVICE_UUID, "Local Dev"))
    assert await controller.activate_self() is True
    assert fake.calls == [("set_active_renderer", 3)]
    await controller._on_active_renderer_changed(3)
    fake.calls.clear()
    assert await controller.activate_self() is True
    assert fake.calls == []  # already active


async def test_verbs_guard_when_own_id_unknown() -> None:
    """Verbs return False and send nothing while own renderer id is unknown."""
    controller, fake = _controller()
    assert await controller.activate_self() is False
    assert await controller.set_volume(50) is False
    assert fake.calls == []


async def test_load_queue_passes_track_ids() -> None:
    """load_queue forwards track_ids and the caller-minted action_uuid."""
    controller, fake = _controller()
    await controller._on_add_renderer(RendererRecord(3, DEVICE_UUID, "Local Dev"))
    action_uuid = uuid.uuid4().bytes
    ok = await controller.load_queue(
        action_uuid=action_uuid,
        track_ids=[1, 2, 3],
        queue_version=QueueVersion(21, 1),
    )
    assert ok is True
    name, kwargs = fake.calls[0]
    assert name == "queue_load_tracks"
    assert kwargs["track_ids"] == [1, 2, 3]
    assert kwargs["action_uuid"] == action_uuid


async def test_play_item_sends_partial_player_state() -> None:
    """play_item sends a PLAYING player state at position 0 for the item."""
    controller, fake = _controller()
    await controller._on_add_renderer(RendererRecord(3, DEVICE_UUID, "Local Dev"))
    await controller.play_item(QueueVersion(22, 1), 1)
    name, kwargs = fake.calls[0]
    assert name == "ctrl_player_state"
    assert kwargs["queue_item_id"] == 1
    assert kwargs["position_ms"] == 0
