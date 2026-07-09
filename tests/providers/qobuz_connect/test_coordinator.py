"""Coordinator wires reduce() to effects with serialized intake."""

from __future__ import annotations

import asyncio
from typing import Any

from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueTrackRef,
    QueueVersion,
    RendererRecord,
)
from music_assistant.providers.qobuz_connect.sync_types import (
    CloudSetActive,
    CloudSetState,
    CloudSnapshot,
    CloudTracksAdded,
    MaPlayTrack,
    MaQueueChanged,
    MaResyncQueue,
    PushAdd,
)

OUR_DEVICE_UUID = b"\x01" * 16


class _RecordingRunner:
    """Records every effect it's asked to run; yields once per run() to expose races."""

    def __init__(self) -> None:
        """Start with an empty effect log."""
        self.effects: list[Any] = []

    async def run(self, effect: Any) -> None:
        """Record the effect, yielding control once first."""
        await asyncio.sleep(0)
        self.effects.append(effect)


class _FakeQueue:
    """Minimal stand-in for MA's PlayerQueue, exposing only what the coordinator reads."""

    def __init__(
        self,
        *,
        current_item: Any = None,
        state: str = "playing",
        elapsed: float = 0.0,
        repeat: str = "off",
    ) -> None:
        """Hold the fixed fields the coordinator's on_ma_* handlers read."""
        self.current_item = current_item
        self.state = state
        self.corrected_elapsed_time = elapsed
        self.repeat_mode = repeat
        self.shuffle_enabled = False


class _FakePlayer:
    """Minimal stand-in for MA's Player, exposing only volume fields."""

    def __init__(self, *, volume_level: int = 50, volume_muted: bool = False) -> None:
        """Hold the fixed volume fields the coordinator's on_ma_volume_event reads."""
        self.volume_level = volume_level
        self.volume_muted = volume_muted


class _FakeBridge:
    """Small recorder/stub for the MA bridge surface the coordinator touches."""

    def __init__(self) -> None:
        """Start with an empty queue-items list and no queue/player configured."""
        self.items: list[Any] = []
        self.queue: _FakeQueue | None = None
        self.player: _FakePlayer | None = None

    def queue_items(self, player_id: str) -> list[Any]:
        """Return the fixed queue-items list."""
        return self.items

    def qobuz_track_id_for(self, item: Any) -> str | None:
        """Return the fake item's Qobuz track id."""
        return item.get("track_id") if isinstance(item, dict) else None

    def get_queue(self, player_id: str) -> _FakeQueue | None:
        """Return the fixed fake queue."""
        return self.queue

    def get_player(self, player_id: str) -> _FakePlayer | None:
        """Return the fixed fake player."""
        return self.player


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in ids)


def _coordinator(
    *, controller_enabled: bool = True, bridge: _FakeBridge | None = None
) -> tuple[QobuzConnectCoordinator, _RecordingRunner, _FakeBridge]:
    runner = _RecordingRunner()
    bridge = bridge if bridge is not None else _FakeBridge()
    coord = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=bridge,  # type: ignore[arg-type]
        device_uuid=OUR_DEVICE_UUID,
        controller_enabled=controller_enabled,
        now=lambda: 1,
    )
    return coord, runner, bridge


async def test_snapshot_then_activate_takes_over() -> None:
    """A cloud snapshot + a playing SET_STATE + SET_ACTIVE takes the renderer over."""
    coord, runner, _bridge = _coordinator()
    await coord._submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1, 2),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=2,
        )
    )
    await coord._submit(
        CloudSetState(
            now_ms=2,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=500,
            current_ref=_refs(2)[0],
            next_ref=None,
        )
    )
    await coord._submit(CloudSetActive(now_ms=3, active=True))
    assert coord.state.active is True
    assert any(isinstance(e, MaResyncQueue) for e in runner.effects)
    assert any(isinstance(e, MaPlayTrack) for e in runner.effects)


async def test_ma_append_pushes_add_and_cloud_echo_confirms() -> None:
    """An MA-origin append emits PushAdd; the matching cloud echo clears the pending proposal."""
    coord, runner, bridge = _coordinator()
    await coord._submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        )
    )
    bridge.items = [{"track_id": "100"}, {"track_id": "101"}]
    await coord.on_ma_queue_event("player")
    assert any(isinstance(e, PushAdd) for e in runner.effects)
    pending_before = coord.state.pending
    assert len(pending_before) == 1
    proposal = pending_before[0]
    assert proposal.target_track_ids == (100, 101)

    await coord._submit(
        CloudTracksAdded(
            now_ms=2,
            version=QueueVersion(6, 1),
            action_uuid=proposal.action_uuid,
            tracks=(QueueTrackRef(queue_item_id=1, track_id="101"),),
            after_index=1,
        )
    )
    pending_after = coord.state.pending
    assert pending_after == ()
    assert tuple(t.track_id for t in coord.state.tracks) == ("100", "101")


async def test_concurrent_submits_serialize_without_losing_a_proposal() -> None:
    """Two concurrent _submit calls process one at a time — no lost-update race on pending."""
    coord, _runner, _bridge = _coordinator()
    await coord._submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        )
    )

    async def _append(action_uuid: bytes, extra_track_id: int) -> None:
        await coord._submit(
            MaQueueChanged(
                now_ms=2,
                action_uuid=action_uuid,
                track_ids=(100, extra_track_id),
                current_track_id=100,
                resolvable=frozenset({100, extra_track_id}),
            )
        )

    await asyncio.gather(
        _append(b"\xaa" * 16, 101),
        _append(b"\xbb" * 16, 102),
    )
    # Without the lock, the second writer's `pending=(*state.pending, proposal)`
    # would overwrite the first's under interleaving and drop a proposal.
    assert len(coord.state.pending) == 2
    assert {p.action_uuid for p in coord.state.pending} == {b"\xaa" * 16, b"\xbb" * 16}


async def test_on_add_renderer_own_device_sets_own_rid() -> None:
    """on_add_renderer resolves own-ness against the coordinator's device_uuid."""
    coord, _runner, _bridge = _coordinator()
    callbacks = coord.build_session_callbacks()
    assert callbacks.on_add_renderer is not None
    await callbacks.on_add_renderer(
        RendererRecord(renderer_id=42, device_uuid=OUR_DEVICE_UUID, friendly_name="us")
    )
    assert coord.state.own_rid == 42


async def test_on_add_renderer_other_device_leaves_own_rid_none() -> None:
    """on_add_renderer does not adopt own_rid for a renderer with a different device_uuid."""
    coord, _runner, _bridge = _coordinator()
    callbacks = coord.build_session_callbacks()
    assert callbacks.on_add_renderer is not None
    await callbacks.on_add_renderer(
        RendererRecord(renderer_id=42, device_uuid=b"\x99" * 16, friendly_name="someone-else")
    )
    assert coord.state.own_rid is None


async def test_controller_disabled_suppresses_ma_queue_push() -> None:
    """With controller_enabled=False, on_ma_queue_event never submits — no PushAdd, no proposal."""
    coord, runner, bridge = _coordinator(controller_enabled=False)
    bridge.items = [{"track_id": "100"}, {"track_id": "101"}]
    await coord.on_ma_queue_event("player")
    assert runner.effects == []
    assert coord.state.pending == ()
