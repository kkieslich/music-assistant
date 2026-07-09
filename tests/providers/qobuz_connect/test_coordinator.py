"""Coordinator wires reduce() to effects with serialized intake."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import music_assistant.providers.qobuz_connect.coordinator as coordinator_module
from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.models import (
    LoopMode,
    PlayingState,
    QueueError,
    QueueStateSnapshot,
    QueueTrackRef,
    QueueVersion,
    RendererRecord,
    RendererStateUpdate,
    SessionStateEvent,
    SetStateEvent,
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
    PushLoop,
    PushMute,
    PushPlayerState,
    PushVolume,
    ReportState,
)

if TYPE_CHECKING:
    import pytest

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


# ---- proposal-timeout timer lifecycle -----------------------------------


async def _seed_and_propose(coord: QobuzConnectCoordinator, bridge: _FakeBridge) -> Any:
    """
    Seed a one-track canonical snapshot, then trigger an MA-origin ADD proposal.

    :param coord: Coordinator to drive.
    :param bridge: The coordinator's fake bridge; its ``items`` are set to
        produce an append relative to the seeded canonical track.
    :return: The resulting pending proposal.
    """
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
    return coord.state.pending[0]


async def test_pending_proposal_schedules_timer() -> None:
    """A newly pending MA-origin proposal gets exactly one live timeout timer."""
    coord, _runner, bridge = _coordinator()
    proposal = await _seed_and_propose(coord, bridge)
    assert set(coord._timers) == {proposal.action_uuid}


async def test_timer_cancelled_on_confirm() -> None:
    """Confirming a proposal cancels its timeout timer instead of leaking it."""
    coord, _runner, bridge = _coordinator()
    proposal = await _seed_and_propose(coord, bridge)
    await coord._submit(
        CloudTracksAdded(
            now_ms=2,
            version=QueueVersion(6, 1),
            action_uuid=proposal.action_uuid,
            tracks=(QueueTrackRef(queue_item_id=1, track_id="101"),),
            after_index=1,
        )
    )
    assert coord.state.pending == ()
    assert proposal.action_uuid not in coord._timers
    assert coord._timers == {}


async def test_timer_fires_and_converges(monkeypatch: pytest.MonkeyPatch) -> None:
    """A proposal that never gets a cloud echo times out and converges MA to canonical."""
    monkeypatch.setattr(coordinator_module, "PROPOSAL_TIMEOUT_S", 0.01)
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
    await coord._submit(
        CloudSetState(
            now_ms=2,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=_refs(0)[0],
            next_ref=None,
        )
    )
    await coord._submit(CloudSetActive(now_ms=3, active=True))
    runner.effects.clear()

    bridge.items = [{"track_id": "100"}, {"track_id": "101"}]
    await coord.on_ma_queue_event("player")
    pending_before = coord.state.pending
    assert len(pending_before) == 1

    await asyncio.sleep(0.05)

    pending_after = coord.state.pending
    assert pending_after == ()
    assert coord._timers == {}
    assert any(isinstance(e, MaResyncQueue) for e in runner.effects)


# ---- build_session_callbacks() translation paths -------------------------


async def test_on_set_state_translates() -> None:
    """The real ``on_set_state`` callback translates a SetStateEvent into CloudSetState."""
    coord, runner, _bridge = _coordinator()
    await coord._submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        )
    )
    callbacks = coord.build_session_callbacks()
    await callbacks.on_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=1234,
            queue_version=None,
            current_item=_refs(1)[0],
            next_item=None,
        )
    )
    assert coord.state.playing is PlayingState.PLAYING
    assert any(
        isinstance(e, MaPlayTrack) and e.track_id == 101 and e.position_ms == 1234
        for e in runner.effects
    )


async def test_on_queue_state_uses_remembered_track_index() -> None:
    """on_queue_state falls back to the track_index remembered from the last SESSION_STATE."""
    coord, _runner, _bridge = _coordinator()
    callbacks = coord.build_session_callbacks()
    await callbacks.on_session_state(
        SessionStateEvent(
            session_uuid=b"\x02" * 16,
            session_id=1,
            queue_version=QueueVersion(1, 0),
            track_index=3,
        )
    )
    tracks = list(_refs(0, 1, 2, 3))
    await callbacks.on_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(2, 0),
            action_uuid=b"\x03" * 16,
            tracks=tracks,
            shuffle_mode=False,
            autoplay_mode=False,
            autoplay_tracks=[],
        )
    )
    # track_index=3 is one-indexed, so the remembered pointer resolves to
    # tracks[2] — proving on_queue_state actually consumed the value
    # on_session_state stashed, rather than defaulting to 0.
    assert coord.state.current_id == int(tracks[2].track_id)


async def test_on_queue_error_falls_back_to_cloud_version() -> None:
    """
    A QueueError with no queue_version still rebases the matching proposal.

    The fallback value the coordinator supplies equals ``state.cloud_version``
    exactly (``CloudQueueError.version`` is non-optional). A rejection is a
    control event exempted from the reducer's top-of-``reduce()`` version-stale
    gate, so it still reaches ``_reject_proposal`` and rebases immediately
    rather than waiting on the ProposalTimeout safety net.
    """
    coord, runner, bridge = _coordinator()
    proposal = await _seed_and_propose(coord, bridge)

    callbacks = coord.build_session_callbacks()
    await callbacks.on_queue_error(
        QueueError(action_uuid=proposal.action_uuid, queue_version=None, code="1", message="failed")
    )

    assert len(coord.state.pending) == 1  # rebased, still pending
    assert coord.state.pending[0].retries_left == proposal.retries_left - 1
    assert coord.state.cloud_version == QueueVersion(5, 1)
    assert any(isinstance(e, PushAdd) for e in runner.effects)  # re-pushed


async def test_on_renderer_state_updated_maps_current_queue_index() -> None:
    """on_renderer_state_updated maps current_queue_index onto current_id while inactive."""
    coord, _runner, _bridge = _coordinator()
    await coord._submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1, 2),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=1,
        )
    )
    assert coord.state.active is False
    callbacks = coord.build_session_callbacks()
    assert callbacks.on_renderer_state_updated is not None
    await callbacks.on_renderer_state_updated(
        RendererStateUpdate(renderer_id=7, current_queue_index=2)
    )
    assert coord.state.current_id == int(_refs(0, 1, 2)[2].track_id)


# ---- MA-side entry points -------------------------------------------------


async def test_on_ma_transport_event() -> None:
    """on_ma_transport_event folds MA's state and reports it as a renderer (ReportState)."""
    coord, runner, bridge = _coordinator()
    bridge.queue = _FakeQueue(current_item={"track_id": "100"}, state="paused", elapsed=12.5)
    await coord.on_ma_transport_event("player")
    assert coord.state.playing is PlayingState.PAUSED
    assert coord.state.current_id == 100
    # MA is the renderer: it reports state (rndrSrvrStateUpdated via ReportState),
    # it does NOT send the ctrlSrvrSetPlayerState controller command (that caused a loop).
    assert any(isinstance(e, ReportState) for e in runner.effects)
    assert not any(isinstance(e, PushPlayerState) for e in runner.effects)


async def test_on_ma_modes_event() -> None:
    """on_ma_modes_event reads MA's repeat mode and pushes a changed loop mode to cloud."""
    coord, runner, bridge = _coordinator()
    bridge.queue = _FakeQueue(repeat="all")
    await coord.on_ma_modes_event("player")
    assert coord.state.loop is LoopMode.REPEAT_ALL
    assert any(isinstance(e, PushLoop) and e.loop is LoopMode.REPEAT_ALL for e in runner.effects)


async def test_on_ma_volume_event() -> None:
    """on_ma_volume_event reads MA's player volume/mute and pushes both to cloud."""
    coord, runner, bridge = _coordinator()
    bridge.player = _FakePlayer(volume_level=42, volume_muted=True)
    await coord.on_ma_volume_event("player")
    assert any(isinstance(e, PushVolume) and e.volume == 42 for e in runner.effects)
    assert any(isinstance(e, PushMute) and e.muted is True for e in runner.effects)


async def test_on_ma_volume_event_dedups_unchanged_volume() -> None:
    """Repeated PLAYER_UPDATED with the same volume/mute pushes to cloud only once."""
    coord, runner, bridge = _coordinator()
    bridge.player = _FakePlayer(volume_level=42, volume_muted=False)

    await coord.on_ma_volume_event("player")
    await coord.on_ma_volume_event("player")

    volume_pushes = [e for e in runner.effects if isinstance(e, PushVolume)]
    mute_pushes = [e for e in runner.effects if isinstance(e, PushMute)]
    assert len(volume_pushes) == 1
    assert len(mute_pushes) == 1

    # A genuine change re-pushes.
    bridge.player = _FakePlayer(volume_level=55, volume_muted=False)
    await coord.on_ma_volume_event("player")

    volume_pushes = [e for e in runner.effects if isinstance(e, PushVolume)]
    assert len(volume_pushes) == 2
    assert volume_pushes[1].volume == 55


async def test_controller_disabled_suppresses_ma_modes_push() -> None:
    """With controller_enabled=False, on_ma_modes_event never submits — no PushLoop."""
    coord, runner, bridge = _coordinator(controller_enabled=False)
    bridge.queue = _FakeQueue(repeat="all")
    await coord.on_ma_modes_event("player")
    assert runner.effects == []
    assert coord.state.loop is LoopMode.OFF
