"""Coordinator wires reduce() to effects with serialized intake."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import music_assistant.providers.qobuz_connect.coordinator as coordinator_module
from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.models import (
    LoopMode,
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudAddRenderer,
    CloudQueueError,
    CloudRendererStateUpdated,
    CloudSessionState,
    CloudSetActive,
    CloudSetState,
    CloudSnapshot,
    CloudTracksAdded,
    Disconnected,
    MaPlayTrack,
    MaQueueChanged,
    MaReleasePlayer,
    MaResyncQueue,
    PushAdd,
    PushLoad,
    PushLoop,
    PushMute,
    PushPlayerState,
    PushRemove,
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

    def __init__(self, *, volume_level: int | None = 50, volume_muted: bool = False) -> None:
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

    def target_player_id(self) -> str:
        """Return the fixed target used by activation/release tests."""
        return "player"

    def qobuz_track_id_for(self, item: Any) -> str | None:
        """Return the fake item's Qobuz track id."""
        return item.get("track_id") if isinstance(item, dict) else None

    def qobuz_track_ids_for(self, item: Any) -> tuple[str, ...]:
        """Return every fake Qobuz alias for an item."""
        if not isinstance(item, dict):
            return ()
        return tuple(item.get("track_ids", (item.get("track_id"),)))

    def get_queue(self, player_id: str) -> _FakeQueue | None:
        """Return the fixed fake queue."""
        return self.queue

    def get_player(self, player_id: str) -> _FakePlayer | None:
        """Return the fixed fake player."""
        return self.player


def _refs(*ids: int) -> tuple[QueueTrackRef, ...]:
    return tuple(QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in ids)


def _coordinator(
    *, bridge: _FakeBridge | None = None
) -> tuple[QobuzConnectCoordinator, _RecordingRunner, _FakeBridge]:
    runner = _RecordingRunner()
    bridge = bridge if bridge is not None else _FakeBridge()
    coord = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=bridge,  # type: ignore[arg-type]
        device_uuid=OUR_DEVICE_UUID,
        now=lambda: 1,
    )
    return coord, runner, bridge


async def test_ma_events_prefer_alias_matching_canonical_track() -> None:
    """A library item with replacement and original mappings must not clear Connect."""
    coord, runner, bridge = _coordinator()
    item = {"track_id": "3972279", "track_ids": ("3972279", "3879020")}
    bridge.items = [item]
    bridge.queue = _FakeQueue(current_item=item, state="playing", elapsed=1.0)
    coord._state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(QueueTrackRef(queue_item_id=3, track_id="3879020"),),
        current_id=3879020,
        playing=PlayingState.PLAYING,
        active=True,
    )

    await coord.on_ma_queue_event("player")
    await coord.on_ma_transport_event("player")

    assert [type(effect) for effect in runner.effects] == [ReportState]
    assert coord._state.current_id == 3879020


async def test_ma_foreign_current_item_is_explicitly_released() -> None:
    """Coordinator distinguishes a foreign current item from an empty current item."""
    coord, runner, bridge = _coordinator()
    bridge.queue = _FakeQueue(current_item={"local_id": "foreign"}, elapsed=90.0)
    coord._state = CanonicalState(
        tracks=(QueueTrackRef(queue_item_id=3, track_id="3879020"),),
        current_id=3879020,
        playing=PlayingState.PLAYING,
        position_ms=45_000,
        active=True,
    )
    coord.transfer_target("player")

    await coord.on_ma_transport_event("player")

    assert coord.state.current_id is None
    assert coord.state.active is False
    assert runner.effects == [MaReleasePlayer(player_id="player")]


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
        )
    )
    await coord._submit(CloudSetActive(now_ms=3, active=True))
    assert coord.state.active is True
    assert any(isinstance(e, MaResyncQueue) for e in runner.effects)
    assert any(isinstance(e, MaPlayTrack) for e in runner.effects)


async def test_new_queue_generation_reduces_while_resync_is_in_flight() -> None:
    """Slow metadata cannot hold the reducer lock or apply dependent stale effects."""

    class _BlockingRunner:
        def __init__(self) -> None:
            self.effects: list[Any] = []
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.block_once = True

        async def run(self, effect: Any) -> None:
            self.effects.append(effect)
            if isinstance(effect, MaResyncQueue) and self.block_once:
                self.block_once = False
                self.started.set()
                await self.release.wait()

    runner = _BlockingRunner()
    bridge = _FakeBridge()
    coord = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=bridge,  # type: ignore[arg-type]
        device_uuid=OUR_DEVICE_UUID,
    )
    await coord._submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(5, 1),
            tracks=_refs(0, 1, 2),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=3,
        )
    )
    await coord._submit(
        CloudSetState(
            now_ms=2,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=_refs(2)[0],
        )
    )

    activation = asyncio.create_task(coord._submit(CloudSetActive(now_ms=3, active=True)))
    await runner.started.wait()
    newer = asyncio.create_task(
        coord._submit(
            CloudSnapshot(
                now_ms=4,
                version=QueueVersion(6, 1),
                tracks=(QueueTrackRef(queue_item_id=20, track_id="200"),),
                autoplay_tracks=(),
                shuffle=False,
                autoplay=False,
                track_index=1,
            )
        )
    )
    await asyncio.sleep(0)

    assert tuple(ref.track_id for ref in coord.state.tracks) == ("200",)
    runner.release.set()
    await asyncio.gather(activation, newer)

    resyncs = [effect for effect in runner.effects if isinstance(effect, MaResyncQueue)]
    assert len(resyncs) == 2
    assert resyncs[0].generation < resyncs[1].generation
    assert not any(
        isinstance(effect, MaPlayTrack) and effect.track_id == 102 for effect in runner.effects
    )


async def test_ma_append_pushes_add_and_cloud_echo_confirms() -> None:
    """An MA-origin append reloads the queue; the matching cloud echo confirms it."""
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
    assert any(isinstance(e, PushLoad) for e in runner.effects)
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


async def test_submit_add_renderer_own_device_sets_own_rid() -> None:
    """Submit resolves own-ness against the coordinator's device_uuid (is_own injection)."""
    coord, _runner, _bridge = _coordinator()
    await coord.submit(CloudAddRenderer(now_ms=1, renderer_id=42, device_uuid=OUR_DEVICE_UUID))
    assert coord.state.own_rid == 42


async def test_submit_add_renderer_other_device_leaves_own_rid_none() -> None:
    """Submit does not adopt own_rid for a renderer with a different device_uuid."""
    coord, _runner, _bridge = _coordinator()
    await coord.submit(CloudAddRenderer(now_ms=1, renderer_id=42, device_uuid=b"\x99" * 16))
    assert coord.state.own_rid is None


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


async def test_submit_set_state_reduces_to_play() -> None:
    """A submitted CloudSetState reaches the reducer and plays the new current track."""
    coord, runner, _bridge = _coordinator()
    await coord.submit(
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
    await coord.submit(
        CloudSetState(
            now_ms=2,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=1234,
            current_ref=_refs(1)[0],
        )
    )
    assert coord.state.playing is PlayingState.PLAYING
    assert any(
        isinstance(e, MaPlayTrack) and e.track_id == 101 and e.position_ms == 1234
        for e in runner.effects
    )


async def test_submit_snapshot_uses_remembered_track_index() -> None:
    """Submit stamps a snapshot with the track_index remembered from the last SESSION_STATE."""
    coord, _runner, _bridge = _coordinator()
    await coord.submit(CloudSessionState(now_ms=1, version=QueueVersion(1, 0), track_index=3))
    tracks = _refs(0, 1, 2, 3)
    await coord.submit(
        CloudSnapshot(
            now_ms=2,
            version=QueueVersion(2, 0),
            tracks=tracks,
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            # Codec always builds snapshots with track_index=0; the coordinator
            # injects the remembered value.
            track_index=0,
        )
    )
    # track_index=3 is one-indexed, so the remembered pointer resolves to
    # tracks[2] — proving submit injected the value SESSION_STATE stashed.
    assert coord.state.current_id == int(tracks[2].track_id)


async def test_submit_queue_error_without_version_falls_back_to_cloud_version() -> None:
    """
    A CloudQueueError with the (0,0) default version rebases the matching proposal.

    The codec builds a version-less wire error as ``QueueVersion()``; submit
    injects ``state.cloud_version`` so the rejection still rebases against the
    version we hold rather than resetting it to (0,0).
    """
    coord, runner, bridge = _coordinator()
    proposal = await _seed_and_propose(coord, bridge)

    await coord.submit(
        CloudQueueError(
            now_ms=1,
            version=QueueVersion(),
            action_uuid=proposal.action_uuid,
            code="1",
            message="failed",
        )
    )

    assert len(coord.state.pending) == 1  # rebased, still pending
    assert coord.state.pending[0].retries_left == proposal.retries_left - 1
    assert coord.state.cloud_version == QueueVersion(5, 1)
    assert any(isinstance(e, PushLoad) for e in runner.effects)  # re-pushed


async def test_submit_renderer_state_updated_maps_current_index() -> None:
    """A submitted CloudRendererStateUpdated maps current_index onto current_id while inactive."""
    coord, _runner, _bridge = _coordinator()
    await coord.submit(
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
    await coord.submit(
        CloudRendererStateUpdated(
            now_ms=2, renderer_id=7, playing=None, position_ms=None, current_index=2
        )
    )
    assert coord.state.current_id == int(_refs(0, 1, 2)[2].track_id)


# ---- MA-side entry points -------------------------------------------------


async def test_on_ma_transport_event() -> None:
    """on_ma_transport_event folds MA's state and reports it as a renderer (ReportState)."""
    coord, runner, bridge = _coordinator()
    coord._state.active = True
    coord._state.own_rid = 42
    coord._state.active_rid = 42
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


async def test_on_ma_volume_event_skips_none_volume() -> None:
    """
    A transient volume_level=None must not be reported to the cloud.

    Network players can report volume_level=None while acking a volume change;
    reporting it as 0 made the Qobuz app show the renderer as muted a short
    while after the user changed volume (live 2026-07-11). Skip until a real
    level is known.
    """
    coord, runner, bridge = _coordinator()
    bridge.player = _FakePlayer(volume_level=None, volume_muted=False)
    await coord.on_ma_volume_event("player")
    assert not any(isinstance(e, PushVolume) for e in runner.effects)
    assert not any(isinstance(e, PushMute) for e in runner.effects)


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


async def test_ma_removal_is_detected_via_unresolvable_getter() -> None:
    """
    Removing a track in MA must reach the cloud as a queue change.

    ``resolvable`` used to be computed from the CURRENT MA queue contents, so
    a user-removed track was filtered out of the canonical side of the diff
    and the removal was invisible — the cloud kept the track and the next
    resync re-added it to MA. Resolvability must come from the metadata
    fail-cache instead: everything not known-unresolvable counts.
    """
    runner = _RecordingRunner()
    bridge = _FakeBridge()
    coord = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=bridge,  # type: ignore[arg-type]
        device_uuid=OUR_DEVICE_UUID,
        unresolvable_getter=lambda: frozenset(),
    )
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
    await coord._submit(CloudSetActive(now_ms=2, active=True))
    runner.effects.clear()
    # User removed the middle track in MA's UI.
    bridge.items = [{"track_id": "100"}, {"track_id": "102"}]
    await coord.on_ma_queue_event("player_1")
    # A pure removal is a native REMOVE (of the middle slot, queue_item_id=1),
    # not a full-queue LOAD.
    removes = [e for e in runner.effects if isinstance(e, PushRemove)]
    assert removes, runner.effects
    assert removes[0].queue_item_ids == (1,)
    assert not any(isinstance(e, PushLoad) for e in runner.effects)


async def test_materialized_autoplay_suffix_is_not_pushed_into_main_queue() -> None:
    """MA echoing the combined main+autoplay view is not a user ADD."""
    coord, runner, bridge = _coordinator()
    coord._state = CanonicalState(
        cloud_version=QueueVersion(5, 1),
        tracks=(QueueTrackRef(queue_item_id=1, track_id="100"),),
        autoplay_tracks=(QueueTrackRef(queue_item_id=2, track_id="200"),),
        current_id=100,
        active=True,
    )
    bridge.items = [{"track_id": "100"}, {"track_id": "200"}]

    await coord.on_ma_queue_event("player")

    assert not any(isinstance(effect, PushAdd | PushLoad) for effect in runner.effects)


async def test_ma_loads_get_distinct_valid_action_and_context_uuids() -> None:
    """The coordinator mints both UUIDs at the impure boundary for every LOAD."""
    coord, runner, bridge = _coordinator()
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
    await coord._submit(CloudSetActive(now_ms=2, active=True))
    runner.effects.clear()

    bridge.items = [{"track_id": "200"}, {"track_id": "201"}]
    await coord.on_ma_queue_event("player")
    bridge.items = [{"track_id": "300"}, {"track_id": "301"}]
    await coord.on_ma_queue_event("player")

    loads = [effect for effect in runner.effects if isinstance(effect, PushLoad)]
    assert len(loads) == 2
    assert all(len(effect.action_uuid) == 16 and any(effect.action_uuid) for effect in loads)
    assert all(len(effect.context_uuid) == 16 and any(effect.context_uuid) for effect in loads)
    assert loads[0].action_uuid != loads[1].action_uuid
    assert loads[0].context_uuid != loads[1].context_uuid


async def test_ma_removal_of_unresolvable_track_is_not_a_change() -> None:
    """A track MA could never materialize disappearing from MA is not a user removal."""
    runner = _RecordingRunner()
    bridge = _FakeBridge()
    coord = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=bridge,  # type: ignore[arg-type]
        device_uuid=OUR_DEVICE_UUID,
        unresolvable_getter=lambda: frozenset({"101"}),
    )
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
    await coord._submit(CloudSetActive(now_ms=2, active=True))
    runner.effects.clear()
    bridge.items = [{"track_id": "100"}, {"track_id": "102"}]
    await coord.on_ma_queue_event("player_1")
    assert not [e for e in runner.effects if isinstance(e, PushLoad)]
    assert coord.state.pending == ()


async def test_volume_dedup_resets_on_disconnect() -> None:
    """After a reconnect the cloud has forgotten our volume — resend the same level."""
    bridge = _FakeBridge()
    bridge.player = _FakePlayer(volume_level=50, volume_muted=False)
    coord, runner, _bridge = _coordinator(bridge=bridge)
    await coord.on_ma_volume_event("player_1")
    await coord._submit(Disconnected(now_ms=1))
    await coord.on_ma_volume_event("player_1")
    pushes = [e for e in runner.effects if isinstance(e, PushVolume)]
    assert len(pushes) == 2, runner.effects


async def test_close_cancels_proposal_timers_and_blocks_new_ones() -> None:
    """close() reaps every timer and prevents post-unload timeout submissions."""
    runner = _RecordingRunner()
    bridge = _FakeBridge()
    coord = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=bridge,  # type: ignore[arg-type]
        device_uuid=OUR_DEVICE_UUID,
    )
    bridge.items = [{"track_id": "100"}]
    await coord.on_ma_queue_event("player_1")
    assert coord._timers
    coord.close()
    assert not coord._timers
    # A late timer callback after close must not spawn a submit task.
    coord._fire_proposal_timeout(b"\x00" * 16)
    await asyncio.sleep(0)
    assert not coord._timers
