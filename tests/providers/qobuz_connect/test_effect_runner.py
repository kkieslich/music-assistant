"""Effect runner maps each Effect to the right session/bridge call."""

from __future__ import annotations

from typing import Any, cast

from music_assistant.providers.qobuz_connect.effect_runner import EffectRunner
from music_assistant.providers.qobuz_connect.models import LoopMode, PlayingState, QueueVersion
from music_assistant.providers.qobuz_connect.sync_types import (
    AskSnapshot,
    Effect,
    MaPause,
    MaPlayTrack,
    MaReleasePlayer,
    MaResume,
    MaResyncQueue,
    MaSeek,
    MaSetLoop,
    MaSetShuffleFlag,
    MaSetVolume,
    PushAdd,
    PushAutoplay,
    PushClear,
    PushLoad,
    PushLoop,
    PushPlayerState,
    PushReorder,
    PushSetActive,
    PushShuffle,
    PushVolume,
    ReportState,
)


class _FakeSession:
    """Small recorder for outbound Qobuz session calls."""

    def __init__(self) -> None:
        """Start with an empty call log."""
        self.calls: list[tuple[str, Any]] = []

    async def send_clear_queue(self, **kw: Any) -> bool:
        """Record a clear-queue call."""
        self.calls.append(("clear", kw))
        return True

    async def send_queue_load_tracks(self, **kw: Any) -> bool:
        """Record a queue-load call."""
        self.calls.append(("load", kw))
        return True

    async def send_queue_add_tracks(self, **kw: Any) -> bool:
        """Record a queue-add call."""
        self.calls.append(("add", kw))
        return True

    async def send_queue_reorder_tracks(self, **kw: Any) -> bool:
        """Record a queue-reorder call."""
        self.calls.append(("reorder", kw))
        return True

    async def send_set_active_renderer(self, renderer_id: int) -> bool:
        """Record a set-active-renderer call."""
        self.calls.append(("set_active", renderer_id))
        return True

    async def send_ctrl_player_state(self, **kw: Any) -> bool:
        """Record a controller player-state call."""
        self.calls.append(("player_state", kw))
        return True

    async def send_volume_changed(self, volume: int) -> bool:
        """Record a volume-changed call."""
        self.calls.append(("volume", volume))
        return True

    async def send_set_loop_mode(self, mode: Any) -> bool:
        """Record a set-loop-mode call."""
        self.calls.append(("loop", mode))
        return True

    async def send_ask_for_queue_state(self, **kw: Any) -> bool:
        """Record an ask-for-queue-state call."""
        self.calls.append(("ask_snapshot", kw))
        return True


class _FakeBridge:
    """Small recorder for MA bridge calls; ``pid`` toggles the no-target guard."""

    def __init__(self, pid: str | None = "player") -> None:
        """Start with an empty call log and a fixed target player id."""
        self.calls: list[tuple[Any, ...]] = []
        self._pid = pid
        self.queue_items_result: list[Any] = []

    def target_player_id(self) -> str | None:
        """Return the fixed target player id (or ``None`` to exercise the guard)."""
        return self._pid

    def queue_items(self, player_id: str) -> list[Any]:
        """Return the fixed queue-items list."""
        return self.queue_items_result

    def qobuz_track_id_for(self, item: Any) -> str | None:
        """Return the fake item's Qobuz track id."""
        return cast("str | None", item.get("track_id")) if isinstance(item, dict) else None

    async def pause(self, pid: str) -> None:
        """Record a pause call."""
        self.calls.append(("pause", pid))

    async def play(self, pid: str) -> None:
        """Record a resume call."""
        self.calls.append(("play", pid))

    async def seek(self, pid: str, position: int) -> None:
        """Record a seek call."""
        self.calls.append(("seek", (pid, position)))

    async def play_index(self, pid: str, index: int, **kw: Any) -> None:
        """Record a play_index call."""
        self.calls.append(("play_index", (pid, index), kw))

    async def play_media(self, pid: str, **kw: Any) -> None:
        """Record a play_media call."""
        self.calls.append(("play_media", pid, kw))

    async def cmd_volume_set(self, pid: str, volume: int) -> None:
        """Record a volume-set call."""
        self.calls.append(("cmd_volume_set", (pid, volume)))

    async def stop_queue(self, pid: str) -> None:
        """Record a stop-queue call."""
        self.calls.append(("stop_queue", pid))

    def clear_queue(self, pid: str, *, skip_stop: bool = False) -> None:
        """Record a clear-queue call."""
        self.calls.append(("clear_queue", (pid, skip_stop)))

    def set_repeat(self, pid: str, value: str) -> None:
        """Record a set-repeat call."""
        self.calls.append(("set_repeat", (pid, value)))

    def set_shuffle_flag(self, pid: str, enabled: bool) -> None:
        """Record a set-shuffle-flag call."""
        self.calls.append(("set_shuffle_flag", (pid, enabled)))

    def set_current_index(self, pid: str, index: int) -> None:
        """Record a set-current-index call."""
        self.calls.append(("set_current_index", (pid, index)))

    def update_items(self, pid: str, items: list[Any]) -> None:
        """Record an update-items call."""
        self.calls.append(("update_items", (pid, items)))


class _FakeMetadata:
    """Returns a canned track (or None) without touching real MA."""

    def __init__(self, track: Any = "resolved-track") -> None:
        """Hold the canned resolution result and a request log."""
        self._track = track
        self.requested: list[str] = []

    async def get_track_or_none(self, track_id: str) -> Any:
        """Record the request and return the canned track."""
        self.requested.append(track_id)
        return self._track


class _FakeReporter:
    """Records ``report_state`` calls."""

    def __init__(self) -> None:
        """Start with an empty call log."""
        self.calls: list[dict[str, Any]] = []

    async def report_state(self, **kw: Any) -> None:
        """Record a report_state call."""
        self.calls.append(kw)


def _runner(
    session: _FakeSession,
    bridge: _FakeBridge,
    metadata: _FakeMetadata | None = None,
    reporter: _FakeReporter | None = None,
    own_rid_getter: Any = lambda: None,
) -> EffectRunner:
    """Build an ``EffectRunner`` wired to fakes (cast past the real collaborator types)."""
    return EffectRunner(
        session=cast("Any", session),
        bridge=cast("Any", bridge),
        metadata=cast("Any", metadata),
        reporter=cast("Any", reporter),
        own_rid_getter=own_rid_getter,
    )


async def test_push_clear_calls_session() -> None:
    """PushClear maps to session.send_clear_queue."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(PushClear(action_uuid=b"\x01" * 16, base_version=QueueVersion(5, 1)))
    assert session.calls
    assert session.calls[0][0] == "clear"


async def test_ma_pause_calls_bridge() -> None:
    """MaPause maps to bridge.pause on the target player."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaPause())
    assert ("pause", "player") in bridge.calls


async def test_push_load_sends_qobuz_track_ids_directly() -> None:
    """PushLoad passes the effect's Qobuz track ids straight through as an int list."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(
        PushLoad(
            action_uuid=b"\x02" * 16,
            base_version=QueueVersion(1, 0),
            track_ids=(111, 222),
            current_index=1,
            context_uuid=b"\x03" * 16,
        )
    )
    kind, kw = session.calls[0]
    assert kind == "load"
    assert kw["track_ids"] == [111, 222]
    assert kw["queue_position"] == 1
    assert kw["queue_version"] == QueueVersion(1, 0)


async def test_push_add_builds_refs_from_qobuz_ids() -> None:
    """PushAdd wraps each Qobuz track id in a QueueTrackRef with queue_item_id=0."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(
        PushAdd(action_uuid=b"\x04" * 16, base_version=QueueVersion(2, 0), track_ids=(555, 666))
    )
    kind, kw = session.calls[0]
    assert kind == "add"
    refs = kw["tracks"]
    assert [r.track_id for r in refs] == ["555", "666"]
    assert all(r.queue_item_id == 0 for r in refs)


async def test_push_reorder_passes_slot_ids_through() -> None:
    """PushReorder's queue_item_ids are already cloud slot ids and pass through unchanged."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(
        PushReorder(
            action_uuid=b"\x05" * 16,
            base_version=QueueVersion(3, 0),
            queue_item_ids=(9, 10, 11),
            insert_after=2,
        )
    )
    kind, kw = session.calls[0]
    assert kind == "reorder"
    assert kw["queue_item_ids"] == [9, 10, 11]
    assert kw["insert_after"] == 2


async def test_push_set_active_uses_rid_getter() -> None:
    """PushSetActive sends the own renderer id when the getter resolves one."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge, own_rid_getter=lambda: 42)
    await runner.run(PushSetActive())
    assert session.calls == [("set_active", 42)]


async def test_push_set_active_noop_when_rid_unknown() -> None:
    """PushSetActive is a no-op while the own renderer id isn't known yet."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(PushSetActive())
    assert session.calls == []


async def test_push_player_state_calls_session() -> None:
    """PushPlayerState maps to session.send_ctrl_player_state."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(
        PushPlayerState(
            playing=PlayingState.PLAYING,
            position_ms=1000,
            queue_version=QueueVersion(1, 0),
            queue_item_id=7,
        )
    )
    kind, kw = session.calls[0]
    assert kind == "player_state"
    assert kw["queue_item_id"] == 7
    assert kw["playing_state"] == PlayingState.PLAYING


async def test_push_volume_calls_session() -> None:
    """PushVolume maps to session.send_volume_changed."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(PushVolume(volume=33))
    assert session.calls == [("volume", 33)]


async def test_push_loop_calls_session() -> None:
    """PushLoop maps to session.send_set_loop_mode."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(PushLoop(loop=LoopMode.REPEAT_ALL))
    assert session.calls == [("loop", LoopMode.REPEAT_ALL)]


async def test_push_autoplay_is_a_noop() -> None:
    """PushAutoplay is a documented no-op — no session verb exists for it."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(PushAutoplay(autoplay=True))
    assert session.calls == []


async def test_push_shuffle_is_a_noop() -> None:
    """PushShuffle is a documented no-op — the reducer never emits it today."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(PushShuffle(shuffle=True, base_version=QueueVersion(1, 0)))
    assert session.calls == []


async def test_ask_snapshot_calls_session_with_version() -> None:
    """AskSnapshot maps to session.send_ask_for_queue_state with a fresh queue_uuid."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(AskSnapshot(version=QueueVersion(9, 2)))
    kind, kw = session.calls[0]
    assert kind == "ask_snapshot"
    assert kw["queue_version"] == QueueVersion(9, 2)
    assert isinstance(kw["queue_uuid"], bytes)
    assert len(kw["queue_uuid"]) == 16


async def test_report_state_delegates_to_reporter() -> None:
    """ReportState maps to reporter.report_state(sync_from_ma=False)."""
    session, bridge, reporter = _FakeSession(), _FakeBridge(), _FakeReporter()
    runner = _runner(session, bridge, reporter=reporter)
    await runner.run(ReportState())
    assert reporter.calls == [{"sync_from_ma": False}]


async def test_ma_resume_calls_bridge() -> None:
    """MaResume maps to bridge.play on the target player."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaResume())
    assert ("play", "player") in bridge.calls


async def test_ma_seek_converts_ms_to_seconds() -> None:
    """MaSeek maps to bridge.seek, converting milliseconds to whole seconds."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaSeek(position_ms=45500))
    assert ("seek", ("player", 45)) in bridge.calls


async def test_ma_play_track_fast_path_uses_play_index() -> None:
    """MaPlayTrack jumps via play_index when the track is already loaded in MA's queue."""
    session, bridge = _FakeSession(), _FakeBridge()
    bridge.queue_items_result = [{"track_id": "111"}, {"track_id": "222"}]
    runner = _runner(session, bridge)
    await runner.run(MaPlayTrack(track_id=222, position_ms=5000))
    assert ("play_index", ("player", 1), {"seek_position": 5}) in bridge.calls


async def test_ma_play_track_falls_back_to_play_media() -> None:
    """MaPlayTrack resolves via metadata and replaces the queue when not already loaded."""
    session, bridge, metadata = _FakeSession(), _FakeBridge(), _FakeMetadata()
    runner = _runner(session, bridge, metadata=metadata)
    await runner.run(MaPlayTrack(track_id=333, position_ms=0))
    assert metadata.requested == ["333"]
    _, pid, kw = next(c for c in bridge.calls if c[0] == "play_media")
    assert pid == "player"
    assert kw["media"] == "resolved-track"


async def test_ma_play_track_unresolved_track_is_noop() -> None:
    """MaPlayTrack drops the effect when the metadata resolver can't find the track."""
    session, bridge, metadata = _FakeSession(), _FakeBridge(), _FakeMetadata(track=None)
    runner = _runner(session, bridge, metadata=metadata)
    await runner.run(MaPlayTrack(track_id=999, position_ms=0))
    assert not any(c[0] == "play_media" for c in bridge.calls)


async def test_ma_resync_queue_reuses_existing_items_and_sets_current_index() -> None:
    """MaResyncQueue reorders MA's existing queue items without any metadata fetches."""
    session, bridge, metadata = _FakeSession(), _FakeBridge(), _FakeMetadata()
    item_a, item_b = {"track_id": "1"}, {"track_id": "2"}
    bridge.queue_items_result = [item_a, item_b]
    runner = _runner(session, bridge, metadata=metadata)
    await runner.run(MaResyncQueue(track_ids=(2, 1), current_track_id=1))
    _, (pid, index) = next(c for c in bridge.calls if c[0] == "set_current_index")
    assert (pid, index) == ("player", 1)
    _, (_pid, items) = next(c for c in bridge.calls if c[0] == "update_items")
    assert items == [item_b, item_a]
    assert metadata.requested == []


async def test_ma_set_loop_maps_to_ma_repeat_mode() -> None:
    """MaSetLoop maps Qobuz LoopMode to MA's RepeatMode string via bridge.set_repeat."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaSetLoop(loop=LoopMode.REPEAT_ONE))
    assert ("set_repeat", ("player", "one")) in bridge.calls


async def test_ma_set_shuffle_flag_calls_bridge() -> None:
    """MaSetShuffleFlag maps to bridge.set_shuffle_flag."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaSetShuffleFlag(shuffle=True))
    assert ("set_shuffle_flag", ("player", True)) in bridge.calls


async def test_ma_set_volume_calls_bridge() -> None:
    """MaSetVolume maps to bridge.cmd_volume_set."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaSetVolume(volume=50))
    assert ("cmd_volume_set", ("player", 50)) in bridge.calls


async def test_ma_release_player_stops_and_clears_queue() -> None:
    """MaReleasePlayer stops then clears the target player's queue."""
    session, bridge = _FakeSession(), _FakeBridge()
    runner = _runner(session, bridge)
    await runner.run(MaReleasePlayer())
    assert ("stop_queue", "player") in bridge.calls
    assert ("clear_queue", ("player", True)) in bridge.calls


async def test_ma_effects_noop_when_no_target_player() -> None:
    """Every MA effect is a silent no-op when target_player_id() returns None."""
    session, bridge = _FakeSession(), _FakeBridge(pid=None)
    runner = _runner(session, bridge)
    effects: list[Effect] = [
        MaPause(),
        MaResume(),
        MaSeek(position_ms=1000),
        MaSetVolume(volume=10),
    ]
    for effect in effects:
        await runner.run(effect)
    assert bridge.calls == []
