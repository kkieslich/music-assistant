"""Tests for Qobuz Connect sync decisions."""

from __future__ import annotations

import asyncio
import contextlib
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
from music_assistant_models.enums import MediaType, PlaybackState

from music_assistant.providers.qobuz_connect.models import (
    BufferState,
    LoopMode,
    PlayingState,
    QueueClearedEvent,
    QueueError,
    QueueLoadAck,
    QueueStateSnapshot,
    QueueTrackRef,
    QueueTracksAddedEvent,
    QueueTracksInsertedEvent,
    QueueTracksRemovedEvent,
    QueueTracksReorderedEvent,
    QueueVersion,
    SetStateEvent,
)
from music_assistant.providers.qobuz_connect.state import (
    PausedSeek,
    PendingQobuzPosition,
)
from music_assistant.providers.qobuz_connect.sync import QobuzConnectSyncEngine


class _FakePlayerQueues:
    """Small recorder for MA queue commands."""

    def __init__(self, queue: Any) -> None:
        self.queue = queue
        self.queue_items: list[Any] = []
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def get(self, player_id: str) -> Any:
        return self.queue if player_id == "player" else None

    async def pause(self, player_id: str) -> None:
        self.calls.append(("pause", (player_id,), {}))
        self.queue.state = PlaybackState.PAUSED

    async def play(self, player_id: str) -> None:
        self.calls.append(("play", (player_id,), {}))
        self.queue.state = PlaybackState.PLAYING

    async def play_index(self, player_id: str, index: int, **kwargs: Any) -> None:
        self.calls.append(("play_index", (player_id, index), kwargs))
        self.queue.state = PlaybackState.PLAYING
        self.queue.current_index = index
        if self.queue_items:
            item = self.queue_items[index]
            self.queue.current_item = SimpleNamespace(
                track_id=getattr(item.media_item, "item_id", None)
            )

    async def seek(self, player_id: str, position: int) -> None:
        self.calls.append(("seek", (player_id, position), {}))

    async def stop(self, player_id: str) -> None:
        self.calls.append(("stop", (player_id,), {}))
        self.queue.state = PlaybackState.IDLE

    def clear(self, queue_id: str, skip_stop: bool = False) -> None:
        self.calls.append(("clear", (queue_id,), {"skip_stop": skip_stop}))
        self.queue_items = []
        self.queue.current_item = None
        self.queue.current_index = None

    async def load(self, queue_id: str, queue_items: list[Any], **kwargs: Any) -> None:
        self.calls.append(("load", (queue_id, queue_items), kwargs))
        self.queue_items = queue_items

    async def play_media(self, queue_id: str, media: Any, **kwargs: Any) -> None:
        self.calls.append(("play_media", (queue_id, media), kwargs))
        self.queue.state = PlaybackState.PLAYING
        self.queue.current_item = SimpleNamespace(track_id=getattr(media[0], "item_id", None))


class _FakeLogger:
    def warning(self, *args: Any) -> None:
        pass

    def info(self, *args: Any) -> None:
        pass

    def debug(self, *args: Any) -> None:
        pass


class _FakeSession:
    """Small recorder for outbound Qobuz session calls."""

    def __init__(self) -> None:
        self.renderer_states: list[dict[str, Any]] = []
        self.queue_loads: list[dict[str, Any]] = []
        self.autoplay_loads: list[dict[str, Any]] = []

    async def send_renderer_state(self, **kwargs: Any) -> None:
        self.renderer_states.append(kwargs)

    async def send_queue_load_tracks(self, **kwargs: Any) -> bool:
        self.queue_loads.append(kwargs)
        return True

    async def send_autoplay_load_tracks(self, **kwargs: Any) -> bool:
        self.autoplay_loads.append(kwargs)
        return True


class _FakeProvider:
    """Provider facade needed by the sync engine."""

    def __init__(self, queue: Any, session: Any | None = None) -> None:
        self.mass = SimpleNamespace(player_queues=_FakePlayerQueues(queue))
        self.logger = _FakeLogger()
        self.qobuz_session = session
        self.qobuz_provider = SimpleNamespace(
            get_track=_fake_get_track,
            get_album_tracks=_fake_get_album_tracks,
        )

    def get_target_player_id(self) -> str | None:
        return "player"

    def get_qobuz_track_id_from_queue_item(self, queue_item: Any) -> str | None:
        return getattr(queue_item, "track_id", None)

    def get_qobuz_provider(self) -> Any:
        return self.qobuz_provider


class _NoTargetProvider(_FakeProvider):
    """Provider facade with no available target player."""

    def get_target_player_id(self) -> str | None:
        return None


async def _fake_get_track(track_id: str) -> Any:
    return SimpleNamespace(
        item_id=track_id,
        name=f"Track {track_id}",
        media_type=MediaType.TRACK,
        uri=f"qobuz://track/{track_id}",
        image=None,
        available=True,
        duration=180,
        album=SimpleNamespace(item_id="123456", image=None, media_type=MediaType.ALBUM),
        track_number=2,
    )


async def _fake_get_album_tracks(_album_id: str) -> list[Any]:
    return [
        SimpleNamespace(item_id="111"),
        SimpleNamespace(item_id="370969289"),
        SimpleNamespace(item_id="333"),
    ]


def _queue(state: PlaybackState, track_id: str = "376286112") -> Any:
    return SimpleNamespace(
        state=state,
        current_item=SimpleNamespace(track_id=track_id),
        current_index=0,
        corrected_elapsed_time=10,
    )


def _event(queue: Any) -> Any:
    return SimpleNamespace(object_id="player", data=queue)


async def _wait_for_reconcile(engine: QobuzConnectSyncEngine) -> None:
    task = cast("Any", engine).command_handler._reconcile_task
    if task:
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_paused_seek_is_stored_without_starting_ma_playback() -> None:
    """Paused Qobuz scrubbing should not seek/reload MA immediately."""
    provider = _FakeProvider(_queue(PlaybackState.PAUSED))
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PAUSED,
            position_ms=50_000,
            current_item=QueueTrackRef(queue_item_id=11, track_id="376286112"),
        )
    )

    assert engine.pending_paused_seek_ms == 50_000
    assert provider.mass.player_queues.calls == []


@pytest.mark.asyncio
async def test_play_after_paused_seek_resumes_at_pending_position() -> None:
    """A later PLAYING command applies the stored paused seek once."""
    provider = _FakeProvider(_queue(PlaybackState.PAUSED))
    engine = QobuzConnectSyncEngine(provider)
    # In production a paused-seek is only ever stored after the engine has
    # learned about the current item via a prior SetState — replicate that
    # so this test exercises the same path the runtime does.
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.paused_seek = PausedSeek(position_ms=50_000)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
            current_item=QueueTrackRef(queue_item_id=11, track_id="376286112"),
        )
    )
    await _wait_for_reconcile(engine)

    assert cast("Any", engine).paused_seek is None
    assert provider.mass.player_queues.calls == [
        ("play_index", ("player", 0), {"seek_position": 50})
    ]


@pytest.mark.asyncio
async def test_paused_seek_is_not_reused_for_new_track() -> None:
    """A paused seek belongs to one Qobuz queue item, not the next selected track."""
    provider = _FakeProvider(_queue(PlaybackState.PAUSED, track_id="old"))
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PAUSED,
            position_ms=80_000,
            current_item=QueueTrackRef(queue_item_id=1, track_id="old"),
        )
    )
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=2, track_id="new"),
        )
    )
    await _wait_for_reconcile(engine)

    assert cast("Any", engine).pending_paused_seek_ms is None
    assert provider.mass.player_queues.calls[-1] == (
        "play_index",
        ("player", 0),
        {"seek_position": 0},
    )


@pytest.mark.asyncio
async def test_new_current_item_without_position_starts_at_zero_not_old_position() -> None:
    """Track changes without currentPosition must not inherit the previous track position."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PAUSED, track_id="old"), session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=1, track_id="old")
    engine.qobuz_state.position_ms = 92_000

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=2, track_id="new"),
        )
    )

    assert session.renderer_states[0]["position_ms"] == 0
    await _wait_for_reconcile(engine)
    assert provider.mass.player_queues.calls[-1] == (
        "play_index",
        ("player", 0),
        {"seek_position": 0},
    )


@pytest.mark.asyncio
async def test_qobuz_play_reports_buffering_before_ma_confirms() -> None:
    """A Qobuz play command should not flicker back to paused while MA loads."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PAUSED)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
            current_item=QueueTrackRef(queue_item_id=11, track_id="376286112"),
        )
    )

    assert session.renderer_states[0]["playing_state"] == PlayingState.PLAYING
    assert engine.qobuz_state.buffer_state == BufferState.BUFFERING
    assert session.renderer_states[0]["buffer_state"] == BufferState.BUFFERING


@pytest.mark.asyncio
async def test_stale_ma_pause_does_not_override_pending_qobuz_play() -> None:
    """While buffering, stale MA state must not undo the latest Qobuz command."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PAUSED)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.BUFFERING
    engine.qobuz_state.position_ms = 25_000

    await engine.report_state()

    assert engine.qobuz_state.playing_state == PlayingState.PLAYING
    assert engine.qobuz_state.buffer_state == BufferState.BUFFERING
    assert session.renderer_states[-1]["playing_state"] == PlayingState.PLAYING
    assert session.renderer_states[-1]["buffer_state"] == BufferState.BUFFERING


@pytest.mark.asyncio
async def test_ma_playing_confirmation_clears_buffering() -> None:
    """Buffering clears once MA reaches the commanded play state."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.BUFFERING

    await engine.report_state()

    assert cast("Any", engine.qobuz_state).buffer_state == BufferState.OK
    assert cast("Any", session.renderer_states[-1])["buffer_state"] == BufferState.OK


@pytest.mark.asyncio
async def test_ma_position_past_pending_seek_clears_buffering() -> None:
    """A slow confirmation can arrive after MA has already passed the exact seek target."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    queue.corrected_elapsed_time = 76.2
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.BUFFERING
    engine.qobuz_position = PendingQobuzPosition(
        target_ms=73_794,
        issued_ms=73_794,
        timestamp_ms=int(time.time() * 1000),
    )

    await engine.report_state()

    assert engine.qobuz_state.buffer_state == BufferState.OK
    assert cast("Any", engine).qobuz_position is None
    assert session.renderer_states[-1]["position_ms"] == 76_200


@pytest.mark.asyncio
async def test_stop_based_pause_confirms_qobuz_pause() -> None:
    """MA outputs that stop on pause should still leave QiOS showing paused."""
    session = _FakeSession()
    queue = _queue(PlaybackState.IDLE)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PAUSED
    engine.qobuz_state.buffer_state = BufferState.BUFFERING
    engine.qobuz_state.position_ms = 33_000

    await engine.report_state()

    assert engine.qobuz_state.playing_state == PlayingState.PAUSED
    assert engine.qobuz_state.buffer_state == BufferState.OK
    assert engine.qobuz_state.position_ms == 33_000
    assert session.renderer_states[-1]["playing_state"] == PlayingState.PAUSED


@pytest.mark.asyncio
async def test_paused_buffering_is_not_sent_to_qobuz() -> None:
    """Only playing startup exposes BUFFERING to clients."""
    session = _FakeSession()
    queue = _queue(PlaybackState.IDLE)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PAUSED
    engine.qobuz_state.buffer_state = BufferState.BUFFERING

    await engine.report_state(sync_from_ma=False)

    assert session.renderer_states[-1]["playing_state"] == PlayingState.PAUSED
    assert session.renderer_states[-1]["buffer_state"] == BufferState.OK


@pytest.mark.asyncio
async def test_mid_track_handoff_reports_target_position_before_loading() -> None:
    """The first report for a handoff should carry Qobuz's target position."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PAUSED, track_id="old"), session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=72_000,
            current_item=QueueTrackRef(queue_item_id=11, track_id="376286112"),
        )
    )

    assert session.renderer_states[0]["position_ms"] == 72_000
    assert engine.qobuz_state.buffer_state == BufferState.BUFFERING
    assert session.renderer_states[0]["buffer_state"] == BufferState.BUFFERING
    assert ("play_media",) not in [(call[0],) for call in provider.mass.player_queues.calls]
    await _wait_for_reconcile(engine)
    assert provider.mass.player_queues.calls[-1] == (
        "play_index",
        ("player", 0),
        {"seek_position": 72},
    )


@pytest.mark.asyncio
async def test_play_after_stop_based_pause_restarts_current_item() -> None:
    """Players that implement pause as stop should still resume from Qobuz play."""
    provider = _FakeProvider(_queue(PlaybackState.IDLE))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PAUSED
    engine.qobuz_state.position_ms = 72_000

    await engine.handle_qobuz_set_state(SetStateEvent(playing_state=PlayingState.PLAYING))
    await _wait_for_reconcile(engine)

    assert provider.mass.player_queues.calls == [
        ("play_index", ("player", 0), {"seek_position": 72})
    ]


@pytest.mark.asyncio
async def test_resume_report_does_not_advance_by_paused_duration(monkeypatch: Any) -> None:
    """Resume should report from the paused position, not from the old pause timestamp."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.IDLE), session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PAUSED
    engine.qobuz_state.position_ms = 72_000
    engine.qobuz_state.position_timestamp_ms = 95_000
    monkeypatch.setattr(
        "music_assistant.providers.qobuz_connect.sync.time.time",
        lambda: 100.0,
    )

    await engine.handle_qobuz_set_state(SetStateEvent(playing_state=PlayingState.PLAYING))

    assert session.renderer_states[-1]["position_ms"] == 72_000


@pytest.mark.asyncio
async def test_playing_report_uses_anchor_not_interpolated_position(monkeypatch: Any) -> None:
    """State reports should not double-interpolate during volume or heartbeat reports."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PLAYING), session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.OK
    engine.qobuz_state.position_ms = 42_000
    engine.qobuz_state.position_timestamp_ms = 100_000
    monkeypatch.setattr(
        "music_assistant.providers.qobuz_connect.sync.time.time",
        lambda: 105.0,
    )

    await engine.report_state(sync_from_ma=False)

    assert session.renderer_states[-1]["position_ms"] == 42_000
    assert session.renderer_states[-1]["position_timestamp_ms"] == 100_000


@pytest.mark.asyncio
async def test_ma_natural_advance_promotes_known_qobuz_next_item() -> None:
    """MA advancing to Qobuz next should update the mirror, not send MA-origin load."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING, track_id="397744036")
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=1, track_id="217628808")
    engine.qobuz_state.next_item = QueueTrackRef(queue_item_id=2, track_id="397744036")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.position_ms = 73_794
    engine.qobuz_position = PendingQobuzPosition(
        target_ms=73_794,
        issued_ms=73_794,
        timestamp_ms=int(time.time() * 1000),
    )

    await engine.handle_ma_queue_event(_event(queue))

    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=2,
        track_id="397744036",
    )
    assert cast("Any", engine).qobuz_state.next_item is None
    assert engine.qobuz_state.position_ms == 10_000
    assert cast("Any", engine).qobuz_position is None
    assert session.queue_loads == []
    assert session.renderer_states[-1]["queue_item_id"] == 2


@pytest.mark.asyncio
async def test_play_with_only_next_item_promotes_next_to_current() -> None:
    """Qobuz can send the selected item as nextQueueItem before PLAYING."""
    provider = _FakeProvider(_queue(PlaybackState.PAUSED, track_id="old"))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.next_item = QueueTrackRef(queue_item_id=11, track_id="376286112")

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
        )
    )
    await _wait_for_reconcile(engine)

    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=11,
        track_id="376286112",
    )
    assert provider.mass.player_queues.calls[-1][0] == "play_index"


@pytest.mark.asyncio
async def test_rapid_next_only_loads_latest_qobuz_item() -> None:
    """Slow MA work for skipped items must not briefly become playback."""
    queue = _queue(PlaybackState.PAUSED, track_id="old")
    provider = _FakeProvider(queue)
    lookup_started = asyncio.Event()
    release_lookup = asyncio.Event()
    lookups: list[str] = []

    async def _slow_get_track(track_id: str) -> Any:
        lookups.append(track_id)
        lookup_started.set()
        await release_lookup.wait()
        return await _fake_get_track(track_id)

    provider.qobuz_provider = SimpleNamespace(
        get_track=_slow_get_track,
        get_album_tracks=_fake_get_album_tracks,
    )
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=5, track_id="track-5"),
        )
    )
    await lookup_started.wait()
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=6, track_id="track-6"),
        )
    )
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=7, track_id="track-7"),
        )
    )
    release_lookup.set()
    await _wait_for_reconcile(engine)

    assert queue.current_item.track_id == "track-7"
    assert provider.mass.player_queues.calls[-1] == (
        "play_index",
        ("player", 0),
        {"seek_position": 0},
    )
    assert [
        getattr(item.media_item, "item_id", None)
        for item in provider.mass.player_queues.queue_items
    ] == ["track-7"]
    assert "track-5" in lookups


@pytest.mark.asyncio
async def test_fast_next_then_pause_does_not_resume_superseded_track() -> None:
    """A later pause command should cancel an older slow play reconciliation."""
    queue = _queue(PlaybackState.PLAYING, track_id="old")
    provider = _FakeProvider(queue)
    lookup_started = asyncio.Event()
    release_lookup = asyncio.Event()

    async def _slow_get_track(track_id: str) -> Any:
        lookup_started.set()
        await release_lookup.wait()
        return await _fake_get_track(track_id)

    provider.qobuz_provider = SimpleNamespace(
        get_track=_slow_get_track,
        get_album_tracks=_fake_get_album_tracks,
    )
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=5, track_id="track-5"),
        )
    )
    await lookup_started.wait()
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PAUSED,
            position_ms=12_000,
            current_item=QueueTrackRef(queue_item_id=6, track_id="track-6"),
        )
    )
    release_lookup.set()
    await _wait_for_reconcile(engine)

    assert provider.mass.player_queues.calls == [("pause", ("player",), {})]
    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=6,
        track_id="track-6",
    )
    assert engine.qobuz_state.playing_state == PlayingState.PAUSED


@pytest.mark.asyncio
async def test_stale_ma_track_does_not_confirm_latest_qobuz_command() -> None:
    """An old MA track must not clear buffering for the latest Qobuz item."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING, track_id="old")
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=7, track_id="track-7")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.BUFFERING
    engine.qobuz_state.position_ms = 44_000

    await engine.report_state()

    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=7,
        track_id="track-7",
    )
    assert engine.qobuz_state.buffer_state == BufferState.BUFFERING
    assert session.renderer_states[-1]["queue_item_id"] == 7
    assert session.renderer_states[-1]["buffer_state"] == BufferState.BUFFERING


@pytest.mark.asyncio
async def test_later_track_command_cancels_pending_seek() -> None:
    """A debounced seek for an old item must not run after a fast next command."""
    queue = _queue(PlaybackState.PLAYING, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=1, track_id="old")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=50_000))
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=2, track_id="new"),
        )
    )
    await asyncio.sleep(0.4)
    await _wait_for_reconcile(engine)

    assert ("seek", ("player", 50), {}) not in provider.mass.player_queues.calls
    assert queue.current_item.track_id == "new"

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=60_000))
    await asyncio.sleep(0.4)

    assert provider.mass.player_queues.calls[-1] == ("seek", ("player", 60), {})
    await engine.stop()


@pytest.mark.asyncio
async def test_unavailable_target_does_not_replay_previous_command() -> None:
    """Missing target players should not crash or mutate MA queue state."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PAUSED, track_id="old")
    provider = _NoTargetProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=7, track_id="new"),
        )
    )
    await _wait_for_reconcile(engine)

    assert provider.mass.player_queues.calls == []
    assert queue.current_item.track_id == "old"
    assert engine.qobuz_state.current_item == QueueTrackRef(queue_item_id=7, track_id="new")
    assert session.renderer_states[0]["queue_item_id"] == 7


@pytest.mark.asyncio
async def test_metadata_only_set_state_updates_mirror_without_report_loop() -> None:
    """Queue metadata updates should not be acknowledged as playback state changes."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PLAYING), session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            queue_version=QueueVersion(major=3, minor=1),
            current_item=QueueTrackRef(queue_item_id=15, track_id="230178773"),
            next_item=QueueTrackRef(queue_item_id=16, track_id="402075129"),
        )
    )

    assert engine.qobuz_state.queue_version == QueueVersion(major=3, minor=1)
    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=15,
        track_id="230178773",
    )
    assert engine.qobuz_state.next_item == QueueTrackRef(
        queue_item_id=16,
        track_id="402075129",
    )
    assert session.renderer_states == []


@pytest.mark.asyncio
async def test_ma_origin_queue_load_uses_current_cloud_version_until_ack() -> None:
    """MA-origin Qobuz playback should not invent queue ids or bump versions locally."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING, track_id="370969289")
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=17, track_id="403341158")
    engine.qobuz_state.queue_version = QueueVersion(major=3, minor=1)

    task = asyncio.create_task(engine.handle_ma_queue_event(_event(queue)))
    while not session.queue_loads:
        await asyncio.sleep(0)

    assert session.queue_loads[0]["track_id"] == "370969289"
    assert session.queue_loads[0]["queue_version"] == QueueVersion(major=3, minor=1)
    assert session.queue_loads[0]["qweb_track_session"] is True
    assert "qobuz_reference_id" not in session.queue_loads[0]
    assert "queue_position" not in session.queue_loads[0]
    assert len(session.queue_loads[0]["context_uuid"]) == 16
    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=17,
        track_id="403341158",
    )
    await engine.handle_queue_load_ack(
        QueueLoadAck(
            action_uuid=session.queue_loads[0]["action_uuid"],
            queue_version=QueueVersion(major=4, minor=0),
            tracks=[QueueTrackRef(queue_item_id=21, track_id="370969289")],
        )
    )
    await task

    assert engine.qobuz_state.queue_version == QueueVersion(major=4, minor=0)
    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=21,
        track_id="370969289",
    )


@pytest.mark.asyncio
async def test_ma_origin_queue_load_uses_qweb_style_track_session_for_slug_album() -> None:
    """Slug album ids still work because QWeb-style load encodes the track id directly."""
    queue = _queue(PlaybackState.PLAYING, track_id="370969289")
    session = _FakeSession()
    provider = _FakeProvider(queue, session=session)

    async def _get_track_with_slug_album(_track_id: str) -> Any:
        return SimpleNamespace(
            item_id="370969289",
            duration=180,
            album=SimpleNamespace(item_id="m652z4hjywkia"),
            track_number=2,
        )

    provider.qobuz_provider = SimpleNamespace(
        get_track=_get_track_with_slug_album,
        get_album_tracks=_fake_get_album_tracks,
    )
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=3, minor=1)

    await engine.handle_ma_queue_event(_event(queue))

    assert session.queue_loads[0]["track_id"] == "370969289"
    assert session.queue_loads[0]["qweb_track_session"] is True
    assert session.autoplay_loads == []


@pytest.mark.asyncio
async def test_pending_ma_origin_load_does_not_prequeue_qobuz_next_echo() -> None:
    """MA-origin command echoes must not replace MA's next item with Qobuz autoplay data."""
    queue = _queue(PlaybackState.PLAYING, track_id="411636993")
    provider = _FakeProvider(queue, session=_FakeSession())
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=3, track_id="411636993")
    engine._pending_queue_loads[b"pending"] = asyncio.get_running_loop().create_future()

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            queue_version=QueueVersion(major=7, minor=2),
            next_item=QueueTrackRef(queue_item_id=4, track_id="382400241"),
        )
    )

    assert provider.mass.player_queues.calls == []


@pytest.mark.asyncio
async def test_unresolved_qobuz_cloud_track_does_not_replace_ma_queue() -> None:
    """Bogus cloud track ids must not crash the websocket or poison MA playback."""
    queue = _queue(PlaybackState.PLAYING, track_id="370969289")
    provider = _FakeProvider(queue, session=_FakeSession())

    async def _get_track_or_fail(track_id: str) -> Any:
        if track_id == "4031841267":
            msg = "track/get not found"
            raise RuntimeError(msg)
        return await _fake_get_track(track_id)

    provider.qobuz_provider = SimpleNamespace(
        get_track=_get_track_or_fail,
        get_album_tracks=_fake_get_album_tracks,
    )
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=365,
            queue_version=QueueVersion(major=6, minor=1),
            current_item=QueueTrackRef(queue_item_id=0, track_id="4031841267"),
            next_item=QueueTrackRef(queue_item_id=1, track_id="4265709145"),
        )
    )

    assert provider.mass.player_queues.calls == []
    assert queue.current_item.track_id == "370969289"


@pytest.mark.asyncio
async def test_ma_origin_queue_error_keeps_existing_qobuz_mirror() -> None:
    """A rejected MA-origin load must not make the Qobuz app show synthetic state."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING, track_id="370969289")
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=17, track_id="403341158")
    engine.qobuz_state.queue_version = QueueVersion(major=3, minor=1)

    task = asyncio.create_task(engine.handle_ma_queue_event(_event(queue)))
    while not session.queue_loads:
        await asyncio.sleep(0)
    await engine.handle_queue_error(
        QueueError(
            action_uuid=session.queue_loads[0]["action_uuid"],
            queue_version=QueueVersion(major=3, minor=1),
            code="ERROR_QUEUE_LOAD_TRACKS",
            message="Queue version mismatch",
        )
    )
    await task

    assert engine.qobuz_state.queue_version == QueueVersion(major=3, minor=1)
    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=17,
        track_id="403341158",
    )
    assert session.renderer_states == []


@pytest.mark.asyncio
async def test_non_qobuz_ma_track_does_not_send_queue_load() -> None:
    """Non-Qobuz MA playback is ignored by Qobuz queue loading."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    queue.current_item = SimpleNamespace()
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_ma_queue_event(_event(queue))

    assert session.queue_loads == []


@pytest.mark.asyncio
async def test_playing_position_echo_inside_tolerance_is_not_a_seek() -> None:
    """Heartbeat positions close to MA's corrected position are ignored as seeks."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=10_200))
    await asyncio.sleep(0.4)

    assert provider.mass.player_queues.calls == []


@pytest.mark.asyncio
async def test_playing_position_divergence_seeks_ma_once() -> None:
    """A real Qobuz playing seek is forwarded to MA."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=50_000))
    await asyncio.sleep(0.4)

    assert provider.mass.player_queues.calls == [("seek", ("player", 50), {})]
    await engine.stop()


@pytest.mark.asyncio
async def test_backward_seek_does_not_confirm_against_old_forward_position() -> None:
    """A backward seek must wait for MA to actually return near the target."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    queue.corrected_elapsed_time = 109
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.OK

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=19_000))
    await asyncio.sleep(0.4)
    await engine.report_state()

    assert engine.qobuz_state.buffer_state == BufferState.BUFFERING
    assert session.renderer_states[-1]["position_ms"] == 19_000
    assert session.renderer_states[-1]["buffer_state"] == BufferState.BUFFERING

    queue.corrected_elapsed_time = 20
    await engine.report_state()

    assert cast("Any", engine.qobuz_state).buffer_state == BufferState.OK
    assert cast("Any", session.renderer_states[-1])["buffer_state"] == BufferState.OK
    await engine.stop()


@pytest.mark.asyncio
async def test_playing_position_command_reports_buffering_until_confirmed() -> None:
    """Playing seek commands freeze QiOS while MA prepares the seeked stream."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PLAYING), session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.buffer_state = BufferState.OK

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=50_000))

    assert session.renderer_states[0]["playing_state"] == PlayingState.PLAYING
    assert session.renderer_states[0]["buffer_state"] == BufferState.BUFFERING
    await engine.stop()


async def test_handle_queue_state_replaces_mirror_tracks_and_flags() -> None:
    """SRVR_CTRL_QUEUE_STATE snapshots replace the mirror's authoritative queue."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    # Pre-populate with stale state to prove the snapshot wins.
    engine.qobuz_state.tracks = [QueueTrackRef(queue_item_id=99, track_id="stale")]
    engine.qobuz_state.shuffle_mode = True

    snapshot = QueueStateSnapshot(
        queue_version=QueueVersion(23, 1),
        action_uuid=b"\x00" * 16,
        tracks=[
            QueueTrackRef(queue_item_id=0, track_id="1065476"),
            QueueTrackRef(queue_item_id=1, track_id="1065477"),
        ],
        shuffle_mode=False,
        autoplay_mode=True,
    )
    await engine.handle_queue_state(snapshot)

    assert engine.qobuz_state.queue_version == QueueVersion(23, 1)
    assert [t.track_id for t in engine.qobuz_state.tracks] == ["1065476", "1065477"]
    assert engine.qobuz_state.shuffle_mode is False
    assert engine.qobuz_state.autoplay_mode is True
    await engine.stop()


async def test_handle_queue_tracks_added_appends_to_mirror() -> None:
    """Per-op TRACKS_ADDED delta extends the mirror's track list."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(24, 1)
    engine.qobuz_state.tracks = [QueueTrackRef(queue_item_id=10, track_id="existing")]

    delta = QueueTracksAddedEvent(
        queue_version=QueueVersion(24, 2),
        action_uuid=b"\x00" * 16,
        tracks=[QueueTrackRef(queue_item_id=16, track_id="1065478")],
    )
    await engine.handle_queue_tracks_added(delta)

    assert engine.qobuz_state.queue_version == QueueVersion(24, 2)
    assert [t.track_id for t in engine.qobuz_state.tracks] == ["existing", "1065478"]
    await engine.stop()


async def test_handle_mode_setters_update_mirror() -> None:
    """SET_LOOP/SHUFFLE/AUTOPLAY mode commands flip the corresponding mirror fields."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_loop_mode(LoopMode.REPEAT_ALL)
    await engine.handle_shuffle_mode(True)
    await engine.handle_autoplay_mode(False)

    assert engine.qobuz_state.loop_mode == LoopMode.REPEAT_ALL
    assert engine.qobuz_state.shuffle_mode is True
    assert engine.qobuz_state.autoplay_mode is False
    await engine.stop()


async def test_handle_queue_tracks_inserted_inserts_at_position() -> None:
    """INSERTED splices new tracks after ``insert_after`` while keeping order."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="a"),
        QueueTrackRef(queue_item_id=2, track_id="b"),
        QueueTrackRef(queue_item_id=3, track_id="c"),
    ]

    await engine.handle_queue_tracks_inserted(
        QueueTracksInsertedEvent(
            queue_version=QueueVersion(7, 1),
            action_uuid=b"\x00" * 16,
            tracks=[QueueTrackRef(queue_item_id=10, track_id="x")],
            insert_after=1,
        )
    )

    assert engine.qobuz_state.queue_version == QueueVersion(7, 1)
    assert [t.track_id for t in engine.qobuz_state.tracks] == ["a", "x", "b", "c"]
    await engine.stop()


async def test_handle_queue_tracks_removed_drops_by_id() -> None:
    """REMOVED drops every track whose queue_item_id matches."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="a"),
        QueueTrackRef(queue_item_id=2, track_id="b"),
        QueueTrackRef(queue_item_id=3, track_id="c"),
    ]

    await engine.handle_queue_tracks_removed(
        QueueTracksRemovedEvent(
            queue_version=QueueVersion(7, 2),
            action_uuid=b"\x00" * 16,
            queue_item_ids=[1, 3],
        )
    )

    assert [t.queue_item_id for t in engine.qobuz_state.tracks] == [2]
    await engine.stop()


async def test_handle_queue_tracks_reordered_moves_block_to_target() -> None:
    """REORDERED moves the listed ids (in order) to sit after insert_after."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="a"),
        QueueTrackRef(queue_item_id=2, track_id="b"),
        QueueTrackRef(queue_item_id=3, track_id="c"),
        QueueTrackRef(queue_item_id=4, track_id="d"),
    ]

    await engine.handle_queue_tracks_reordered(
        QueueTracksReorderedEvent(
            queue_version=QueueVersion(7, 3),
            action_uuid=b"\x00" * 16,
            queue_item_ids=[1, 2],
            insert_after=2,
        )
    )

    # After removing 1 and 2, the remaining queue is [c, d]; inserting
    # the moved block at index 2 places them at the end.
    assert [t.track_id for t in engine.qobuz_state.tracks] == ["c", "d", "a", "b"]
    await engine.stop()


async def test_handle_queue_cleared_empties_mirror() -> None:
    """QUEUE_CLEARED zeroes the mirror's track list and bumps the version."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.tracks = [QueueTrackRef(queue_item_id=99, track_id="stale")]

    await engine.handle_queue_cleared(
        QueueClearedEvent(
            queue_version=QueueVersion(8, 0),
            action_uuid=b"\x00" * 16,
        )
    )

    assert engine.qobuz_state.tracks == []
    assert engine.qobuz_state.queue_version == QueueVersion(8, 0)
    await engine.stop()


# ---------------------------------------------------------------------------
# Regression tests for the seek-storm + track-replace cancellation bugs
# observed on a Raspberry Pi deployment (May 2026). See PR description for the
# full timeline. These two scenarios both stem from a shared root cause: the
# reconcile pipeline treats every Qobuz state event as something that can
# cancel work already in flight to MA, even when the new event is purely
# additive (e.g. a position-only seek arriving during a track replace).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rapid_seeks_during_buffering_do_not_stack_ma_seek_calls() -> None:
    """Repeated Qobuz seeks while MA hasn't caught up to the prior seek must not stack."""
    queue = _queue(PlaybackState.PLAYING)
    # MA stays "stuck at 10s" for the whole test — simulates a slow AirPlay
    # restart that hasn't surfaced the new position yet. Three Qobuz seeks
    # outside the 350ms debounce window should still result in *one* bridge
    # seek, not three, because the first one is still in flight.
    queue.corrected_elapsed_time = 10
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=60_000))
    await asyncio.sleep(0.4)
    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=120_000))
    await asyncio.sleep(0.4)
    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=180_000))
    await asyncio.sleep(0.4)

    seek_calls = [call for call in provider.mass.player_queues.calls if call[0] == "seek"]
    assert len(seek_calls) == 1, (
        f"Expected exactly 1 bridge.seek while MA is still buffering the first one, "
        f"got {len(seek_calls)}: {seek_calls}"
    )
    await engine.stop()


@pytest.mark.asyncio
async def test_deferred_seek_target_reissues_after_ma_confirms_first() -> None:
    """When MA finally catches up to an in-flight seek, a newer pending target re-issues."""
    queue = _queue(PlaybackState.PLAYING)
    queue.corrected_elapsed_time = 10
    provider = _FakeProvider(queue)
    session = _FakeSession()
    provider.qobuz_session = session
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=60_000))
    await asyncio.sleep(0.4)
    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=180_000))
    await asyncio.sleep(0.4)

    seek_calls = [call for call in provider.mass.player_queues.calls if call[0] == "seek"]
    assert seek_calls == [("seek", ("player", 60), {})], (
        f"Expected only the first seek so far; got {seek_calls}"
    )

    # MA finally catches up to the first issued target.
    queue.corrected_elapsed_time = 60
    await engine.report_state()
    await asyncio.sleep(0.4)

    seek_calls = [call for call in provider.mass.player_queues.calls if call[0] == "seek"]
    assert seek_calls == [
        ("seek", ("player", 60), {}),
        ("seek", ("player", 180), {}),
    ], f"Expected the deferred 180s target to fire once MA confirmed 60s; got {seek_calls}"
    await engine.stop()


@pytest.mark.asyncio
async def test_skip_then_immediate_seek_does_not_leave_player_stopped() -> None:
    """Position-only seek arriving mid-track-replace must not abort the new track's play_index."""
    queue = _queue(PlaybackState.PLAYING, track_id="old")
    provider = _FakeProvider(queue)
    load_gate = asyncio.Event()
    load_reached = asyncio.Event()
    orig_load = provider.mass.player_queues.load

    async def gated_load(*args: Any, **kwargs: Any) -> None:
        load_reached.set()
        await load_gate.wait()
        await orig_load(*args, **kwargs)

    cast("Any", provider.mass.player_queues).load = gated_load

    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=1, track_id="old")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    track_change_task = asyncio.create_task(
        engine.handle_qobuz_set_state(
            SetStateEvent(
                playing_state=PlayingState.PLAYING,
                current_item=QueueTrackRef(queue_item_id=2, track_id="new"),
            )
        )
    )

    await load_reached.wait()
    # The replace has already done stop_queue + clear; it's parked on load_queue.
    # A position-only seek arrives now — exactly the race that left the player
    # stopped on the Pi.
    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=45_000))

    load_gate.set()
    await track_change_task
    await _wait_for_reconcile(engine)
    await asyncio.sleep(0.5)

    play_index_calls = [
        call for call in provider.mass.player_queues.calls if call[0] == "play_index"
    ]
    assert play_index_calls, (
        f"play_index was never called; player would be left stopped. "
        f"Calls: {provider.mass.player_queues.calls}"
    )
    # The play_index that actually started the new track must respect the
    # seek that came in during the replace — otherwise the user would see
    # the track start from zero and then jump.
    final_play_index = play_index_calls[-1]
    assert final_play_index[2].get("seek_position") == 45, (
        f"Expected play_index to use the seek position from the position-only event "
        f"that arrived during the replace, got {final_play_index}"
    )
    await engine.stop()
