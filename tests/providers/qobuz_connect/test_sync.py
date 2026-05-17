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
    Origin,
    OutboundActionKind,
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
    SessionStateEvent,
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
        self._next_qid: int = 1000

    def _make_qid(self) -> str:
        self._next_qid += 1
        return f"qid-{self._next_qid}"

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
        # Ensure every item has a queue_item_id the reconciler can address.
        for item in queue_items:
            if getattr(item, "queue_item_id", None) is None:
                item.queue_item_id = self._make_qid()
        # Mirror the real ``mass.player_queues.load`` behavior so tests can
        # exercise positional inserts: keep the existing prefix above the
        # insert point and the existing suffix below it.
        insert_at_index = kwargs.get("insert_at_index", 0)
        keep_played = kwargs.get("keep_played", False)
        keep_remaining = kwargs.get("keep_remaining", False)
        prev = self.queue_items[:insert_at_index] if keep_played else []
        tail = self.queue_items[insert_at_index:] if keep_remaining else []
        self.queue_items = prev + queue_items + tail

    async def play_media(self, queue_id: str, media: Any, **kwargs: Any) -> None:
        self.calls.append(("play_media", (queue_id, media), kwargs))
        option = kwargs.get("option")
        if option is not None and option.name == "ADD":
            # Append to the queue, don't disrupt current playback (the
            # contract we're testing — ``play_media(option=ADD)`` is the
            # background reconciler's non-disrupting extend path).
            self.queue_items = self.queue_items + [
                SimpleNamespace(media_item=track, queue_item_id=self._make_qid()) for track in media
            ]
            return
        # Treat any other option (or no option) as a REPLACE-style call.
        self.queue.state = PlaybackState.PLAYING
        first = media[0] if isinstance(media, list) else media
        self.queue.current_item = SimpleNamespace(track_id=getattr(first, "item_id", None))

    def delete_item(self, queue_id: str, item_id_or_index: int | str) -> None:
        self.calls.append(("delete_item", (queue_id, item_id_or_index), {}))
        if queue_id != "player":
            return
        if isinstance(item_id_or_index, int):
            if 0 <= item_id_or_index < len(self.queue_items):
                del self.queue_items[item_id_or_index]
            return
        self.queue_items = [
            item
            for item in self.queue_items
            if getattr(item, "queue_item_id", None) != item_id_or_index
        ]

    def update_items(self, queue_id: str, items: list[Any]) -> None:
        self.calls.append(("update_items", (queue_id, items), {}))
        if queue_id == "player":
            self.queue_items = list(items)

    def items(self, queue_id: str, **_kwargs: Any) -> list[Any]:
        return self.queue_items if queue_id == "player" else []


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
        self.queue_state_asks: list[dict[str, Any]] = []
        self.queue_adds: list[dict[str, Any]] = []
        self.queue_inserts: list[dict[str, Any]] = []
        self.queue_removes: list[dict[str, Any]] = []
        self.queue_reorders: list[dict[str, Any]] = []
        self.clear_queues: list[dict[str, Any]] = []

    async def send_renderer_state(self, **kwargs: Any) -> None:
        self.renderer_states.append(kwargs)

    async def send_queue_load_tracks(self, **kwargs: Any) -> bool:
        self.queue_loads.append(kwargs)
        return True

    async def send_autoplay_load_tracks(self, **kwargs: Any) -> bool:
        self.autoplay_loads.append(kwargs)
        return True

    async def send_ask_for_queue_state(self, **kwargs: Any) -> bool:
        self.queue_state_asks.append(kwargs)
        return True

    async def send_queue_add_tracks(self, **kwargs: Any) -> bool:
        self.queue_adds.append(kwargs)
        return True

    async def send_queue_insert_tracks(self, **kwargs: Any) -> bool:
        self.queue_inserts.append(kwargs)
        return True

    async def send_queue_remove_tracks(self, **kwargs: Any) -> bool:
        self.queue_removes.append(kwargs)
        return True

    async def send_queue_reorder_tracks(self, **kwargs: Any) -> bool:
        self.queue_reorders.append(kwargs)
        return True

    async def send_clear_queue(self, **kwargs: Any) -> bool:
        self.clear_queues.append(kwargs)
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
        # Tests synthesize a few different shapes:
        # - ``queue.current_item`` is a ``SimpleNamespace(track_id=...)`` (the
        #   fake ``play_index`` constructs these).
        # - ``queue_items()`` returns raw ``QueueItem``s from ``load()``; here
        #   the Qobuz id lives on ``media_item.item_id``.
        # - ``play_media(option=ADD)`` items wrap a ``media_item=track`` shim.
        direct = getattr(queue_item, "track_id", None)
        if direct is not None:
            return cast("str", direct)
        media_item = getattr(queue_item, "media_item", None)
        if media_item is not None:
            item_id = getattr(media_item, "item_id", None)
            if item_id is not None:
                return cast("str", item_id)
        return None

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
        # ``provider`` lets ``get_qobuz_track_id_from_queue_item`` recognise
        # these items as Qobuz tracks — required by the preload's
        # already-in-MA dedup check.
        provider="qobuz",
        provider_mappings=(),
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


async def _wait_for_preload(engine: QobuzConnectSyncEngine) -> None:
    task = cast("Any", engine).command_handler._preload_task
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


# ---------------------------------------------------------------------------
# Regression tests for the renderer-switch cleanup gap observed in the same
# May 2026 testing session. When the user picks a different renderer in the
# Qobuz app, the cloud sends SRVR_RNDR_SET_ACTIVE(false). The receiver must
# stop playback *and* drop the queue items it loaded — otherwise the next
# time MA's UI is opened the user sees the abandoned Qobuz Connect tracks
# still queued — and it must stop echoing the now-stale Qobuz mirror back
# to the cloud.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_release_target_player_stops_and_clears_ma_queue() -> None:
    """Deactivation must both stop *and* clear the MA queue."""
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.release_target_player()

    call_kinds = [call[0] for call in provider.mass.player_queues.calls]
    assert "stop" in call_kinds, f"Expected stop, got: {call_kinds}"
    assert "clear" in call_kinds, f"Expected clear, got: {call_kinds}"
    # clear must come after stop so the player tears the stream down cleanly
    # before the queue items are dropped.
    assert call_kinds.index("clear") > call_kinds.index("stop"), call_kinds


@pytest.mark.asyncio
async def test_release_target_player_resets_qobuz_mirror() -> None:
    """Deactivation must wipe the Qobuz mirror so heartbeat reports stop firing."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.release_target_player()
    session.renderer_states.clear()
    await engine.report_state(sync_from_ma=False)

    assert cast("Any", engine).qobuz_state.current_item is None
    assert session.renderer_states == [], (
        f"No state should reach the cloud after release; got {session.renderer_states}"
    )


@pytest.mark.asyncio
async def test_ma_queue_event_after_release_does_not_send_origin_load() -> None:
    """MA queue events after deactivation must not echo as MA-origin queue loads."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING, track_id="370969289")
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.release_target_player()
    session.queue_loads.clear()
    await engine.handle_ma_queue_event(_event(queue))

    assert session.queue_loads == [], (
        f"No MA-origin load should be sent after release; got {session.queue_loads}"
    )


@pytest.mark.asyncio
async def test_release_then_activate_lets_qobuz_drive_again() -> None:
    """A later SET_STATE from Qobuz must work normally after release+activate."""
    queue = _queue(PlaybackState.IDLE)
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="old")
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.release_target_player()
    engine.set_active(active=True)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=20, track_id="new"),
        )
    )
    await _wait_for_reconcile(engine)

    play_index_calls = [
        call for call in provider.mass.player_queues.calls if call[0] == "play_index"
    ]
    assert play_index_calls, (
        f"play_index should fire after re-activation; got {provider.mass.player_queues.calls}"
    )


# ---------------------------------------------------------------------------
# Regression tests for full-queue loading via SRVR_CTRL_SESSION_STATE +
# CTRL_SRVR_ASK_FOR_QUEUE_STATE. The reference Web Client (handoff__client_b
# capture) sends the ask immediately when session-state arrives; without it
# the cloud never delivers SRVR_CTRL_QUEUE_STATE and MA's queue only ever
# sees current+next from SET_STATE.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_state_triggers_ask_for_queue_state() -> None:
    """A ``SRVR_CTRL_SESSION_STATE`` must elicit an ``ASK_FOR_QUEUE_STATE`` out."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.IDLE), session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_session_state(
        SessionStateEvent(
            session_uuid=b"\x01" * 16,
            session_id=42,
            queue_version=QueueVersion(major=17, minor=1),
            track_index=3,
        )
    )

    assert len(session.queue_state_asks) == 1, (
        f"Expected exactly one ASK_FOR_QUEUE_STATE; got {session.queue_state_asks}"
    )
    ask = session.queue_state_asks[0]
    assert ask["queue_version"] == QueueVersion(major=17, minor=1)
    assert isinstance(ask["queue_uuid"], bytes)
    assert len(ask["queue_uuid"]) == 16, (
        "Action correlator must be 16 raw bytes (uuid4().bytes), to match the "
        "Web Client's pattern observed in the handoff capture."
    )


@pytest.mark.asyncio
async def test_set_state_triggers_ask_for_queue_state_when_session_state_missing() -> None:
    """Renderer-role: cloud doesn't push SESSION_STATE, so SET_STATE qv triggers the ask.

    The Qobuz Web Client captures show ``SESSION_STATE`` as the natural ask trigger,
    but the cloud only pushes it to clients in the controller role (user-login JWT).
    We authenticate with a device-session JWT from ``/connect`` and never receive
    ``SESSION_STATE`` — so the ask is driven from the first ``SET_STATE`` that
    carries a real queue version. Verified locally May 2026 (removing the device-
    role handshake fields caused the cloud to close the WS with a type-1 ERROR).
    """
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.IDLE), session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=7, track_id="175293611"),
            next_item=QueueTrackRef(queue_item_id=8, track_id="264108972"),
        )
    )
    await _wait_for_reconcile(engine)

    assert len(session.queue_state_asks) == 1, (
        f"Expected an ASK driven by SET_STATE's queueVersion; got {session.queue_state_asks}"
    )
    assert session.queue_state_asks[0]["queue_version"] == QueueVersion(major=47, minor=1)


@pytest.mark.asyncio
async def test_ask_for_queue_state_coalesces_repeated_observations_at_same_qv() -> None:
    """Repeated SET_STATE events at the same ``queue_version`` produce one ask."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PLAYING), session=session)
    engine = QobuzConnectSyncEngine(provider)

    for _ in range(3):
        await engine.handle_qobuz_set_state(
            SetStateEvent(
                playing_state=PlayingState.PLAYING,
                queue_version=QueueVersion(major=47, minor=1),
                current_item=QueueTrackRef(queue_item_id=7, track_id="175293611"),
            )
        )
    await _wait_for_reconcile(engine)

    assert len(session.queue_state_asks) == 1, (
        f"Same-qv observations must coalesce; got {session.queue_state_asks}"
    )


@pytest.mark.asyncio
async def test_ask_for_queue_state_re_fires_on_qv_bump() -> None:
    """Every distinct ``queue_version`` the cloud announces triggers a fresh ask.

    The Qobuz app bumps ``queue_version`` on every queue mutation. Without
    re-asking, the mirror keeps the stale track list and the reconciler has
    nothing to apply (the local-test reorder bug observed in production).
    """
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PLAYING), session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=7, track_id="175293611"),
        )
    )
    await _wait_for_reconcile(engine)
    await engine.handle_queue_version(QueueVersion(major=47, minor=2))
    await engine.handle_queue_version(QueueVersion(major=47, minor=3))

    assert [
        (ask["queue_version"].major, ask["queue_version"].minor) for ask in session.queue_state_asks
    ] == [(47, 1), (47, 2), (47, 3)], (
        f"Each new qv must trigger a fresh ask; got {session.queue_state_asks}"
    )


@pytest.mark.asyncio
async def test_reactivation_re_arms_the_queue_state_ask() -> None:
    """After release + set_active(True), the next queueVersion must ask again."""
    session = _FakeSession()
    provider = _FakeProvider(_queue(PlaybackState.PLAYING), session=session)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=7, track_id="175293611"),
        )
    )
    await _wait_for_reconcile(engine)
    assert len(session.queue_state_asks) == 1

    await engine.release_target_player()
    engine.set_active(active=True)
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=48, minor=1),
            current_item=QueueTrackRef(queue_item_id=9, track_id="t9"),
        )
    )
    await _wait_for_reconcile(engine)

    assert len(session.queue_state_asks) == 2, (
        f"Reactivation must rearm the ask; got {session.queue_state_asks}"
    )


@pytest.mark.asyncio
async def test_session_state_updates_mirror_queue_version() -> None:
    """Session-state's queue version must land on the mirror before the ask fires."""
    provider = _FakeProvider(_queue(PlaybackState.IDLE), session=_FakeSession())
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=2, minor=0)

    await engine.handle_session_state(
        SessionStateEvent(
            session_uuid=b"\x01" * 16,
            session_id=99,
            queue_version=QueueVersion(major=45, minor=1),
        )
    )

    assert engine.qobuz_state.queue_version == QueueVersion(major=45, minor=1)


@pytest.mark.asyncio
async def test_replace_loads_only_current_and_next_for_fast_first_audio() -> None:
    """Initial replace must stay fast: current + next only, even if the snapshot is present.

    The full snapshot is filled in via the background preload — see
    :func:`test_queue_state_snapshot_schedules_background_preload`.
    """
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    # Snapshot already on the mirror (e.g. QUEUE_STATE arrived first).
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
        QueueTrackRef(queue_item_id=3, track_id="t3"),
        QueueTrackRef(queue_item_id=4, track_id="t4"),
    ]

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=2, track_id="t2"),
            next_item=QueueTrackRef(queue_item_id=3, track_id="t3"),
        )
    )
    await _wait_for_reconcile(engine)

    load_calls = [c for c in provider.mass.player_queues.calls if c[0] == "load"]
    loaded_ids = [getattr(qi.media_item, "item_id", None) for qi in load_calls[-1][1][1]]
    assert loaded_ids == ["t2", "t3"], (
        f"Initial replace must be fast (current+next only); got {loaded_ids}. "
        "The rest of the queue is the background preload's job."
    )
    play_index_calls = [c for c in provider.mass.player_queues.calls if c[0] == "play_index"]
    assert play_index_calls[-1][1][1] == 0, (
        f"play_index must target the just-loaded position 0, got {play_index_calls[-1]}"
    )


@pytest.mark.asyncio
async def test_replace_falls_back_to_current_next_without_snapshot() -> None:
    """Pre-snapshot behaviour: load only current + next when no full list is known."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    # qobuz_state.tracks is empty — snapshot hasn't landed yet.
    assert engine.qobuz_state.tracks == []

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            current_item=QueueTrackRef(queue_item_id=2, track_id="current"),
            next_item=QueueTrackRef(queue_item_id=3, track_id="next"),
        )
    )
    await _wait_for_reconcile(engine)

    load_calls = [c for c in provider.mass.player_queues.calls if c[0] == "load"]
    assert load_calls, f"Expected a load; got {provider.mass.player_queues.calls}"
    loaded_items = load_calls[-1][1][1]
    loaded_ids = [getattr(qi.media_item, "item_id", None) for qi in loaded_items]
    assert loaded_ids == ["current", "next"], f"Got {loaded_ids}"


@pytest.mark.asyncio
async def test_queue_state_snapshot_schedules_background_preload() -> None:
    """Snapshot landing after SET_STATE must extend MA in the background, not restart it.

    Production race: SET_STATE fills MA with 2 items, then QUEUE_STATE arrives
    80-300 ms later with the full list. The fix isn't a full re-load (which
    restarts the audio stream on AirPlay) — it's a chunked
    ``play_media(option=ADD)`` background task. Verified in production log
    May 2026: ``Qobuz QUEUE_STATE qv=47.1 tracks=600``.
    """
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)

    # 1. SET_STATE first — only current + next get loaded into MA.
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=2, track_id="t2"),
            next_item=QueueTrackRef(queue_item_id=3, track_id="t3"),
        )
    )
    await _wait_for_reconcile(engine)
    loads_after_set_state = sum(1 for c in provider.mass.player_queues.calls if c[0] == "load")
    play_indexes_after_set_state = sum(
        1 for c in provider.mass.player_queues.calls if c[0] == "play_index"
    )

    # 2. QUEUE_STATE response arrives with the full track list — must not
    # trigger another stop/clear/load/play_index cycle.
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=47, minor=1),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=1, track_id="t1"),
                QueueTrackRef(queue_item_id=2, track_id="t2"),
                QueueTrackRef(queue_item_id=3, track_id="t3"),
                QueueTrackRef(queue_item_id=4, track_id="t4"),
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)

    calls_after = provider.mass.player_queues.calls
    # Destructive loads (= full queue replacements) must not happen again
    # after the initial replace. History inserts via
    # ``load(insert_at_index=…, keep_played=True, keep_remaining=True)``
    # are fine — they prepend to the queue without touching playback.
    destructive_loads_after = sum(
        1 for c in calls_after if c[0] == "load" and not c[2].get("keep_played", False)
    )
    play_indexes_after = sum(1 for c in calls_after if c[0] == "play_index")
    stops_after = sum(1 for c in calls_after if c[0] == "stop")
    clears_after = sum(1 for c in calls_after if c[0] == "clear")
    # The only destructive load is the initial SET_STATE replace.
    assert destructive_loads_after == loads_after_set_state, (
        f"Snapshot reconcile must not trigger another destructive load; calls={calls_after}"
    )
    assert play_indexes_after == play_indexes_after_set_state, (
        "Snapshot reconcile must not call play_index — playback continues uninterrupted"
    )
    # ``stop`` is fine if it was needed for the initial replace; what we
    # don't want is a *second* stop after the snapshot lands.
    initial_stop_present = stops_after <= 1
    assert initial_stop_present, f"Snapshot preload must not stop the queue; calls={calls_after}"
    assert clears_after <= 1, f"Snapshot preload must not clear the queue; calls={calls_after}"

    # The actual extension came via play_media(option=ADD).
    add_calls = [
        c
        for c in calls_after
        if c[0] == "play_media" and c[2].get("option") is not None and c[2]["option"].name == "ADD"
    ]
    assert add_calls, f"Expected a play_media(option=ADD) call; got {calls_after}"
    added_ids: list[str | None] = []
    for call in add_calls:
        added_ids.extend(getattr(track, "item_id", None) for track in call[1][1])
    # Snapshot has t1..t4 with current=t2 at index 1. Preload appends from
    # current_idx+1 onwards. ``t3`` is skipped (already in MA as ``next_item``).
    assert added_ids == ["t4"], (
        f"Preload must append snapshot[current_idx+1:] minus already-present items; got {added_ids}"
    )


@pytest.mark.asyncio
async def test_preload_chunks_large_queue() -> None:
    """600-track snapshot must be appended via multiple chunked ADD calls."""
    from music_assistant.providers.qobuz_connect.command_handler import (  # noqa: PLC0415
        PRELOAD_CHUNK_SIZE,
    )

    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    total_tracks = 120  # 5x the chunk size to give us multiple ADD calls

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=1, track_id="t0"),
        )
    )
    await _wait_for_reconcile(engine)

    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=47, minor=1),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=i + 1, track_id=f"t{i}") for i in range(total_tracks)
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)

    add_calls = [
        c
        for c in provider.mass.player_queues.calls
        if c[0] == "play_media" and c[2].get("option") is not None and c[2]["option"].name == "ADD"
    ]
    assert len(add_calls) >= 2, (
        f"Expected chunked ADD calls for 120 tracks; got {len(add_calls)} calls"
    )
    for call in add_calls:
        chunk = call[1][1]
        assert 0 < len(chunk) <= PRELOAD_CHUNK_SIZE, (
            f"Each chunk must be ≤ {PRELOAD_CHUNK_SIZE}; got chunk of {len(chunk)}"
        )
    added_ids: list[str | None] = []
    for call in add_calls:
        added_ids.extend(getattr(track, "item_id", None) for track in call[1][1])
    # current_item=t0 at index 0 → preload covers t1..t119.
    assert added_ids == [f"t{i}" for i in range(1, total_tracks)], (
        f"Preload must cover every snapshot track after current; got {len(added_ids)} ids"
    )


@pytest.mark.asyncio
async def test_repeated_queue_state_with_same_version_does_not_re_preload() -> None:
    """Identical queue_version snapshots must not re-trigger the preload."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=2, track_id="t2"),
        )
    )
    await _wait_for_reconcile(engine)

    snapshot = QueueStateSnapshot(
        queue_version=QueueVersion(major=47, minor=1),
        action_uuid=b"\x00" * 16,
        tracks=[
            QueueTrackRef(queue_item_id=1, track_id="t1"),
            QueueTrackRef(queue_item_id=2, track_id="t2"),
            QueueTrackRef(queue_item_id=3, track_id="t3"),
        ],
        shuffle_mode=False,
        autoplay_mode=False,
    )
    await engine.handle_queue_state(snapshot)
    await _wait_for_preload(engine)
    add_calls_after_first = sum(
        1
        for c in provider.mass.player_queues.calls
        if c[0] == "play_media" and c[2].get("option") is not None and c[2]["option"].name == "ADD"
    )

    await engine.handle_queue_state(snapshot)
    await _wait_for_preload(engine)
    add_calls_after_second = sum(
        1
        for c in provider.mass.player_queues.calls
        if c[0] == "play_media" and c[2].get("option") is not None and c[2]["option"].name == "ADD"
    )
    assert add_calls_after_second == add_calls_after_first, (
        f"Duplicate snapshot must not re-preload; "
        f"first run made {add_calls_after_first} ADD calls, second run made {add_calls_after_second}"
    )


@pytest.mark.asyncio
async def test_preload_skips_unresolvable_tracks_within_chunk() -> None:
    """One unresolvable track must not abort the chunk; resolvable ones still ADD."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)

    async def _selectively_resolve(track_id: str) -> Any:
        if track_id == "bad":
            msg = "no such track"
            raise RuntimeError(msg)
        return await _fake_get_track(track_id)

    provider.qobuz_provider = SimpleNamespace(
        get_track=_selectively_resolve,
        get_album_tracks=_fake_get_album_tracks,
    )
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=1, track_id="t1"),
        )
    )
    await _wait_for_reconcile(engine)

    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=47, minor=1),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=1, track_id="t1"),
                QueueTrackRef(queue_item_id=2, track_id="t2"),
                QueueTrackRef(queue_item_id=3, track_id="bad"),
                QueueTrackRef(queue_item_id=4, track_id="t4"),
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)

    add_calls = [
        c
        for c in provider.mass.player_queues.calls
        if c[0] == "play_media" and c[2].get("option") is not None and c[2]["option"].name == "ADD"
    ]
    added_ids: list[str | None] = []
    for call in add_calls:
        added_ids.extend(getattr(track, "item_id", None) for track in call[1][1])
    assert added_ids == ["t2", "t4"], (
        f"Unresolvable 'bad' must be skipped without aborting the rest; got {added_ids}"
    )


@pytest.mark.asyncio
async def test_preload_cancels_on_new_set_state() -> None:
    """A new SET_STATE during preload must cancel the preload — no further ADDs."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)

    # Gate the second metadata fetch so we can interleave a new SET_STATE.
    resolve_gate = asyncio.Event()
    resolve_started = asyncio.Event()
    resolved_ids: list[str] = []

    async def _gated_resolve(track_id: str) -> Any:
        if track_id == "tg":
            resolve_started.set()
            await resolve_gate.wait()
        resolved_ids.append(track_id)
        return await _fake_get_track(track_id)

    provider.qobuz_provider = SimpleNamespace(
        get_track=_gated_resolve,
        get_album_tracks=_fake_get_album_tracks,
    )
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=1, track_id="t1"),
        )
    )
    await _wait_for_reconcile(engine)

    snapshot_tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="tg"),  # the gated one
        QueueTrackRef(queue_item_id=3, track_id="t3"),
    ]
    snapshot_task = asyncio.create_task(
        engine.handle_queue_state(
            QueueStateSnapshot(
                queue_version=QueueVersion(major=47, minor=1),
                action_uuid=b"\x00" * 16,
                tracks=snapshot_tracks,
                shuffle_mode=False,
                autoplay_mode=False,
            )
        )
    )
    await resolve_started.wait()

    # Mid-preload, the user skips to a new track. Generation bumps; preload
    # should cancel and not issue any further ADD calls for the old snapshot.
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=48, minor=1),
            current_item=QueueTrackRef(queue_item_id=99, track_id="brand-new"),
        )
    )
    resolve_gate.set()
    await snapshot_task
    await _wait_for_preload(engine)
    await _wait_for_reconcile(engine)

    add_calls = [
        c
        for c in provider.mass.player_queues.calls
        if c[0] == "play_media" and c[2].get("option") is not None and c[2]["option"].name == "ADD"
    ]
    assert not add_calls, (
        f"Preload must not ADD anything after generation supersedes it; got {add_calls}"
    )


@pytest.mark.asyncio
async def test_skip_to_already_loaded_track_does_not_wipe_queue() -> None:
    """Skip-to-track within the preloaded queue must use play_index, not replace.

    Mirrors plex_connect/player_remote.py:1187 (handle_skip_to) — when the
    controller picks a track already sitting in MA's queue, we jump via
    ``play_index`` instead of stop+clear+load+play_index. Otherwise every
    skip-next would wipe the preloaded 600 tracks and force a refetch.
    """
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)

    # 1. Initial connect: SET_STATE + QUEUE_STATE → preload fills the queue.
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=1, track_id="t1"),
            next_item=QueueTrackRef(queue_item_id=2, track_id="t2"),
        )
    )
    await _wait_for_reconcile(engine)
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=47, minor=1),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=1, track_id="t1"),
                QueueTrackRef(queue_item_id=2, track_id="t2"),
                QueueTrackRef(queue_item_id=3, track_id="t3"),
                QueueTrackRef(queue_item_id=4, track_id="t4"),
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)
    ma_queue_after_preload = provider.mass.player_queues.queue_items
    assert len(ma_queue_after_preload) >= 3, (
        f"Preload must have filled the queue; got {len(ma_queue_after_preload)} items"
    )

    calls_before_skip = list(provider.mass.player_queues.calls)

    # 2. User skips to t3 in the Qobuz app. Cloud sends SET_STATE with
    # current_item=t3. t3 is already in MA's loaded queue.
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=3, track_id="t3"),
        )
    )
    await _wait_for_reconcile(engine)
    await _wait_for_preload(engine)

    new_calls = provider.mass.player_queues.calls[len(calls_before_skip) :]
    # No stop / clear / load — the loaded queue is intact.
    new_call_kinds = [c[0] for c in new_calls]
    assert "stop" not in new_call_kinds, f"Skip must not stop the queue; got {new_call_kinds}"
    assert "clear" not in new_call_kinds, f"Skip must not clear the queue; got {new_call_kinds}"
    assert "load" not in new_call_kinds, (
        f"Skip must not re-load the queue (would force a refetch); got {new_call_kinds}"
    )
    # A play_index call to t3's existing position fired.
    play_index_after_skip = [c for c in new_calls if c[0] == "play_index"]
    assert play_index_after_skip, f"Skip must call play_index; got new calls {new_calls}"
    # The skipped-to index is wherever t3 sits in the loaded list.
    expected_idx = next(
        i
        for i, qi in enumerate(ma_queue_after_preload)
        if getattr(qi.media_item, "item_id", None) == "t3"
    )
    assert play_index_after_skip[-1][1][1] == expected_idx, (
        f"play_index must target t3's existing index {expected_idx}; "
        f"got {play_index_after_skip[-1]}"
    )


@pytest.mark.asyncio
async def test_replace_resets_preload_dedup_so_partial_queue_can_refill() -> None:
    """If a replace wipes MA mid-preload, the dedup flag must be reset.

    Scenario: snapshot is huge, preload is still in flight, user skip-tos
    a track that's in the snapshot but hasn't been added to MA yet.
    ``_find_item_in_ma_queue`` doesn't find it → ``_replace_ma_queue_from_qobuz``
    fires → MA's preloaded items are wiped to just ``[new_current, next]``.
    Without resetting ``_last_preloaded_qv`` the post-replace
    ``maybe_preload_remaining_tracks`` would see the same qv and bail,
    stranding MA at 2 items.

    Unit-level check: after ``_replace_ma_queue_from_qobuz`` runs, the
    dedup flag is ``None`` regardless of whether the snapshot changed.
    """
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    handler = cast("Any", engine).command_handler
    # Simulate a preload having happened earlier in the session.
    handler._last_reconciled_qv = (47, 1)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=47, minor=1),
            current_item=QueueTrackRef(queue_item_id=2, track_id="t2"),
        )
    )
    await _wait_for_reconcile(engine)

    assert handler._last_reconciled_qv is None, (
        "Replace must reset the reconcile dedup so the same-qv snapshot can re-reconcile after a wipe."
    )


@pytest.mark.asyncio
async def test_natural_advance_then_metadata_set_state_preserves_preloaded_queue() -> None:
    """Regression guard: metadata-only SET_STATE must never mutate MA's queue.

    Before the bidirectional-sync rewrite this scenario ended in a 2-item
    queue after every track change, because ``_prequeue_next_item`` ran on
    every metadata-only SET_STATE and used ``play_media(option=REPLACE_NEXT)``
    which wiped everything past current. The prequeue path is gone now —
    queue mutations all flow through the cloud-side delta handlers — so the
    structural fix is to verify that a metadata-only SET_STATE issues no
    ``play_media`` calls at all.
    """
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=48, minor=1),
            current_item=QueueTrackRef(queue_item_id=21, track_id="t1"),
            next_item=QueueTrackRef(queue_item_id=22, track_id="t2"),
        )
    )
    await _wait_for_reconcile(engine)
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=48, minor=1),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=21, track_id="t1"),
                QueueTrackRef(queue_item_id=22, track_id="t2"),
                QueueTrackRef(queue_item_id=23, track_id="t3"),
                QueueTrackRef(queue_item_id=24, track_id="t4"),
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)
    pre_advance_queue_len = len(provider.mass.player_queues.queue_items)
    assert pre_advance_queue_len >= 3, (
        f"Reconcile must have extended MA's queue past current+next; "
        f"got {pre_advance_queue_len} items"
    )

    # Simulate MA naturally advancing t1 → t2.
    queue.current_item = SimpleNamespace(track_id="t2")
    queue.current_index = 1
    queue.state = PlaybackState.PLAYING
    await engine.handle_ma_queue_event(_event(queue))

    handler = cast("Any", engine).command_handler
    promoted_current = engine.qobuz_state.current_item
    assert promoted_current is not None, "Mirror must keep a current_item after advance"
    assert promoted_current.track_id == "t2", "Mirror must promote next→current on natural advance"

    calls_before_metadata = len(provider.mass.player_queues.calls)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            queue_version=QueueVersion(major=48, minor=1),
            next_item=QueueTrackRef(queue_item_id=23, track_id="t3"),
        )
    )
    metadata_task = handler._metadata_task
    if metadata_task is not None:
        with contextlib.suppress(asyncio.CancelledError):
            await metadata_task

    new_calls = provider.mass.player_queues.calls[calls_before_metadata:]
    play_media_calls = [c for c in new_calls if c[0] == "play_media"]
    assert not play_media_calls, (
        f"Metadata-only SET_STATE post-advance must not issue play_media "
        f"(would wipe preloaded queue tail); got {play_media_calls}"
    )
    assert len(provider.mass.player_queues.queue_items) == pre_advance_queue_len, (
        f"MA's queue length must be unchanged; was {pre_advance_queue_len}, "
        f"now {len(provider.mass.player_queues.queue_items)}"
    )


# ---------------------------------------------------------------------------
# Cloud → MA queue mutation propagation (Qobuz app edits the queue → MA reflects).
# These guard the user-reported bug "modifying queue in Qobuz app leaves MA
# with 2 items": every SRVR_CTRL_QUEUE_* delta must trigger the reconciler so
# MA's queue tracks the cloud's view.
# ---------------------------------------------------------------------------


async def _seed_loaded_playlist(engine: QobuzConnectSyncEngine) -> None:
    """Bootstrap a playlist into MA so the cloud→MA delta tests have a baseline."""
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=50, minor=0),
            current_item=QueueTrackRef(queue_item_id=1, track_id="t1"),
            next_item=QueueTrackRef(queue_item_id=2, track_id="t2"),
        )
    )
    await _wait_for_reconcile(engine)
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=50, minor=0),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=1, track_id="t1"),
                QueueTrackRef(queue_item_id=2, track_id="t2"),
                QueueTrackRef(queue_item_id=3, track_id="t3"),
                QueueTrackRef(queue_item_id=4, track_id="t4"),
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)


@pytest.mark.asyncio
async def test_qobuz_app_adds_track_propagates_to_ma_queue() -> None:
    """SRVR_CTRL_QUEUE_TRACKS_ADDED appends the new track to MA without restarting playback."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_loaded_playlist(engine)

    pre_len = len(provider.mass.player_queues.queue_items)
    pre_play_indexes = sum(1 for c in provider.mass.player_queues.calls if c[0] == "play_index")
    pre_stops = sum(1 for c in provider.mass.player_queues.calls if c[0] == "stop")

    # Qobuz app appends t5 to the queue.
    await engine.handle_queue_tracks_added(
        QueueTracksAddedEvent(
            queue_version=QueueVersion(major=50, minor=1),
            action_uuid=b"\x00" * 16,
            tracks=[QueueTrackRef(queue_item_id=5, track_id="t5")],
        )
    )
    await _wait_for_preload(engine)

    post_play_indexes = sum(1 for c in provider.mass.player_queues.calls if c[0] == "play_index")
    post_stops = sum(1 for c in provider.mass.player_queues.calls if c[0] == "stop")
    assert post_play_indexes == pre_play_indexes, "ADDED delta must not call play_index"
    assert post_stops == pre_stops, "ADDED delta must not stop playback"

    ma_ids = [
        getattr(item.media_item, "item_id", None)
        for item in provider.mass.player_queues.queue_items
    ]
    assert "t5" in ma_ids, f"ADDED delta must extend MA's queue with t5; got {ma_ids}"
    assert len(ma_ids) > pre_len, "MA's queue must grow"


@pytest.mark.asyncio
async def test_qobuz_app_removes_track_propagates_to_ma_queue() -> None:
    """SRVR_CTRL_QUEUE_TRACKS_REMOVED drops the named items from MA's queue."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_loaded_playlist(engine)

    initial_ids = [
        getattr(item.media_item, "item_id", None)
        for item in provider.mass.player_queues.queue_items
    ]
    assert "t3" in initial_ids, f"Baseline must have t3 loaded; got {initial_ids}"

    # Qobuz app removes t3.
    await engine.handle_queue_tracks_removed(
        QueueTracksRemovedEvent(
            queue_version=QueueVersion(major=50, minor=1),
            action_uuid=b"\x00" * 16,
            queue_item_ids=[3],
        )
    )
    await _wait_for_preload(engine)

    ma_ids = [
        getattr(item.media_item, "item_id", None)
        for item in provider.mass.player_queues.queue_items
    ]
    assert "t3" not in ma_ids, f"REMOVED delta must drop t3 from MA; got {ma_ids}"
    # The currently-playing track must remain.
    assert "t1" in ma_ids, f"Current item must survive remove; got {ma_ids}"


@pytest.mark.asyncio
async def test_snapshot_loads_history_tracks_before_current_into_ma_queue() -> None:
    """Tracks ahead of and *behind* ``current_idx`` in the Qobuz snapshot land in MA's queue.

    Without history preservation, skipping back inside a 600-track playlist
    triggers a fresh ``QUEUE_LOAD_TRACKS`` round-trip. With it, the
    skip-backward target is already sitting at the front of MA's queue and
    ``play_index`` does the rest in a single call.

    Scenario: SET_STATE puts the user on t3 (the third track); QUEUE_STATE
    delivers a 5-track snapshot. MA's queue must include t1 and t2
    (history) at the front, plus t4 and t5 (tail).
    """
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=51, minor=0),
            current_item=QueueTrackRef(queue_item_id=3, track_id="t3"),
            next_item=QueueTrackRef(queue_item_id=4, track_id="t4"),
        )
    )
    await _wait_for_reconcile(engine)
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=51, minor=0),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=1, track_id="t1"),
                QueueTrackRef(queue_item_id=2, track_id="t2"),
                QueueTrackRef(queue_item_id=3, track_id="t3"),
                QueueTrackRef(queue_item_id=4, track_id="t4"),
                QueueTrackRef(queue_item_id=5, track_id="t5"),
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)

    ma_ids = [
        engine.bridge.qobuz_track_id_for(item) for item in provider.mass.player_queues.queue_items
    ]
    # History (t1, t2) must be present alongside the tail (t4, t5).
    assert "t1" in ma_ids, f"History track t1 must be loaded; got {ma_ids}"
    assert "t2" in ma_ids, f"History track t2 must be loaded; got {ma_ids}"
    assert "t3" in ma_ids, f"Current track t3 must remain; got {ma_ids}"
    assert "t5" in ma_ids, f"Tail track t5 must be loaded; got {ma_ids}"


# ---------------------------------------------------------------------------
# Reorder reconciliation (Qobuz app shuffles tracks → MA queue order matches).
# Each test:
#  - bootstraps a playing playlist via _seed_full_playlist (current = "t3")
#  - delivers a fresh snapshot at a higher qv with a reordered track list
#  - asserts that MA's queue order, current_index, and the currently-playing
#    item all line up with the mirror's view
# ---------------------------------------------------------------------------


async def _seed_full_playlist(
    engine: QobuzConnectSyncEngine,
    track_ids: list[str],
    current_track_id: str,
) -> None:
    """Bootstrap MA with a known multi-track playing playlist for reorder tests."""
    current_qid = 100 + track_ids.index(current_track_id)
    next_idx = track_ids.index(current_track_id) + 1
    next_item = (
        QueueTrackRef(queue_item_id=100 + next_idx, track_id=track_ids[next_idx])
        if next_idx < len(track_ids)
        else None
    )
    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            queue_version=QueueVersion(major=70, minor=0),
            current_item=QueueTrackRef(queue_item_id=current_qid, track_id=current_track_id),
            next_item=next_item,
        )
    )
    await _wait_for_reconcile(engine)
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=70, minor=0),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=100 + idx, track_id=tid)
                for idx, tid in enumerate(track_ids)
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)


def _ma_track_ids(provider: _FakeProvider) -> list[str | None]:
    return [
        getattr(item.media_item, "item_id", None)
        for item in provider.mass.player_queues.queue_items
    ]


async def _deliver_reorder_snapshot(
    engine: QobuzConnectSyncEngine,
    track_ids: list[str],
    *,
    qv_minor: int = 1,
) -> None:
    """Push a new snapshot at higher qv that reflects a Qobuz-app reorder."""
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(major=70, minor=qv_minor),
            action_uuid=b"\x00" * 16,
            tracks=[
                QueueTrackRef(queue_item_id=100 + idx, track_id=tid)
                for idx, tid in enumerate(track_ids)
            ],
            shuffle_mode=False,
            autoplay_mode=False,
        )
    )
    await _wait_for_preload(engine)


@pytest.mark.asyncio
async def test_reorder_within_tail_aligns_ma_queue() -> None:
    """Pure tail reorder: ``[a,b,c,X,d,e,f]`` → ``[a,b,c,X,e,d,f]``."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "b", "c", "X", "d", "e", "f"], "X")

    await _deliver_reorder_snapshot(engine, ["a", "b", "c", "X", "e", "d", "f"])

    assert _ma_track_ids(provider) == ["a", "b", "c", "X", "e", "d", "f"]
    assert queue.current_index == 3  # X still at index 3


@pytest.mark.asyncio
async def test_reorder_within_history_aligns_ma_queue() -> None:
    """Pure history reorder: ``[a,b,c,X,d,e]`` → ``[c,b,a,X,d,e]``."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "b", "c", "X", "d", "e"], "X")

    await _deliver_reorder_snapshot(engine, ["c", "b", "a", "X", "d", "e"])

    assert _ma_track_ids(provider) == ["c", "b", "a", "X", "d", "e"]
    assert queue.current_index == 3  # X still at index 3 — prefix size unchanged


@pytest.mark.asyncio
async def test_reorder_history_to_tail_shifts_current_index() -> None:
    """History → tail: ``[a,b,c,X,d,e]`` → ``[b,c,X,a,d,e]`` shifts current 3 → 2."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "b", "c", "X", "d", "e"], "X")
    assert queue.current_index == 3

    await _deliver_reorder_snapshot(engine, ["b", "c", "X", "a", "d", "e"])

    assert _ma_track_ids(provider) == ["b", "c", "X", "a", "d", "e"]
    assert queue.current_index == 2, (
        f"X moved up one position (a left history); got current_index={queue.current_index}"
    )


@pytest.mark.asyncio
async def test_reorder_tail_to_history_shifts_current_index() -> None:
    """Tail → history: ``[a,X,b,c,d,e]`` → ``[a,b,X,c,d,e]`` shifts current 1 → 2."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "X", "b", "c", "d", "e"], "X")
    assert queue.current_index == 1

    await _deliver_reorder_snapshot(engine, ["a", "b", "X", "c", "d", "e"])

    assert _ma_track_ids(provider) == ["a", "b", "X", "c", "d", "e"]
    assert queue.current_index == 2, (
        f"X shifted down one position (b moved before it); got current_index={queue.current_index}"
    )


@pytest.mark.asyncio
async def test_reorder_does_not_disturb_audio() -> None:
    """Reorder must not call ``stop`` / ``play_index`` / ``clear`` — only ``update_items``."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "b", "X", "c", "d"], "X")
    calls_before_reorder = len(provider.mass.player_queues.calls)

    await _deliver_reorder_snapshot(engine, ["b", "X", "a", "c", "d"])

    new_calls = provider.mass.player_queues.calls[calls_before_reorder:]
    kinds = [c[0] for c in new_calls]
    assert "stop" not in kinds, f"Reorder must not stop playback; got {kinds}"
    assert "play_index" not in kinds, f"Reorder must not call play_index; got {kinds}"
    assert "clear" not in kinds, f"Reorder must not clear the queue; got {kinds}"
    assert "update_items" in kinds, f"Reorder must call update_items; got {kinds}"


@pytest.mark.asyncio
async def test_reorder_does_not_echo_to_cloud() -> None:
    """The ``update_items`` call inside the reconciler runs under Origin.QOBUZ.

    Without that scope, MA's ``QUEUE_ITEMS_UPDATED`` event would be picked
    up by our outbound differ and a wrong-direction ``REMOVE_TRACKS`` /
    ``REORDER_TRACKS`` would fire at the cloud with a stale version.
    """
    session = _FakeSession()
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "b", "X", "c", "d"], "X")
    outbound_before = (
        len(session.queue_adds)
        + len(session.queue_inserts)
        + len(session.queue_removes)
        + len(session.queue_reorders)
        + len(session.clear_queues)
    )

    await _deliver_reorder_snapshot(engine, ["b", "X", "a", "c", "d"])

    outbound_after = (
        len(session.queue_adds)
        + len(session.queue_inserts)
        + len(session.queue_removes)
        + len(session.queue_reorders)
        + len(session.clear_queues)
    )
    assert outbound_after == outbound_before, (
        f"Reorder must not echo to cloud; new outbound calls beyond {outbound_before}"
    )


@pytest.mark.asyncio
async def test_reorder_already_aligned_is_noop() -> None:
    """Receiving a snapshot identical to MA's current order issues no ``update_items``."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_full_playlist(engine, ["a", "b", "X", "c", "d"], "X")
    calls_before = len(provider.mass.player_queues.calls)

    # Same order, new qv. The dedup gate keys on qv so the reconciler does
    # iterate; the reorder pass must short-circuit on the new_ids==old_ids
    # check.
    await _deliver_reorder_snapshot(engine, ["a", "b", "X", "c", "d"], qv_minor=2)

    new_calls = provider.mass.player_queues.calls[calls_before:]
    update_calls = [c for c in new_calls if c[0] == "update_items"]
    assert not update_calls, f"Identical snapshot must not trigger update_items; got {update_calls}"


@pytest.mark.asyncio
async def test_qobuz_app_clears_queue_clears_ma_keeping_current() -> None:
    """SRVR_CTRL_QUEUE_CLEARED empties MA's queue except the currently playing item."""
    queue = _queue(PlaybackState.IDLE, track_id="old")
    provider = _FakeProvider(queue)
    engine = QobuzConnectSyncEngine(provider)
    await _seed_loaded_playlist(engine)

    queue.current_index = 0  # currently playing t1

    await engine.handle_queue_cleared(
        QueueClearedEvent(
            queue_version=QueueVersion(major=50, minor=2),
            action_uuid=b"\x00" * 16,
        )
    )
    await _wait_for_preload(engine)

    ma_ids = [
        getattr(item.media_item, "item_id", None)
        for item in provider.mass.player_queues.queue_items
    ]
    # Current track t1 stays; everything else is gone.
    assert "t1" in ma_ids, f"Currently playing track must remain after CLEARED; got {ma_ids}"
    assert all(tid == "t1" for tid in ma_ids), (
        f"All non-current items must be removed after CLEARED; got {ma_ids}"
    )


# ---------------------------------------------------------------------------
# MA → Qobuz outbound differ (user edits MA's queue → Qobuz app reflects it).
# ---------------------------------------------------------------------------


def _ma_items_event(player_id: str, items: list[Any]) -> Any:
    """Build a ``QUEUE_ITEMS_UPDATED`` event with the given queue items."""
    queue = SimpleNamespace(state=PlaybackState.PLAYING, items=items)
    return SimpleNamespace(object_id=player_id, data=queue)


@pytest.mark.asyncio
async def test_ma_removed_track_sends_queue_remove_tracks_to_cloud() -> None:
    """Removing a track in MA's UI sends ``CTRL_SRVR_QUEUE_REMOVE_TRACKS`` to Qobuz."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    # Mirror has 4 tracks; MA has 3 (t2 removed).
    engine.qobuz_state.queue_version = QueueVersion(major=52, minor=0)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
        QueueTrackRef(queue_item_id=3, track_id="t3"),
        QueueTrackRef(queue_item_id=4, track_id="t4"),
    ]
    provider.mass.player_queues.queue_items = [
        SimpleNamespace(media_item=SimpleNamespace(item_id=tid), queue_item_id=f"ma-{tid}")
        for tid in ("t1", "t3", "t4")
    ]

    await engine.handle_ma_queue_items_updated(
        _ma_items_event("player", provider.mass.player_queues.queue_items)
    )

    assert len(session.queue_removes) == 1, (
        f"Outbound REMOVE_TRACKS expected; sent={session.queue_removes}"
    )
    payload = session.queue_removes[0]
    assert payload["queue_item_ids"] == [2], (
        f"Outbound REMOVE must carry the Qobuz queue_item_id; got {payload}"
    )
    assert (payload["queue_version"].major, payload["queue_version"].minor) == (52, 0), (
        f"Outbound REMOVE must assert the current mirror version (cloud increments); "
        f"got {payload['queue_version']}"
    )
    # Mirror optimistically applies the removal so the next reconcile is a no-op.
    assert all(ref.queue_item_id != 2 for ref in engine.qobuz_state.tracks), (
        f"Mirror must drop t2 optimistically; got {engine.qobuz_state.tracks}"
    )


@pytest.mark.asyncio
async def test_ma_appended_track_sends_queue_add_tracks_to_cloud() -> None:
    """Appending a track at the end of MA's queue sends ``CTRL_SRVR_QUEUE_ADD_TRACKS``."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=52, minor=0)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
    ]
    provider.mass.player_queues.queue_items = [
        SimpleNamespace(media_item=SimpleNamespace(item_id="t1"), queue_item_id="ma-t1"),
        SimpleNamespace(media_item=SimpleNamespace(item_id="t2"), queue_item_id="ma-t2"),
        SimpleNamespace(media_item=SimpleNamespace(item_id="t3"), queue_item_id="ma-t3"),
    ]

    await engine.handle_ma_queue_items_updated(
        _ma_items_event("player", provider.mass.player_queues.queue_items)
    )

    assert len(session.queue_adds) == 1, f"Outbound ADD_TRACKS expected; sent={session.queue_adds}"
    payload = session.queue_adds[0]
    appended_track_ids = [ref.track_id for ref in payload["tracks"]]
    assert appended_track_ids == ["t3"], (
        f"Outbound ADD must include the new tail track ids; got {appended_track_ids}"
    )


@pytest.mark.asyncio
async def test_ma_forward_reorder_sends_reorder_tracks_to_cloud() -> None:
    """User drags a track later in MA's queue → cloud gets one REORDER_TRACKS.

    Mirror has [t1, t2, t3, t4]; user drags t2 to slot 3 → MA has [t1, t3, t4, t2].
    Detector recognizes this as a single-item move (src=1, dst=3) and emits
    REORDER_TRACKS with the moved item's Qobuz queue_item_id and the new
    insert position.
    """
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=58, minor=2)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
        QueueTrackRef(queue_item_id=3, track_id="t3"),
        QueueTrackRef(queue_item_id=4, track_id="t4"),
    ]
    # MA has same set, t2 moved to position 3.
    provider.mass.player_queues.queue_items = [
        SimpleNamespace(media_item=SimpleNamespace(item_id=tid), queue_item_id=f"ma-{tid}")
        for tid in ("t1", "t3", "t4", "t2")
    ]

    await engine.handle_ma_queue_items_updated(
        _ma_items_event("player", provider.mass.player_queues.queue_items)
    )

    assert len(session.queue_reorders) == 1, (
        f"Forward reorder must emit REORDER_TRACKS; got reorders={session.queue_reorders}"
    )
    payload = session.queue_reorders[0]
    assert payload["queue_item_ids"] == [2], (
        f"REORDER must carry the moved Qobuz queue_item_id; got {payload}"
    )
    assert payload["insert_after"] == 3, (
        f"REORDER insert_after must be the new dst index; got {payload}"
    )
    assert (payload["queue_version"].major, payload["queue_version"].minor) == (58, 2)


@pytest.mark.asyncio
async def test_ma_backward_reorder_sends_reorder_tracks_to_cloud() -> None:
    """User drags a track earlier in MA's queue → cloud gets one REORDER_TRACKS."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=58, minor=3)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
        QueueTrackRef(queue_item_id=3, track_id="t3"),
        QueueTrackRef(queue_item_id=4, track_id="t4"),
    ]
    # User drags t4 to position 1 → MA has [t1, t4, t2, t3].
    provider.mass.player_queues.queue_items = [
        SimpleNamespace(media_item=SimpleNamespace(item_id=tid), queue_item_id=f"ma-{tid}")
        for tid in ("t1", "t4", "t2", "t3")
    ]

    await engine.handle_ma_queue_items_updated(
        _ma_items_event("player", provider.mass.player_queues.queue_items)
    )

    assert len(session.queue_reorders) == 1
    payload = session.queue_reorders[0]
    assert payload["queue_item_ids"] == [4]
    assert payload["insert_after"] == 1


@pytest.mark.asyncio
async def test_ma_complex_reorder_skips_outbound_emit() -> None:
    """Two-item reorders are not yet supported — the differ logs and skips."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=59, minor=0)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
        QueueTrackRef(queue_item_id=3, track_id="t3"),
        QueueTrackRef(queue_item_id=4, track_id="t4"),
    ]
    # Reverse two pairs — not a single-item shift.
    provider.mass.player_queues.queue_items = [
        SimpleNamespace(media_item=SimpleNamespace(item_id=tid), queue_item_id=f"ma-{tid}")
        for tid in ("t2", "t1", "t4", "t3")
    ]

    await engine.handle_ma_queue_items_updated(
        _ma_items_event("player", provider.mass.player_queues.queue_items)
    )

    assert session.queue_reorders == [], (
        "Complex reorder must not emit a partial REORDER message; "
        "let the snapshot resync via the next qv bump"
    )


@pytest.mark.asyncio
async def test_ma_cleared_queue_sends_clear_to_cloud() -> None:
    """Clearing MA's queue (with mirror non-empty) sends ``CTRL_SRVR_CLEAR_QUEUE``."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=52, minor=0)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
    ]
    provider.mass.player_queues.queue_items = []

    await engine.handle_ma_queue_items_updated(_ma_items_event("player", []))

    assert len(session.clear_queues) == 1, (
        f"Outbound CLEAR_QUEUE expected; sent={session.clear_queues}"
    )
    assert engine.qobuz_state.tracks == [], "Mirror must clear optimistically"


@pytest.mark.asyncio
async def test_ma_event_during_qobuz_origin_does_not_emit_to_cloud() -> None:
    """While applying an inbound Qobuz delta, MA-event echoes must not loop back."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=52, minor=0)
    engine.qobuz_state.tracks = [QueueTrackRef(queue_item_id=1, track_id="t1")]
    provider.mass.player_queues.queue_items = []

    from music_assistant.providers.qobuz_connect.state import (  # noqa: PLC0415
        origin_scope,
    )

    async with origin_scope(engine, Origin.QOBUZ):
        await engine.handle_ma_queue_items_updated(_ma_items_event("player", []))

    assert session.clear_queues == [], (
        "MA events during a QOBUZ-origin scope must not echo back to the cloud"
    )


@pytest.mark.asyncio
async def test_ma_items_event_during_active_reconcile_does_not_emit() -> None:
    """While the MA-reconcile task is in flight, the outbound differ stays silent.

    MA's queue diverges from the mirror mid-reconcile (we're surgically
    adding/removing items to catch up). A ``QUEUE_ITEMS_UPDATED`` event in
    that window must not be interpreted as a user edit — the version-
    mismatch error the user saw in production traces back to this
    feedback loop firing a stale-version ``REMOVE_TRACKS`` to the cloud.
    """
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=60, minor=0)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
    ]
    provider.mass.player_queues.queue_items = []

    handler = cast("Any", engine).command_handler
    never_completes: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def _stay_in_flight() -> None:
        await never_completes

    handler._preload_task = asyncio.create_task(_stay_in_flight())
    try:
        assert handler.is_reconciling() is True, (
            "is_reconciling() must report True while a preload (MA-reconcile) task is in flight"
        )
        await engine.handle_ma_queue_items_updated(_ma_items_event("player", []))
        assert session.queue_removes == [], (
            "Outbound differ must skip MA events while the reconciler is in flight"
        )
        assert session.clear_queues == [], (
            "Outbound differ must skip MA events while the reconciler is in flight"
        )
    finally:
        never_completes.set_result(None)
        with contextlib.suppress(asyncio.CancelledError):
            await handler._preload_task


@pytest.mark.asyncio
async def test_outbound_action_uuid_echo_skips_reconciler() -> None:
    """Cloud echoing back our own action_uuid must not re-trigger MA reconcile."""
    session = _FakeSession()
    queue = _queue(PlaybackState.PLAYING)
    provider = _FakeProvider(queue, session=session)
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.queue_version = QueueVersion(major=52, minor=0)
    engine.qobuz_state.tracks = [
        QueueTrackRef(queue_item_id=1, track_id="t1"),
        QueueTrackRef(queue_item_id=2, track_id="t2"),
    ]

    action_uuid = engine.register_outbound_action(
        OutboundActionKind.REMOVE, QueueVersion(major=52, minor=1)
    )

    # Echo arrives — must consume ledger entry and not call schedule_reconcile.
    consumed = engine.consume_outbound_action(action_uuid)
    assert consumed is not None
    assert consumed.kind == OutboundActionKind.REMOVE
    assert engine.consume_outbound_action(action_uuid) is None, (
        "Ledger entry must be removed after first consume"
    )
