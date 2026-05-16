"""Tests for Qobuz Connect sync decisions."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest
from music_assistant_models.enums import PlaybackState

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueError,
    QueueLoadAck,
    QueueTrackRef,
    QueueVersion,
    SetStateEvent,
)
from music_assistant.providers.qobuz_connect.sync import QobuzConnectSyncEngine


class _FakePlayerQueues:
    """Small recorder for MA queue commands."""

    def __init__(self, queue: Any) -> None:
        self.queue = queue
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

    async def seek(self, player_id: str, position: int) -> None:
        self.calls.append(("seek", (player_id, position), {}))

    async def stop(self, player_id: str) -> None:
        self.calls.append(("stop", (player_id,), {}))
        self.queue.state = PlaybackState.IDLE

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

    def get_target_player_id(self) -> str:
        return "player"

    def get_qobuz_track_id_from_queue_item(self, queue_item: Any) -> str | None:
        return getattr(queue_item, "track_id", None)

    def get_qobuz_provider(self) -> Any:
        return self.qobuz_provider


async def _fake_get_track(track_id: str) -> Any:
    return SimpleNamespace(
        item_id=track_id,
        duration=180,
        album=SimpleNamespace(item_id="123456"),
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
    engine.pending_paused_seek_ms = 50_000

    await engine.handle_qobuz_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
            current_item=QueueTrackRef(queue_item_id=11, track_id="376286112"),
        )
    )

    assert cast("Any", engine).pending_paused_seek_ms is None
    assert provider.mass.player_queues.calls == [
        ("play_index", ("player", 0), {"seek_position": 50})
    ]


@pytest.mark.asyncio
async def test_play_after_stop_based_pause_restarts_current_item() -> None:
    """Players that implement pause as stop should still resume from Qobuz play."""
    provider = _FakeProvider(_queue(PlaybackState.IDLE))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=11, track_id="376286112")
    engine.qobuz_state.playing_state = PlayingState.PAUSED
    engine.qobuz_state.position_ms = 72_000

    await engine.handle_qobuz_set_state(SetStateEvent(playing_state=PlayingState.PLAYING))

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

    assert engine.qobuz_state.current_item == QueueTrackRef(
        queue_item_id=11,
        track_id="376286112",
    )
    assert provider.mass.player_queues.calls[0][0] == "play_media"


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

    assert provider.mass.player_queues.calls == []


@pytest.mark.asyncio
async def test_playing_position_divergence_seeks_ma_once() -> None:
    """A real Qobuz playing seek is forwarded to MA."""
    provider = _FakeProvider(_queue(PlaybackState.PLAYING))
    engine = QobuzConnectSyncEngine(provider)
    engine.qobuz_state.playing_state = PlayingState.PLAYING

    await engine.handle_qobuz_set_state(SetStateEvent(position_ms=50_000))

    assert provider.mass.player_queues.calls == [("seek", ("player", 50), {})]
