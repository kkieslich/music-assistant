"""
Tests for the controller-role playback takeover.

Controller-joined connections never receive a renderer-directed
``SET_STATE`` with track refs (handoff capture + live 2026-07-08). On
``SRVR_RNDR_SET_ACTIVE`` the provider must continue the session's
playback by itself from the controller-side mirror: queue snapshot +
``SESSION_STATE.trackIndex`` + ``SRVR_CTRL_RENDERER_STATE_UPDATED``
broadcasts.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from music_assistant_models.enums import PlaybackState

from music_assistant.providers.qobuz_connect.models import (
    OutboundActionKind,
    PlayingState,
    QueueClearedEvent,
    QueueStateSnapshot,
    QueueTrackRef,
    QueueVersion,
    RendererStateUpdate,
    SessionStateEvent,
    SetStateEvent,
)
from music_assistant.providers.qobuz_connect.sync import QobuzConnectSyncEngine

from .test_sync import _FakeProvider, _FakeSession, _queue


def _engine(*, controller: Any | None = None) -> QobuzConnectSyncEngine:
    provider = _FakeProvider(_queue(PlaybackState.IDLE), session=_FakeSession())
    if controller is not None:
        provider.controller = controller  # type: ignore[attr-defined]
    return QobuzConnectSyncEngine(cast("Any", provider))


def _tracks(count: int) -> list[QueueTrackRef]:
    return [QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in range(count)]


class _SetStateSpy:
    """Records synthesized SET_STATE events instead of running the real pipeline."""

    def __init__(self, engine: QobuzConnectSyncEngine) -> None:
        self.events: list[SetStateEvent] = []
        self._real = engine.command_handler

    async def handle_set_state(self, event: SetStateEvent) -> None:
        self.events.append(event)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _install_spy(engine: QobuzConnectSyncEngine) -> _SetStateSpy:
    spy = _SetStateSpy(engine)
    engine.command_handler = cast("Any", spy)
    return spy


async def test_session_state_stores_track_index() -> None:
    """SESSION_STATE's trackIndex (a next-track read pointer) maps to current index."""
    engine = _engine()
    await engine.handle_session_state(
        SessionStateEvent(
            session_uuid=b"\x01" * 16,
            session_id=1,
            queue_version=QueueVersion(30, 1),
            track_index=3,
        )
    )
    # trackIndex=3 means "next track is index 3" -> current is index 2
    assert engine.qobuz_state.track_index == 2


async def test_session_state_track_index_zero_clamps() -> None:
    """A fresh queue (pointer 0, nothing played) resolves to index 0."""
    engine = _engine()
    await engine.handle_session_state(
        SessionStateEvent(
            session_uuid=b"\x01" * 16,
            session_id=1,
            queue_version=QueueVersion(30, 1),
            track_index=0,
        )
    )
    assert engine.qobuz_state.track_index == 0


async def test_renderer_state_updated_feeds_mirror_while_inactive() -> None:
    """Another renderer's broadcast updates playing state, position and index."""
    engine = _engine()
    engine.qobuz_state.tracks = _tracks(5)
    await engine.handle_renderer_state_updated(
        RendererStateUpdate(
            renderer_id=1,
            playing_state=PlayingState.PLAYING,
            position_ms=42_000,
            duration_ms=180_000,
            current_queue_index=3,
        )
    )
    assert engine.qobuz_state.playing_state is PlayingState.PLAYING
    assert engine.qobuz_state.position_ms == 42_000
    assert engine.qobuz_state.track_index == 3
    # current_item must stay unset while we're not the target — it gates
    # the heartbeat reporter, and inactive renderers must stay silent.
    assert engine.qobuz_state.current_item is None


async def test_renderer_state_updated_ignores_own_echo() -> None:
    """Broadcasts carrying our own rendererId are echoes of our reports."""
    controller = SimpleNamespace(own_renderer_id=3, active_renderer_id=None)
    engine = _engine(controller=controller)
    await engine.handle_renderer_state_updated(
        RendererStateUpdate(renderer_id=3, playing_state=PlayingState.PLAYING)
    )
    assert engine.qobuz_state.playing_state is not PlayingState.PLAYING


async def test_renderer_state_updated_ignored_while_we_are_active() -> None:
    """Stale broadcasts from other renderers don't clobber our state while active."""
    controller = SimpleNamespace(own_renderer_id=3, active_renderer_id=3)
    engine = _engine(controller=controller)
    await engine.handle_renderer_state_updated(
        RendererStateUpdate(renderer_id=1, playing_state=PlayingState.PAUSED, position_ms=5)
    )
    assert engine.qobuz_state.position_ms == 0


async def test_takeover_starts_playback_from_mirror() -> None:
    """Remote PLAYING + known track index synthesizes a rich SET_STATE."""
    engine = _engine()
    spy = _install_spy(engine)
    engine.qobuz_state.tracks = _tracks(5)
    engine.qobuz_state.track_index = 2
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.qobuz_state.position_ms = 30_000

    await engine.takeover_playback()

    assert len(spy.events) == 1
    event = spy.events[0]
    assert event.playing_state is PlayingState.PLAYING
    assert event.position_ms == 30_000
    assert event.current_item is engine.qobuz_state.tracks[2]
    assert event.next_item is engine.qobuz_state.tracks[3]


async def test_takeover_reports_only_when_session_not_playing() -> None:
    """A paused/idle session is announced but not auto-started."""
    engine = _engine()
    spy = _install_spy(engine)
    engine.qobuz_state.tracks = _tracks(2)
    engine.qobuz_state.track_index = 1
    engine.qobuz_state.playing_state = PlayingState.PAUSED

    await engine.takeover_playback()

    assert spy.events == []
    # current track resolved anyway, so a following slim play command works
    assert engine.qobuz_state.current_item is engine.qobuz_state.tracks[1]


async def test_takeover_suppressed_after_ma_origin_activation() -> None:
    """The activation echo of our own ``activate_self`` must not take over."""
    engine = _engine()
    spy = _install_spy(engine)
    engine.qobuz_state.tracks = _tracks(2)
    engine.qobuz_state.playing_state = PlayingState.PLAYING
    engine.suppress_takeover_once()

    await engine.takeover_playback()
    assert spy.events == []

    # one-shot: the next activation takes over normally
    await engine.takeover_playback()
    assert len(spy.events) == 1


async def test_deactivation_preserves_queue_mirror_but_silences_reporter() -> None:
    """Tracks/index survive deactivation; current_item (the report gate) does not."""
    engine = _engine()
    engine.qobuz_state.tracks = _tracks(3)
    engine.qobuz_state.track_index = 1
    engine.qobuz_state.queue_version = QueueVersion(30, 1)
    engine.qobuz_state.current_item = engine.qobuz_state.tracks[1]

    await engine.release_target_player()

    # mypy keeps current_item narrowed from the assignment above; runtime
    # replaced the whole mirror.
    assert engine.qobuz_state.current_item is None
    assert len(engine.qobuz_state.tracks) == 3  # type: ignore[unreachable]
    assert engine.qobuz_state.track_index == 1
    assert engine.qobuz_state.queue_version == QueueVersion(30, 1)


async def test_slim_play_command_falls_back_to_mirror_track_index() -> None:
    """A track-less PLAYING command resolves the current track from the mirror."""
    engine = _engine()
    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(30, 1),
            action_uuid=b"\x02" * 16,
            tracks=_tracks(4),
        )
    )
    engine.qobuz_state.track_index = 2

    await engine.handle_qobuz_set_state(SetStateEvent(playing_state=PlayingState.PLAYING))
    reconcile_task = engine.command_handler._reconcile_task
    if reconcile_task is not None:
        await reconcile_task
    assert engine.qobuz_state.current_item is not None
    assert engine.qobuz_state.current_item.track_id == "102"
    engine.command_handler.cancel_tasks()


async def test_clear_echo_drops_current_anchor() -> None:
    """Our own QUEUE_CLEARED echo must silence the stale current-track report."""
    engine = _engine()
    engine.qobuz_state.tracks = _tracks(3)
    engine.qobuz_state.current_item = engine.qobuz_state.tracks[1]
    engine.qobuz_state.next_item = engine.qobuz_state.tracks[2]
    action_uuid = engine.register_outbound_action(OutboundActionKind.CLEAR, QueueVersion(30, 1))

    await engine.handle_queue_cleared(
        QueueClearedEvent(queue_version=QueueVersion(31, 1), action_uuid=action_uuid)
    )

    assert engine.qobuz_state.current_item is None
    # mypy keeps current_item narrowed from the assignment above; runtime
    # cleared it via the echo path.
    assert engine.qobuz_state.next_item is None  # type: ignore[unreachable]
    assert engine.qobuz_state.tracks == []


async def test_snapshot_prunes_stale_current_anchor() -> None:
    """A snapshot without the mirror's current item must drop the anchor."""
    engine = _engine()
    engine.qobuz_state.current_item = QueueTrackRef(queue_item_id=99, track_id="dead")

    await engine.handle_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(31, 1),
            action_uuid=b"\x02" * 16,
            tracks=_tracks(2),
        )
    )

    assert engine.qobuz_state.current_item is None
    # mypy keeps current_item narrowed from the assignment above.
    engine.command_handler.cancel_tasks()  # type: ignore[unreachable]


async def test_connection_lost_drops_active_flag() -> None:
    """A fresh connection is never active — reports must stop until reactivation."""
    engine = _engine()
    engine.set_active(active=True)
    engine.suppress_takeover_once()

    engine.handle_connection_lost()

    assert engine._is_active is False
    assert engine._suppress_takeover_once is False
