"""Tests for wiring ``QobuzConnectProvider`` to the reducer-based sync core."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.media_items import Track

from music_assistant.providers.qobuz_connect import (
    CONF_ENABLE_CONTROLLER,
    CONF_HTTP_PORT,
    CONF_INITIAL_VOLUME,
    CONF_MAX_QUALITY,
    CONF_PUBLISH_NAME,
    CONF_TARGET_PLAYER,
    PLAYER_ID_AUTO,
    QobuzConnectProvider,
    _ReporterHost,
)
from music_assistant.providers.qobuz_connect import outbound_reporter as outbound_reporter_module
from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.effect_runner import EffectRunner
from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QueueStateSnapshot,
    QueueTrackRef,
    QueueVersion,
    SetStateEvent,
)
from music_assistant.providers.qobuz_connect.outbound_reporter import OutboundReporter
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    PushVolume,
    ReportState,
)

if TYPE_CHECKING:
    import pytest
    from music_assistant_models.event import MassEvent


def _fake_event(object_id: str) -> MassEvent:
    """Build a duck-typed ``MassEvent`` — only ``object_id``/``data`` are read."""
    return cast("MassEvent", SimpleNamespace(object_id=object_id, data=None))


def _fake_player(player_id: str = "player_1") -> SimpleNamespace:
    return SimpleNamespace(
        player_id=player_id,
        display_name="Player One",
        state=SimpleNamespace(playback_state=MAPlaybackState.IDLE),
        group_volume=50,
        group_volume_muted=False,
        volume_level=50,
        volume_muted=False,
    )


def _fake_qobuz_provider() -> SimpleNamespace:
    async def _get_track(track_id: str) -> Track:
        return Track(
            item_id=track_id,
            provider="qobuz",
            name=f"Track {track_id}",
            provider_mappings=set(),
            duration=200,
        )

    return SimpleNamespace(domain="qobuz", instance_id="qobuz", get_track=_get_track)


def _make_provider(
    target_player: SimpleNamespace | None = None,
) -> tuple[QobuzConnectProvider, MagicMock]:
    """Build a real ``QobuzConnectProvider`` against a mocked ``mass``; returns both."""
    player = target_player or _fake_player()

    mass = MagicMock()
    mass.streams.bind_ip = "127.0.0.1"
    mass.players.all_players.return_value = [player]
    mass.players.get_player.return_value = player
    mass.player_queues.items.return_value = []
    mass.player_queues.get.return_value = None
    mass.player_queues.play = AsyncMock()
    mass.player_queues.pause = AsyncMock()
    mass.player_queues.stop = AsyncMock()
    mass.player_queues.play_media = AsyncMock()
    mass.player_queues.play_index = AsyncMock()
    mass.player_queues.update_items = MagicMock()
    qobuz_provider = _fake_qobuz_provider()
    mass.get_provider.side_effect = lambda domain: qobuz_provider if domain == "qobuz" else None

    manifest = MagicMock()
    manifest.domain = "qobuz_connect"

    values: dict[str, Any] = {
        CONF_TARGET_PLAYER: PLAYER_ID_AUTO,
        CONF_PUBLISH_NAME: "Test Qobuz Connect",
        CONF_HTTP_PORT: 8695,
        CONF_MAX_QUALITY: "27",
        CONF_INITIAL_VOLUME: 25,
        CONF_ENABLE_CONTROLLER: True,
    }
    config = MagicMock()
    config.instance_id = "qobuz_connect--test"
    config.name = "Test Qobuz Connect"
    config.get_value.side_effect = lambda key, *_a, **_k: values.get(key, "GLOBAL")

    return QobuzConnectProvider(mass, manifest, config), mass


def test_setup_constructs_coordinator_and_effect_runner() -> None:
    """The provider builds a coordinator + effect runner instead of the retired sync engine."""
    provider, _mass = _make_provider()

    assert isinstance(provider._coordinator, QobuzConnectCoordinator)
    assert isinstance(provider._effect_runner, EffectRunner)
    assert not hasattr(provider, "_sync")
    assert not hasattr(provider, "controller")


def test_build_session_callbacks_delegates_to_coordinator() -> None:
    """Most callbacks are the coordinator's own translators; on_set_active/on_quality are overridden."""
    provider, _mass = _make_provider()
    callbacks = provider._build_session_callbacks()

    assert callbacks.on_set_state == provider._coordinator._on_set_state
    assert callbacks.on_queue_state == provider._coordinator._on_queue_state
    assert callbacks.on_volume == provider._coordinator._on_volume
    assert callbacks.on_volume_delta == provider._coordinator._on_volume_delta
    assert callbacks.on_add_renderer == provider._coordinator._on_add_renderer
    assert callbacks.on_disconnected == provider._coordinator._on_disconnected

    # Provider-level overrides that do strictly more than the reducer.
    assert callbacks.on_set_active == provider._on_set_active
    assert callbacks.on_quality == provider._on_quality_change


async def test_snapshot_then_activate_reaches_ma_play() -> None:
    """An inbound snapshot + SET_STATE + SET_ACTIVE through the session callbacks plays on MA."""
    provider, mass = _make_provider()
    callbacks = provider._build_session_callbacks()

    await callbacks.on_queue_state(
        QueueStateSnapshot(
            queue_version=QueueVersion(1, 0),
            action_uuid=b"\x00" * 16,
            tracks=[QueueTrackRef(queue_item_id=1, track_id="501")],
        )
    )
    assert provider._coordinator.state.current_id == 501

    await callbacks.on_set_state(
        SetStateEvent(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
            current_item=QueueTrackRef(queue_item_id=1, track_id="501"),
        )
    )
    assert provider._coordinator.state.playing is PlayingState.PLAYING

    await callbacks.on_set_active(True)

    assert provider._coordinator.state.active is True
    mass.player_queues.play_media.assert_awaited_once()
    _, kwargs = mass.player_queues.play_media.await_args
    assert kwargs["queue_id"] == "player_1"
    assert kwargs["media"].item_id == "501"


async def test_on_set_active_broadcasts_volume_and_quality_before_delegating() -> None:
    """Activation still broadcasts MA volume + a quality report, then submits to the coordinator."""
    provider, _mass = _make_provider()
    provider._session = MagicMock()
    provider._session.send_quality_reports = AsyncMock()
    provider._session.send_volume_changed = AsyncMock()
    provider._session.send_volume_muted = AsyncMock()

    await provider._on_set_active(True)

    provider._session.send_volume_changed.assert_awaited_once()
    provider._session.send_quality_reports.assert_awaited_once_with(provider._max_quality)
    assert provider._coordinator.state.active is True


async def test_on_set_active_false_releases_without_broadcast() -> None:
    """Deactivation does not broadcast volume/quality, only deactivates the coordinator."""
    provider, _mass = _make_provider()
    provider._session = MagicMock()
    provider._session.send_quality_reports = AsyncMock()
    provider._session.send_volume_changed = AsyncMock()
    provider._session.send_volume_muted = AsyncMock()

    await provider._on_set_active(True)
    provider._session.send_volume_changed.reset_mock()
    provider._session.send_quality_reports.reset_mock()

    await provider._on_set_active(False)

    provider._session.send_volume_changed.assert_not_awaited()
    provider._session.send_quality_reports.assert_not_awaited()
    assert provider._coordinator.state.active is False


async def test_ma_queue_event_delegates_to_transport_and_modes() -> None:
    """QUEUE_UPDATED delegates to both the transport and modes coordinator entry points."""
    provider, _mass = _make_provider()
    provider._coordinator = AsyncMock()
    event = _fake_event("player_1")

    await provider._on_ma_queue_event(event)

    provider._coordinator.on_ma_transport_event.assert_awaited_once_with("player_1")
    provider._coordinator.on_ma_modes_event.assert_awaited_once_with("player_1")


async def test_ma_queue_items_event_delegates_to_queue_event() -> None:
    """QUEUE_ITEMS_UPDATED delegates to the coordinator's queue entry point."""
    provider, _mass = _make_provider()
    provider._coordinator = AsyncMock()
    event = _fake_event("player_1")

    await provider._on_ma_queue_items_event(event)

    provider._coordinator.on_ma_queue_event.assert_awaited_once_with("player_1")


async def test_player_updated_event_delegates_to_volume_event() -> None:
    """PLAYER_UPDATED delegates to the coordinator's volume entry point."""
    provider, _mass = _make_provider()
    provider._coordinator = AsyncMock()
    event = _fake_event("player_1")

    await provider._on_ma_player_updated(event)

    provider._coordinator.on_ma_volume_event.assert_awaited_once_with("player_1")


async def test_cloud_effect_noops_when_no_session() -> None:
    """A cloud Push*/ReportState effect through the real EffectRunner no-ops with no session."""
    provider, _mass = _make_provider()
    assert provider._session is None

    # _LiveSessionProxy must swallow the send with no live session rather than
    # raising AttributeError; ReportState short-circuits in report_state() too.
    await provider._effect_runner.run(PushVolume(volume=30))
    await provider._effect_runner.run(ReportState())


def test_reporter_host_projects_canonical_state_with_duration() -> None:
    """_ReporterHost projects CanonicalState + a live MA duration into the QobuzMirror."""
    current_queue_item = SimpleNamespace(duration=200)  # seconds
    fake_queue = SimpleNamespace(current_item=current_queue_item)
    fake_bridge = SimpleNamespace(
        target_player_id=lambda: "player_1",
        get_queue=lambda _pid: fake_queue,
    )
    state = CanonicalState(
        cloud_version=QueueVersion(3, 2),
        tracks=(QueueTrackRef(queue_item_id=7, track_id="501"),),
        current_id=501,
        playing=PlayingState.PLAYING,
        position_ms=1234,
        position_anchor_ms=9999,
    )
    host = _ReporterHost(cast("Any", fake_bridge), lambda: state)

    mirror = host.qobuz_state

    assert mirror.current_item is not None
    assert mirror.current_item.track_id == "501"
    assert mirror.playing_state is PlayingState.PLAYING
    assert mirror.position_ms == 1234
    assert mirror.queue_version == QueueVersion(3, 2)
    # duration is read live from MA's queue (seconds -> ms) rather than the
    # hardcoded 0 CanonicalState carries.
    assert mirror.duration_ms == 200_000


def test_reporter_host_duration_falls_back_to_zero_without_queue() -> None:
    """_ReporterHost falls back to duration_ms=0 when MA has no current queue item."""
    fake_bridge = SimpleNamespace(
        target_player_id=lambda: "player_1",
        get_queue=lambda _pid: None,
    )
    state = CanonicalState(current_id=None)
    host = _ReporterHost(cast("Any", fake_bridge), lambda: state)

    assert host.qobuz_state.duration_ms == 0


def test_reporter_host_is_active_reflects_canonical_state() -> None:
    """_ReporterHost._is_active mirrors the projected CanonicalState.active flag."""
    fake_bridge = SimpleNamespace(
        target_player_id=lambda: "player_1",
        get_queue=lambda _pid: None,
    )

    active_host = _ReporterHost(cast("Any", fake_bridge), lambda: CanonicalState(active=True))
    assert active_host._is_active is True

    inactive_host = _ReporterHost(cast("Any", fake_bridge), lambda: CanonicalState(active=False))
    assert inactive_host._is_active is False


async def test_heartbeat_loop_survives_tick_against_real_reporter_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    The 5-second heartbeat loop survives a tick when reading ``_is_active`` off the real host.

    Regression guard: the loop reads ``self._engine._is_active`` outside its
    exception suppression, so if ``_ReporterHost`` didn't expose ``_is_active``
    the very first tick would raise ``AttributeError`` and silently kill the
    heartbeat task (report_state() itself never touches it, so this path is
    invisible to a plain report_state() call).
    """
    monkeypatch.setattr(outbound_reporter_module, "STATE_REPORT_INTERVAL_S", 0.01)
    # session is None so report_state() early-returns cleanly after the tick.
    fake_bridge = SimpleNamespace(
        session=None,
        target_player_id=lambda: "player_1",
        get_queue=lambda _pid: None,
    )
    host = _ReporterHost(cast("Any", fake_bridge), lambda: CanonicalState(active=True))
    reporter = OutboundReporter(cast("Any", host))

    await reporter.start()
    try:
        await asyncio.sleep(0.05)
        # Before the fix the task would be ``.done()`` with an AttributeError.
        assert reporter._heartbeat_task is not None
        assert not reporter._heartbeat_task.done()
    finally:
        await reporter.stop()


async def test_ma_events_ignore_non_target_player() -> None:
    """A MA event for a different player id is filtered out before reaching the coordinator."""
    provider, _mass = _make_provider()
    provider._coordinator = AsyncMock()
    event = _fake_event("some_other_player")

    await provider._on_ma_queue_event(event)
    await provider._on_ma_queue_items_event(event)
    await provider._on_ma_player_updated(event)

    provider._coordinator.on_ma_transport_event.assert_not_awaited()
    provider._coordinator.on_ma_modes_event.assert_not_awaited()
    provider._coordinator.on_ma_queue_event.assert_not_awaited()
    provider._coordinator.on_ma_volume_event.assert_not_awaited()
