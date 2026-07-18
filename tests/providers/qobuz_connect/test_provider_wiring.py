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
)
from music_assistant.providers.qobuz_connect import outbound_reporter as outbound_reporter_module
from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.effect_runner import EffectRunner
from music_assistant.providers.qobuz_connect.models import (
    BufferState,
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


async def test_reporter_emits_canonical_state_with_live_duration() -> None:
    """report_state derives the current item from CanonicalState and ships the live MA duration."""
    sent: dict[str, Any] = {}

    class _Session:
        async def send_renderer_state(self, **kwargs: Any) -> bool:
            sent.update(kwargs)
            return True

    state = CanonicalState(
        cloud_version=QueueVersion(3, 2),
        tracks=(QueueTrackRef(queue_item_id=7, track_id="501"),),
        current_id=501,
        playing=PlayingState.PLAYING,
        position_ms=1234,
        position_anchor_ms=9999,
    )
    reporter = OutboundReporter(
        session_getter=lambda: cast("Any", _Session()),
        state_getter=lambda: state,
        duration_getter=lambda: 200_000,
        active_getter=lambda: True,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.report_state()

    assert sent["queue_item_id"] == 7
    assert sent["playing_state"] is PlayingState.PLAYING
    assert sent["position_ms"] == 1234
    # PLAYING ships the raw anchor pair so the client interpolates exactly once.
    assert sent["position_timestamp_ms"] == 9999
    assert sent["queue_version"] == QueueVersion(3, 2)
    assert sent["duration_ms"] == 200_000


async def test_reporter_ships_buffering_with_frozen_anchor() -> None:
    """A BUFFERING canonical state reaches the wire as BUFFERING with a frozen (now) anchor."""
    sent: dict[str, Any] = {}

    class _Session:
        async def send_renderer_state(self, **kwargs: Any) -> bool:
            sent.update(kwargs)
            return True

    state = CanonicalState(
        cloud_version=QueueVersion(3, 2),
        tracks=(QueueTrackRef(queue_item_id=7, track_id="501"),),
        current_id=501,
        playing=PlayingState.PLAYING,
        position_ms=1234,
        position_anchor_ms=9999,
        buffer_state=BufferState.BUFFERING,
    )
    reporter = OutboundReporter(
        session_getter=lambda: cast("Any", _Session()),
        state_getter=lambda: state,
        duration_getter=lambda: 200_000,
        active_getter=lambda: True,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.report_state()

    assert sent["buffer_state"] is BufferState.BUFFERING
    assert sent["position_ms"] == 1234
    # BUFFERING ships a frozen anchor (timestamp = now), not the raw anchor_ms.
    assert sent["position_timestamp_ms"] != 9999


async def test_reporter_skips_when_no_current_item() -> None:
    """report_state early-returns (no frame) when canonical has no current track."""
    calls = 0

    class _Session:
        async def send_renderer_state(self, **_kwargs: Any) -> bool:
            nonlocal calls
            calls += 1
            return True

    reporter = OutboundReporter(
        session_getter=lambda: cast("Any", _Session()),
        state_getter=lambda: CanonicalState(current_id=None),
        duration_getter=lambda: 0,
        active_getter=lambda: True,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.report_state()
    assert calls == 0


def test_current_track_duration_ms_reads_live_ma_queue() -> None:
    """The provider's duration getter reads the live MA queue item (seconds -> ms)."""
    provider, mass = _make_provider()
    mass.player_queues.get.return_value = SimpleNamespace(
        current_item=SimpleNamespace(duration=200)
    )
    assert provider._current_track_duration_ms() == 200_000

    mass.player_queues.get.return_value = None
    assert provider._current_track_duration_ms() == 0


def test_reporter_active_getter_requires_active_and_target_player() -> None:
    """The heartbeat active-getter is (state.active AND a target player exists)."""
    provider, mass = _make_provider()

    provider._coordinator._state = CanonicalState(active=False)
    assert provider._reporter._active_getter() is False

    provider._coordinator._state = CanonicalState(active=True)
    assert provider._reporter._active_getter() is True

    # Target player vanished: even while active, the heartbeat must fall silent
    # (a frozen canonical state otherwise reports stale PLAYING forever).
    mass.players.all_players.return_value = []
    mass.players.get_player.return_value = None
    provider._pinned_target_id = None
    assert provider._reporter._active_getter() is False


async def test_heartbeat_loop_survives_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """The 5-second heartbeat loop survives a tick reading its active-getter + state."""
    monkeypatch.setattr(outbound_reporter_module, "STATE_REPORT_INTERVAL_S", 0.01)
    # session_getter returns None so report_state() early-returns cleanly.
    reporter = OutboundReporter(
        session_getter=lambda: None,
        state_getter=lambda: CanonicalState(active=True),
        duration_getter=lambda: 0,
        active_getter=lambda: True,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.start()
    try:
        await asyncio.sleep(0.05)
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


def test_auto_target_stays_pinned_while_session_active() -> None:
    """
    While the Connect session is active, the auto target must not retarget.

    ``__auto__`` used to re-resolve "any PLAYING player, else players[0]" on
    every event and effect — so pausing from the app (target no longer
    PLAYING) or another player starting playback silently redirected
    commands and state sync to a different player mid-session.
    """
    playing = _fake_player("p1")
    playing.state.playback_state = MAPlaybackState.PLAYING
    other = _fake_player("p2")
    provider, mass = _make_provider(target_player=playing)
    players = {"p1": playing, "p2": other}
    mass.players.all_players.return_value = [playing, other]
    mass.players.get_player.side_effect = lambda pid: players.get(pid)

    assert provider.get_target_player_id() == "p1"
    provider._coordinator._state = CanonicalState(active=True)

    # The app pauses (target no longer PLAYING) and another player starts.
    playing.state.playback_state = MAPlaybackState.PAUSED
    other.state.playback_state = MAPlaybackState.PLAYING
    assert provider.get_target_player_id() == "p1"


def test_auto_target_repins_when_pinned_player_disappears() -> None:
    """A vanished pinned player falls back to a fresh resolution."""
    playing = _fake_player("p1")
    playing.state.playback_state = MAPlaybackState.PLAYING
    other = _fake_player("p2")
    provider, mass = _make_provider(target_player=playing)
    players = {"p1": playing, "p2": other}
    mass.players.all_players.return_value = [playing, other]
    mass.players.get_player.side_effect = lambda pid: players.get(pid)

    assert provider.get_target_player_id() == "p1"
    provider._coordinator._state = CanonicalState(active=True)

    del players["p1"]
    mass.players.all_players.return_value = [other]
    assert provider.get_target_player_id() == "p2"


def test_missing_configured_player_warns_once() -> None:
    """
    A vanished pinned player must not warn on every event of every player.

    The subscriptions are unfiltered, so this warning used to fire several
    times per second for days — a log flood that buried real errors.
    """
    provider, mass = _make_provider()
    provider._target_player_id = "gone"
    mass.players.get_player.side_effect = lambda _pid: None
    provider.logger = MagicMock()

    assert provider.get_target_player_id() is None
    assert provider.get_target_player_id() is None
    assert provider.logger.warning.call_count == 1


async def test_quality_change_survives_missing_qobuz_provider() -> None:
    """
    A quality tap in the app must not tear down the websocket.

    ``_on_quality_change`` is a session dispatcher callback; its unguarded
    ``get_qobuz_provider()`` raised ``InvalidDataError`` whenever the native
    qobuz provider was briefly absent, and the exception recycled the whole
    connection.
    """
    provider, mass = _make_provider()
    mass.get_provider.side_effect = lambda _domain: None
    mass.config.save_provider_config = AsyncMock()
    provider.logger = MagicMock()

    await provider._on_quality_change(6)  # must not raise

    assert provider._max_quality == 6


async def test_unload_blocks_late_websocket_setup_and_reaps_timers(
    tmp_path: Any,
) -> None:
    """After unload, an in-flight handshake must not spawn a zombie session."""
    provider, mass = _make_provider()
    mass.storage_path = str(tmp_path)
    provider._flight_recorder._base_dir = tmp_path / "diag"
    bridge_items = [{"track_id": "100"}]
    provider._coordinator._bridge = SimpleNamespace(  # type: ignore[assignment]
        queue_items=lambda _pid: bridge_items,
        qobuz_track_id_for=lambda item: item["track_id"],
        get_queue=lambda _pid: None,
        get_player=lambda _pid: None,
    )
    await provider._coordinator.on_ma_queue_event("p1")
    assert provider._coordinator._timers

    await provider.unload()

    assert not provider._coordinator._timers
    await provider._setup_websocket(None)
    assert provider._session is None


def test_bridge_queue_items_reads_the_full_queue() -> None:
    """
    The bridge must never see a truncated MA queue.

    ``player_queues.items()`` defaults to ``limit=500``; a cloud queue larger
    than that (observed live: 1834 tracks) would read back truncated, making
    the differ believe the user replaced the queue with its first 500 tracks
    — and push a LOAD that truncates the real cloud queue in the app.
    """
    provider, mass = _make_provider()
    provider._bridge.queue_items("p1")
    _args, kwargs = mass.player_queues.items.call_args
    assert kwargs.get("limit", 500) >= 10_000
