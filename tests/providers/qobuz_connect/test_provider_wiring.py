"""Tests for wiring ``QobuzConnectProvider`` to the reducer-based sync core."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from music_assistant_models.enums import PlaybackState as MAPlaybackState
from music_assistant_models.errors import InvalidDataError
from music_assistant_models.media_items import Track

from music_assistant.providers import qobuz_connect as provider_module
from music_assistant.providers.qobuz_connect import (
    CONF_HTTP_PORT,
    CONF_INITIAL_VOLUME,
    CONF_MAX_QUALITY,
    CONF_PUBLISH_NAME,
    CONF_QOBUZ_PROVIDER,
    CONF_TARGET_PLAYER,
    PLAYER_ID_AUTO,
    QobuzConnectProvider,
)
from music_assistant.providers.qobuz_connect import outbound_reporter as outbound_reporter_module
from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.effect_runner import EffectRunner
from music_assistant.providers.qobuz_connect.models import (
    AudioQualityReport,
    BufferState,
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.outbound_reporter import OutboundReporter
from music_assistant.providers.qobuz_connect.setup_flow import build_setup_entries
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudActiveRendererChanged,
    CloudSetState,
    CloudSnapshot,
    PushVolume,
    ReportState,
)

if TYPE_CHECKING:
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


def _fake_qobuz_provider(native_quality: str = "27") -> SimpleNamespace:
    async def _get_track(track_id: str) -> Track:
        return Track(
            item_id=track_id,
            provider="qobuz",
            name=f"Track {track_id}",
            provider_mappings=set(),
            duration=200,
        )

    config = MagicMock()
    config.get_value.return_value = native_quality
    return SimpleNamespace(
        domain="qobuz",
        instance_id="qobuz",
        config=config,
        get_track=_get_track,
    )


def _make_provider(
    target_player: SimpleNamespace | None = None,
    *,
    max_quality: str = "27",
    native_quality: str = "27",
    qobuz_provider_id: str = "qobuz",
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
    mass.config.get_raw_provider_config_value.return_value = None
    qobuz_provider = _fake_qobuz_provider(native_quality)
    qobuz_provider.instance_id = qobuz_provider_id
    mass.get_provider.side_effect = lambda instance_id: (
        qobuz_provider if instance_id == qobuz_provider_id else None
    )

    manifest = MagicMock()
    manifest.domain = "qobuz_connect"

    values: dict[str, Any] = {
        CONF_TARGET_PLAYER: PLAYER_ID_AUTO,
        CONF_PUBLISH_NAME: "Test Qobuz Connect",
        CONF_HTTP_PORT: 8695,
        CONF_MAX_QUALITY: max_quality,
        CONF_INITIAL_VOLUME: 25,
        CONF_QOBUZ_PROVIDER: qobuz_provider_id,
    }
    config = MagicMock()
    config.instance_id = "qobuz_connect--test"
    config.name = "Test Qobuz Connect"
    config.get_value.side_effect = lambda key, *_a, **_k: values.get(key, "GLOBAL")

    return QobuzConnectProvider(mass, manifest, config), mass


async def test_config_entries_select_qobuz_instance_and_suggest_unused_port() -> None:
    """New instances select an account explicitly and avoid ports used by sibling instances."""
    mass = MagicMock()
    mass.players.all_players.return_value = []
    qobuz_one = SimpleNamespace(instance_id="qobuz--one", name="Qobuz One")
    qobuz_two = SimpleNamespace(instance_id="qobuz--two", name="Qobuz Two")
    connect_one = SimpleNamespace(
        instance_id="qobuz_connect--one",
        get_value=lambda key: 8695 if key == CONF_HTTP_PORT else None,
    )

    async def configs(*_args: Any, provider_domain: str, **_kwargs: Any) -> list[Any]:
        if provider_domain == "qobuz":
            return [qobuz_one, qobuz_two]
        if provider_domain == "qobuz_connect":
            assert not _kwargs.get("include_values"), (
                "expanded Connect configs recurse through get_config_entries"
            )
            return [connect_one]
        return []

    mass.config.get_provider_configs = AsyncMock(side_effect=configs)

    entries = await build_setup_entries(mass)
    by_key = {entry.key: entry for entry in entries}

    assert [entry.key for entry in entries] == [
        CONF_QOBUZ_PROVIDER,
        CONF_TARGET_PLAYER,
        CONF_PUBLISH_NAME,
        CONF_HTTP_PORT,
        CONF_INITIAL_VOLUME,
    ]
    assert [(option.value, option.title) for option in by_key[CONF_QOBUZ_PROVIDER].options] == [
        ("qobuz--one", "Qobuz One"),
        ("qobuz--two", "Qobuz Two"),
    ]
    assert by_key[CONF_QOBUZ_PROVIDER].default_value == "qobuz--one"
    assert by_key[CONF_HTTP_PORT].default_value == 8696


async def test_existing_instance_does_not_collide_with_itself_when_suggesting_port() -> None:
    """Editing an instance may retain its current default port."""
    mass = MagicMock()
    mass.players.all_players.return_value = []
    qobuz = SimpleNamespace(instance_id="qobuz--one", name="Qobuz One")
    connect = SimpleNamespace(
        instance_id="qobuz_connect--one",
        get_value=lambda key: 8695 if key == CONF_HTTP_PORT else None,
    )

    async def configs(*_args: Any, provider_domain: str, **_kwargs: Any) -> list[Any]:
        return [qobuz] if provider_domain == "qobuz" else [connect]

    mass.config.get_provider_configs = AsyncMock(side_effect=configs)

    entries = await build_setup_entries(mass, instance_id="qobuz_connect--one")

    assert next(entry for entry in entries if entry.key == CONF_HTTP_PORT).default_value == 8695


async def test_loaded_provider_exposes_quality_as_runtime_option() -> None:
    """Loaded providers expose only the dynamically editable quality option."""
    provider, _mass = _make_provider()

    entries = await provider.get_config_entries()

    assert [entry.key for entry in entries] == [CONF_MAX_QUALITY]


def test_setup_data_precedes_legacy_config_values() -> None:
    """Setup-flow data wins over values stored by the retired config schema."""
    provider, mass = _make_provider()
    mass.config.get_provider_setup_value.side_effect = lambda _instance_id, key, default=None: {
        CONF_PUBLISH_NAME: "Setup name",
        CONF_HTTP_PORT: 8795,
    }.get(key, default)
    mass.config.decrypt_string.side_effect = lambda value: value
    mass.config.get.side_effect = lambda key, default=None: {
        f"providers/{provider.instance_id}/setup_data": {
            CONF_PUBLISH_NAME: "Setup name",
            CONF_HTTP_PORT: 8795,
        }
    }.get(key, default)
    provider = QobuzConnectProvider(mass, provider.manifest, provider.config)

    assert provider._publish_name == "Setup name"
    assert provider._http_port == 8795


def test_selected_qobuz_instance_is_used_for_streams() -> None:
    """Native Qobuz work is routed through the explicitly selected account."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--selected")

    assert provider.get_qobuz_provider().instance_id == "qobuz--selected"
    mass.get_provider.assert_called_with("qobuz--selected")


def test_missing_selected_qobuz_instance_has_actionable_error() -> None:
    """A deleted or unloaded selected account is identified by instance id."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--missing")
    mass.get_provider.return_value = None
    mass.get_provider.side_effect = None

    with pytest.raises(InvalidDataError, match="qobuz--missing"):
        provider.get_qobuz_provider()


def test_manifest_declares_native_qobuz_dependency() -> None:
    """MA delays receiver loading until a native Qobuz provider is available."""
    manifest_path = (
        Path(provider_module.__file__).with_name("manifest.json")
        if provider_module.__file__
        else None
    )
    assert manifest_path is not None

    manifest = json.loads(manifest_path.read_text())

    assert manifest["depends_on"] == "qobuz"


@pytest.mark.parametrize(
    ("selected_provider", "message"),
    [
        (None, "qobuz--selected"),
        (SimpleNamespace(domain="tidal"), "qobuz--selected"),
    ],
)
async def test_handle_async_init_rejects_invalid_selected_qobuz_provider(
    selected_provider: SimpleNamespace | None,
    message: str,
) -> None:
    """Availability-critical init validates the exact selected native instance."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--selected")
    mass.get_provider.side_effect = None
    mass.get_provider.return_value = selected_provider

    with pytest.raises(InvalidDataError, match=message):
        await provider.handle_async_init()


async def test_handle_async_init_rejects_disabled_selected_qobuz_provider() -> None:
    """A configured but disabled native instance is unavailable to the receiver."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--disabled")
    disabled = SimpleNamespace(domain="qobuz", instance_id="qobuz--disabled", available=False)

    def get_provider(instance_id: str, return_unavailable: bool = False) -> Any:
        assert instance_id == "qobuz--disabled"
        return disabled if return_unavailable else None

    mass.get_provider.side_effect = get_provider

    with pytest.raises(InvalidDataError, match="qobuz--disabled"):
        await provider.handle_async_init()


async def test_handle_async_init_retries_cleanly_after_selected_qobuz_loads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delayed selected instance can initialize on MA's dependency-triggered retry."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--selected")
    native = _fake_qobuz_provider()
    native.instance_id = "qobuz--selected"
    mass.get_provider.side_effect = None
    mass.get_provider.return_value = None

    with pytest.raises(InvalidDataError):
        await provider.handle_async_init()

    discovery = MagicMock()
    discovery.start = AsyncMock()
    monkeypatch.setattr(provider_module, "QobuzConnectDiscovery", lambda **_kwargs: discovery)
    provider._flight_recorder = MagicMock()
    provider._flight_recorder.start = AsyncMock()
    provider._flight_recorder.stop = AsyncMock()
    provider._reporter = MagicMock()
    provider._reporter.start = AsyncMock()
    provider._reporter.stop = AsyncMock()
    mass.subscribe.side_effect = [MagicMock(), MagicMock(), MagicMock()]
    mass.get_provider.return_value = native

    await provider.handle_async_init()

    discovery.start.assert_awaited_once()


async def test_handle_async_init_rolls_back_partial_runtime_on_discovery_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A discovery error cannot leave reporters, subscriptions, or partial state running."""
    provider, mass = _make_provider()
    unsubs = [MagicMock(), MagicMock(), MagicMock()]
    mass.subscribe.side_effect = unsubs
    provider._flight_recorder = MagicMock()
    provider._flight_recorder.start = AsyncMock()
    provider._flight_recorder.stop = AsyncMock()
    provider._reporter = MagicMock()
    provider._reporter.start = AsyncMock()
    provider._reporter.stop = AsyncMock()
    discovery = MagicMock()
    discovery.start = AsyncMock(side_effect=OSError("occupied"))
    discovery.stop = AsyncMock()
    monkeypatch.setattr(provider_module, "QobuzConnectDiscovery", lambda **_kwargs: discovery)

    with pytest.raises(OSError, match="occupied"):
        await provider.handle_async_init()

    for unsubscribe in unsubs:
        unsubscribe.assert_called_once_with()
    provider._reporter.stop.assert_awaited_once()
    provider._flight_recorder.stop.assert_awaited_once()
    discovery.stop.assert_awaited_once()
    assert provider._discovery is None
    assert provider._unsubscribe_queue_events is None
    assert provider._unsubscribe_queue_items_events is None
    assert provider._unsubscribe_player_events is None


def test_queue_mapping_does_not_require_selected_provider_to_be_loaded_yet() -> None:
    """Early player events may resolve IDs while native Qobuz is still starting."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--selected")
    mass.get_provider.side_effect = lambda _instance_id: None
    item = SimpleNamespace(
        media_item=SimpleNamespace(
            media_type="track",
            provider="qobuz--selected",
            item_id="123",
            provider_mappings=(),
        )
    )

    assert provider.get_qobuz_track_id_from_queue_item(item) == "123"


def test_queue_mapping_exposes_every_qobuz_alias() -> None:
    """Library items retain every Qobuz mapping so Connect can match canonical ids."""
    provider, _mass = _make_provider(qobuz_provider_id="qobuz--selected")
    item = SimpleNamespace(
        media_item=SimpleNamespace(
            media_type="track",
            provider="library",
            item_id="1051",
            provider_mappings=(
                SimpleNamespace(
                    provider_domain="qobuz",
                    provider_instance="qobuz--selected",
                    item_id="3972279",
                ),
                SimpleNamespace(
                    provider_domain="qobuz",
                    provider_instance="qobuz--selected",
                    item_id="3879020",
                ),
            ),
        )
    )

    assert provider.get_qobuz_track_ids_from_queue_item(item) == ("3972279", "3879020")


def test_setup_constructs_coordinator_and_effect_runner() -> None:
    """The provider builds a coordinator + effect runner instead of the retired sync engine."""
    provider, _mass = _make_provider()

    assert isinstance(provider._coordinator, QobuzConnectCoordinator)
    assert isinstance(provider._effect_runner, EffectRunner)
    assert not hasattr(provider, "_sync")
    assert not hasattr(provider, "controller")


def test_build_session_callbacks_delegates_to_coordinator() -> None:
    """Provider callbacks wrap coordinator intake where lifecycle work is required."""
    provider, _mass = _make_provider()
    callbacks = provider._build_session_callbacks()

    assert callbacks.submit == provider._submit_cloud_event
    assert callbacks.on_disconnected == provider._on_session_disconnected

    # Provider-level overrides that do strictly more than direct coordinator callbacks.
    assert callbacks.on_set_active == provider._on_set_active
    assert callbacks.on_quality == provider._on_quality_change
    assert callbacks.on_connected == provider._on_session_connected


async def test_snapshot_then_activate_reaches_ma_play() -> None:
    """An inbound snapshot + SET_STATE + SET_ACTIVE through the session callbacks plays on MA."""
    provider, mass = _make_provider()
    callbacks = provider._build_session_callbacks()

    await callbacks.submit(
        CloudSnapshot(
            now_ms=1,
            version=QueueVersion(1, 0),
            tracks=(QueueTrackRef(queue_item_id=1, track_id="501"),),
            autoplay_tracks=(),
            shuffle=False,
            autoplay=False,
            track_index=0,
        )
    )
    assert provider._coordinator.state.current_id == 501

    await callbacks.submit(
        CloudSetState(
            now_ms=2,
            version=None,
            playing=PlayingState.PLAYING,
            position_ms=0,
            current_ref=QueueTrackRef(queue_item_id=1, track_id="501"),
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
    provider._session.send_volume_changed = AsyncMock()
    provider._session.send_volume_muted = AsyncMock()
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_current = AsyncMock()

    await provider._on_set_active(True)

    provider._session.send_volume_changed.assert_awaited_once()
    provider._quality_reporter.report_current.assert_awaited_once_with(provider._max_quality)
    assert provider._coordinator.state.active is True


async def test_on_set_active_false_releases_without_broadcast() -> None:
    """Deactivation does not broadcast volume/quality, only deactivates the coordinator."""
    provider, _mass = _make_provider()
    provider._session = MagicMock()
    provider._session.send_volume_changed = AsyncMock()
    provider._session.send_volume_muted = AsyncMock()
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_current = AsyncMock()

    await provider._on_set_active(True)
    provider._session.send_volume_changed.reset_mock()
    provider._quality_reporter.report_current.reset_mock()

    await provider._on_set_active(False)

    provider._session.send_volume_changed.assert_not_awaited()
    provider._quality_reporter.report_current.assert_not_awaited()
    assert provider._coordinator.state.active is False


async def test_activation_suppresses_and_deactivation_restores_ma_autoplay() -> None:
    """Activation leases and suppresses MA autoplay until deactivation."""
    provider, mass = _make_provider()
    queue = SimpleNamespace(autoplay_enabled=True)
    mass.player_queues.get.return_value = queue
    mass.player_queues.set_autoplay.side_effect = lambda _player_id, enabled: setattr(
        queue, "autoplay_enabled", enabled
    )

    await provider._on_set_active(True)
    assert queue.autoplay_enabled is False

    await provider._on_set_active(False)
    assert queue.autoplay_enabled is True


async def test_repeated_activation_does_not_overwrite_restore_value() -> None:
    """Repeated activation preserves the autoplay value captured initially."""
    provider, mass = _make_provider()
    queue = SimpleNamespace(autoplay_enabled=True)
    mass.player_queues.get.return_value = queue
    mass.player_queues.set_autoplay.side_effect = lambda _player_id, enabled: setattr(
        queue, "autoplay_enabled", enabled
    )

    await provider._on_set_active(True)
    await provider._on_set_active(True)
    await provider._on_set_active(False)

    assert queue.autoplay_enabled is True


async def test_repeated_activation_transfers_autoplay_lease_to_replacement_player() -> None:
    """A vanished active target restores its lease before suppressing the replacement."""
    p1 = _fake_player("p1")
    p2 = _fake_player("p2")
    provider, mass = _make_provider(p1)
    players = {"p1": p1, "p2": p2}
    queues = {
        "p1": SimpleNamespace(autoplay_enabled=True),
        "p2": SimpleNamespace(autoplay_enabled=True),
    }
    mass.players.all_players.return_value = [p1, p2]
    mass.players.get_player.side_effect = players.get
    mass.player_queues.get.side_effect = queues.get
    mass.player_queues.set_autoplay.side_effect = lambda player_id, enabled: setattr(
        queues[player_id], "autoplay_enabled", enabled
    )

    def autoplay_enabled(player_id: str) -> bool:
        return bool(queues[player_id].autoplay_enabled)

    await provider._on_set_active(True)
    assert autoplay_enabled("p1") is False

    players.pop("p1")
    mass.players.all_players.return_value = [p2]
    await provider._on_set_active(True)

    assert autoplay_enabled("p1") is True
    assert autoplay_enabled("p2") is False
    assert provider._coordinator._owned_target_player_id == "p2"

    await provider._on_set_active(False)

    assert autoplay_enabled("p1") is True
    assert autoplay_enabled("p2") is True


async def test_deactivation_releases_replacement_when_active_target_disappeared() -> None:
    """Deactivation reconciles a vanished pin before choosing the release target."""
    p1 = _fake_player("p1")
    p2 = _fake_player("p2")
    provider, mass = _make_provider(p1)
    players = {"p1": p1, "p2": p2}
    mass.players.all_players.return_value = [p1, p2]
    mass.players.get_player.side_effect = players.get
    mass.player_queues.stop = AsyncMock()
    mass.player_queues.clear = MagicMock()

    await provider._on_set_active(True)
    players.pop("p1")
    mass.players.all_players.return_value = [p2]

    await provider._on_set_active(False)

    mass.player_queues.stop.assert_awaited_once_with("p2")
    mass.player_queues.clear.assert_called_once_with("p2", skip_stop=True)
    assert provider._pinned_target_id is None


async def test_unload_restores_autoplay_on_the_captured_player_only() -> None:
    """Unload restores autoplay only on the player captured by the lease."""
    provider, mass = _make_provider()
    original = SimpleNamespace(autoplay_enabled=True)
    replacement = SimpleNamespace(autoplay_enabled=False)
    queues = {"player_1": original, "player_2": replacement}
    mass.player_queues.get.side_effect = queues.get
    mass.player_queues.set_autoplay.side_effect = lambda player_id, enabled: setattr(
        queues[player_id], "autoplay_enabled", enabled
    )
    cast("Any", provider._flight_recorder).stop = AsyncMock()

    await provider._on_set_active(True)
    provider._ma_autoplay_lease = ("player_1", True)
    provider._pinned_target_id = "player_2"
    await provider.unload()

    assert original.autoplay_enabled is True
    assert replacement.autoplay_enabled is False


async def test_unload_prevents_in_flight_activation_from_suppressing_autoplay() -> None:
    """Activation awaiting setup work cannot acquire a lease after unload starts."""
    provider, mass = _make_provider()
    queue = SimpleNamespace(autoplay_enabled=True)
    mass.player_queues.get.return_value = queue
    mass.player_queues.set_autoplay.side_effect = lambda _player_id, enabled: setattr(
        queue, "autoplay_enabled", enabled
    )
    activation_waiting = asyncio.Event()
    resume_activation = asyncio.Event()

    async def wait_during_activation(_quality: int) -> None:
        activation_waiting.set()
        await resume_activation.wait()

    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_current = AsyncMock(side_effect=wait_during_activation)
    cast("Any", provider._flight_recorder).stop = AsyncMock()

    activation = asyncio.create_task(provider._on_set_active(True))
    await asyncio.wait_for(activation_waiting.wait(), timeout=1)
    await provider.unload()
    resume_activation.set()
    await activation

    assert queue.autoplay_enabled is True
    assert provider._ma_autoplay_lease is None


async def test_active_renderer_change_restores_ma_autoplay() -> None:
    """Losing cloud renderer ownership through submit restores MA autoplay."""
    provider, mass = _make_provider()
    queue = SimpleNamespace(autoplay_enabled=True)
    mass.player_queues.get.return_value = queue
    mass.player_queues.set_autoplay.side_effect = lambda _player_id, enabled: setattr(
        queue, "autoplay_enabled", enabled
    )
    callbacks = provider._build_session_callbacks()

    await callbacks.on_set_active(True)
    assert queue.autoplay_enabled is False

    await callbacks.submit(CloudActiveRendererChanged(now_ms=2, renderer_id=99))

    assert provider._coordinator.state.active is False
    assert queue.autoplay_enabled is True


async def test_disconnect_restores_ma_autoplay() -> None:
    """Disconnecting restores MA autoplay when canonical ownership is dropped."""
    provider, mass = _make_provider()
    queue = SimpleNamespace(autoplay_enabled=True)
    mass.player_queues.get.return_value = queue
    mass.player_queues.set_autoplay.side_effect = lambda _player_id, enabled: setattr(
        queue, "autoplay_enabled", enabled
    )
    callbacks = provider._build_session_callbacks()

    await callbacks.on_set_active(True)
    assert queue.autoplay_enabled is False
    assert callbacks.on_disconnected is not None

    await callbacks.on_disconnected()

    assert provider._coordinator.state.active is False
    assert vars(queue)["autoplay_enabled"] is True
    assert vars(provider)["_pinned_target_id"] is None


async def test_queue_update_reasserts_autoplay_suppression_while_active() -> None:
    """MA queue updates reassert suppression without replacing the lease."""
    provider, mass = _make_provider()
    queue = SimpleNamespace(
        autoplay_enabled=True,
        state=SimpleNamespace(value="idle"),
        current_item=None,
        corrected_elapsed_time=0,
        repeat_mode=SimpleNamespace(value="off"),
    )
    mass.player_queues.get.return_value = queue
    mass.player_queues.set_autoplay.side_effect = lambda _player_id, enabled: setattr(
        queue, "autoplay_enabled", enabled
    )

    await provider._on_set_active(True)
    assert queue.autoplay_enabled is False

    queue.autoplay_enabled = True
    await provider._on_ma_queue_event(_fake_event("player_1"))
    assert queue.autoplay_enabled is False

    await provider._on_set_active(False)
    assert queue.autoplay_enabled is True


async def test_deactivation_releases_the_player_captured_at_activation() -> None:
    """Auto target changes cannot redirect a delayed release to another player."""
    p1 = _fake_player("p1")
    p2 = _fake_player("p2")
    provider, mass = _make_provider(p1)
    players = {"p1": p1, "p2": p2}
    mass.players.all_players.return_value = [p1, p2]
    mass.players.get_player.side_effect = players.get
    mass.player_queues.stop = AsyncMock()
    mass.player_queues.clear = MagicMock()
    provider._session = MagicMock()
    provider._session.send_volume_changed = AsyncMock()
    provider._session.send_volume_muted = AsyncMock()
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_current = AsyncMock()

    await provider._on_set_active(True)
    mass.players.all_players.return_value = [p2, p1]
    p2.state.playback_state = MAPlaybackState.PLAYING
    await provider._on_set_active(False)

    mass.player_queues.stop.assert_awaited_once_with("p1")
    mass.player_queues.clear.assert_called_once_with("p1", skip_stop=True)


async def test_ma_queue_event_delegates_to_transport_and_modes() -> None:
    """QUEUE_UPDATED delegates to both the transport and modes coordinator entry points."""
    provider, _mass = _make_provider()
    provider._coordinator = AsyncMock()
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_file = AsyncMock()
    event = _fake_event("player_1")

    await provider._on_ma_queue_event(event)

    provider._coordinator.on_ma_transport_event.assert_awaited_once_with("player_1")
    provider._coordinator.on_ma_modes_event.assert_awaited_once_with("player_1")
    provider._quality_reporter.report_file.assert_awaited_once_with(None)


async def test_ma_queue_items_event_delegates_to_queue_event() -> None:
    """QUEUE_ITEMS_UPDATED delegates to the coordinator's queue entry point."""
    provider, _mass = _make_provider()
    provider._coordinator = AsyncMock()
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_file = AsyncMock()
    event = _fake_event("player_1")

    await provider._on_ma_queue_items_event(event)

    provider._coordinator.on_ma_queue_event.assert_awaited_once_with("player_1")
    provider._quality_reporter.report_file.assert_awaited_once_with(None)


def test_current_file_quality_reads_real_ma_stream_details() -> None:
    """A 24/44.1 stream stays 24/44.1 even when the configured ceiling is 24/192."""
    provider, mass = _make_provider()
    mass.player_queues.get.return_value = SimpleNamespace(
        current_item=SimpleNamespace(
            streamdetails=SimpleNamespace(
                audio_format=SimpleNamespace(
                    content_type="flac",
                    sample_rate=44_100,
                    bit_depth=24,
                    channels=2,
                )
            )
        )
    )

    assert provider._current_file_quality() == AudioQualityReport(
        quality=7,
        sampling_rate=44_100,
        bit_depth=24,
        channels=2,
    )


def test_current_file_quality_is_unknown_until_stream_details_resolve() -> None:
    """The provider must not invent actual properties before MA resolves the stream."""
    provider, mass = _make_provider()
    mass.player_queues.get.return_value = SimpleNamespace(
        current_item=SimpleNamespace(streamdetails=None)
    )

    assert provider._current_file_quality() is None


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


async def test_reporter_uses_current_duplicate_occurrence_index() -> None:
    """The second copy of a repeated track reports its own cloud queue-item id."""
    sent: dict[str, Any] = {}

    class _Session:
        async def send_renderer_state(self, **kwargs: Any) -> bool:
            sent.update(kwargs)
            return True

    state = CanonicalState(
        tracks=(
            QueueTrackRef(queue_item_id=10, track_id="501"),
            QueueTrackRef(queue_item_id=11, track_id="501"),
        ),
        current_id=501,
    )
    reporter = OutboundReporter(
        session_getter=lambda: cast("Any", _Session()),
        state_getter=lambda: state,
        duration_getter=lambda: 200_000,
        active_getter=lambda: True,
        current_index_getter=lambda: 1,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.report_state()

    assert sent["queue_item_id"] == 11


async def test_reporter_resolves_current_autoplay_item() -> None:
    """A continuation track reports the cloud slot from the autoplay tail."""
    sent: dict[str, Any] = {}

    class _Session:
        async def send_renderer_state(self, **kwargs: Any) -> bool:
            sent.update(kwargs)
            return True

    state = CanonicalState(
        tracks=(QueueTrackRef(queue_item_id=10, track_id="100"),),
        autoplay_tracks=(QueueTrackRef(queue_item_id=20, track_id="200"),),
        current_id=200,
    )
    reporter = OutboundReporter(
        session_getter=lambda: cast("Any", _Session()),
        state_getter=lambda: state,
        duration_getter=lambda: 200_000,
        active_getter=lambda: True,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.report_state()

    assert sent["queue_item_id"] == 20


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


async def test_reporter_skips_without_confirmed_renderer_ownership() -> None:
    """Direct ReportState effects stay silent until the cloud confirms ownership."""
    session = MagicMock()
    session.send_renderer_state = AsyncMock()
    reporter = OutboundReporter(
        session_getter=lambda: session,
        state_getter=lambda: CanonicalState(
            tracks=(QueueTrackRef(queue_item_id=7, track_id="501"),),
            current_id=501,
            active=True,
        ),
        duration_getter=lambda: 0,
        active_getter=lambda: False,
        logger=outbound_reporter_module.LOGGER,
    )

    await reporter.report_state()

    session.send_renderer_state.assert_not_awaited()


def test_current_track_duration_ms_reads_live_ma_queue() -> None:
    """The provider's duration getter reads the live MA queue item (seconds -> ms)."""
    provider, mass = _make_provider()
    mass.player_queues.get.return_value = SimpleNamespace(
        current_item=SimpleNamespace(duration=200)
    )
    assert provider._current_track_duration_ms() == 200_000

    mass.player_queues.get.return_value = None
    assert provider._current_track_duration_ms() == 0


async def test_bridge_relative_volume_clamps_and_mute_uses_ma_commands() -> None:
    """Relative controls read current state and stay inside MA's 0-100 range."""
    player = _fake_player()
    player.volume_level = 3
    provider, mass = _make_provider(player)
    mass.players.cmd_volume_set = AsyncMock()
    mass.players.cmd_volume_mute = AsyncMock()

    await provider._bridge.adjust_volume(player.player_id, -7)
    player.volume_level = 98
    await provider._bridge.adjust_volume(player.player_id, 7)
    await provider._bridge.set_muted(player.player_id, True)

    assert mass.players.cmd_volume_set.await_args_list == [
        ((player.player_id, 0),),
        ((player.player_id, 100),),
    ]
    mass.players.cmd_volume_mute.assert_awaited_once_with(player.player_id, True)


async def test_connected_log_is_emitted_only_from_confirmed_callback() -> None:
    """Starting the background loop is distinct from a confirmed websocket connection."""
    provider, _mass = _make_provider()
    provider.logger = MagicMock()

    await provider._on_session_connected()

    provider.logger.info.assert_called_once_with("Qobuz Connect WebSocket connected")


def test_reporter_active_getter_requires_active_and_target_player() -> None:
    """Reporting requires confirmed ownership plus an available target player."""
    provider, mass = _make_provider()

    provider._coordinator._state = CanonicalState(active=False)
    assert provider._reporter._active_getter() is False

    provider._coordinator._state = CanonicalState(active=True, own_rid=42, active_rid=42)
    assert provider._reporter._active_getter() is True

    provider._coordinator._state = CanonicalState(active=True, own_rid=42, active_rid=9)
    assert provider._reporter._active_getter() is False

    provider._coordinator._state = CanonicalState(active=True, own_rid=42, active_rid=42)

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


def test_configured_target_return_waits_until_active_fallback_releases() -> None:
    """A recovered configured target cannot redirect a live fallback session."""
    configured = _fake_player("configured")
    fallback = _fake_player("fallback")
    provider, mass = _make_provider(fallback)
    provider._target_player_id = configured.player_id
    players = {"fallback": fallback}
    mass.players.all_players.return_value = [fallback]
    mass.players.get_player.side_effect = players.get

    assert provider.get_target_player_id() == "fallback"
    provider._coordinator._state = CanonicalState(active=True)
    players["configured"] = configured
    mass.players.all_players.return_value = [configured, fallback]

    assert provider.get_target_player_id() == "fallback"

    provider._coordinator._state = CanonicalState(active=False)
    provider._pinned_target_id = None
    assert provider.get_target_player_id() == "configured"


def test_missing_configured_player_falls_back_to_auto_and_warns_once() -> None:
    """
    A pinned target that no longer resolves must fall back to auto, not kill playback.

    A stale/offline pinned target (seen live after MA's player-id scheme drifted
    from ``up<hex>`` to a dashed uuid) used to return ``None`` from
    ``get_target_player_id``, so ``MaPlayTrack``/``MaResyncQueue`` were dropped
    with "no target player configured" and the receiver went active-but-silent —
    "connect, press play, nothing happens". Now it warns once, then resolves an
    available player so playback still works. The warning must fire once, not on
    every unfiltered MA event (a log flood otherwise).
    """
    provider, mass = _make_provider()
    provider._target_player_id = "gone"
    fallback = _fake_player("fallback_1")
    mass.players.all_players.return_value = [fallback]
    mass.players.get_player.side_effect = lambda pid: fallback if pid == "fallback_1" else None
    provider.logger = MagicMock()

    assert provider.get_target_player_id() == "fallback_1"
    assert provider.get_target_player_id() == "fallback_1"
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


async def test_connect_quality_save_failure_still_updates_native_qobuz() -> None:
    """The two config writes are independent so one failure cannot skip the other."""
    provider, mass = _make_provider()
    calls: list[str] = []

    async def save(domain: str, *_args: Any, **_kwargs: Any) -> None:
        calls.append(domain)
        if domain == provider.domain:
            raise RuntimeError("connect save failed")

    mass.config.save_provider_config = AsyncMock(side_effect=save)

    await provider._on_quality_change(6)

    assert calls == [provider.domain, "qobuz"]


async def test_native_quality_save_failure_still_reports_selected_maximum() -> None:
    """A config backend failure must not suppress the renderer's wire response."""
    provider, mass = _make_provider()

    async def save(domain: str, *_args: Any, **_kwargs: Any) -> None:
        if domain == "qobuz":
            raise RuntimeError("native save failed")

    mass.config.save_provider_config = AsyncMock(side_effect=save)
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_current = AsyncMock()

    await provider._on_quality_change(7)

    provider._quality_reporter.report_current.assert_awaited_once_with(7)


async def test_ma_side_quality_update_synchronizes_native_qobuz() -> None:
    """Changing Connect quality in MA updates the native provider stream setting."""
    provider, mass = _make_provider(qobuz_provider_id="qobuz--selected")
    config = MagicMock()
    config.get_value.return_value = "7"
    provider._quality_reporter = MagicMock()
    provider._quality_reporter.report_current = AsyncMock()
    mass.config.save_provider_config = AsyncMock()

    await provider.update_config(config, {f"values/{CONF_MAX_QUALITY}"})

    mass.config.save_provider_config.assert_awaited_once_with(
        "qobuz",
        {"quality": "7"},
        "qobuz--selected",
    )
    provider._quality_reporter.report_current.assert_awaited_once_with(7)


def test_auto_quality_resolves_native_provider_config() -> None:
    """Auto never reaches the wire as protocol quality zero."""
    provider, _mass = _make_provider(max_quality="0", native_quality="7")

    assert provider._configured_max_quality == 0
    assert provider._max_quality == 7
    assert provider._device_config.max_quality == 7


async def test_activation_reconciles_explicit_quality_after_restart() -> None:
    """An explicit persisted ceiling becomes native stream quality on activation."""
    provider, mass = _make_provider(max_quality="6", native_quality="27")
    mass.config.save_provider_config = AsyncMock()

    await provider._on_set_active(True)

    mass.config.save_provider_config.assert_awaited_once_with(
        "qobuz",
        {"quality": "6"},
        "qobuz",
    )


async def test_auto_quality_does_not_write_native_quality_on_activation() -> None:
    """AUTO follows the native provider without claiming shared quality ownership."""
    provider, mass = _make_provider(max_quality="0", native_quality="7")
    mass.config.save_provider_config = AsyncMock()

    await provider._on_set_active(True)

    mass.config.save_provider_config.assert_not_awaited()


async def test_auto_quality_refreshes_from_shared_native_quality_on_activation() -> None:
    """AUTO observes a quality changed by another receiver since this one loaded."""
    provider, mass = _make_provider(max_quality="0", native_quality="7")
    native = provider.get_qobuz_provider()
    cast("Any", native.config.get_value).return_value = "6"
    mass.config.save_provider_config = AsyncMock()

    await provider._on_set_active(True)

    assert provider._max_quality == 6
    assert provider._device_config.max_quality == 6
    mass.config.save_provider_config.assert_not_awaited()


async def test_last_active_explicit_receiver_owns_shared_native_quality() -> None:
    """Two receivers sharing one account reconcile in activation order."""
    receiver_a, mass = _make_provider(max_quality="6", native_quality="27")
    receiver_b, _other_mass = _make_provider(max_quality="27", native_quality="27")
    receiver_b.mass = mass
    native = receiver_a.get_qobuz_provider()
    current_quality = "27"
    cast("Any", native.config.get_value).side_effect = lambda _key: current_quality
    saved_qualities: list[str] = []

    async def save(
        domain: str,
        values: dict[str, Any],
        _instance_id: str,
    ) -> None:
        nonlocal current_quality
        if domain == "qobuz":
            current_quality = cast("str", values["quality"])
            saved_qualities.append(current_quality)

    mass.config.save_provider_config = AsyncMock(side_effect=save)

    await receiver_a._on_set_active(True)
    await receiver_b._on_set_active(True)

    assert saved_qualities == ["6", "27"]


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
