"""Safety tests for the live Qobuz/MA integration harness."""

from __future__ import annotations

import json
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QConnectMessageType,
)
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec
from tests.providers.qobuz_connect.protocol_capture import ma_probe as ma_probe_module
from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    BLACKHOLE_PLAYER_ID,
    IntegrationSession,
    SafetyPreflight,
    ScenarioResult,
    ScenarioStatus,
    default_probe,
)
from tests.providers.qobuz_connect.protocol_capture.ma_probe import (
    MANAGED_CONNECT_TARGET,
    MAProbe,
)
from tests.providers.qobuz_connect.protocol_capture.qobuz_page import QobuzPage
from tests.providers.qobuz_connect.protocol_capture.ws_recorder import WsRecorder


class FakeQobuzPage:
    """Minimal observable Qobuz output picker for preflight tests."""

    def __init__(self, outputs: tuple[str, ...], selected: str) -> None:
        """Store visible and selected outputs."""
        self.outputs = outputs
        self.selected = selected
        self.played_urls: list[str] = []

    async def network_output_names(self) -> tuple[str, ...]:
        """Return the visible network outputs."""
        return self.outputs

    async def select_local_output(self, expected_name: str = "Web Player Chrome") -> None:
        """Select the expected browser output."""
        self.selected = expected_name

    async def selected_output_name(self) -> str:
        """Return the active output."""
        return self.selected

    async def is_local_output_selected(self) -> bool:
        """Return whether the fake browser-local output is selected."""
        return self.selected in {"Web Player Chrome", "Standardmäßige Audioausgabe"}

    async def play_album_by_url(self, url: str) -> None:
        """Record attempted playback."""
        self.played_urls.append(url)


def _provider(target: str, name: str = "Local Dev Hardening abc123") -> dict[str, Any]:
    return {
        "domain": "qobuz_connect",
        "status": "loaded",
        "values": {"target_player": target, "publish_name": name},
    }


def _player(
    player_id: str = BLACKHOLE_PLAYER_ID,
    *,
    name: str = "BlackHole 2ch",
    provider: str = "local_audio",
    available: bool = True,
) -> dict[str, Any]:
    return {
        "player_id": player_id,
        "name": name,
        "provider": provider,
        "available": available,
    }


def _query(providers: list[dict[str, Any]], players: list[dict[str, Any]]) -> Any:
    async def query(command: str, _args: dict[str, object]) -> object:
        if command == "config/providers":
            return providers
        if command == "players/all":
            return players
        raise AssertionError(f"unexpected MA query: {command}")

    return query


async def test_preflight_rejects_wrong_target_player() -> None:
    """A configured audible player must abort before any playback action."""
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage(("Local Dev Hardening abc123",), "Landwarekan"),
        ma_query_func=_query(
            [_provider("7dd0558a-0992-5b04-a757-66a07edbc824")],
            [_player()],
        ),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
    )

    result = await preflight.run()

    assert not result.passed
    assert any("target player" in failure for failure in result.failures)


async def test_preflight_requires_exact_unique_cloud_renderer_name() -> None:
    """A fuzzy or duplicate cloud name must not be accepted as the test receiver."""
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage(("Local Dev Hardening", "Music Assistant"), "Landwarekan"),
        ma_query_func=_query([_provider(BLACKHOLE_PLAYER_ID)], [_player()]),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
    )

    result = await preflight.run()

    assert not result.passed
    assert any("exactly once" in failure for failure in result.failures)


async def test_preflight_rejects_unavailable_or_nonlocal_blackhole() -> None:
    """The expected UUID alone is insufficient when the player is unavailable or wrong."""
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage(("Local Dev Hardening abc123",), "Landwarekan"),
        ma_query_func=_query(
            [_provider(BLACKHOLE_PLAYER_ID)],
            [_player(provider="airplay", available=False)],
        ),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
    )

    result = await preflight.run()

    assert not result.passed
    assert any("available local_audio" in failure for failure in result.failures)


async def test_preflight_accepts_exact_blackhole_and_browser_output() -> None:
    """Exact MA configuration, player identity, receiver name, and output pass."""
    qobuz = FakeQobuzPage(("Local Dev Hardening abc123",), "Landwarekan")
    preflight = SafetyPreflight(
        qobuz=qobuz,
        ma_query_func=_query([_provider(BLACKHOLE_PLAYER_ID)], [_player()]),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
    )

    result = await preflight.run()

    assert result.passed
    assert result.failures == ()
    assert await qobuz.selected_output_name() == "Web Player Chrome"


async def test_preflight_accepts_expanded_provider_config_entries() -> None:
    """The real config API wraps each persisted value in a ConfigEntry payload."""
    provider = _provider(BLACKHOLE_PLAYER_ID)
    provider["values"] = {
        key: {"key": key, "value": value} for key, value in provider["values"].items()
    }
    qobuz = FakeQobuzPage(("Local Dev Hardening abc123",), "Landwarekan")
    preflight = SafetyPreflight(
        qobuz=qobuz,
        ma_query_func=_query([provider], [_player()]),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
    )

    result = await preflight.run()

    assert result.passed


async def test_preflight_accepts_setup_flow_config_from_managed_probe() -> None:
    """Setup-only fields omitted by the config API are verified through the managed probe."""
    provider: dict[str, Any] = {
        "domain": "qobuz_connect",
        "instance_id": "qobuz_connect--test",
        "status": "loaded",
        "last_error": None,
        "values": {"max_quality": {"key": "max_quality", "value": "27"}},
    }
    qobuz = FakeQobuzPage((MANAGED_CONNECT_TARGET,), "Landwarekan")
    preflight = SafetyPreflight(
        qobuz=qobuz,
        ma_query_func=_query([provider], [_player()]),
        connect_target=MANAGED_CONNECT_TARGET,
        managed_pid=1234,
        managed_config_func=lambda: {
            "instance_id": "qobuz_connect--test",
            "target_player": BLACKHOLE_PLAYER_ID,
            "publish_name": MANAGED_CONNECT_TARGET,
        },
    )

    result = await preflight.run()

    assert result.passed


async def test_preflight_rejects_wrong_setup_flow_target_from_managed_probe() -> None:
    """The managed config source must still reject an audible target before playback."""
    provider = {
        "domain": "qobuz_connect",
        "instance_id": "qobuz_connect--test",
        "status": "loaded",
        "last_error": None,
        "values": {"max_quality": {"key": "max_quality", "value": "27"}},
    }
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage((MANAGED_CONNECT_TARGET,), "Landwarekan"),
        ma_query_func=_query([provider], [_player()]),
        connect_target=MANAGED_CONNECT_TARGET,
        managed_pid=1234,
        managed_config_func=lambda: {
            "instance_id": "qobuz_connect--test",
            "target_player": "audible-player",
            "publish_name": MANAGED_CONNECT_TARGET,
        },
    )

    result = await preflight.run()

    assert not result.passed
    assert any("target player" in failure for failure in result.failures)


async def test_preflight_rejects_unloaded_managed_provider() -> None:
    """Managed disk identity cannot make an unavailable API provider safe."""
    provider: dict[str, Any] = {
        "domain": "qobuz_connect",
        "instance_id": "qobuz_connect--test",
        "status": "unavailable",
        "last_error": None,
        "values": {},
    }
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage((MANAGED_CONNECT_TARGET,), "Landwarekan"),
        ma_query_func=_query([provider], [_player()]),
        connect_target=MANAGED_CONNECT_TARGET,
        managed_pid=1234,
        managed_config_func=lambda: {
            "instance_id": "qobuz_connect--test",
            "target_player": BLACKHOLE_PLAYER_ID,
            "publish_name": MANAGED_CONNECT_TARGET,
        },
    )

    result = await preflight.run()

    assert not result.passed
    assert any("not loaded" in failure for failure in result.failures)


def test_managed_probe_pins_setup_flow_config_and_restores_exact_state(tmp_path: Any) -> None:
    """Managed runs pin setup_data and restore both config stores exactly."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    settings = data_dir / "settings.json"
    original = {
        "encryption_key": "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
        "providers": {
            "qobuz--native": {
                "domain": "qobuz",
                "values": {"quality": "27"},
            },
            "qobuz_connect--test": {
                "domain": "qobuz_connect",
                "setup_data": {
                    "target_player": "audible-player",
                    "publish_name": "Original receiver",
                    "http_port": 8795,
                    "qobuz_provider": "qobuz--native",
                    "initial_volume": 12,
                    "keep_setup": "unchanged",
                },
                "values": {"keep_option": True},
            },
        },
    }
    settings.write_text(json.dumps(original))
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=data_dir,
        cache_dir=tmp_path / "cache",
    )

    probe._pin_target(BLACKHOLE_PLAYER_ID)

    pinned = json.loads(settings.read_text())
    connect = pinned["providers"]["qobuz_connect--test"]
    assert connect["setup_data"]["target_player"].startswith("_encrypted_")
    assert connect["setup_data"]["publish_name"].startswith("_encrypted_")
    assert connect["setup_data"]["qobuz_provider"].startswith("_encrypted_")
    assert connect["setup_data"]["http_port"] == 8695
    assert connect["setup_data"]["initial_volume"] == 25
    assert connect["setup_data"]["keep_setup"] == "unchanged"
    assert connect["values"] == {"keep_option": True, "max_quality": "27"}
    assert probe.managed_connect_config() == {
        "instance_id": "qobuz_connect--test",
        "target_player": BLACKHOLE_PLAYER_ID,
        "publish_name": MANAGED_CONNECT_TARGET,
        "http_port": 8695,
        "qobuz_provider": "qobuz--native",
        "initial_volume": 25,
    }

    probe._restore_target()

    assert json.loads(settings.read_text()) == deepcopy(original)


def test_managed_probe_rolls_back_when_started_process_exits(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An early child exit must fail startup and restore provider config."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    settings = data_dir / "settings.json"
    original = {
        "encryption_key": "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
        "providers": {
            "qobuz--native": {"domain": "qobuz", "values": {}},
            "qobuz_connect--test": {
                "domain": "qobuz_connect",
                "values": {"target_player": "original-player"},
            },
        },
    }
    settings.write_text(json.dumps(original))

    class ExitedProcess:
        """Minimal child process that has already exited."""

        pid = 4321

        def poll(self) -> int:
            """Return the child exit status."""
            return 7

        def terminate(self) -> None:
            """Record no work for an exited child."""

        def wait(self, timeout: float) -> int:
            """Return the existing exit status."""
            assert timeout == 20
            return 7

    monkeypatch.setattr(
        ma_probe_module,
        "_listening_port_owners",
        lambda _ports: {},
        raising=False,
    )
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_args, **_kwargs: ExitedProcess(),
    )
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=data_dir,
        cache_dir=tmp_path / "cache",
    )

    with pytest.raises(RuntimeError, match="exited with status 7"):
        probe.start(connect_timeout=0.01)

    assert probe.managed_pid is None
    assert json.loads(settings.read_text()) == original


def test_managed_probe_accepts_current_qobuz_connected_marker(tmp_path: Any) -> None:
    """Startup readiness must follow the provider's current connection log."""
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
    )
    probe._proc = cast("Any", SimpleNamespace(pid=4321, poll=lambda: None))
    probe.log_path.write_text(
        "INFO [music_assistant.Qobuz Connect] Qobuz Connect WebSocket connected\n"
    )

    probe._wait_until_ready(0.01)


def test_managed_probe_rejects_occupied_ports_before_pinning(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An incumbent listener must abort before copied provider state changes."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    settings = data_dir / "settings.json"
    original = {
        "encryption_key": "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
        "providers": {
            "qobuz--native": {"domain": "qobuz", "values": {}},
            "qobuz_connect--test": {
                "domain": "qobuz_connect",
                "values": {"target_player": "original-player"},
            },
        },
    }
    settings.write_text(json.dumps(original))
    monkeypatch.setattr(
        ma_probe_module,
        "_listening_port_owners",
        lambda _ports: {8095: 999},
        raising=False,
    )
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("must not launch with occupied ports"),
    )
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=data_dir,
        cache_dir=tmp_path / "cache",
    )

    with pytest.raises(RuntimeError, match="occupied"):
        probe.start()

    assert json.loads(settings.read_text()) == original


def test_managed_probe_requires_live_child_to_own_both_ports(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Process identity is valid only while its two expected listeners exist."""
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
    )
    probe._proc = cast("Any", SimpleNamespace(pid=4321, poll=lambda: None))
    owned_ports = {8095: 4321, 8695: 4321}
    monkeypatch.setattr(
        ma_probe_module,
        "_process_listening_port_owners",
        lambda _pid, _ports: owned_ports,
    )

    assert probe.owns_managed_ports()

    owned_ports.pop(8695)

    assert not probe.owns_managed_ports()


def test_managed_probe_removes_disposable_copy_when_restore_fails(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restore error must not leave copied credential-bearing state behind."""
    run_dir = tempfile.TemporaryDirectory(dir=tmp_path)
    run_root = Path(run_dir.name)
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=run_root / "data",
        cache_dir=run_root / "cache",
        run_dir=run_dir,
    )

    def fail_restore() -> None:
        raise RuntimeError("restore failed")

    monkeypatch.setattr(probe, "_restore_target", fail_restore)

    with pytest.raises(RuntimeError, match="restore failed"):
        probe.stop()

    assert not run_root.exists()


def test_managed_probe_reaps_process_after_forced_kill(tmp_path: Any) -> None:
    """A child ignoring graceful termination must be reaped after forced kill."""

    class HungProcess:
        """Minimal process that exits only after a forced kill."""

        pid = 4321

        def __init__(self) -> None:
            self.killed = False
            self.reaped = False

        def poll(self) -> None:
            """Report that the child is still running."""

        def terminate(self) -> None:
            """Ignore graceful termination."""

        def wait(self, timeout: float) -> int:
            """Time out until killed, then reap the child."""
            assert timeout == 20
            if not self.killed:
                raise subprocess.TimeoutExpired(cmd="managed-ma", timeout=timeout)
            self.reaped = True
            return -9

        def kill(self) -> None:
            """Make the child available for reaping."""
            self.killed = True

    process = HungProcess()
    probe = MAProbe(
        log_path=tmp_path / "ma.log",
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
    )
    probe._proc = cast("Any", process)

    probe.stop()

    assert process.reaped
    assert probe.managed_pid is None


def test_default_probe_uses_disposable_data_and_cache_copies(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Managed runs must never start MA on the ignored seed directories."""
    source_data = tmp_path / ".mass-data"
    source_cache = tmp_path / ".mass-cache"
    source_data.mkdir()
    source_cache.mkdir()
    (source_data / "seed").write_text("data")
    (source_cache / "seed").write_text("cache")
    monkeypatch.chdir(tmp_path)

    probe = default_probe()

    assert probe.data_dir != source_data
    assert probe.cache_dir != source_cache
    assert (probe.data_dir / "seed").read_text() == "data"
    assert (probe.cache_dir / "seed").read_text() == "cache"
    run_root = probe.data_dir.parent

    probe.stop()

    assert not run_root.exists()


async def test_preflight_rejects_attached_process_without_identity() -> None:
    """An unmanaged or unidentified MA process cannot be used for playback."""
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage(("Local Dev Hardening abc123",), "Web Player Chrome"),
        ma_query_func=_query([_provider(BLACKHOLE_PLAYER_ID)], [_player()]),
        connect_target="Local Dev Hardening abc123",
        managed_pid=None,
    )

    result = await preflight.run()

    assert not result.passed
    assert any("managed MA process" in failure for failure in result.failures)


async def test_preflight_rejects_managed_process_without_port_ownership() -> None:
    """A stale child identity cannot authorize playback through another server."""
    preflight = SafetyPreflight(
        qobuz=FakeQobuzPage(("Local Dev Hardening abc123",), "Web Player Chrome"),
        ma_query_func=_query([_provider(BLACKHOLE_PLAYER_ID)], [_player()]),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
        managed_ports_owned_func=lambda: False,
    )

    result = await preflight.run()

    assert not result.passed
    assert any("does not own harness ports" in failure for failure in result.failures)


async def test_reset_cannot_reach_playback_when_preflight_fails() -> None:
    """The integration session must guard the action, not merely expose a helper."""
    qobuz = FakeQobuzPage(("Local Dev Hardening abc123",), "Landwarekan")
    preflight = SafetyPreflight(
        qobuz=qobuz,
        ma_query_func=_query([_provider("wrong-player")], [_player()]),
        connect_target="Local Dev Hardening abc123",
        managed_pid=1234,
    )
    session = IntegrationSession(
        client=cast("Any", SimpleNamespace(qobuz=qobuz)),
        ma=cast("Any", SimpleNamespace()),
        preflight=preflight,
    )

    with pytest.raises(RuntimeError, match="safety preflight failed"):
        await session.reset_to_clean_state()

    assert qobuz.played_urls == []


def test_skipped_scenario_is_not_a_pass() -> None:
    """A missing required live prerequisite must not produce a green run."""
    result = ScenarioResult(scenario="required")

    result.skip("no token")

    assert result.status is ScenarioStatus.SKIP
    assert not result.passed


def test_websocket_queue_reconstruction_preserves_pairs_after_reorder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The authoritative cloud-queue oracle keeps slot/track pairs through reorder."""

    def track(slot: int, qid: int) -> SimpleNamespace:
        return SimpleNamespace(queueItemId=slot, trackId=qid)

    loaded = SimpleNamespace(
        messageType=QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED,
        srvrCtrlQueueTracksLoaded=SimpleNamespace(
            tracks=[track(1, 101), track(2, 102), track(3, 103)]
        ),
    )
    reordered = SimpleNamespace(
        messageType=QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_REORDERED,
        srvrCtrlQueueTracksReordered=SimpleNamespace(queueItemIds=[3], insertAfter=0),
    )
    recorder = WsRecorder(cast("Any", None))
    monkeypatch.setattr(recorder, "_incoming_messages", lambda: [loaded, reordered])

    assert recorder.cloud_queue_track_ids() == ("103", "101", "102")


async def test_qobuz_page_reads_exact_track_ids_from_player_state() -> None:
    """Browser assertions use Qobuz IDs, not ambiguous localized titles."""
    page = SimpleNamespace(
        evaluate=AsyncResult(
            {
                "currentIndex": 1,
                "trackIds": [1065476, 1065477, 1065477],
                "queueVersion": "12.4",
            }
        )
    )
    qobuz = QobuzPage(cast("Any", page))

    assert await qobuz.current_track_id() == "1065477"
    assert await qobuz.queue_track_ids() == ("1065476", "1065477", "1065477")


async def test_qobuz_page_reloads_before_clicking_missing_play_control() -> None:
    """A missing player toggle is recovered without risking a duplicate click."""

    class Toggle:
        """Player toggle that appears only after the page reloads."""

        def __init__(self) -> None:
            self.visible = False
            self.clicks = 0

        @property
        def first(self) -> Toggle:
            """Return the first matching fake locator."""
            return self

        async def wait_for(self, **_kwargs: object) -> None:
            """Fail until navigation rebuilds the player controls."""
            if not self.visible:
                raise RuntimeError("player control missing")

        async def click(self, **_kwargs: object) -> None:
            """Record the transport action."""
            if not self.visible:
                raise RuntimeError("player control missing")
            self.clicks += 1

    toggle = Toggle()
    navigations = 0

    async def goto(_url: str, *, wait_until: str) -> None:
        """Make the transport control visible after one reload."""
        nonlocal navigations
        assert wait_until == "domcontentloaded"
        navigations += 1
        toggle.visible = True

    page = SimpleNamespace(
        locator=lambda _selector: toggle,
        goto=goto,
    )
    qobuz = QobuzPage(cast("Any", page))

    await qobuz.play()

    assert navigations == 1
    assert toggle.clicks == 1


async def test_qobuz_page_transport_sends_explicit_controller_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pause/resume must not depend on stale or absent player-state buttons."""
    qobuz = QobuzPage(cast("Any", SimpleNamespace()))
    frames: list[bytes] = []

    async def record_frame(frame: bytes) -> None:
        """Record the frame sent through the authenticated browser socket."""
        frames.append(frame)

    monkeypatch.setattr(qobuz, "send_qconnect_frame", record_frame)

    await qobuz.pause()
    await qobuz.resume()

    assert len(frames) == 2
    codec = QobuzConnectCodec(b"\0" * 16)
    states: list[int] = []
    for frame in frames:
        outer = codec.decode_frame(frame)
        assert outer is not None
        assert outer.payload is not None
        batch = codec.decode_qconnect_batch(outer.payload)
        assert batch is not None
        assert len(batch.messages) == 1
        message = batch.messages[0]
        assert message.messageType == QConnectMessageType.CTRL_SRVR_SET_PLAYER_STATE
        states.append(message.ctrlSrvrSetPlayerState.playingState)
    assert states == [int(PlayingState.PAUSED), int(PlayingState.PLAYING)]


class AsyncResult:
    """Callable returning one awaitable fixture value."""

    def __init__(self, value: object) -> None:
        """Store the result value."""
        self.value = value

    async def __call__(self, _expression: str) -> object:
        """Return the stored result."""
        return self.value
