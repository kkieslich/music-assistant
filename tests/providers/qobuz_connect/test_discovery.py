"""Lifecycle tests for Qobuz Connect local discovery."""

from __future__ import annotations

import socket
from unittest.mock import AsyncMock

import pytest

from music_assistant.providers.qobuz_connect.discovery import QobuzConnectDiscovery
from music_assistant.providers.qobuz_connect.models import DeviceConfig


def _discovery() -> QobuzConnectDiscovery:
    """Build discovery bound to an ephemeral local TCP port."""
    return QobuzConnectDiscovery(
        device=DeviceConfig(
            name="Test receiver",
            uuid="00000000-0000-0000-0000-000000000001",
            http_port=0,
            bind_address="127.0.0.1",
            max_quality=27,
        ),
        on_connect=lambda _tokens: None,
        quality_getter=lambda: 27,
    )


async def test_start_rolls_back_tcp_listener_when_mdns_registration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both primary and fallback mDNS failure leave no HTTP runtime behind."""
    discovery = _discovery()
    monkeypatch.setattr(
        discovery,
        "_register_mdns",
        AsyncMock(side_effect=RuntimeError("primary and fallback failed")),
    )

    with pytest.raises(RuntimeError, match="primary and fallback failed"):
        await discovery.start()

    assert discovery._app is None
    assert discovery._runner is None
    assert discovery._site is None
    assert discovery._zeroconf is None
    assert discovery._service_info is None


async def test_start_rolls_back_runner_when_http_port_is_occupied() -> None:
    """An occupied port leaves no runner, listener, or mDNS state."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        port = occupied.getsockname()[1]
        discovery = _discovery()
        discovery.device.http_port = port

        with pytest.raises(OSError, match="in use"):
            await discovery.start()

    assert discovery._app is None
    assert discovery._runner is None
    assert discovery._site is None
    assert discovery._zeroconf is None
    assert discovery._service_info is None


async def test_stop_cleans_tcp_runtime_even_when_mdns_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One cleanup error cannot skip the listener and runner owners."""
    discovery = _discovery()
    site = AsyncMock()
    runner = AsyncMock()
    discovery._app = object()  # type: ignore[assignment]
    discovery._site = site
    discovery._runner = runner
    monkeypatch.setattr(
        discovery,
        "_unregister_mdns",
        AsyncMock(side_effect=RuntimeError("unregister failed")),
    )

    with pytest.raises(RuntimeError, match="unregister failed"):
        await discovery.stop()

    site.stop.assert_awaited_once()
    runner.cleanup.assert_awaited_once()
    assert all(value is None for value in (discovery._app, discovery._runner, discovery._site))
