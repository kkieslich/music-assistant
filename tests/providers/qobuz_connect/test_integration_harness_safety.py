"""Safety tests for the live Qobuz/MA integration harness."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from tests.providers.qobuz_connect.protocol_capture.integration_harness import (
    BLACKHOLE_PLAYER_ID,
    IntegrationSession,
    SafetyPreflight,
)


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
