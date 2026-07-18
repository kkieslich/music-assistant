"""Tests for Qobuz Connect websocket token self-refresh."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
import uuid
from typing import TYPE_CHECKING

import music_assistant.providers.qobuz_connect.session as session_module
from music_assistant.providers.qobuz_connect.models import (
    ConnectTokens,
    DeviceConfig,
    JWTConnectToken,
)
from music_assistant.providers.qobuz_connect.session import (
    TOKEN_REFRESH_BUFFER,
    QobuzConnectSession,
    SessionCallbacks,
)

if TYPE_CHECKING:
    import pytest


def _callbacks() -> SessionCallbacks:
    async def _noop(*_args: object, **_kwargs: object) -> None:
        return None

    return SessionCallbacks(**{f.name: _noop for f in dataclasses.fields(SessionCallbacks)})


def _device() -> DeviceConfig:
    return DeviceConfig(
        name="MA", uuid=str(uuid.uuid4()), http_port=8695, bind_address="0.0.0.0", max_quality=27
    )


async def test_wait_for_valid_token_self_refreshes_expiring_token() -> None:
    """An expiring token triggers the refresher and the fresh token is adopted."""
    fresh = JWTConnectToken(jwt="new", exp=int(time.time()) + 3600, endpoint="wss://e")
    calls: list[int] = []

    async def refresher() -> JWTConnectToken:
        calls.append(1)
        return fresh

    session = QobuzConnectSession(_device(), _callbacks(), token_refresher=refresher)
    session._should_run = True
    # Token within the refresh buffer window counts as expiring.
    session._ws_token = JWTConnectToken(jwt="old", exp=int(time.time()) + 5, endpoint="wss://e")

    ok = await asyncio.wait_for(session._wait_for_valid_token(TOKEN_REFRESH_BUFFER), timeout=2)

    assert ok is True
    assert session._ws_token is fresh
    assert len(calls) == 1


async def test_wait_for_valid_token_skips_refresh_when_token_valid() -> None:
    """A comfortably valid token returns immediately without calling the refresher."""
    calls: list[int] = []

    async def refresher() -> JWTConnectToken | None:
        calls.append(1)
        return None

    session = QobuzConnectSession(_device(), _callbacks(), token_refresher=refresher)
    session._should_run = True
    session._ws_token = JWTConnectToken(jwt="ok", exp=int(time.time()) + 3600, endpoint="wss://e")

    ok = await asyncio.wait_for(session._wait_for_valid_token(TOKEN_REFRESH_BUFFER), timeout=2)

    assert ok is True
    assert calls == []


async def test_refresh_returning_near_expiry_token_backs_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A "successful" refresh that mints an already-expiring token must not spin.

    The retry delay used to apply only when the refresher returned None; a
    token that was valid-shaped but within the expiry buffer (short-lived
    mint, or homelab clock ahead of Qobuz) made the loop call createToken
    back-to-back with zero delay — hammering the API for as long as the
    condition lasted.
    """
    monkeypatch.setattr(session_module, "TOKEN_REFRESH_RETRY_DELAY", 0.05)
    calls: list[int] = []

    async def refresher() -> JWTConnectToken:
        calls.append(1)
        # Yield so wait_for can cancel us — the unfixed loop has no other
        # await point at all.
        await asyncio.sleep(0)
        return JWTConnectToken(jwt="short", exp=int(time.time()) + 5, endpoint="wss://e")

    session = QobuzConnectSession(_device(), _callbacks(), token_refresher=refresher)
    session._should_run = True

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(session._wait_for_valid_token(TOKEN_REFRESH_BUFFER), timeout=0.3)

    assert len(calls) <= 8, f"refresher hot-looped: {len(calls)} calls in 0.3s"


async def test_handshake_tokens_during_slow_refresh_are_picked_up_promptly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    set_tokens() landing while a self-refresh is in flight must not be lost.

    The update event used to be cleared AFTER the refresher call, wiping the
    handshake's wakeup; the fresh token then sat unnoticed for a full retry
    delay while the device looked dead in the app.
    """
    monkeypatch.setattr(session_module, "TOKEN_REFRESH_RETRY_DELAY", 5.0)

    async def slow_failing_refresher() -> JWTConnectToken | None:
        await asyncio.sleep(0.05)
        return None

    session = QobuzConnectSession(_device(), _callbacks(), token_refresher=slow_failing_refresher)
    session._should_run = True

    async def handshake_arrives() -> None:
        await asyncio.sleep(0.01)
        session.set_tokens(
            ConnectTokens(
                session_id=str(uuid.uuid4()),
                ws_token=JWTConnectToken(
                    jwt="fresh", exp=int(time.time()) + 3600, endpoint="wss://e"
                ),
            )
        )

    handshake = asyncio.get_running_loop().create_task(handshake_arrives())
    ok = await asyncio.wait_for(session._wait_for_valid_token(TOKEN_REFRESH_BUFFER), timeout=1.0)
    await handshake

    assert ok is True


async def test_set_tokens_cancels_previous_close_task() -> None:
    """Back-to-back handshakes must not orphan the previous close task."""

    class _StuckWs:
        async def close(self) -> None:
            await asyncio.Event().wait()

    session = QobuzConnectSession(_device(), _callbacks())
    session._should_run = True
    session._ws = _StuckWs()  # type: ignore[assignment]

    def _tokens(jwt: str) -> ConnectTokens:
        return ConnectTokens(
            session_id=str(uuid.uuid4()),
            ws_token=JWTConnectToken(jwt=jwt, exp=int(time.time()) + 3600, endpoint="wss://e"),
        )

    session.set_tokens(_tokens("one"))
    first = session._token_refresh_close_task
    assert first is not None
    await asyncio.sleep(0)
    session.set_tokens(_tokens("two"))
    await asyncio.sleep(0)
    assert first.cancelled() or first.done()
    # Drop the stuck fake socket so stop() doesn't await its blocked close().
    session._ws = None
    await session.stop()
