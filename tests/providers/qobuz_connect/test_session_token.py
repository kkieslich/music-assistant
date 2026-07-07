"""Tests for Qobuz Connect websocket token self-refresh."""

from __future__ import annotations

import asyncio
import dataclasses
import time
import uuid

from music_assistant.providers.qobuz_connect.models import DeviceConfig, JWTConnectToken
from music_assistant.providers.qobuz_connect.session import (
    TOKEN_REFRESH_BUFFER,
    QobuzConnectSession,
    SessionCallbacks,
)


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
