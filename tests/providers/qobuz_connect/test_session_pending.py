"""Tests for pending-message queuing across reconnects."""

from __future__ import annotations

import dataclasses
import time
import uuid

import music_assistant.providers.qobuz_connect.session as session_module
from music_assistant.providers.qobuz_connect.models import DeviceConfig, JWTConnectToken
from music_assistant.providers.qobuz_connect.session import (
    PENDING_MAX_AGE,
    QobuzConnectSession,
    SessionCallbacks,
)


class FakeWebSocket:
    """Collects frames sent over the fake connection."""

    def __init__(self) -> None:
        """Initialize fake websocket."""
        self.sent: list[bytes] = []

    async def send(self, data: bytes) -> None:
        """Collect sent frames."""
        self.sent.append(data)


def _callbacks() -> SessionCallbacks:
    async def _noop(*_args: object, **_kwargs: object) -> None:
        return None

    return SessionCallbacks(**{f.name: _noop for f in dataclasses.fields(SessionCallbacks)})


def _session() -> QobuzConnectSession:
    device = DeviceConfig(
        name="MA", uuid=str(uuid.uuid4()), http_port=8695, bind_address="0.0.0.0", max_quality=27
    )
    session = QobuzConnectSession(device, _callbacks())
    session._ws_token = JWTConnectToken(jwt="j", exp=int(time.time()) + 3600, endpoint="wss://e")
    return session


async def test_send_while_disconnected_queues_and_flushes_fresh_frames() -> None:
    """Frames queued during an outage are flushed on reconnect while fresh."""
    session = _session()
    assert await session.send_message(b"frame-1") is False
    fake = FakeWebSocket()
    session._ws = fake  # type: ignore[assignment]
    session._is_connected = True
    await session._flush_pending_messages()
    assert fake.sent == [b"frame-1"]
    assert session._pending_messages == []


async def test_flush_drops_stale_frames() -> None:
    """
    Frames older than PENDING_MAX_AGE are dropped, not flushed.

    The cloud rejects frames whose envelope timestamp is older than ~2s
    ("Message too old ... age: 2100ms", observed live 2026-07-08) and the
    rejection can kill the connection — replaying stale frames after a
    reconnect causes a disconnect loop.
    """
    session = _session()
    await session.send_message(b"stale-frame")
    # Backdate the queued entry beyond the staleness window.
    ts, data = session._pending_messages[0]
    session._pending_messages[0] = (ts - PENDING_MAX_AGE - 1.0, data)
    await session.send_message(b"fresh-frame")

    fake = FakeWebSocket()
    session._ws = fake  # type: ignore[assignment]
    session._is_connected = True
    await session._flush_pending_messages()

    assert fake.sent == [b"fresh-frame"]
    assert session._pending_messages == []


async def test_pending_queue_is_bounded_while_disconnected() -> None:
    """
    An extended outage must not grow the pending-frame queue without limit.

    Only the last ~2s of frames survive the reconnect flush anyway; keeping
    every heartbeat frame of a multi-hour WAN outage was pure memory growth.
    """
    session = _session()
    for i in range(session_module.MAX_PENDING_MESSAGES + 25):
        await session.send_message(bytes([i % 251]))
    assert len(session._pending_messages) == session_module.MAX_PENDING_MESSAGES
    # Newest frames are the ones kept.
    assert session._pending_messages[-1][1] == bytes(
        [(session_module.MAX_PENDING_MESSAGES + 24) % 251]
    )
