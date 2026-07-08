"""Tests for renderer rejoin-on-error."""

from __future__ import annotations

import dataclasses
import time
import uuid

from music_assistant.providers.qobuz_connect.models import (
    DeviceConfig,
    JWTConnectToken,
    OuterMessageType,
    SessionRole,
)
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec
from music_assistant.providers.qobuz_connect.session import (
    REJOIN_MIN_INTERVAL,
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


def _session(role: SessionRole = SessionRole.RENDERER) -> tuple[QobuzConnectSession, FakeWebSocket]:
    device = DeviceConfig(
        name="MA", uuid=str(uuid.uuid4()), http_port=8695, bind_address="0.0.0.0", max_quality=27
    )
    session = QobuzConnectSession(device, _callbacks(), role=role)
    session._ws_token = JWTConnectToken(jwt="j", exp=int(time.time()) + 3600, endpoint="wss://e")
    session._session_uuid = uuid.uuid4().bytes
    fake = FakeWebSocket()
    session._ws = fake  # type: ignore[assignment]
    session._is_connected = True
    return session, fake


def _error_frame() -> bytes:
    # Outer ERROR with an empty body is enough to trigger the path.
    codec = QobuzConnectCodec(uuid.uuid4().bytes)
    return codec._pack_frame(OuterMessageType.ERROR, b"")


def _inner_error_frame() -> bytes:
    # PAYLOAD frame whose batch carries a message-level error (messageType 1)
    # — the shape the cloud actually uses when it answers state reports from
    # a deregistered renderer (observed live 2026-07-08).
    from typing import Any  # noqa: PLC0415

    from music_assistant.providers.qobuz_connect.proto import (  # noqa: PLC0415
        qconnect_payload_pb2 as _payload_pb2,
    )

    payload_pb2: Any = _payload_pb2
    codec = QobuzConnectCodec(uuid.uuid4().bytes)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = 1  # MESSAGE_TYPE_ERROR
    msg.error.code = "1"
    msg.error.message = "renderer not registered"
    return codec._encode_batch(msg)


async def test_renderer_rejoins_after_error_frame() -> None:
    """Test that renderer re-joins after receiving an ERROR frame."""
    session, fake = _session()
    await session._handle_message(_error_frame())
    # SUBSCRIBE + JOIN_SESSION were re-sent.
    assert len(fake.sent) == 2


async def test_rejoin_is_rate_limited() -> None:
    """Test that rejoin is rate-limited to prevent ERROR storms."""
    session, fake = _session()
    await session._handle_message(_error_frame())
    await session._handle_message(_error_frame())
    assert len(fake.sent) == 2  # second error within REJOIN_MIN_INTERVAL: no resend
    assert REJOIN_MIN_INTERVAL > 0


async def test_controller_session_does_not_rejoin_as_renderer() -> None:
    """Test that controller sessions do not rejoin on ERROR."""
    session, fake = _session(role=SessionRole.CONTROLLER)
    await session._handle_message(_error_frame())
    assert fake.sent == []


async def test_renderer_rejoins_after_inner_error_message() -> None:
    """A message-level error inside a PAYLOAD batch also triggers the rejoin."""
    session, fake = _session()
    await session._handle_message(_inner_error_frame())
    # SUBSCRIBE + JOIN_SESSION were re-sent.
    assert len(fake.sent) == 2


async def test_inner_error_rejoin_shares_rate_limit_with_outer() -> None:
    """Inner and outer error paths share one rate-limit window."""
    session, fake = _session()
    await session._handle_message(_inner_error_frame())
    await session._handle_message(_error_frame())
    assert len(fake.sent) == 2  # second trigger within REJOIN_MIN_INTERVAL
