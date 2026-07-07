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
