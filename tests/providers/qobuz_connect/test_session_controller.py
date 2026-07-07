"""Tests for the controller-role session handshake and dispatcher routing."""

from __future__ import annotations

import dataclasses
import uuid
from typing import Any

from music_assistant.providers.qobuz_connect.inbound_dispatcher import InboundDispatcher
from music_assistant.providers.qobuz_connect.models import (
    DeviceConfig,
    QConnectMessageType,
    RendererRecord,
    SessionRole,
)
from music_assistant.providers.qobuz_connect.proto import qconnect_payload_pb2 as _payload_pb2
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec
from music_assistant.providers.qobuz_connect.session import QobuzConnectSession, SessionCallbacks

payload_pb2: Any = _payload_pb2


def _callbacks(**overrides: Any) -> SessionCallbacks:
    async def _noop(*_args: object, **_kwargs: object) -> None:
        return None

    values: dict[str, Any] = {f.name: _noop for f in dataclasses.fields(SessionCallbacks)}
    values.update(overrides)
    return SessionCallbacks(**values)


def _device() -> DeviceConfig:
    return DeviceConfig(
        name="MA", uuid=str(uuid.uuid4()), http_port=8695, bind_address="0.0.0.0", max_quality=27
    )


def test_session_role_defaults_to_renderer() -> None:
    """A session constructed without ``role`` behaves as a renderer."""
    session = QobuzConnectSession(_device(), _callbacks())
    assert session.role is SessionRole.RENDERER


def test_controller_session_role_stored() -> None:
    """Passing ``role=SessionRole.CONTROLLER`` is stored on the session."""
    session = QobuzConnectSession(_device(), _callbacks(), role=SessionRole.CONTROLLER)
    assert session.role is SessionRole.CONTROLLER


async def test_dispatcher_routes_add_renderer_to_callback() -> None:
    """An ADD_RENDERER broadcast is parsed and forwarded when a callback is set."""
    received: list[RendererRecord] = []

    async def on_add(record: RendererRecord) -> None:
        received.append(record)

    callbacks = _callbacks(on_add_renderer=on_add)
    dispatcher = InboundDispatcher(QobuzConnectCodec(uuid.uuid4().bytes), callbacks)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_ADD_RENDERER
    msg.srvrCtrlAddRenderer.rendererId = 3
    msg.srvrCtrlAddRenderer.renderer.deviceUuid = b"\x01" * 16
    await dispatcher.dispatch(msg)
    assert received
    assert received[0].renderer_id == 3


async def test_dispatcher_ignores_add_renderer_without_callback() -> None:
    """A None callback (renderer-role session) keeps the old ignore behavior."""
    callbacks = _callbacks(on_add_renderer=None)
    dispatcher = InboundDispatcher(QobuzConnectCodec(uuid.uuid4().bytes), callbacks)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_ADD_RENDERER
    msg.srvrCtrlAddRenderer.rendererId = 3
    await dispatcher.dispatch(msg)  # must not raise


async def test_dispatcher_routes_active_renderer_changed() -> None:
    """An ACTIVE_RENDERER_CHANGED broadcast is parsed and forwarded to the callback."""
    received: list[int] = []

    async def on_changed(renderer_id: int) -> None:
        received.append(renderer_id)

    callbacks = _callbacks(on_active_renderer_changed=on_changed)
    dispatcher = InboundDispatcher(QobuzConnectCodec(uuid.uuid4().bytes), callbacks)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_ACTIVE_RENDERER_CHANGED
    msg.srvrCtrlActiveRendererChanged.rendererId = 4
    await dispatcher.dispatch(msg)
    assert received == [4]
