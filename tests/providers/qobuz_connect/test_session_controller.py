"""Tests for the controller session handshake and dispatcher routing."""

from __future__ import annotations

import uuid
from typing import Any

from music_assistant.providers.qobuz_connect.inbound_dispatcher import InboundDispatcher
from music_assistant.providers.qobuz_connect.models import DeviceConfig, QConnectMessageType
from music_assistant.providers.qobuz_connect.proto import qconnect_payload_pb2 as _payload_pb2
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec
from music_assistant.providers.qobuz_connect.session import QobuzConnectSession, SessionCallbacks
from music_assistant.providers.qobuz_connect.sync_types import (
    CloudActiveRendererChanged,
    CloudAddRenderer,
    Event,
)

payload_pb2: Any = _payload_pb2


def _callbacks(**overrides: Any) -> SessionCallbacks:
    async def _noop(*_args: object, **_kwargs: object) -> None:
        return None

    values: dict[str, Any] = {
        "submit": _noop,
        "on_set_active": _noop,
        "on_quality": _noop,
        "on_disconnected": _noop,
    }
    values.update(overrides)
    return SessionCallbacks(**values)


def _device() -> DeviceConfig:
    return DeviceConfig(
        name="MA", uuid=str(uuid.uuid4()), http_port=8695, bind_address="0.0.0.0", max_quality=27
    )


def test_session_constructs_without_role() -> None:
    """A session is always controller now — it constructs with no role argument."""
    session = QobuzConnectSession(_device(), _callbacks())
    assert session is not None


async def test_dispatcher_submits_add_renderer_event() -> None:
    """An ADD_RENDERER broadcast is parsed into a CloudAddRenderer and submitted."""
    received: list[Event] = []

    async def submit(event: Event) -> None:
        received.append(event)

    callbacks = _callbacks(submit=submit)
    dispatcher = InboundDispatcher(QobuzConnectCodec(uuid.uuid4().bytes), callbacks)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_ADD_RENDERER
    msg.srvrCtrlAddRenderer.rendererId = 3
    msg.srvrCtrlAddRenderer.renderer.deviceUuid = b"\x01" * 16
    await dispatcher.dispatch(msg)
    assert len(received) == 1
    assert isinstance(received[0], CloudAddRenderer)
    assert received[0].renderer_id == 3


async def test_dispatcher_submits_active_renderer_changed_event() -> None:
    """An ACTIVE_RENDERER_CHANGED broadcast is parsed into an event and submitted."""
    received: list[Event] = []

    async def submit(event: Event) -> None:
        received.append(event)

    callbacks = _callbacks(submit=submit)
    dispatcher = InboundDispatcher(QobuzConnectCodec(uuid.uuid4().bytes), callbacks)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_ACTIVE_RENDERER_CHANGED
    msg.srvrCtrlActiveRendererChanged.rendererId = 4
    await dispatcher.dispatch(msg)
    assert len(received) == 1
    assert isinstance(received[0], CloudActiveRendererChanged)
    assert received[0].renderer_id == 4


async def test_dispatch_contains_handler_exceptions() -> None:
    """
    A raising callback must not propagate out of dispatch().

    dispatch() runs inline in the session's receive loop; before containment
    any translator/reducer bug recycled the entire websocket, turning a
    single bad message into reconnect churn.
    """
    codec = QobuzConnectCodec(b"\x01" * 16)

    async def _boom(_active: bool) -> None:
        raise RuntimeError("handler bug")

    callbacks = _callbacks(on_set_active=_boom)
    dispatcher = InboundDispatcher(codec, callbacks)
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_RNDR_SET_ACTIVE
    msg.srvrRndrSetActive.active = True

    await dispatcher.dispatch(msg)  # must not raise
