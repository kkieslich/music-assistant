"""Tests for controller-role additions to the qobuz_connect models."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import (
    QConnectMessageType,
    RendererRecord,
)


def test_controller_message_type_values_match_proto() -> None:
    """New enum members carry the wire ids from qconnect_payload.proto."""
    assert QConnectMessageType.CTRL_SRVR_JOIN_SESSION == 61  # type: ignore[comparison-overlap]
    assert QConnectMessageType.CTRL_SRVR_SET_ACTIVE_RENDERER == 63  # type: ignore[comparison-overlap]
    assert QConnectMessageType.CTRL_SRVR_SET_VOLUME == 64  # type: ignore[comparison-overlap]
    assert QConnectMessageType.CTRL_SRVR_MUTE_VOLUME == 73  # type: ignore[comparison-overlap]
    assert QConnectMessageType.SRVR_CTRL_RENDERER_STATE_UPDATED == 82  # type: ignore[comparison-overlap]
    assert QConnectMessageType.SRVR_CTRL_ADD_RENDERER == 83  # type: ignore[comparison-overlap]
    assert QConnectMessageType.SRVR_CTRL_UPDATE_RENDERER == 84  # type: ignore[comparison-overlap]
    assert QConnectMessageType.SRVR_CTRL_REMOVE_RENDERER == 85  # type: ignore[comparison-overlap]
    assert QConnectMessageType.SRVR_CTRL_ACTIVE_RENDERER_CHANGED == 86  # type: ignore[comparison-overlap]


def test_renderer_record_defaults() -> None:
    """RendererRecord holds id + uuid; friendly name is optional."""
    record = RendererRecord(renderer_id=3, device_uuid=b"\x01" * 16)
    assert record.renderer_id == 3
    assert record.device_uuid == b"\x01" * 16
    assert record.friendly_name == ""
