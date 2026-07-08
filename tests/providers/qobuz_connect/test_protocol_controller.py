"""Tests for the controller-role codec additions."""

from __future__ import annotations

import uuid
from typing import Any

from music_assistant.providers.qobuz_connect.models import (
    PlayingState,
    QConnectMessageType,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.proto import qconnect_envelope_pb2 as _envelope_pb2
from music_assistant.providers.qobuz_connect.proto import qconnect_payload_pb2 as _payload_pb2
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec

payload_pb2: Any = _payload_pb2
envelope_pb2: Any = _envelope_pb2

DEVICE_UUID = uuid.UUID("11111111-2222-3333-4444-555555555555").bytes
ACTION_UUID = uuid.UUID("12345678-1234-5678-1234-567812345678").bytes


def _codec() -> QobuzConnectCodec:
    return QobuzConnectCodec(DEVICE_UUID)


def _strip_outer(frame: bytes) -> bytes:
    """Return the payload bytes of a single outer frame (type byte + varint length)."""
    length = 0
    shift = 0
    pos = 1
    while True:
        byte = frame[pos]
        length |= (byte & 0x7F) << shift
        pos += 1
        if not byte & 0x80:
            break
        shift += 7
    return frame[pos : pos + length]


def _first_inner_message(frame: bytes) -> Any:
    payload_msg = envelope_pb2.Payload()
    payload_msg.ParseFromString(_strip_outer(frame))
    batch = payload_pb2.QConnectBatch()
    batch.ParseFromString(payload_msg.payload)
    return batch.messages[0]


def test_encode_subscribe_without_session_uuid_has_empty_channels() -> None:
    """Controller subscribe: proto=1, no channels."""
    frame = _codec().encode_subscribe(None)
    sub = envelope_pb2.Subscribe()
    sub.ParseFromString(_strip_outer(frame))
    assert sub.proto == 1
    assert list(sub.channels) == []


def test_encode_subscribe_with_session_uuid_keeps_channel() -> None:
    """Renderer subscribe still carries the session uuid channel."""
    session_uuid = uuid.uuid4().bytes
    frame = _codec().encode_subscribe(session_uuid)
    sub = envelope_pb2.Subscribe()
    sub.ParseFromString(_strip_outer(frame))
    assert list(sub.channels) == [session_uuid]


def test_encode_ctrl_join_session_carries_device_info() -> None:
    """Controller join: deviceInfo only, no sessionUuid, SPEAKER type."""
    frame = _codec().encode_ctrl_join_session(DEVICE_UUID, "Local Dev", 27)
    inner = _first_inner_message(frame)
    assert inner.messageType == QConnectMessageType.CTRL_SRVR_JOIN_SESSION
    join = inner.ctrlSrvrJoinSession
    assert not join.HasField("sessionUuid")
    assert join.deviceInfo.deviceUuid == DEVICE_UUID
    assert join.deviceInfo.friendlyName == "Local Dev"
    assert join.deviceInfo.capabilities.maxAudioQuality == 4  # protocol value for 27


def test_encode_set_active_renderer() -> None:
    """Controller active-renderer switch carries the target renderer id."""
    frame = _codec().encode_set_active_renderer(3)
    inner = _first_inner_message(frame)
    assert inner.messageType == QConnectMessageType.CTRL_SRVR_SET_ACTIVE_RENDERER
    assert inner.ctrlSrvrSetActiveRenderer.rendererId == 3


def test_encode_ctrl_set_volume_and_mute() -> None:
    """Volume and mute commands carry the target renderer id and value."""
    codec = _codec()
    vol = _first_inner_message(codec.encode_ctrl_set_volume(3, 80))
    assert vol.messageType == QConnectMessageType.CTRL_SRVR_SET_VOLUME
    assert vol.ctrlSrvrSetVolume.rendererId == 3
    assert vol.ctrlSrvrSetVolume.volume == 80
    mute = _first_inner_message(codec.encode_ctrl_mute_volume(3, muted=True))
    assert mute.messageType == QConnectMessageType.CTRL_SRVR_MUTE_VOLUME
    assert mute.ctrlSrvrMuteVolume.rendererId == 3
    assert mute.ctrlSrvrMuteVolume.value is True


def test_encode_ctrl_set_player_state_partial_fields() -> None:
    """Seek variant sets only currentPosition (matches web-client capture)."""
    codec = _codec()
    seek = _first_inner_message(codec.encode_ctrl_set_player_state(position_ms=82000))
    assert seek.messageType == QConnectMessageType.CTRL_SRVR_SET_PLAYER_STATE
    state = seek.ctrlSrvrSetPlayerState
    assert state.currentPosition == 82000
    assert not state.HasField("playingState")
    assert not state.HasField("currentQueueItem")
    play_item = _first_inner_message(
        codec.encode_ctrl_set_player_state(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
            queue_version=QueueVersion(22, 1),
            queue_item_id=1,
        )
    )
    item_state = play_item.ctrlSrvrSetPlayerState
    assert item_state.playingState == int(PlayingState.PLAYING)
    assert item_state.currentQueueItem.id == 1
    assert item_state.currentQueueItem.queueVersion.major == 22


def test_encode_queue_load_tracks_packs_track_ids() -> None:
    """
    Multi-track load matches the reference web-client shape.

    N little-endian uint32 ids packed into wire field 3, plus the fields the
    cloud validates: contextUuid (exactly 16 bytes — omitting it is rejected
    with ERROR_QUEUE_LOAD_TRACKS "byte array must have length 16", observed
    live 2026-07-08) and explicitly-present shufflePivotQueueItemId=0 /
    shuffleMode=False.
    """
    context_uuid = uuid.UUID("99999999-8888-7777-6666-555555555555").bytes
    frame = _codec().encode_queue_load_tracks(
        action_uuid=ACTION_UUID,
        track_id="",
        queue_version=QueueVersion(21, 1),
        track_ids=[3879017, 3879018],
        context_uuid=context_uuid,
    )
    inner = _first_inner_message(frame)
    load = inner.ctrlSrvrQueueLoadTracks
    expected = (3879017).to_bytes(4, "little") + (3879018).to_bytes(4, "little")
    assert load.sessionUuid == expected
    assert load.contextUuid == context_uuid
    assert load.HasField("shufflePivotQueueItemId")
    assert load.shufflePivotQueueItemId == 0
    assert load.HasField("shuffleMode")
    assert load.shuffleMode is False


def test_parse_add_and_remove_renderer_and_active_changed() -> None:
    """Renderer-registry parsers decode add/remove/active-changed events."""
    codec = _codec()
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_ADD_RENDERER
    msg.srvrCtrlAddRenderer.rendererId = 3
    msg.srvrCtrlAddRenderer.renderer.deviceUuid = DEVICE_UUID
    msg.srvrCtrlAddRenderer.renderer.friendlyName = "Local Dev"
    record = codec.parse_add_renderer(msg)
    assert record is not None
    assert (record.renderer_id, record.device_uuid, record.friendly_name) == (
        3,
        DEVICE_UUID,
        "Local Dev",
    )

    removed = payload_pb2.QConnectMessage()
    removed.messageType = QConnectMessageType.SRVR_CTRL_REMOVE_RENDERER
    removed.srvrCtrlRemoveRenderer.rendererId = 3
    assert codec.parse_remove_renderer(removed) == 3

    changed = payload_pb2.QConnectMessage()
    changed.messageType = QConnectMessageType.SRVR_CTRL_ACTIVE_RENDERER_CHANGED
    changed.srvrCtrlActiveRendererChanged.rendererId = 4
    assert codec.parse_active_renderer_changed(changed) == 4
