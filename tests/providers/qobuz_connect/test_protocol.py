"""Tests for the provider-local Qobuz Connect protocol codec."""

from __future__ import annotations

import uuid
from typing import Any

from music_assistant.providers.qobuz_connect import _normalize_quality_id
from music_assistant.providers.qobuz_connect.models import (
    BufferState,
    LoopMode,
    OuterMessageType,
    PlayingState,
    QConnectMessageType,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.proto import qconnect_payload_pb2 as _payload_pb2
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec

payload_pb2: Any = _payload_pb2

DEVICE_UUID = uuid.UUID("11111111-2222-3333-4444-555555555555").bytes
SESSION_UUID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").bytes
ACTION_UUID = uuid.UUID("12345678-1234-5678-1234-567812345678").bytes


def _first_inner_message(frame: bytes) -> Any:
    codec = QobuzConnectCodec(DEVICE_UUID)
    decoded = codec.decode_frame(frame)
    assert decoded is not None
    assert decoded.msg_type == OuterMessageType.PAYLOAD
    assert decoded.payload is not None
    batch = codec.decode_qconnect_batch(decoded.payload)
    assert batch is not None
    assert len(batch.messages) == 1
    return batch.messages[0]


def test_encode_renderer_state_update() -> None:
    """Renderer state reports carry canonical queue/version/position fields."""
    codec = QobuzConnectCodec(DEVICE_UUID)

    msg = _first_inner_message(
        codec.encode_renderer_state(
            playing_state=PlayingState.PLAYING,
            buffer_state=BufferState.BUFFERING,
            position_ms=42_000,
            position_timestamp_ms=1_700_000_000_000,
            duration_ms=180_000,
            queue_item_id=11,
            queue_version=QueueVersion(15, 1),
        )
    )

    assert msg.messageType == QConnectMessageType.RNDR_SRVR_STATE_UPDATED
    state = msg.rndrSrvrStateUpdated.state
    assert state.playingState == PlayingState.PLAYING
    assert state.bufferState == BufferState.BUFFERING
    assert state.currentPosition.value == 42_000
    assert state.currentPosition.timestamp == 1_700_000_000_000
    assert state.duration == 180_000
    assert state.currentQueueItemId == 11
    assert state.queueVersion.major == 15
    assert state.queueVersion.minor == 1


def test_normalize_quality_id_accepts_protocol_and_qobuz_format_ids() -> None:
    """Qobuz Connect uses protocol quality ids while MA Qobuz uses format ids."""
    assert _normalize_quality_id(1) == 5
    assert _normalize_quality_id(2) == 6
    assert _normalize_quality_id(3) == 7
    assert _normalize_quality_id(4) == 27
    assert _normalize_quality_id(5) == 5
    assert _normalize_quality_id(6) == 6
    assert _normalize_quality_id(7) == 7
    assert _normalize_quality_id(27) == 27
    assert _normalize_quality_id(99) is None


def test_encode_controller_queue_load_tracks() -> None:
    """MA-origin track selection is encoded as a controller queue load command."""
    codec = QobuzConnectCodec(DEVICE_UUID)

    msg = _first_inner_message(
        codec.encode_queue_load_tracks(
            action_uuid=ACTION_UUID,
            track_id="376286112",
            queue_version=QueueVersion(3, 2),
            qobuz_reference_id=123456,
            queue_position=4,
        )
    )

    assert msg.messageType == QConnectMessageType.CTRL_SRVR_QUEUE_LOAD_TRACKS
    load = msg.ctrlSrvrQueueLoadTracks
    assert load.actionUuid == ACTION_UUID
    assert not load.HasField("sessionUuid")
    assert load.qobuzReferenceUuid == 123456
    assert load.queueVersion.major == 3
    assert load.queueVersion.minor == 2
    assert load.queuePosition == 4
    assert load.autoplayReset is True


def test_encode_qweb_style_controller_queue_load_tracks() -> None:
    """QWeb sends the selected track id as four little-endian bytes in field 3."""
    codec = QobuzConnectCodec(DEVICE_UUID)

    msg = _first_inner_message(
        codec.encode_queue_load_tracks(
            action_uuid=ACTION_UUID,
            track_id="370969289",
            queue_version=QueueVersion(10, 1),
            context_uuid=SESSION_UUID,
            qweb_track_session=True,
        )
    )

    assert msg.messageType == QConnectMessageType.CTRL_SRVR_QUEUE_LOAD_TRACKS
    load = msg.ctrlSrvrQueueLoadTracks
    assert load.actionUuid == ACTION_UUID
    assert load.sessionUuid == bytes.fromhex("c98a1c16")
    assert load.contextUuid == SESSION_UUID
    assert not load.HasField("qobuzReferenceUuid")
    assert not load.HasField("queuePosition")
    assert load.autoplayReset is True


def test_encode_controller_autoplay_load_tracks() -> None:
    """MA-origin fallback can send exact Qobuz track ids through autoplay loading."""
    codec = QobuzConnectCodec(DEVICE_UUID)

    msg = _first_inner_message(
        codec.encode_autoplay_load_tracks(
            action_uuid=ACTION_UUID,
            track_ids=[376286112],
            queue_version=QueueVersion(3, 2),
            context_uuid=SESSION_UUID,
        )
    )

    assert msg.messageType == QConnectMessageType.CTRL_SRVR_AUTOPLAY_ADD_TRACKS
    load = msg.ctrlSrvrAutoplayLoadTracks
    assert load.actionUuid == ACTION_UUID
    assert list(load.trackIds) == [376286112]
    assert load.queueVersion.major == 3
    assert load.queueVersion.minor == 2
    assert load.contextUuid == SESSION_UUID
    assert load.autoplayReset is True
    assert load.prepend is True
    assert load.append is False


def test_parse_full_set_state() -> None:
    """Full SET_STATE exposes play state, position, current item, and next item."""
    message = payload_pb2.QConnectMessage()
    message.messageType = QConnectMessageType.SRVR_RNDR_SET_STATE
    state = message.srvrRndrSetState
    state.playingState = PlayingState.PLAYING
    state.currentPosition = 12_345
    state.queueVersion.major = 15
    state.queueVersion.minor = 1
    state.currentQueueItem.queueItemId = 11
    state.currentQueueItem.trackId = 376286112
    state.currentQueueItem.contextUuid = SESSION_UUID
    state.nextQueueItem.queueItemId = 12
    state.nextQueueItem.trackId = 402777208

    event = QobuzConnectCodec.parse_set_state(message)

    assert event is not None
    assert event.playing_state == PlayingState.PLAYING
    assert event.position_ms == 12_345
    assert event.queue_version == QueueVersion(15, 1)
    assert event.current_item is not None
    assert event.current_item.queue_item_id == 11
    assert event.current_item.track_id == "376286112"
    assert event.current_item.context_uuid == SESSION_UUID
    assert event.next_item is not None
    assert event.next_item.queue_item_id == 12
    assert event.next_item.track_id == "402777208"


def test_parse_set_state_ignores_empty_next_track_sentinel() -> None:
    """Qobuz uses max uint values to mean there is no next queue item."""
    message = payload_pb2.QConnectMessage()
    message.messageType = QConnectMessageType.SRVR_RNDR_SET_STATE
    state = message.srvrRndrSetState
    state.playingState = PlayingState.PAUSED
    state.currentQueueItem.queueItemId = 0
    state.currentQueueItem.trackId = 370969289
    state.nextQueueItem.queueItemId = 0xFFFFFFFFFFFFFFFF
    state.nextQueueItem.trackId = 0xFFFFFFFF

    event = QobuzConnectCodec.parse_set_state(message)

    assert event is not None
    assert event.current_item is not None
    assert event.current_item.track_id == "370969289"
    assert event.next_item is None


def test_parse_queue_ack() -> None:
    """Queue command responses surface action UUIDs and queue versions."""
    ack_msg = payload_pb2.QConnectMessage()
    ack_msg.messageType = QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED
    ack = ack_msg.srvrCtrlQueueTracksLoaded
    ack.actionUuid = ACTION_UUID
    ack.queueVersion.major = 7
    ack.queueVersion.minor = 3
    ack.queuePosition = 1
    ack.qobuzReferenceUuid = 376286112
    ack.tracks.add(queueItemId=21, trackId=111)
    ack.tracks.add(queueItemId=22, trackId=222)

    parsed_ack = QobuzConnectCodec.parse_queue_load_ack(ack_msg)

    assert parsed_ack is not None
    assert parsed_ack.action_uuid == ACTION_UUID
    assert parsed_ack.queue_version == QueueVersion(7, 3)
    assert parsed_ack.queue_position == 1
    assert parsed_ack.qobuz_reference_id == 376286112
    assert [track.queue_item_id for track in parsed_ack.tracks] == [21, 22]
    assert [track.track_id for track in parsed_ack.tracks] == ["111", "222"]


def test_parse_autoplay_ack() -> None:
    """Autoplay command responses expose explicit loaded track refs."""
    autoplay_ack_msg = payload_pb2.QConnectMessage()
    autoplay_ack_msg.messageType = QConnectMessageType.SRVR_CTRL_AUTOPLAY_TRACKS_LOADED
    autoplay_ack = autoplay_ack_msg.srvrCtrlAutoplayTracksLoaded
    autoplay_ack.actionUuid = ACTION_UUID
    autoplay_ack.queueVersion.major = 8
    autoplay_ack.queueVersion.minor = 1
    autoplay_ack.tracks.add(queueItemId=23, trackId=376286112)

    parsed_autoplay_ack = QobuzConnectCodec.parse_autoplay_load_ack(autoplay_ack_msg)

    assert parsed_autoplay_ack is not None
    assert parsed_autoplay_ack.action_uuid == ACTION_UUID
    assert parsed_autoplay_ack.queue_version == QueueVersion(8, 1)
    assert [track.queue_item_id for track in parsed_autoplay_ack.tracks] == [23]
    assert [track.track_id for track in parsed_autoplay_ack.tracks] == ["376286112"]


def test_parse_queue_state_snapshot() -> None:
    """Full queue-state snapshots surface the canonical track list + flags."""
    state_msg = payload_pb2.QConnectMessage()
    state_msg.messageType = QConnectMessageType.SRVR_CTRL_QUEUE_STATE
    state = state_msg.srvrCtrlQueueState
    state.queueVersion.major = 23
    state.queueVersion.minor = 1
    state.actionUuid = ACTION_UUID
    # Verified shape from capture queue_mutations__client_b frame 5: the
    # snapshot opens with a track whose queueItemId is unset (current track,
    # idx 0) and continues with explicit queueItemIds for the rest.
    state.tracks.add(trackId=1065476, contextUuid=b"ctx-uuid-16-byte")
    state.tracks.add(queueItemId=1, trackId=1065477, contextUuid=b"ctx-uuid-16-byte")
    state.tracks.add(queueItemId=2, trackId=1065478, contextUuid=b"ctx-uuid-16-byte")
    state.shuffleMode = False
    state.autoplayMode = True
    state.autoplayTracks.add(queueItemId=99, trackId=2000001)

    parsed = QobuzConnectCodec.parse_queue_state(state_msg)

    assert parsed is not None
    assert parsed.queue_version == QueueVersion(23, 1)
    assert parsed.action_uuid == ACTION_UUID
    assert [t.queue_item_id for t in parsed.tracks] == [0, 1, 2]
    assert [t.track_id for t in parsed.tracks] == ["1065476", "1065477", "1065478"]
    assert all(t.context_uuid == b"ctx-uuid-16-byte" for t in parsed.tracks)
    assert parsed.shuffle_mode is False
    assert parsed.autoplay_mode is True
    assert [t.track_id for t in parsed.autoplay_tracks] == ["2000001"]


def test_parse_queue_tracks_added_delta() -> None:
    """Tracks-added delta carries the appended refs + a context UUID."""
    msg = payload_pb2.QConnectMessage()
    msg.messageType = QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_ADDED
    evt = msg.srvrCtrlQueueTracksAdded
    evt.queueVersion.major = 24
    evt.queueVersion.minor = 2
    evt.actionUuid = ACTION_UUID
    evt.tracks.add(queueItemId=16, trackId=1065478)
    evt.contextUuid = b"add-context-16-b"

    parsed = QobuzConnectCodec.parse_queue_tracks_added(msg)

    assert parsed is not None
    assert parsed.queue_version == QueueVersion(24, 2)
    assert parsed.action_uuid == ACTION_UUID
    assert [(t.queue_item_id, t.track_id) for t in parsed.tracks] == [(16, "1065478")]
    assert parsed.context_uuid == b"add-context-16-b"


def test_parse_set_loop_mode_maps_proto_enum() -> None:
    """LOOP_MODE deserializes to our locale-stable ``LoopMode`` enum."""
    for proto_value, expected in [
        (1, LoopMode.OFF),
        (2, LoopMode.REPEAT_ONE),
        (3, LoopMode.REPEAT_ALL),
    ]:
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.SRVR_RNDR_SET_LOOP_MODE
        msg.srvrRndrSetLoopMode.mode = proto_value
        assert QobuzConnectCodec.parse_set_loop_mode(msg) == expected


def test_parse_set_shuffle_and_autoplay_mode() -> None:
    """SHUFFLE_MODE / AUTOPLAY_MODE surface the toggled boolean."""
    shuffle_msg = payload_pb2.QConnectMessage()
    shuffle_msg.messageType = QConnectMessageType.SRVR_RNDR_SET_SHUFFLE_MODE
    shuffle_msg.srvrRndrSetShuffleMode.shuffleOn = True

    autoplay_msg = payload_pb2.QConnectMessage()
    autoplay_msg.messageType = QConnectMessageType.SRVR_RNDR_SET_AUTOPLAY_MODE
    autoplay_msg.srvrRndrSetAutoplayMode.autoplayOn = False

    assert QobuzConnectCodec.parse_set_shuffle_mode(shuffle_msg) is True
    assert QobuzConnectCodec.parse_set_autoplay_mode(autoplay_msg) is False


def test_encode_volume_muted_round_trip() -> None:
    """Mute reports encode as a one-bool payload the cloud can re-decode."""
    codec = QobuzConnectCodec(DEVICE_UUID)

    for muted in (True, False):
        msg = _first_inner_message(codec.encode_volume_muted(muted))
        assert msg.messageType == QConnectMessageType.RNDR_SRVR_VOLUME_MUTED
        assert msg.rndrSrvrVolumeMuted.value is muted


def test_parse_queue_error_and_version_change() -> None:
    """Queue errors and version-change messages expose their typed fields."""
    error_msg = payload_pb2.QConnectMessage()
    error_msg.messageType = QConnectMessageType.SRVR_CTRL_QUEUE_ERROR_MESSAGE
    error = error_msg.srvrCtrlQueueErrorMessage
    error.actionUuid = ACTION_UUID
    error.queueVersion.major = 8
    error.queueVersion.minor = 0
    error.error.code = "NOPE"
    error.error.message = "Rejected"

    parsed_error = QobuzConnectCodec.parse_queue_error(error_msg)

    assert parsed_error is not None
    assert parsed_error.action_uuid == ACTION_UUID
    assert parsed_error.queue_version == QueueVersion(8, 0)
    assert parsed_error.code == "NOPE"
    assert parsed_error.message == "Rejected"

    version_msg = payload_pb2.QConnectMessage()
    version_msg.messageType = QConnectMessageType.SRVR_CTRL_QUEUE_VERSION_CHANGED
    version_msg.srvrCtrlQueueVersionChanged.queueVersion.major = 9
    version_msg.srvrCtrlQueueVersionChanged.queueVersion.minor = 4

    assert QobuzConnectCodec.parse_queue_version_changed(version_msg) == QueueVersion(9, 4)
