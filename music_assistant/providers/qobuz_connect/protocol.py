"""Qobuz Connect websocket frame and protobuf helpers."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from .models import (
    QUALITY_AUDIO_PROPERTIES,
    QUALITY_TO_PROTOCOL,
    BufferState,
    OuterMessageType,
    PlayingState,
    QConnectMessageType,
    QueueError,
    QueueLoadAck,
    QueueTrackRef,
    QueueVersion,
    SetStateEvent,
)
from .proto import qconnect_common_pb2 as _common_pb2
from .proto import qconnect_envelope_pb2 as _envelope_pb2
from .proto import qconnect_payload_pb2 as _payload_pb2
from .proto import qconnect_queue_pb2 as _queue_pb2

common_pb2: Any = _common_pb2
envelope_pb2: Any = _envelope_pb2
payload_pb2: Any = _payload_pb2
queue_pb2: Any = _queue_pb2

LOGGER = logging.getLogger(__name__)

QCONNECT_PROTO = 1


@dataclass(slots=True)
class DecodedFrame:
    """Decoded outer websocket frame."""

    msg_type: OuterMessageType
    msg_id: int = 0
    msg_date: int = 0
    payload: bytes | None = None
    error_code: int = 0
    error_message: str = ""


class QobuzConnectCodec:
    """Encode and decode the Qobuz Connect websocket protocol."""

    def __init__(self, device_uuid: bytes) -> None:
        """Initialize codec."""
        self.device_uuid = device_uuid
        self._msg_counter = 0

    def _next_msg_id(self) -> int:
        self._msg_counter += 1
        return self._msg_counter

    @staticmethod
    def now_ms() -> int:
        """Return current time in milliseconds."""
        return int(time.time() * 1000)

    def encode_authenticate(self, jwt: str) -> bytes:
        """Encode websocket authentication frame."""
        msg = envelope_pb2.Authenticate()
        msg.msgId = self._next_msg_id()
        msg.msgDate = self.now_ms()
        msg.jwt = jwt
        return self._pack_frame(OuterMessageType.AUTHENTICATE, msg.SerializeToString())

    def encode_subscribe(self, session_uuid: bytes) -> bytes:
        """Encode websocket subscribe frame."""
        msg = envelope_pb2.Subscribe()
        msg.msgId = self._next_msg_id()
        msg.msgDate = self.now_ms()
        msg.proto = QCONNECT_PROTO
        msg.channels.append(session_uuid)
        return self._pack_frame(OuterMessageType.SUBSCRIBE, msg.SerializeToString())

    def encode_payload(
        self, inner_payload: bytes, dest_channels: list[bytes] | None = None
    ) -> bytes:
        """Encode a QConnect payload batch into an outer websocket frame."""
        msg = envelope_pb2.Payload()
        msg.msgId = self._next_msg_id()
        msg.msgDate = self.now_ms()
        msg.proto = QCONNECT_PROTO
        msg.src = self.device_uuid
        if dest_channels:
            msg.dests.extend(dest_channels)
        msg.payload = inner_payload
        return self._pack_frame(OuterMessageType.PAYLOAD, msg.SerializeToString())

    def encode_join_session(
        self,
        device_uuid: bytes,
        friendly_name: str,
        session_uuid: bytes,
        max_audio_quality: int,
    ) -> bytes:
        """Encode renderer join-session message."""
        device_info = common_pb2.DeviceInfo()
        device_info.deviceUuid = device_uuid
        device_info.friendlyName = friendly_name
        device_info.brand = "Music Assistant"
        device_info.model = "Qobuz Connect"
        device_info.type = common_pb2.DEVICE_TYPE_SPEAKER
        device_info.softwareVersion = "ma-qobuz-connect"

        caps = common_pb2.DeviceCapabilities()
        caps.minAudioQuality = 1
        caps.maxAudioQuality = QUALITY_TO_PROTOCOL.get(max_audio_quality, 4)
        caps.volumeRemoteControl = 2
        device_info.capabilities.CopyFrom(caps)

        join = payload_pb2.RndrSrvrJoinSession()
        join.sessionUuid = session_uuid
        join.deviceInfo.CopyFrom(device_info)
        join.reason = 1
        join.isActive = True

        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_JOIN_SESSION
        msg.rndrSrvrJoinSession.CopyFrom(join)
        return self._encode_batch(msg)

    def encode_renderer_state(
        self,
        *,
        playing_state: PlayingState,
        position_ms: int,
        position_timestamp_ms: int,
        duration_ms: int,
        queue_item_id: int,
        queue_version: QueueVersion,
    ) -> bytes:
        """Encode renderer state update for the Qobuz app."""
        state = common_pb2.QueueRendererState()
        state.playingState = int(playing_state)
        state.bufferState = int(BufferState.OK)
        state.currentPosition.timestamp = position_timestamp_ms
        state.currentPosition.value = position_ms
        state.duration = duration_ms
        state.currentQueueItemId = queue_item_id
        state.queueVersion.major = queue_version.major
        state.queueVersion.minor = queue_version.minor

        state_updated = payload_pb2.RndrSrvrStateUpdated()
        state_updated.state.CopyFrom(state)

        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_STATE_UPDATED
        msg.rndrSrvrStateUpdated.CopyFrom(state_updated)
        return self._encode_batch(msg)

    def encode_queue_load_tracks(
        self,
        *,
        action_uuid: bytes,
        track_id: str,
        queue_version: QueueVersion,
        qobuz_reference_id: int | None = None,
        queue_position: int = 0,
        autoplay_reset: bool = True,
        context_uuid: bytes | None = None,
        qweb_track_session: bool = False,
    ) -> bytes:
        """Encode controller queue-load command."""
        queue_load = queue_pb2.CtrlSrvrQueueLoadTracks()
        queue_load.queueVersion.major = queue_version.major
        queue_load.queueVersion.minor = queue_version.minor
        queue_load.actionUuid = action_uuid
        if qweb_track_session:
            queue_load.sessionUuid = int(track_id).to_bytes(4, "little", signed=False)
        if qobuz_reference_id is not None:
            queue_load.queuePosition = queue_position
            queue_load.qobuzReferenceUuid = qobuz_reference_id
        queue_load.autoplayReset = autoplay_reset
        if context_uuid:
            queue_load.contextUuid = context_uuid

        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_QUEUE_LOAD_TRACKS
        msg.ctrlSrvrQueueLoadTracks.CopyFrom(queue_load)
        return self._encode_batch(msg)

    def encode_autoplay_load_tracks(
        self,
        *,
        action_uuid: bytes,
        track_ids: list[int],
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
        autoplay_reset: bool = True,
        autoplay_loading: bool = False,
        prepend: bool = True,
        append: bool = False,
    ) -> bytes:
        """Encode controller autoplay load command with explicit Qobuz track ids."""
        autoplay_load = queue_pb2.CtrlSrvrAutoplayLoadTracks()
        autoplay_load.queueVersion.major = queue_version.major
        autoplay_load.queueVersion.minor = queue_version.minor
        autoplay_load.actionUuid = action_uuid
        autoplay_load.trackIds.extend(track_ids)
        autoplay_load.autoplayReset = autoplay_reset
        autoplay_load.autoplayLoading = autoplay_loading
        autoplay_load.prepend = prepend
        autoplay_load.append = append
        if context_uuid:
            autoplay_load.contextUuid = context_uuid

        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_AUTOPLAY_ADD_TRACKS
        msg.ctrlSrvrAutoplayLoadTracks.CopyFrom(autoplay_load)
        return self._encode_batch(msg)

    def encode_player_state(
        self,
        *,
        playing_state: PlayingState,
        position_ms: int,
        queue_version: QueueVersion,
        queue_item_id: int,
    ) -> bytes:
        """Encode controller player-state command."""
        state = payload_pb2.CtrlSrvrSetPlayerState()
        state.playingState = int(playing_state)
        state.currentPosition = position_ms
        state.currentQueueItem.queueVersion.major = queue_version.major
        state.currentQueueItem.queueVersion.minor = queue_version.minor
        state.currentQueueItem.id = queue_item_id

        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_PLAYER_STATE
        msg.ctrlSrvrSetPlayerState.CopyFrom(state)
        return self._encode_batch(msg)

    def encode_volume_changed(self, volume: int) -> bytes:
        """Encode renderer volume-changed update."""
        vol_msg = payload_pb2.RndrSrvrVolumeChanged()
        vol_msg.volume = volume
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_VOLUME_CHANGED
        msg.rndrSrvrVolumeChanged.CopyFrom(vol_msg)
        return self._encode_batch(msg)

    def encode_file_audio_quality_changed(
        self,
        quality: int,
        sampling_rate: int = 0,
        bit_depth: int = 0,
        nb_channels: int = 0,
    ) -> bytes:
        """Encode current file quality update."""
        defaults = QUALITY_AUDIO_PROPERTIES.get(quality, (44100, 16, 2))
        quality_msg = payload_pb2.RndrSrvrFileAudioQualityChanged()
        quality_msg.sampling_rate = sampling_rate or defaults[0]
        quality_msg.bit_depth = bit_depth or defaults[1]
        quality_msg.nb_channels = nb_channels or defaults[2]
        quality_msg.audio_quality = QUALITY_TO_PROTOCOL.get(quality, 4)
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_FILE_AUDIO_QUALITY_CHANGED
        msg.rndrSrvrFileAudioQualityChanged.CopyFrom(quality_msg)
        return self._encode_batch(msg)

    def encode_device_audio_quality_changed(
        self,
        quality: int,
        sampling_rate: int = 0,
        bit_depth: int = 0,
        nb_channels: int = 0,
    ) -> bytes:
        """Encode device quality update."""
        defaults = QUALITY_AUDIO_PROPERTIES.get(quality, (44100, 16, 2))
        quality_msg = payload_pb2.RndrSrvrDeviceAudioQualityChanged()
        quality_msg.sampling_rate = sampling_rate or defaults[0]
        quality_msg.bit_depth = bit_depth or defaults[1]
        quality_msg.nb_channels = nb_channels or defaults[2]
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_DEVICE_AUDIO_QUALITY_CHANGED
        msg.rndrSrvrDeviceAudioQualityChanged.CopyFrom(quality_msg)
        return self._encode_batch(msg)

    def encode_max_audio_quality_changed(self, quality: int, network_type: int = 1) -> bytes:
        """Encode max quality update."""
        quality_msg = payload_pb2.RndrSrvrMaxAudioQualityChanged()
        quality_msg.audio_quality = QUALITY_TO_PROTOCOL.get(quality, 4)
        quality_msg.network_type = network_type
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED
        msg.rndrSrvrMaxAudioQualityChanged.CopyFrom(quality_msg)
        return self._encode_batch(msg)

    def decode_frame(self, data: bytes) -> DecodedFrame | None:
        """Decode an outer websocket frame."""
        if len(data) < 2:
            return None
        try:
            msg_type = OuterMessageType(data[0])
        except ValueError:
            LOGGER.warning("Unknown Qobuz outer message type: %s", data[0])
            return None

        length, offset = self._decode_varint(data, 1)
        if offset < 0:
            return None
        payload = data[offset : offset + length]
        try:
            if msg_type == OuterMessageType.PAYLOAD:
                msg = envelope_pb2.Payload()
                msg.ParseFromString(payload)
                return DecodedFrame(
                    msg_type=msg_type, msg_id=msg.msgId, msg_date=msg.msgDate, payload=msg.payload
                )
            if msg_type == OuterMessageType.ERROR:
                msg = common_pb2.Error()
                msg.ParseFromString(payload)
                return DecodedFrame(
                    msg_type=msg_type, error_code=msg.code, error_message=msg.message
                )
            if msg_type == OuterMessageType.DISCONNECT:
                msg = envelope_pb2.Disconnect()
                msg.ParseFromString(payload)
                return DecodedFrame(msg_type=msg_type, msg_id=msg.msgId, msg_date=msg.msgDate)
        except Exception:
            LOGGER.exception("Failed to decode Qobuz frame")
            return None
        return DecodedFrame(msg_type=msg_type, payload=payload)

    @staticmethod
    def decode_qconnect_batch(payload: bytes) -> Any | None:
        """Decode an inner QConnect batch."""
        batch = payload_pb2.QConnectBatch()
        try:
            batch.ParseFromString(payload)
        except Exception:
            LOGGER.exception("Failed to decode QConnect batch")
            return None
        return batch

    @staticmethod
    def parse_set_state(message: Any) -> SetStateEvent | None:
        """Parse a raw QConnect SET_STATE message."""
        if not message.HasField("srvrRndrSetState"):
            return None
        state = message.srvrRndrSetState
        event = SetStateEvent()
        if state.HasField("playingState"):
            event.playing_state = PlayingState(state.playingState)
        if state.HasField("currentPosition"):
            event.position_ms = state.currentPosition
        if state.HasField("queueVersion"):
            event.queue_version = QueueVersion(state.queueVersion.major, state.queueVersion.minor)
        if state.HasField("currentQueueItem"):
            event.current_item = _parse_track_ref(state.currentQueueItem)
        if state.HasField("nextQueueItem"):
            event.next_item = _parse_track_ref(state.nextQueueItem)
        return event

    @staticmethod
    def parse_queue_load_ack(message: Any) -> QueueLoadAck | None:
        """Parse server queue-load acknowledgement."""
        if not message.HasField("srvrCtrlQueueTracksLoaded"):
            return None
        load = message.srvrCtrlQueueTracksLoaded
        return QueueLoadAck(
            action_uuid=load.actionUuid,
            queue_version=QueueVersion(load.queueVersion.major, load.queueVersion.minor),
            tracks=[
                track_ref
                for track in load.tracks
                if (track_ref := _parse_track_ref(track)) is not None
            ],
            queue_position=load.queuePosition if load.HasField("queuePosition") else 0,
            qobuz_reference_id=(
                load.qobuzReferenceUuid if load.HasField("qobuzReferenceUuid") else None
            ),
        )

    @staticmethod
    def parse_autoplay_load_ack(message: Any) -> QueueLoadAck | None:
        """Parse server autoplay-load acknowledgement."""
        if not message.HasField("srvrCtrlAutoplayTracksLoaded"):
            return None
        load = message.srvrCtrlAutoplayTracksLoaded
        return QueueLoadAck(
            action_uuid=load.actionUuid,
            queue_version=QueueVersion(load.queueVersion.major, load.queueVersion.minor),
            tracks=[
                track_ref
                for track in load.tracks
                if (track_ref := _parse_track_ref(track)) is not None
            ],
        )

    @staticmethod
    def parse_queue_error(message: Any) -> QueueError | None:
        """Parse server queue error."""
        if not message.HasField("srvrCtrlQueueErrorMessage"):
            return None
        error_msg = message.srvrCtrlQueueErrorMessage
        error = error_msg.error
        return QueueError(
            action_uuid=error_msg.actionUuid,
            queue_version=QueueVersion(error_msg.queueVersion.major, error_msg.queueVersion.minor),
            code=error.code,
            message=error.message,
        )

    @staticmethod
    def parse_queue_version_changed(message: Any) -> QueueVersion | None:
        """Parse queue-version-changed message."""
        if not message.HasField("srvrCtrlQueueVersionChanged"):
            return None
        version = message.srvrCtrlQueueVersionChanged.queueVersion
        return QueueVersion(version.major, version.minor)

    def _encode_batch(self, *messages: Any) -> bytes:
        batch = payload_pb2.QConnectBatch()
        batch.messagesTime = self.now_ms()
        batch.messagesId = self._next_msg_id()
        batch.messages.extend(messages)
        return self.encode_payload(batch.SerializeToString())

    @staticmethod
    def _pack_frame(msg_type: OuterMessageType, data: bytes) -> bytes:
        frame = bytearray([msg_type])
        length = len(data)
        while length > 0x7F:
            frame.append((length & 0x7F) | 0x80)
            length >>= 7
        frame.append(length & 0x7F)
        frame.extend(data)
        return bytes(frame)

    @staticmethod
    def _decode_varint(data: bytes, start: int) -> tuple[int, int]:
        value = 0
        shift = 0
        offset = start
        while offset < len(data):
            byte = data[offset]
            value |= (byte & 0x7F) << shift
            offset += 1
            if not byte & 0x80:
                return value, offset
            shift += 7
        return -1, -1


def _parse_track_ref(track_ref: Any) -> QueueTrackRef | None:
    if track_ref.queueItemId == 0xFFFFFFFFFFFFFFFF or track_ref.trackId == 0xFFFFFFFF:
        return None
    return QueueTrackRef(
        queue_item_id=track_ref.queueItemId,
        track_id=str(track_ref.trackId),
        context_uuid=track_ref.contextUuid if track_ref.contextUuid else None,
    )
