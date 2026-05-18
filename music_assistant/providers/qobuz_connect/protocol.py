"""
Qobuz Connect codec: outer WebSocket frames + inner protobuf messages.

Owns:
- ``QobuzConnectCodec`` — encoders for every outbound message type
  (AUTHENTICATE, SUBSCRIBE, RNDR_SRVR_*, CTRL_SRVR_*) and parsers for
  every inbound message type (SRVR_RNDR_SET_STATE, SRVR_CTRL_QUEUE_*,
  SRVR_RNDR_SET_VOLUME, SRVR_RNDR_SET_ACTIVE, ...). Pure functions of
  bytes ↔ typed events from :mod:`.models`.
- The outer-frame format: ``[msg_type:1][varint_length][payload]``
  expressed by ``decode_frame`` / the per-message encode helpers.

Exposes:
- ``QobuzConnectCodec`` and ``DecodedFrame``
- Module-level constants ``QCONNECT_PROTO`` and the
  ``OuterMessageType`` / ``QConnectMessageType`` re-exports
  (real definitions live in :mod:`.models`).

Depends on:
- :mod:`.models` for enums and dataclasses.
- ``.proto.qconnect_*_pb2`` generated modules. **Do not delete those
  generated files** — they're committed and imported directly.
- No imports from MA or the WebSocket transport — the codec is pure.

See :doc:`ARCHITECTURE` for the full inbound/outbound message tables
mapping wire types ↔ codec functions ↔ handlers.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from .models import (
    QUALITY_AUDIO_PROPERTIES,
    QUALITY_TO_PROTOCOL,
    BufferState,
    LoopMode,
    OuterMessageType,
    PlayingState,
    QConnectMessageType,
    QueueClearedEvent,
    QueueError,
    QueueLoadAck,
    QueueStateSnapshot,
    QueueTrackRef,
    QueueTracksAddedEvent,
    QueueTracksInsertedEvent,
    QueueTracksRemovedEvent,
    QueueTracksReorderedEvent,
    QueueVersion,
    SessionStateEvent,
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
        """Encode websocket subscribe frame.

        Note on the captures: the reference Web Client sends SUBSCRIBE with
        empty channels, but it authenticates with a *user-login JWT* and
        the cloud routes events to it as a *controller*. We authenticate
        with a *device-session JWT* from ``/connect`` and the cloud routes
        events to us as a *renderer*; that role apparently requires the
        session UUID in the channel list — switching to empty channels
        caused the cloud to reject the handshake with a type-1 ERROR
        immediately on connect (May 2026 local test).
        """
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
        """Encode renderer join-session message.

        The Qobuz Web Client doesn't send this (it's a controller role
        authenticating with a user JWT), but a renderer authenticating
        with a device-session JWT from ``/connect`` must — otherwise the
        cloud closes the WS with a type-1 ERROR after the SUBSCRIBE.
        """
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
        buffer_state: BufferState,
        position_ms: int,
        position_timestamp_ms: int,
        duration_ms: int,
        queue_item_id: int,
        queue_version: QueueVersion,
    ) -> bytes:
        """Encode renderer state update for the Qobuz app."""
        state = common_pb2.QueueRendererState()
        state.playingState = int(playing_state)
        state.bufferState = int(buffer_state)
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

    def encode_ask_for_queue_state(
        self,
        *,
        queue_version: QueueVersion,
        queue_uuid: bytes,
    ) -> bytes:
        """Encode ``CTRL_SRVR_ASK_FOR_QUEUE_STATE`` — request the full queue snapshot.

        ``queue_uuid`` is generated locally as an action correlator (mirrors
        the ``action_uuid`` pattern in :meth:`encode_queue_load_tracks`).
        The cloud echoes it back in the resulting ``SRVR_CTRL_QUEUE_STATE``
        message's ``actionUuid``.
        """
        ask = payload_pb2.CtrlSrvrAskForQueueState()
        ask.queueVersion.major = queue_version.major
        ask.queueVersion.minor = queue_version.minor
        ask.queueUuid = queue_uuid
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_ASK_FOR_QUEUE_STATE
        msg.ctrlSrvrAskForQueueState.CopyFrom(ask)
        return self._encode_batch(msg)

    def encode_set_loop_mode(self, mode: LoopMode) -> bytes:
        """Encode ``CTRL_SRVR_SET_LOOP_MODE`` — tell the cloud our loop preference."""
        loop = payload_pb2.CtrlSrvrSetLoopMode()
        loop.mode = int(mode)
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_LOOP_MODE
        msg.ctrlSrvrSetLoopMode.CopyFrom(loop)
        return self._encode_batch(msg)

    def encode_set_shuffle_mode(
        self,
        *,
        shuffle_on: bool,
        queue_version: QueueVersion,
        current_queue_item_id: int,
        action_uuid: bytes,
    ) -> bytes:
        """Encode ``CTRL_SRVR_SET_SHUFFLE_MODE`` — tell the cloud our shuffle preference.

        The cloud needs the current queue_version + the queue_item_id of
        whatever's playing right now so it can pin the playing track as the
        shuffle anchor (it re-orders the rest around it instead of yanking
        the audio).
        """
        shuffle = payload_pb2.CtrlSrvrSetShuffleMode()
        shuffle.queueVersion.major = queue_version.major
        shuffle.queueVersion.minor = queue_version.minor
        shuffle.actionUuid = action_uuid
        shuffle.shuffleOn = shuffle_on
        shuffle.currentQueueItemId = current_queue_item_id
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_SHUFFLE_MODE
        msg.ctrlSrvrSetShuffleMode.CopyFrom(shuffle)
        return self._encode_batch(msg)

    def encode_clear_queue(self, queue_version: QueueVersion) -> bytes:
        """Encode ``CTRL_SRVR_CLEAR_QUEUE`` — drop all queue items."""
        clear = queue_pb2.CtrlSrvrClearQueue()
        clear.queueVersion.major = queue_version.major
        clear.queueVersion.minor = queue_version.minor
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_CLEAR_QUEUE
        msg.ctrlSrvrClearQueue.CopyFrom(clear)
        return self._encode_batch(msg)

    def encode_queue_add_tracks(
        self,
        *,
        action_uuid: bytes,
        tracks: list[QueueTrackRef],
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
        autoplay_reset: bool = False,
    ) -> bytes:
        """Encode ``CTRL_SRVR_QUEUE_ADD_TRACKS`` — append tracks to the end of the queue."""
        add = queue_pb2.CtrlSrvrQueueAddTracks()
        add.queueVersion.major = queue_version.major
        add.queueVersion.minor = queue_version.minor
        add.actionUuid = action_uuid
        _pack_track_refs(add.tracks, tracks)
        add.autoplayReset = autoplay_reset
        if context_uuid:
            add.contextUuid = context_uuid
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_QUEUE_ADD_TRACKS
        msg.ctrlSrvrQueueAddTracks.CopyFrom(add)
        return self._encode_batch(msg)

    def encode_queue_insert_tracks(
        self,
        *,
        action_uuid: bytes,
        tracks: list[QueueTrackRef],
        insert_after: int,
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
        autoplay_reset: bool = False,
    ) -> bytes:
        """Encode ``CTRL_SRVR_QUEUE_INSERT_TRACKS`` — insert tracks after ``insert_after`` index."""
        insert = queue_pb2.CtrlSrvrQueueInsertTracks()
        insert.queueVersion.major = queue_version.major
        insert.queueVersion.minor = queue_version.minor
        insert.actionUuid = action_uuid
        _pack_track_refs(insert.tracks, tracks)
        insert.insertAfter = insert_after
        insert.autoplayReset = autoplay_reset
        if context_uuid:
            insert.contextUuid = context_uuid
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_QUEUE_INSERT_TRACKS
        msg.ctrlSrvrQueueInsertTracks.CopyFrom(insert)
        return self._encode_batch(msg)

    def encode_queue_remove_tracks(
        self,
        *,
        action_uuid: bytes,
        queue_item_ids: list[int],
        queue_version: QueueVersion,
        autoplay_reset: bool = False,
    ) -> bytes:
        """Encode ``CTRL_SRVR_QUEUE_REMOVE_TRACKS`` — remove items by ``queue_item_id``."""
        remove = queue_pb2.CtrlSrvrQueueRemoveTracks()
        remove.queueVersion.major = queue_version.major
        remove.queueVersion.minor = queue_version.minor
        remove.actionUuid = action_uuid
        remove.queueItemIds.extend(queue_item_ids)
        remove.autoplayReset = autoplay_reset
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_QUEUE_REMOVE_TRACKS
        msg.ctrlSrvrQueueRemoveTracks.CopyFrom(remove)
        return self._encode_batch(msg)

    def encode_queue_reorder_tracks(
        self,
        *,
        action_uuid: bytes,
        queue_item_ids: list[int],
        insert_after: int,
        queue_version: QueueVersion,
        autoplay_reset: bool = False,
    ) -> bytes:
        """Encode ``CTRL_SRVR_QUEUE_REORDER_TRACKS`` — move the listed items after ``insert_after``."""
        reorder = queue_pb2.CtrlSrvrQueueReorderTracks()
        reorder.queueVersion.major = queue_version.major
        reorder.queueVersion.minor = queue_version.minor
        reorder.actionUuid = action_uuid
        reorder.queueItemIds.extend(queue_item_ids)
        reorder.insertAfter = insert_after
        reorder.autoplayReset = autoplay_reset
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_QUEUE_REORDER_TRACKS
        msg.ctrlSrvrQueueReorderTracks.CopyFrom(reorder)
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

    @staticmethod
    def parse_session_state(message: Any) -> SessionStateEvent | None:
        """Parse ``SRVR_CTRL_SESSION_STATE`` — the cloud's session-bound queue version."""
        if not message.HasField("srvrCtrlSessionState"):
            return None
        state = message.srvrCtrlSessionState
        return SessionStateEvent(
            session_uuid=state.sessionUuid,
            session_id=state.sessionId,
            queue_version=QueueVersion(state.queueVersion.major, state.queueVersion.minor),
            track_index=state.trackIndex,
        )

    @staticmethod
    def parse_queue_state(message: Any) -> QueueStateSnapshot | None:
        """Parse a full ``SRVR_CTRL_QUEUE_STATE`` queue snapshot."""
        if not message.HasField("srvrCtrlQueueState"):
            return None
        state = message.srvrCtrlQueueState
        return QueueStateSnapshot(
            queue_version=QueueVersion(state.queueVersion.major, state.queueVersion.minor),
            action_uuid=state.actionUuid,
            tracks=[ref for track in state.tracks if (ref := _parse_track_ref(track)) is not None],
            shuffle_mode=state.shuffleMode if state.HasField("shuffleMode") else False,
            autoplay_mode=state.autoplayMode if state.HasField("autoplayMode") else False,
            autoplay_tracks=[
                ref
                for track in state.autoplayTracks
                if (ref := _parse_track_ref(track)) is not None
            ],
        )

    @staticmethod
    def parse_queue_tracks_added(message: Any) -> QueueTracksAddedEvent | None:
        """Parse a ``SRVR_CTRL_QUEUE_TRACKS_ADDED`` queue-delta."""
        if not message.HasField("srvrCtrlQueueTracksAdded"):
            return None
        evt = message.srvrCtrlQueueTracksAdded
        return QueueTracksAddedEvent(
            queue_version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            tracks=[ref for track in evt.tracks if (ref := _parse_track_ref(track)) is not None],
            context_uuid=evt.contextUuid if evt.HasField("contextUuid") else None,
        )

    @staticmethod
    def parse_queue_tracks_inserted(message: Any) -> QueueTracksInsertedEvent | None:
        """Parse a ``SRVR_CTRL_QUEUE_TRACKS_INSERTED`` queue-delta."""
        if not message.HasField("srvrCtrlQueueTracksInserted"):
            return None
        evt = message.srvrCtrlQueueTracksInserted
        return QueueTracksInsertedEvent(
            queue_version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            tracks=[ref for track in evt.tracks if (ref := _parse_track_ref(track)) is not None],
            insert_after=evt.insertAfter if evt.HasField("insertAfter") else 0,
            context_uuid=evt.contextUuid if evt.HasField("contextUuid") else None,
        )

    @staticmethod
    def parse_queue_tracks_removed(message: Any) -> QueueTracksRemovedEvent | None:
        """Parse a ``SRVR_CTRL_QUEUE_TRACKS_REMOVED`` queue-delta."""
        if not message.HasField("srvrCtrlQueueTracksRemoved"):
            return None
        evt = message.srvrCtrlQueueTracksRemoved
        return QueueTracksRemovedEvent(
            queue_version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            queue_item_ids=list(evt.queueItemIds),
        )

    @staticmethod
    def parse_queue_tracks_reordered(message: Any) -> QueueTracksReorderedEvent | None:
        """Parse a ``SRVR_CTRL_QUEUE_TRACKS_REORDERED`` queue-delta."""
        if not message.HasField("srvrCtrlQueueTracksReordered"):
            return None
        evt = message.srvrCtrlQueueTracksReordered
        return QueueTracksReorderedEvent(
            queue_version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            queue_item_ids=list(evt.queueItemIds),
            insert_after=evt.insertAfter if evt.HasField("insertAfter") else 0,
        )

    @staticmethod
    def parse_queue_cleared(message: Any) -> QueueClearedEvent | None:
        """Parse a ``SRVR_CTRL_QUEUE_CLEARED`` notification."""
        if not message.HasField("srvrCtrlQueueCleared"):
            return None
        evt = message.srvrCtrlQueueCleared
        return QueueClearedEvent(
            queue_version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
        )

    @staticmethod
    def parse_set_loop_mode(message: Any) -> LoopMode | None:
        """Parse a ``SRVR_RNDR_SET_LOOP_MODE`` renderer command."""
        if not message.HasField("srvrRndrSetLoopMode"):
            return None
        mode = message.srvrRndrSetLoopMode
        if not mode.HasField("mode"):
            return None
        try:
            return LoopMode(mode.mode)
        except ValueError:
            return LoopMode.UNKNOWN

    @staticmethod
    def parse_set_shuffle_mode(message: Any) -> bool | None:
        """Parse a ``SRVR_RNDR_SET_SHUFFLE_MODE`` renderer command."""
        if not message.HasField("srvrRndrSetShuffleMode"):
            return None
        evt = message.srvrRndrSetShuffleMode
        return evt.shuffleOn if evt.HasField("shuffleOn") else None

    @staticmethod
    def parse_set_autoplay_mode(message: Any) -> bool | None:
        """Parse a ``SRVR_RNDR_SET_AUTOPLAY_MODE`` renderer command."""
        if not message.HasField("srvrRndrSetAutoplayMode"):
            return None
        evt = message.srvrRndrSetAutoplayMode
        return evt.autoplayOn if evt.HasField("autoplayOn") else None

    def encode_volume_muted(self, muted: bool) -> bytes:
        """Encode a renderer ``RNDR_SRVR_VOLUME_MUTED`` event for the Qobuz app."""
        body = payload_pb2.RndrSrvrVolumeMuted()
        body.value = muted
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_VOLUME_MUTED
        msg.rndrSrvrVolumeMuted.CopyFrom(body)
        return self._encode_batch(msg)

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


def _pack_track_refs(repeated_field: Any, refs: list[QueueTrackRef]) -> None:
    """Append each ``ref`` onto a protobuf ``repeated QueueTrackRef`` field."""
    for ref in refs:
        wire = repeated_field.add()
        wire.queueItemId = ref.queue_item_id
        wire.trackId = int(ref.track_id)
        if ref.context_uuid:
            wire.contextUuid = ref.context_uuid
