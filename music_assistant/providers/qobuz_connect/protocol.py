"""
Qobuz Connect codec: outer WebSocket frames + inner protobuf messages.

Owns:
- ``QobuzConnectCodec`` — encoders for every outbound message type
  (AUTHENTICATE, SUBSCRIBE, RNDR_SRVR_*, CTRL_SRVR_*) and parsers for
  every inbound message type (SRVR_RNDR_SET_STATE, SRVR_CTRL_QUEUE_*,
  SRVR_RNDR_SET_VOLUME, SRVR_RNDR_SET_ACTIVE, ...). The ``parse_*`` methods
  build :mod:`.sync_types` events straight from the wire (no per-message DTO
  layer); each stamps ``now_ms`` from the injected clock.
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
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .models import (
    QUALITY_TO_PROTOCOL,
    AudioQualityReport,
    BufferState,
    LoopMode,
    OuterMessageType,
    PlayingState,
    QConnectMessageType,
    QueueTrackRef,
    QueueVersion,
)
from .proto import qconnect_common_pb2 as _common_pb2
from .proto import qconnect_envelope_pb2 as _envelope_pb2
from .proto import qconnect_payload_pb2 as _payload_pb2
from .proto import qconnect_queue_pb2 as _queue_pb2
from .sync_types import (
    CloudActiveRendererChanged,
    CloudAddRenderer,
    CloudAutoplaySet,
    CloudAutoplayTracksLoaded,
    CloudCleared,
    CloudLoadAck,
    CloudLoopSet,
    CloudQueueError,
    CloudRemoveRenderer,
    CloudRendererStateUpdated,
    CloudSessionState,
    CloudSetState,
    CloudShuffleSet,
    CloudSnapshot,
    CloudStateRequest,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudVersionChanged,
    CloudVolume,
    CloudVolumeDelta,
)

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

    def __init__(self, device_uuid: bytes, now: Callable[[], int] | None = None) -> None:
        """
        Initialize codec.

        :param device_uuid: This device's Qobuz uuid (bytes).
        :param now: Wall-clock-ms provider stamped onto every parsed event;
            defaults to real time, injectable for deterministic tests.
        """
        self.device_uuid = device_uuid
        self._msg_counter = 0
        self._now = now or self.now_ms

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

    def encode_subscribe(self, session_uuid: bytes | None) -> bytes:
        """
        Encode websocket subscribe frame.

        :param session_uuid: Channel to subscribe to. Renderers pass the
            session UUID from the app handshake; controllers pass ``None``
            (empty channel list). The role itself is declared by the JOIN
            message that follows, not by the subscription (verified against
            web-client captures, 2026-07-07).
        """
        msg = envelope_pb2.Subscribe()
        msg.msgId = self._next_msg_id()
        msg.msgDate = self.now_ms()
        msg.proto = QCONNECT_PROTO
        if session_uuid is not None:
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

    def encode_ctrl_join_session(self, device_uuid: bytes, name: str, max_quality: int) -> bytes:
        """
        Encode ``CTRL_SRVR_JOIN_SESSION`` — the controller-role hello.

        :param device_uuid: Device identity; pass the renderer's uuid so the
            cloud merges this connection into the existing renderer entry
            instead of adding a duplicate picker entry.
        :param name: Friendly name (only used if no renderer entry exists yet).
        :param max_quality: Qobuz quality id (5/6/7/27) for the capabilities.
        """
        device_info = self._build_device_info(device_uuid, name, max_quality)

        join = payload_pb2.CtrlSrvrJoinSession()
        join.deviceInfo.CopyFrom(device_info)
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_JOIN_SESSION
        msg.ctrlSrvrJoinSession.CopyFrom(join)
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
        track_ids: list[int] | None = None,
    ) -> bytes:
        """
        Encode controller queue-load command.

        :param track_ids: Full replacement track list; packed little-endian
            into wire field 3 (misnamed ``sessionUuid`` in the
            reverse-engineered proto — it carries track ids, verified
            against web-client captures).
        """
        queue_load = queue_pb2.CtrlSrvrQueueLoadTracks()
        queue_load.queueVersion.major = queue_version.major
        queue_load.queueVersion.minor = queue_version.minor
        queue_load.actionUuid = action_uuid
        if track_ids is not None:
            queue_load.sessionUuid = b"".join(
                tid.to_bytes(4, "little", signed=False) for tid in track_ids
            )
            # The reference web client always sends these two with explicit
            # presence on full-list loads; the cloud rejects loads without
            # them / without a 16-byte contextUuid (ERROR_QUEUE_LOAD_TRACKS
            # "byte array must have length 16", observed live 2026-07-08).
            queue_load.shufflePivotQueueItemId = 0
            queue_load.shuffleMode = False
        elif qweb_track_session:
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
        """
        Encode ``CTRL_SRVR_ASK_FOR_QUEUE_STATE`` — request the full queue snapshot.

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

    def encode_ctrl_set_max_quality(self, renderer_id: int, quality: int) -> bytes:
        """Encode a controller request to change a renderer's maximum quality."""
        command = payload_pb2.CtrlSrvrSetMaxAudioQuality()
        command.rendererId = renderer_id
        command.maxAudioQuality = QUALITY_TO_PROTOCOL.get(quality, 4)
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_MAX_AUDIO_QUALITY
        msg.ctrlSrvrSetMaxAudioQuality.CopyFrom(command)
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
        """
        Encode ``CTRL_SRVR_SET_SHUFFLE_MODE`` — tell the cloud our shuffle preference.

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

    def encode_ctrl_set_player_state(
        self,
        *,
        playing_state: PlayingState | None = None,
        position_ms: int | None = None,
        queue_version: QueueVersion | None = None,
        queue_item_id: int | None = None,
    ) -> bytes:
        """
        Encode ``CTRL_SRVR_SET_PLAYER_STATE`` with only the given fields.

        The web client sends partial frames: pause/resume set only
        ``playingState``, seek sets only ``currentPosition``, and
        play-this-item sets all three (position 0).
        """
        state = payload_pb2.CtrlSrvrSetPlayerState()
        if playing_state is not None:
            state.playingState = int(playing_state)
        if position_ms is not None:
            state.currentPosition = position_ms
        if queue_item_id is not None and queue_version is not None:
            state.currentQueueItem.queueVersion.major = queue_version.major
            state.currentQueueItem.queueVersion.minor = queue_version.minor
            state.currentQueueItem.id = queue_item_id
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_PLAYER_STATE
        msg.ctrlSrvrSetPlayerState.CopyFrom(state)
        return self._encode_batch(msg)

    def encode_set_active_renderer(self, renderer_id: int) -> bytes:
        """Encode ``CTRL_SRVR_SET_ACTIVE_RENDERER`` — route playback to ``renderer_id``."""
        set_active = payload_pb2.CtrlSrvrSetActiveRenderer()
        set_active.rendererId = renderer_id
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_ACTIVE_RENDERER
        msg.ctrlSrvrSetActiveRenderer.CopyFrom(set_active)
        return self._encode_batch(msg)

    def encode_ctrl_set_volume(self, renderer_id: int, volume: int) -> bytes:
        """Encode ``CTRL_SRVR_SET_VOLUME`` — command absolute volume on a renderer."""
        set_volume = payload_pb2.CtrlSrvrSetVolume()
        set_volume.rendererId = renderer_id
        set_volume.volume = volume
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_SET_VOLUME
        msg.ctrlSrvrSetVolume.CopyFrom(set_volume)
        return self._encode_batch(msg)

    def encode_ctrl_mute_volume(self, renderer_id: int, *, muted: bool) -> bytes:
        """Encode ``CTRL_SRVR_MUTE_VOLUME`` — command mute state on a renderer."""
        mute = payload_pb2.CtrlSrvrMuteVolume()
        mute.rendererId = renderer_id
        mute.value = muted
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.CTRL_SRVR_MUTE_VOLUME
        msg.ctrlSrvrMuteVolume.CopyFrom(mute)
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
        report: AudioQualityReport,
    ) -> bytes:
        """Encode current file quality update."""
        quality_msg = payload_pb2.RndrSrvrFileAudioQualityChanged()
        quality_msg.sampling_rate = report.sampling_rate
        quality_msg.bit_depth = report.bit_depth
        quality_msg.nb_channels = report.channels
        quality_msg.audio_quality = QUALITY_TO_PROTOCOL.get(report.quality, 4)
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_FILE_AUDIO_QUALITY_CHANGED
        msg.rndrSrvrFileAudioQualityChanged.CopyFrom(quality_msg)
        return self._encode_batch(msg)

    def encode_device_audio_quality_changed(
        self,
        report: AudioQualityReport,
    ) -> bytes:
        """Encode device quality update."""
        quality_msg = payload_pb2.RndrSrvrDeviceAudioQualityChanged()
        quality_msg.sampling_rate = report.sampling_rate
        quality_msg.bit_depth = report.bit_depth
        quality_msg.nb_channels = report.channels
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

    def parse_set_state(self, message: Any) -> CloudSetState | None:
        """Parse a raw QConnect SET_STATE message into a ``CloudSetState`` event."""
        if not message.HasField("srvrRndrSetState"):
            return None
        state = message.srvrRndrSetState
        playing: PlayingState | None = None
        if state.HasField("playingState"):
            # The wire enum has values MA's PlayingState doesn't (e.g.
            # PLAYING_STATE_UNKNOWN = 0); degrade instead of raising into
            # the receive loop.
            try:
                playing = PlayingState(state.playingState)
            except ValueError:
                playing = None
        version: QueueVersion | None = None
        if state.HasField("queueVersion"):
            version = QueueVersion(state.queueVersion.major, state.queueVersion.minor)
        return CloudSetState(
            now_ms=self._now(),
            version=version,
            playing=playing,
            position_ms=state.currentPosition if state.HasField("currentPosition") else None,
            current_ref=(
                _parse_track_ref(state.currentQueueItem)
                if state.HasField("currentQueueItem")
                else None
            ),
        )

    def parse_queue_load_ack(self, message: Any) -> CloudLoadAck | None:
        """Parse server queue-load acknowledgement into a ``CloudLoadAck`` event."""
        if not message.HasField("srvrCtrlQueueTracksLoaded"):
            return None
        load = message.srvrCtrlQueueTracksLoaded
        return CloudLoadAck(
            now_ms=self._now(),
            version=QueueVersion(load.queueVersion.major, load.queueVersion.minor),
            action_uuid=load.actionUuid,
            tracks=tuple(
                ref for track in load.tracks if (ref := _parse_track_ref(track)) is not None
            ),
            queue_position=load.queuePosition if load.HasField("queuePosition") else 0,
        )

    def parse_autoplay_load_ack(self, message: Any) -> CloudAutoplayTracksLoaded | None:
        """Parse server autoplay-load ack into a ``CloudAutoplayTracksLoaded`` event."""
        if not message.HasField("srvrCtrlAutoplayTracksLoaded"):
            return None
        load = message.srvrCtrlAutoplayTracksLoaded
        return CloudAutoplayTracksLoaded(
            now_ms=self._now(),
            version=QueueVersion(load.queueVersion.major, load.queueVersion.minor),
            action_uuid=load.actionUuid,
            tracks=tuple(
                ref for track in load.tracks if (ref := _parse_track_ref(track)) is not None
            ),
        )

    def parse_queue_error(self, message: Any) -> CloudQueueError | None:
        """Parse server queue error into a ``CloudQueueError`` event."""
        if not message.HasField("srvrCtrlQueueErrorMessage"):
            return None
        error_msg = message.srvrCtrlQueueErrorMessage
        error = error_msg.error
        return CloudQueueError(
            now_ms=self._now(),
            version=QueueVersion(error_msg.queueVersion.major, error_msg.queueVersion.minor),
            action_uuid=error_msg.actionUuid,
            code=str(error.code),
            message=error.message,
        )

    def parse_queue_version_changed(self, message: Any) -> CloudVersionChanged | None:
        """Parse queue-version-changed into a ``CloudVersionChanged`` event."""
        if not message.HasField("srvrCtrlQueueVersionChanged"):
            return None
        version = message.srvrCtrlQueueVersionChanged.queueVersion
        return CloudVersionChanged(
            now_ms=self._now(), version=QueueVersion(version.major, version.minor)
        )

    def parse_session_state(self, message: Any) -> CloudSessionState | None:
        """Parse ``SRVR_CTRL_SESSION_STATE`` into a ``CloudSessionState`` event."""
        if not message.HasField("srvrCtrlSessionState"):
            return None
        state = message.srvrCtrlSessionState
        return CloudSessionState(
            now_ms=self._now(),
            version=QueueVersion(state.queueVersion.major, state.queueVersion.minor),
            track_index=state.trackIndex,
        )

    def parse_queue_state(self, message: Any) -> CloudSnapshot | None:
        """
        Parse a full ``SRVR_CTRL_QUEUE_STATE`` queue snapshot.

        The cloud carries the queue in two parts: ``tracks`` is the
        underlying (unshuffled) track list and ``shuffledTrackIndexes`` is
        the permutation the user sees when shuffle is on. We bake the
        user-facing order into ``CloudSnapshot.tracks`` here so the
        mirror always represents what MA should display — without this,
        reconciliation would force MA back to the unshuffled order every
        time the cloud reports a shuffled snapshot.
        """
        if not message.HasField("srvrCtrlQueueState"):
            return None
        state = message.srvrCtrlQueueState
        base_tracks = [
            ref for track in state.tracks if (ref := _parse_track_ref(track)) is not None
        ]
        shuffle_on = state.shuffleMode if state.HasField("shuffleMode") else False
        shuffled_indexes = list(state.shuffledTrackIndexes)
        if shuffle_on and shuffled_indexes:
            # Reorder by the cloud's shuffle permutation. Defensive bounds
            # check — an out-of-range index just gets skipped rather than
            # crashing the parse.
            effective_tracks = [
                base_tracks[i] for i in shuffled_indexes if 0 <= i < len(base_tracks)
            ]
        else:
            effective_tracks = base_tracks
        return CloudSnapshot(
            now_ms=self._now(),
            version=QueueVersion(state.queueVersion.major, state.queueVersion.minor),
            tracks=tuple(effective_tracks),
            autoplay_tracks=tuple(
                ref
                for track in state.autoplayTracks
                if (ref := _parse_track_ref(track)) is not None
            ),
            shuffle=shuffle_on,
            autoplay=state.autoplayMode if state.HasField("autoplayMode") else False,
            # The snapshot carries no track pointer of its own; the coordinator
            # injects the last SESSION_STATE trackIndex it saw.
            track_index=0,
        )

    def parse_queue_tracks_added(self, message: Any) -> CloudTracksAdded | None:
        """Parse ``SRVR_CTRL_QUEUE_TRACKS_ADDED`` into a ``CloudTracksAdded`` event."""
        if not message.HasField("srvrCtrlQueueTracksAdded"):
            return None
        evt = message.srvrCtrlQueueTracksAdded
        return CloudTracksAdded(
            now_ms=self._now(),
            version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            tracks=tuple(
                ref for track in evt.tracks if (ref := _parse_track_ref(track)) is not None
            ),
        )

    def parse_queue_tracks_inserted(self, message: Any) -> CloudTracksInserted | None:
        """Parse ``SRVR_CTRL_QUEUE_TRACKS_INSERTED`` into a ``CloudTracksInserted`` event."""
        if not message.HasField("srvrCtrlQueueTracksInserted"):
            return None
        evt = message.srvrCtrlQueueTracksInserted
        return CloudTracksInserted(
            now_ms=self._now(),
            version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            tracks=tuple(
                ref for track in evt.tracks if (ref := _parse_track_ref(track)) is not None
            ),
            insert_after=evt.insertAfter if evt.HasField("insertAfter") else 0,
        )

    def parse_queue_tracks_removed(self, message: Any) -> CloudTracksRemoved | None:
        """Parse ``SRVR_CTRL_QUEUE_TRACKS_REMOVED`` into a ``CloudTracksRemoved`` event."""
        if not message.HasField("srvrCtrlQueueTracksRemoved"):
            return None
        evt = message.srvrCtrlQueueTracksRemoved
        return CloudTracksRemoved(
            now_ms=self._now(),
            version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            queue_item_ids=tuple(evt.queueItemIds),
        )

    def parse_queue_tracks_reordered(self, message: Any) -> CloudTracksReordered | None:
        """Parse ``SRVR_CTRL_QUEUE_TRACKS_REORDERED`` into a ``CloudTracksReordered`` event."""
        if not message.HasField("srvrCtrlQueueTracksReordered"):
            return None
        evt = message.srvrCtrlQueueTracksReordered
        return CloudTracksReordered(
            now_ms=self._now(),
            version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
            queue_item_ids=tuple(evt.queueItemIds),
            insert_after=evt.insertAfter if evt.HasField("insertAfter") else 0,
        )

    def parse_queue_cleared(self, message: Any) -> CloudCleared | None:
        """Parse ``SRVR_CTRL_QUEUE_CLEARED`` into a ``CloudCleared`` event."""
        if not message.HasField("srvrCtrlQueueCleared"):
            return None
        evt = message.srvrCtrlQueueCleared
        return CloudCleared(
            now_ms=self._now(),
            version=QueueVersion(evt.queueVersion.major, evt.queueVersion.minor),
            action_uuid=evt.actionUuid,
        )

    def parse_set_volume(self, message: Any) -> CloudVolume | CloudVolumeDelta | None:
        """Parse ``SRVR_RNDR_SET_VOLUME`` into a ``CloudVolume`` / ``CloudVolumeDelta`` event."""
        if not message.HasField("srvrRndrSetVolume"):
            return None
        vol = message.srvrRndrSetVolume
        if vol.HasField("volume"):
            return CloudVolume(now_ms=self._now(), volume=vol.volume)
        if vol.HasField("volumeDelta"):
            return CloudVolumeDelta(now_ms=self._now(), delta=vol.volumeDelta)
        return None

    def parse_set_loop_mode(self, message: Any) -> CloudLoopSet | None:
        """Parse ``SRVR_RNDR_SET_LOOP_MODE`` into a ``CloudLoopSet`` event."""
        if not message.HasField("srvrRndrSetLoopMode"):
            return None
        mode = message.srvrRndrSetLoopMode
        if not mode.HasField("mode"):
            return None
        try:
            loop = LoopMode(mode.mode)
        except ValueError:
            loop = LoopMode.UNKNOWN
        return CloudLoopSet(now_ms=self._now(), action_uuid=None, loop=loop)

    def parse_set_shuffle_mode(self, message: Any) -> CloudShuffleSet | None:
        """Parse ``SRVR_RNDR_SET_SHUFFLE_MODE`` into a ``CloudShuffleSet`` event."""
        if not message.HasField("srvrRndrSetShuffleMode"):
            return None
        evt = message.srvrRndrSetShuffleMode
        if not evt.HasField("shuffleOn"):
            return None
        return CloudShuffleSet(now_ms=self._now(), action_uuid=None, shuffle=evt.shuffleOn)

    def parse_set_autoplay_mode(self, message: Any) -> CloudAutoplaySet | None:
        """Parse ``SRVR_RNDR_SET_AUTOPLAY_MODE`` into a ``CloudAutoplaySet`` event."""
        if not message.HasField("srvrRndrSetAutoplayMode"):
            return None
        evt = message.srvrRndrSetAutoplayMode
        if not evt.HasField("autoplayOn"):
            return None
        return CloudAutoplaySet(now_ms=self._now(), action_uuid=None, autoplay=evt.autoplayOn)

    def parse_state_request(self, _message: Any) -> CloudStateRequest:
        """Parse ``CTRL_SRVR_ASK_FOR_RENDERER_STATE`` into a ``CloudStateRequest`` event."""
        return CloudStateRequest(now_ms=self._now())

    def parse_add_renderer(self, msg: Any) -> CloudAddRenderer | None:
        """Parse ``SRVR_CTRL_ADD_RENDERER`` into a ``CloudAddRenderer`` event."""
        if not msg.HasField("srvrCtrlAddRenderer"):
            return None
        add = msg.srvrCtrlAddRenderer
        # is_own needs the coordinator's own device uuid; it is injected there.
        return CloudAddRenderer(
            now_ms=self._now(),
            renderer_id=add.rendererId,
            device_uuid=bytes(add.renderer.deviceUuid),
        )

    def parse_remove_renderer(self, msg: Any) -> CloudRemoveRenderer | None:
        """Parse ``SRVR_CTRL_REMOVE_RENDERER`` into a ``CloudRemoveRenderer`` event."""
        if not msg.HasField("srvrCtrlRemoveRenderer"):
            return None
        return CloudRemoveRenderer(
            now_ms=self._now(), renderer_id=int(msg.srvrCtrlRemoveRenderer.rendererId)
        )

    def parse_active_renderer_changed(self, msg: Any) -> CloudActiveRendererChanged | None:
        """Parse ``SRVR_CTRL_ACTIVE_RENDERER_CHANGED`` into a ``CloudActiveRendererChanged`` event."""
        if not msg.HasField("srvrCtrlActiveRendererChanged"):
            return None
        return CloudActiveRendererChanged(
            now_ms=self._now(), renderer_id=int(msg.srvrCtrlActiveRendererChanged.rendererId)
        )

    def parse_renderer_state_updated(self, msg: Any) -> CloudRendererStateUpdated | None:
        """Parse ``SRVR_CTRL_RENDERER_STATE_UPDATED`` into a ``CloudRendererStateUpdated`` event."""
        if not msg.HasField("srvrCtrlRendererStateUpdated"):
            return None
        upd = msg.srvrCtrlRendererStateUpdated
        state = upd.state
        playing: PlayingState | None = None
        if state.HasField("playingState"):
            try:
                playing = PlayingState(state.playingState)
            except ValueError:
                playing = None
        position_ms: int | None = None
        if state.HasField("currentPosition") and state.currentPosition.HasField("value"):
            position_ms = int(state.currentPosition.value)
        return CloudRendererStateUpdated(
            now_ms=self._now(),
            renderer_id=int(upd.rendererId),
            playing=playing,
            position_ms=position_ms,
            current_index=(
                int(state.currentQueueIndex) if state.HasField("currentQueueIndex") else None
            ),
        )

    def encode_volume_muted(self, muted: bool) -> bytes:
        """Encode a renderer ``RNDR_SRVR_VOLUME_MUTED`` event for the Qobuz app."""
        body = payload_pb2.RndrSrvrVolumeMuted()
        body.value = muted
        msg = payload_pb2.QConnectMessage()
        msg.messageType = QConnectMessageType.RNDR_SRVR_VOLUME_MUTED
        msg.rndrSrvrVolumeMuted.CopyFrom(body)
        return self._encode_batch(msg)

    def _build_device_info(self, device_uuid: bytes, name: str, max_quality: int) -> Any:
        """Build the ``DeviceInfo`` shared by renderer and controller JOIN messages."""
        device_info = common_pb2.DeviceInfo()
        device_info.deviceUuid = device_uuid
        device_info.friendlyName = name
        device_info.brand = "Music Assistant"
        device_info.model = "Qobuz Connect"
        device_info.type = common_pb2.DEVICE_TYPE_SPEAKER
        device_info.softwareVersion = "ma-qobuz-connect"

        caps = common_pb2.DeviceCapabilities()
        caps.minAudioQuality = 1
        caps.maxAudioQuality = QUALITY_TO_PROTOCOL.get(max_quality, 4)
        caps.volumeRemoteControl = 2
        device_info.capabilities.CopyFrom(caps)
        return device_info

    def _next_msg_id(self) -> int:
        self._msg_counter += 1
        return self._msg_counter

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
