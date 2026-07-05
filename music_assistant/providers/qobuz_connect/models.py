"""
Shared DTOs, enums and constants for the qobuz_connect provider.

Owns:
- Protocol constants: ``OAUTH_APP_ID``, ``QUALITY_TO_PROTOCOL`` /
  ``QUALITY_TO_HTTP`` / ``QUALITY_AUDIO_PROPERTIES`` (Qobuz quality-id
  mappings for the three places Qobuz expects them).
- Wire-level enums: ``OuterMessageType``, ``QConnectMessageType``,
  ``PlayingState``, ``BufferState``, ``LoopMode``, ``Origin``.
- Discovery-side DTOs: ``DeviceConfig``, ``JWTApiToken``,
  ``JWTConnectToken``, ``ConnectTokens``.
- State DTOs: ``QueueVersion``, ``QueueTrackRef``, ``SetStateEvent``,
  ``QueueLoadAck``, ``QueueError``, ``QueueStateSnapshot``,
  ``QueueTracksAddedEvent``, ``QueueTracksInsertedEvent``,
  ``QueueTracksRemovedEvent``, ``QueueTracksReorderedEvent``,
  ``QueueClearedEvent``, ``QobuzMirror`` (the canonical remote-state
  snapshot held by the sync engine).

Exposes:
- All of the above as importable names.

Depends on:
- Standard library only. **No MA imports, no protobuf imports.** This
  module is the lingua franca that the codec, transport, discovery and
  sync layers communicate with — it must stay framework-free.

See :doc:`ARCHITECTURE` for which fields are part of the canonical mirror
vs. ephemeral per-action state (the latter is mostly still in
:mod:`.sync`; consolidating it here is Phase C of the plan).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    from typing import Any

OAUTH_APP_ID = "304027809"

QUALITY_TO_HTTP = {
    5: "MP3",
    6: "LOSSLESS",
    7: "HIRES_L1",
    27: "HIRES_L3",
}

QUALITY_TO_PROTOCOL = {5: 1, 6: 2, 7: 3, 27: 4}
PROTOCOL_TO_QUALITY = {protocol: quality for quality, protocol in QUALITY_TO_PROTOCOL.items()}
QUALITY_AUDIO_PROPERTIES = {
    5: (44100, 16, 2),
    6: (44100, 16, 2),
    7: (96000, 24, 2),
    27: (192000, 24, 2),
}


class OuterMessageType(IntEnum):
    """Qobuz cloud websocket envelope message types."""

    AUTHENTICATE = 1
    SUBSCRIBE = 2
    PAYLOAD = 6
    ERROR = 9
    DISCONNECT = 10


class QConnectMessageType(IntEnum):
    """QConnect payload message types used by this provider."""

    RNDR_SRVR_JOIN_SESSION = 21
    RNDR_SRVR_STATE_UPDATED = 23
    RNDR_SRVR_VOLUME_CHANGED = 25
    RNDR_SRVR_FILE_AUDIO_QUALITY_CHANGED = 26
    RNDR_SRVR_DEVICE_AUDIO_QUALITY_CHANGED = 27
    RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED = 28
    RNDR_SRVR_VOLUME_MUTED = 29
    SRVR_RNDR_SET_STATE = 41
    SRVR_RNDR_SET_VOLUME = 42
    SRVR_RNDR_SET_ACTIVE = 43
    SRVR_RNDR_SET_MAX_AUDIO_QUALITY = 44
    SRVR_RNDR_SET_LOOP_MODE = 45
    SRVR_RNDR_SET_SHUFFLE_MODE = 46
    SRVR_RNDR_SET_AUTOPLAY_MODE = 47
    CTRL_SRVR_CLEAR_QUEUE = 65
    CTRL_SRVR_QUEUE_LOAD_TRACKS = 66
    CTRL_SRVR_QUEUE_INSERT_TRACKS = 67
    CTRL_SRVR_QUEUE_ADD_TRACKS = 68
    CTRL_SRVR_QUEUE_REMOVE_TRACKS = 69
    CTRL_SRVR_QUEUE_REORDER_TRACKS = 70
    CTRL_SRVR_SET_SHUFFLE_MODE = 71
    CTRL_SRVR_SET_LOOP_MODE = 72
    CTRL_SRVR_SET_PLAYER_STATE = 62
    CTRL_SRVR_ASK_FOR_QUEUE_STATE = 76
    CTRL_SRVR_ASK_FOR_RENDERER_STATE = 77
    CTRL_SRVR_AUTOPLAY_ADD_TRACKS = 79
    SRVR_CTRL_SESSION_STATE = 81
    SRVR_CTRL_QUEUE_ERROR_MESSAGE = 88
    SRVR_CTRL_QUEUE_CLEARED = 89
    SRVR_CTRL_QUEUE_STATE = 90
    SRVR_CTRL_QUEUE_TRACKS_LOADED = 91
    SRVR_CTRL_QUEUE_TRACKS_INSERTED = 92
    SRVR_CTRL_QUEUE_TRACKS_ADDED = 93
    SRVR_CTRL_QUEUE_TRACKS_REMOVED = 94
    SRVR_CTRL_QUEUE_TRACKS_REORDERED = 95
    SRVR_CTRL_AUTOPLAY_TRACKS_LOADED = 103
    SRVR_CTRL_QUEUE_VERSION_CHANGED = 105


class PlayingState(IntEnum):
    """Qobuz playing states."""

    STOPPED = 1
    PLAYING = 2
    PAUSED = 3


class BufferState(IntEnum):
    """Qobuz buffer states."""

    UNKNOWN = 0
    BUFFERING = 1
    OK = 2
    ERROR = 3
    UNDERRUN = 4


class LoopMode(IntEnum):
    """Qobuz loop / repeat mode (mirrors ``LoopMode`` in qconnect_common.proto)."""

    UNKNOWN = 0
    OFF = 1
    REPEAT_ONE = 2
    REPEAT_ALL = 3


class Origin(StrEnum):
    """Source of a state transition."""

    QOBUZ = "qobuz"
    MA = "ma"
    ACK = "ack"


@dataclass(slots=True)
class JWTConnectToken:
    """WebSocket JWT token received from the Qobuz app."""

    jwt: str = ""
    exp: int = 0
    endpoint: str = ""

    def is_valid(self) -> bool:
        """Return whether this token can open a websocket session."""
        return bool(self.jwt and self.exp and self.endpoint)


@dataclass(slots=True)
class JWTApiToken:
    """Qobuz API JWT token received from the Qobuz app."""

    jwt: str = ""
    exp: int = 0


@dataclass(slots=True)
class ConnectTokens:
    """Tokens received from the Qobuz app discovery handshake."""

    session_id: str = ""
    ws_token: JWTConnectToken | None = None
    api_token: JWTApiToken | None = None

    def is_valid(self) -> bool:
        """Return whether all required websocket fields are present."""
        return bool(self.session_id and self.ws_token and self.ws_token.is_valid())


@dataclass(slots=True)
class DeviceConfig:
    """Qobuz Connect device identity."""

    name: str
    uuid: str
    http_port: int
    bind_address: str
    max_quality: int


@dataclass(slots=True)
class QueueVersion:
    """Qobuz queue version."""

    major: int = 0
    minor: int = 0


@dataclass(slots=True)
class QueueTrackRef:
    """Reference to a Qobuz queue item."""

    queue_item_id: int
    track_id: str
    context_uuid: bytes | None = None


@dataclass(slots=True)
class SetStateEvent:
    """Decoded server-to-renderer state command."""

    playing_state: PlayingState | None = None
    position_ms: int | None = None
    queue_version: QueueVersion | None = None
    current_item: QueueTrackRef | None = None
    next_item: QueueTrackRef | None = None


@dataclass(slots=True)
class SessionStateEvent:
    """
    Decoded ``SRVR_CTRL_SESSION_STATE`` — the cloud's "you're connected" frame.

    Carries the queue identity (``queue_version``) the receiver must echo
    when sending ``CTRL_SRVR_ASK_FOR_QUEUE_STATE`` to obtain the full
    track list, plus the session identifier the controller would need
    if it ever explicitly asked for renderer state.
    """

    session_uuid: bytes
    session_id: int
    queue_version: QueueVersion
    track_index: int = 0


@dataclass(slots=True)
class QueueLoadAck:
    """Server acknowledgement for a controller queue-load command."""

    action_uuid: bytes
    queue_version: QueueVersion
    tracks: list[QueueTrackRef] = field(default_factory=list)
    queue_position: int = 0
    qobuz_reference_id: int | None = None


@dataclass(slots=True)
class QueueStateSnapshot:
    """
    Full queue snapshot pushed by the cloud (``SRVR_CTRL_QUEUE_STATE``).

    Authoritative state — replaces the mirror's queue when received.
    Emitted on every fresh connect / resync (``CTRL_SRVR_ASK_FOR_QUEUE_STATE``)
    and whenever the cloud needs to re-broadcast canonical state.
    """

    queue_version: QueueVersion
    action_uuid: bytes
    tracks: list[QueueTrackRef] = field(default_factory=list)
    shuffle_mode: bool = False
    autoplay_mode: bool = False
    autoplay_tracks: list[QueueTrackRef] = field(default_factory=list)


@dataclass(slots=True)
class QueueTracksAddedEvent:
    """Server delta: tracks appended to the queue (``SRVR_CTRL_QUEUE_TRACKS_ADDED``)."""

    queue_version: QueueVersion
    action_uuid: bytes
    tracks: list[QueueTrackRef] = field(default_factory=list)
    context_uuid: bytes | None = None


@dataclass(slots=True)
class QueueTracksInsertedEvent:
    """Server delta: tracks inserted after a given position (``SRVR_CTRL_QUEUE_TRACKS_INSERTED``)."""

    queue_version: QueueVersion
    action_uuid: bytes
    tracks: list[QueueTrackRef] = field(default_factory=list)
    insert_after: int = 0
    context_uuid: bytes | None = None


@dataclass(slots=True)
class QueueTracksRemovedEvent:
    """Server delta: tracks removed by queue-item id (``SRVR_CTRL_QUEUE_TRACKS_REMOVED``)."""

    queue_version: QueueVersion
    action_uuid: bytes
    queue_item_ids: list[int] = field(default_factory=list)


@dataclass(slots=True)
class QueueTracksReorderedEvent:
    """Server delta: queue items moved after a given position (``SRVR_CTRL_QUEUE_TRACKS_REORDERED``)."""

    queue_version: QueueVersion
    action_uuid: bytes
    queue_item_ids: list[int] = field(default_factory=list)
    insert_after: int = 0


@dataclass(slots=True)
class QueueClearedEvent:
    """Server notification: queue cleared (``SRVR_CTRL_QUEUE_CLEARED``)."""

    queue_version: QueueVersion
    action_uuid: bytes


@dataclass(slots=True)
class QueueError:
    """Server queue command error."""

    action_uuid: bytes
    queue_version: QueueVersion | None = None
    code: str = ""
    message: str = ""


@dataclass(slots=True)
class QobuzMirror:
    """Provider's local mirror of Qobuz cloud/app state."""

    queue_version: QueueVersion = field(default_factory=QueueVersion)
    current_item: QueueTrackRef | None = None
    next_item: QueueTrackRef | None = None
    playing_state: PlayingState = PlayingState.STOPPED
    buffer_state: BufferState = BufferState.OK
    position_ms: int = 0
    position_timestamp_ms: int = 0
    duration_ms: int = 0
    # Populated from SRVR_CTRL_QUEUE_STATE snapshots + SRVR_CTRL_QUEUE_TRACKS_*
    # delta messages. Phase B populates this; Phase C will use it to drive
    # MA-side queue reconciliation.
    tracks: list[QueueTrackRef] = field(default_factory=list)
    loop_mode: LoopMode = LoopMode.OFF
    shuffle_mode: bool = False
    autoplay_mode: bool = False


class OutboundActionKind(StrEnum):
    """Kind of queue mutation we sent to the Qobuz cloud and now expect to echo back."""

    LOAD = "load"
    ADD = "add"
    INSERT = "insert"
    REMOVE = "remove"
    REORDER = "reorder"
    CLEAR = "clear"
    SHUFFLE = "shuffle"
    LOOP = "loop"


@dataclass(slots=True)
class OutboundActionMeta:
    """
    Ledger entry for a queue-mutation command we initiated.

    The cloud echoes ``CTRL_SRVR_QUEUE_*`` commands back as their
    ``SRVR_CTRL_QUEUE_*`` counterpart with the same ``action_uuid``.
    When we see our own action_uuid come back we still update the
    mirror (the cloud assigns ``queue_item_id``s on add/insert) but
    skip the inbound MA reconciler — MA's queue already reflects
    the change because we originated it.
    """

    kind: OutboundActionKind
    queue_version: QueueVersion
    expires_at: float
    future: asyncio.Future[Any] | None = None
