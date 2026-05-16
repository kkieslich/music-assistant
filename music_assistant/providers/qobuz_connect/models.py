"""Local Qobuz Connect protocol and sync models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum

OAUTH_APP_ID = "304027809"

QUALITY_TO_HTTP = {
    5: "MP3",
    6: "LOSSLESS",
    7: "HIRES_L1",
    27: "HIRES_L3",
}

QUALITY_TO_PROTOCOL = {5: 1, 6: 2, 7: 3, 27: 4}
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
    SRVR_RNDR_SET_STATE = 41
    SRVR_RNDR_SET_VOLUME = 42
    SRVR_RNDR_SET_ACTIVE = 43
    SRVR_RNDR_SET_MAX_AUDIO_QUALITY = 44
    SRVR_RNDR_SET_LOOP_MODE = 45
    SRVR_RNDR_SET_SHUFFLE_MODE = 46
    SRVR_RNDR_SET_AUTOPLAY_MODE = 47
    CTRL_SRVR_QUEUE_LOAD_TRACKS = 66
    CTRL_SRVR_SET_PLAYER_STATE = 62
    CTRL_SRVR_ASK_FOR_RENDERER_STATE = 77
    CTRL_SRVR_AUTOPLAY_ADD_TRACKS = 79
    SRVR_CTRL_QUEUE_ERROR_MESSAGE = 88
    SRVR_CTRL_QUEUE_STATE = 90
    SRVR_CTRL_QUEUE_TRACKS_LOADED = 91
    SRVR_CTRL_AUTOPLAY_TRACKS_LOADED = 103
    SRVR_CTRL_QUEUE_VERSION_CHANGED = 105


class PlayingState(IntEnum):
    """Qobuz playing states."""

    STOPPED = 1
    PLAYING = 2
    PAUSED = 3


class BufferState(IntEnum):
    """Qobuz buffer states."""

    OK = 2


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
class QueueLoadAck:
    """Server acknowledgement for a controller queue-load command."""

    action_uuid: bytes
    queue_version: QueueVersion
    tracks: list[QueueTrackRef] = field(default_factory=list)
    queue_position: int = 0
    qobuz_reference_id: int | None = None


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
    position_ms: int = 0
    position_timestamp_ms: int = 0
    duration_ms: int = 0
