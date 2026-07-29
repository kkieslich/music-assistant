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
- Wire value types: ``QueueVersion``, ``QueueTrackRef`` (shared by the codec
  and the ``sync_types`` events; the codec parses frames straight into those
  events, so no per-message DTO layer exists any more).

Exposes:
- All of the above as importable names.

Depends on:
- Standard library only. **No MA imports, no protobuf imports.** This
  module is the lingua franca that the codec, transport, discovery and
  sync layers communicate with — it must stay framework-free.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, StrEnum

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


@dataclass(frozen=True, slots=True)
class AudioQualityReport:
    """Actual audio properties for a Qobuz renderer quality report."""

    quality: int
    sampling_rate: int
    bit_depth: int
    channels: int


def quality_id_for_format(content_type: object, sampling_rate: int, bit_depth: int) -> int:
    """Return the Qobuz quality id matching an actual stream format."""
    encoding = str(content_type).lower()
    if "mp3" in encoding or "mpeg" in encoding:
        return 5
    if bit_depth <= 16:
        return 6
    if sampling_rate <= 96_000:
        return 7
    return 27


class OuterMessageType(IntEnum):
    """Qobuz cloud websocket envelope message types."""

    AUTHENTICATE = 1
    SUBSCRIBE = 2
    PAYLOAD = 6
    ERROR = 9
    DISCONNECT = 10


class QConnectMessageType(IntEnum):
    """QConnect payload message types used by this provider."""

    ERROR = 1
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
    CTRL_SRVR_JOIN_SESSION = 61
    CTRL_SRVR_SET_PLAYER_STATE = 62
    CTRL_SRVR_SET_ACTIVE_RENDERER = 63
    CTRL_SRVR_SET_VOLUME = 64
    CTRL_SRVR_CLEAR_QUEUE = 65
    CTRL_SRVR_QUEUE_LOAD_TRACKS = 66
    CTRL_SRVR_QUEUE_INSERT_TRACKS = 67
    CTRL_SRVR_QUEUE_ADD_TRACKS = 68
    CTRL_SRVR_QUEUE_REMOVE_TRACKS = 69
    CTRL_SRVR_QUEUE_REORDER_TRACKS = 70
    CTRL_SRVR_SET_SHUFFLE_MODE = 71
    CTRL_SRVR_SET_LOOP_MODE = 72
    CTRL_SRVR_MUTE_VOLUME = 73
    CTRL_SRVR_SET_MAX_AUDIO_QUALITY = 74
    CTRL_SRVR_ASK_FOR_QUEUE_STATE = 76
    CTRL_SRVR_ASK_FOR_RENDERER_STATE = 77
    CTRL_SRVR_AUTOPLAY_ADD_TRACKS = 79
    SRVR_CTRL_SESSION_STATE = 81
    SRVR_CTRL_RENDERER_STATE_UPDATED = 82
    SRVR_CTRL_ADD_RENDERER = 83
    SRVR_CTRL_UPDATE_RENDERER = 84
    SRVR_CTRL_REMOVE_RENDERER = 85
    SRVR_CTRL_ACTIVE_RENDERER_CHANGED = 86
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
