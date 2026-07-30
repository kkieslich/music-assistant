"""
Pure data types for the sync reducer: canonical state, events, and effects.

This module defines the immutable value types that drive the version-gated
reducer. No behavior, no imports from MA, session, or async libraries — just
dataclasses and enums that allow the reducer to stay pure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from .models import BufferState, LoopMode, PlayingState, QueueTrackRef, QueueVersion

# ---- canonical state ----


@dataclass(slots=True)
class CanonicalState:
    """
    The reducer's mutable state — a snapshot of what MA and Qobuz agree on.

    The reducer returns copies via `dataclasses.replace()`, so this is
    intentionally non-frozen for copy-friendly mutation.
    """

    cloud_version: QueueVersion = field(default_factory=QueueVersion)
    tracks: tuple[QueueTrackRef, ...] = field(default_factory=tuple)
    autoplay_tracks: tuple[QueueTrackRef, ...] = field(default_factory=tuple)
    current_id: int | None = None  # Qobuz track ID of the current track, if any.
    playing: PlayingState = PlayingState.STOPPED
    position_ms: int = 0
    position_anchor_ms: int = 0
    settling_position: bool = False
    """
    True while we've applied a cloud-commanded seek/track-change and are
    waiting for MA's reported position to catch up to the commanded target.

    MA's ``corrected_elapsed_time`` lags an audio transition by ~1s (it keeps
    reporting the pre-transition track/position until the new/seeked stream
    reports back). While settling, the transport lane holds the commanded
    target instead of adopting MA's stale report, so the app's slider doesn't
    jump. Cleared once MA converges to the target or the settle window expires.
    """
    buffer_state: BufferState = BufferState.OK
    """
    Protocol-native transition signal reported on the wire. Set to BUFFERING
    exactly where ``settling_position`` is set (cloud-commanded track change,
    seek, resume, takeover play, load-ack switch) and back to OK where settling
    clears; the app freezes its position interpolation while it reads BUFFERING,
    which is what actually hides MA's ~1s stale-position lag from the user.
    """
    loop: LoopMode = LoopMode.OFF
    autoplay: bool = False
    active: bool = False
    activation_requested: bool = False
    release_pending: bool = False
    """
    True after cloud ownership loss until MA confirms the released player
    stopped. Delayed PLAYING events from that release must not reacquire the
    renderer; a later user-originated play can acquire after STOPPED clears it.
    """
    own_rid: int | None = None
    active_rid: int | None = None
    pending: tuple[Proposal, ...] = field(default_factory=tuple)
    last_asked_version: QueueVersion = field(default_factory=QueueVersion)
    """The cloud queue_version we last sent an AskSnapshot for (dedup)."""


# ---- proposals ----


class ProposalKind(StrEnum):
    """Kind of queue mutation proposed by a state change."""

    LOAD = "LOAD"
    ADD = "ADD"
    INSERT = "INSERT"
    REMOVE = "REMOVE"
    REORDER = "REORDER"
    CLEAR = "CLEAR"
    SHUFFLE = "SHUFFLE"


@dataclass(slots=True, frozen=True)
class Proposal:
    """
    A pending queue mutation awaiting cloud echo.

    The reducer generates proposals; the shell sends them to cloud and
    fires a ProposalTimeout if no echo is seen in time.
    """

    action_uuid: bytes
    base_version: QueueVersion
    kind: ProposalKind
    target_track_ids: tuple[int, ...]  # Qobuz track IDs.
    current_track_id: int | None  # Qobuz track ID.
    retries_left: int = 1
    push_payload_ids: tuple[int, ...] = ()
    context_uuid: bytes = b""
    """
    Qobuz track ids to actually send on the wire for LOAD/ADD/INSERT — the
    full list for LOAD, the appended tail for ADD, the inserted ids for
    INSERT. For REMOVE it carries the REMOVED qids (canonical minus the
    surviving ``target_track_ids``), translated to cloud slot ids at emit
    time. Unused (empty) for REORDER/CLEAR, which translate to slot ids at
    emit time.
    """


# ---- events: cloud list lane ----


@dataclass(slots=True, frozen=True)
class CloudSnapshot:
    """
    Full queue pushed by cloud on fresh connect or resync.

    Authoritative state — replaces the reducer's queue when received.
    """

    now_ms: int
    version: QueueVersion
    tracks: tuple[QueueTrackRef, ...]
    autoplay_tracks: tuple[QueueTrackRef, ...]
    shuffle: bool
    autoplay: bool
    track_index: int


@dataclass(slots=True, frozen=True)
class CloudTracksAdded:
    """Server delta: tracks appended to queue."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    tracks: tuple[QueueTrackRef, ...]


@dataclass(slots=True, frozen=True)
class CloudTracksInserted:
    """Server delta: tracks inserted after position."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    tracks: tuple[QueueTrackRef, ...]
    insert_after: int


@dataclass(slots=True, frozen=True)
class CloudTracksRemoved:
    """Server delta: tracks removed by queue-item id."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    queue_item_ids: tuple[int, ...]


@dataclass(slots=True, frozen=True)
class CloudTracksReordered:
    """Server delta: queue items moved after position."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    queue_item_ids: tuple[int, ...]
    insert_after: int


@dataclass(slots=True, frozen=True)
class CloudCleared:
    """Server notification: queue cleared."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes


@dataclass(slots=True, frozen=True)
class CloudLoadAck:
    """Server acknowledgement for controller queue-load command."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    tracks: tuple[QueueTrackRef, ...]
    queue_position: int


@dataclass(slots=True, frozen=True)
class CloudQueueError:
    """Server queue command error."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    code: str
    message: str


@dataclass(slots=True, frozen=True)
class CloudVersionChanged:
    """Server notification: queue version incremented."""

    now_ms: int
    version: QueueVersion


@dataclass(slots=True, frozen=True)
class CloudAutoplayTracksLoaded:
    """Server notification: autoplay queue refreshed."""

    now_ms: int
    version: QueueVersion
    action_uuid: bytes
    tracks: tuple[QueueTrackRef, ...]


# ---- events: cloud transport lane ----


@dataclass(slots=True, frozen=True)
class CloudSetState:
    """Server-to-renderer state command."""

    now_ms: int
    version: QueueVersion | None
    playing: PlayingState | None
    position_ms: int | None
    current_ref: QueueTrackRef | None


@dataclass(slots=True, frozen=True)
class CloudRendererStateUpdated:
    """
    Active renderer's state broadcast to controller.

    The only live signal a controller-joined connection gets about
    what the currently active renderer is playing.
    """

    now_ms: int
    renderer_id: int
    playing: PlayingState | None
    position_ms: int | None
    current_index: int | None


@dataclass(slots=True, frozen=True)
class CloudStateRequest:
    """Shell request for state query."""

    now_ms: int


# ---- events: cloud session lane ----


@dataclass(slots=True, frozen=True)
class CloudSessionState:
    """
    Server notification: session identity and initial queue version.

    The receiver must echo this queue_version when asking for full
    queue state via snapshot request.
    """

    now_ms: int
    version: QueueVersion
    track_index: int


@dataclass(slots=True, frozen=True)
class CloudSetActive:
    """Server command: activate or deactivate this renderer."""

    now_ms: int
    active: bool


@dataclass(slots=True, frozen=True)
class CloudActiveRendererChanged:
    """Server notification: a different renderer is now active."""

    now_ms: int
    renderer_id: int


@dataclass(slots=True, frozen=True)
class CloudAddRenderer:
    """Server notification: a new renderer joined the session."""

    now_ms: int
    renderer_id: int
    device_uuid: bytes
    is_own: bool = False
    """
    Whether ``device_uuid`` matches this renderer's own device uuid.

    Own-ness requires uuid-comparison context the pure reducer doesn't hold,
    so the coordinator resolves it and carries the verdict on the event.
    """


@dataclass(slots=True, frozen=True)
class CloudRemoveRenderer:
    """Server notification: a renderer left the session."""

    now_ms: int
    renderer_id: int


# ---- events: cloud modes / side channels ----


@dataclass(slots=True, frozen=True)
class CloudLoopSet:
    """Server notification or command: loop mode changed."""

    now_ms: int
    action_uuid: bytes | None
    loop: LoopMode


@dataclass(slots=True, frozen=True)
class CloudShuffleSet:
    """
    Server notification or command: shuffle flag changed.

    Actual reorder rides on CloudSnapshot or CloudTracksReordered.
    """

    now_ms: int
    action_uuid: bytes | None
    shuffle: bool


@dataclass(slots=True, frozen=True)
class CloudAutoplaySet:
    """Server notification or command: autoplay mode changed."""

    now_ms: int
    action_uuid: bytes | None
    autoplay: bool


@dataclass(slots=True, frozen=True)
class CloudVolume:
    """Server notification: volume level changed."""

    now_ms: int
    volume: int


@dataclass(slots=True, frozen=True)
class CloudVolumeDelta:
    """Server notification: volume delta applied."""

    now_ms: int
    delta: int


@dataclass(slots=True, frozen=True)
class CloudMute:
    """Server notification: mute state changed."""

    now_ms: int
    muted: bool


@dataclass(slots=True, frozen=True)
class CloudQuality:
    """Server notification: audio quality changed."""

    now_ms: int
    quality: int


# ---- events: MA events ----


@dataclass(slots=True, frozen=True)
class MaQueueChanged:
    """
    MA queue mutation: tracks changed.

    The action_uuid is pre-minted by the coordinator so the reducer
    stays pure — it never generates uuids.
    """

    now_ms: int
    action_uuid: bytes
    track_ids: tuple[int, ...]  # Qobuz track IDs.
    current_track_id: int | None  # Qobuz track ID.
    resolvable: frozenset[int]  # Qobuz track IDs MA could materialize.
    context_uuid: bytes = b""


@dataclass(slots=True, frozen=True)
class MaTransportChanged:
    """MA transport state: play/pause/seek."""

    now_ms: int
    playing: PlayingState
    current_track_id: int | None  # Qobuz track ID.
    position_ms: int
    target_player_id: str | None = None
    current_item_unmappable: bool = False


@dataclass(slots=True, frozen=True)
class MaModesChanged:
    """MA modes: loop and autoplay flags."""

    now_ms: int
    action_uuid: bytes
    loop: LoopMode
    autoplay: bool


@dataclass(slots=True, frozen=True)
class MaVolumeChanged:
    """MA volume: level and mute state."""

    now_ms: int
    volume: int
    muted: bool


# ---- events: lifecycle ----


@dataclass(slots=True, frozen=True)
class Disconnected:
    """WebSocket disconnected."""

    now_ms: int


@dataclass(slots=True, frozen=True)
class ProposalTimeout:
    """Shell-fired event: proposal got no cloud echo in time."""

    now_ms: int
    action_uuid: bytes


# ---- effects: to cloud ----


@dataclass(slots=True, frozen=True)
class PushLoad:
    """Push queue load command to cloud."""

    action_uuid: bytes
    base_version: QueueVersion
    track_ids: tuple[int, ...]
    current_index: int
    context_uuid: bytes


@dataclass(slots=True, frozen=True)
class PushAdd:
    """Push queue add command to cloud."""

    action_uuid: bytes
    base_version: QueueVersion
    track_ids: tuple[int, ...]
    context_uuid: bytes


@dataclass(slots=True, frozen=True)
class PushInsert:
    """Push queue insert command to cloud."""

    action_uuid: bytes
    base_version: QueueVersion
    track_ids: tuple[int, ...]
    insert_after: int
    context_uuid: bytes


@dataclass(slots=True, frozen=True)
class PushRemove:
    """Push queue remove command to cloud."""

    action_uuid: bytes
    base_version: QueueVersion
    queue_item_ids: tuple[int, ...]


@dataclass(slots=True, frozen=True)
class PushReorder:
    """Push queue reorder command to cloud."""

    action_uuid: bytes
    base_version: QueueVersion
    queue_item_ids: tuple[int, ...]
    insert_after: int


@dataclass(slots=True, frozen=True)
class PushClear:
    """Push queue clear command to cloud."""

    action_uuid: bytes
    base_version: QueueVersion


@dataclass(slots=True, frozen=True)
class PushSetActive:
    """Push renderer activation command to cloud."""


@dataclass(slots=True, frozen=True)
class PushPlayerState:
    """Push player state update to cloud."""

    playing: PlayingState | None
    position_ms: int | None
    queue_version: QueueVersion | None
    queue_item_id: int | None


@dataclass(slots=True, frozen=True)
class PushLoop:
    """Push loop mode change to cloud."""

    loop: LoopMode


@dataclass(slots=True, frozen=True)
class PushVolume:
    """Push volume change to cloud."""

    volume: int


@dataclass(slots=True, frozen=True)
class PushMute:
    """Push mute state change to cloud."""

    muted: bool


@dataclass(slots=True, frozen=True)
class PushQuality:
    """Push quality change to cloud."""

    quality: int


@dataclass(slots=True, frozen=True)
class AskSnapshot:
    """Request full queue snapshot from cloud."""

    version: QueueVersion


@dataclass(slots=True, frozen=True)
class ReportState:
    """Emit renderer state update now."""


# ---- effects: to MA ----


@dataclass(slots=True, frozen=True)
class MaPlayTrack:
    """Instruct MA to play a track (the ONLY audio-restarting effect)."""

    track_id: int  # Qobuz track ID.
    position_ms: int


@dataclass(slots=True, frozen=True)
class MaPause:
    """Instruct MA to pause playback."""


@dataclass(slots=True, frozen=True)
class MaResume:
    """Instruct MA to resume playback."""


@dataclass(slots=True, frozen=True)
class MaSeek:
    """Instruct MA to seek to position."""

    position_ms: int


@dataclass(slots=True, frozen=True)
class MaResyncQueue:
    """Instruct MA to update queue metadata/order without restarting audio."""

    track_ids: tuple[int, ...]  # Qobuz track IDs.
    current_track_id: int | None  # Qobuz track ID.
    generation: int = 0


@dataclass(slots=True, frozen=True)
class MaSetLoop:
    """Instruct MA to set loop mode."""

    loop: LoopMode


@dataclass(slots=True, frozen=True)
class MaSetShuffleFlag:
    """Instruct MA to set shuffle flag."""

    shuffle: bool


@dataclass(slots=True, frozen=True)
class MaSetVolume:
    """Instruct MA to set volume."""

    volume: int


@dataclass(slots=True, frozen=True)
class MaAdjustVolume:
    """Instruct MA to adjust volume relative to its current level."""

    delta: int


@dataclass(slots=True, frozen=True)
class MaSetMuted:
    """Instruct MA to set the player mute state."""

    muted: bool


@dataclass(slots=True, frozen=True)
class MaReleasePlayer:
    """Deactivation: instruct MA to stop and clear queue."""

    player_id: str | None = None


# ---- result container ----


@dataclass(slots=True, frozen=True)
class ReduceResult:
    """Container returned by reduce(): new state and effects to emit."""

    state: CanonicalState
    effects: tuple[Effect, ...]


# ---- type aliases ----


Event = (
    CloudSnapshot
    | CloudTracksAdded
    | CloudTracksInserted
    | CloudTracksRemoved
    | CloudTracksReordered
    | CloudCleared
    | CloudLoadAck
    | CloudQueueError
    | CloudVersionChanged
    | CloudAutoplayTracksLoaded
    | CloudSetState
    | CloudRendererStateUpdated
    | CloudStateRequest
    | CloudSessionState
    | CloudSetActive
    | CloudActiveRendererChanged
    | CloudAddRenderer
    | CloudRemoveRenderer
    | CloudLoopSet
    | CloudShuffleSet
    | CloudAutoplaySet
    | CloudVolume
    | CloudVolumeDelta
    | CloudMute
    | CloudQuality
    | MaQueueChanged
    | MaTransportChanged
    | MaModesChanged
    | MaVolumeChanged
    | Disconnected
    | ProposalTimeout
)

Effect = (
    PushLoad
    | PushAdd
    | PushInsert
    | PushRemove
    | PushReorder
    | PushClear
    | PushSetActive
    | PushPlayerState
    | PushLoop
    | PushVolume
    | PushMute
    | PushQuality
    | AskSnapshot
    | ReportState
    | MaPlayTrack
    | MaPause
    | MaResume
    | MaSeek
    | MaResyncQueue
    | MaSetLoop
    | MaSetShuffleFlag
    | MaSetVolume
    | MaAdjustVolume
    | MaSetMuted
    | MaReleasePlayer
)
