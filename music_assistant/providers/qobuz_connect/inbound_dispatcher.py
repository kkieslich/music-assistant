"""
Inbound message dispatcher for the Qobuz Connect cloud websocket.

Replaces the ~60-line ``if/elif`` chain that grew inside
:mod:`.session` as the renderer learned more message types — at Phase B
Tier 1 it had already passed Ruff's PLR0915 statement budget and needed
a ``# noqa`` to commit. Phase C moves the routing here so:

- adding a new message type is one entry in the dispatch dict + one
  small handler method, both colocated with their peers;
- the transport (:mod:`.session`) shrinks back to "open WebSocket,
  decode envelopes, hand off" with no protocol-policy knowledge;
- tests can drive ``InboundDispatcher.dispatch(msg)`` directly with a
  hand-built ``QConnectMessage`` instead of going through the network.

The dispatcher owns no state beyond its collaborators (codec, callbacks,
logger) — each call delegates parsing back to the codec and the typed
event back out to the provider via callbacks. Tier-3 broadcasts about
*other* renderers are listed by numeric type id and logged at debug
without falling through.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from .models import QConnectMessageType

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .protocol import QobuzConnectCodec
    from .session import SessionCallbacks


LOGGER = logging.getLogger(__name__)


# Message types that Qobuz broadcasts to controllers about state changes
# on *other* renderers (or session-wide concerns). They reach this
# renderer because of how subscription routing works in Qobuz Connect,
# but no MA action is required — they're explicitly acknowledged here
# so they don't fall through to the "Unhandled" warning. Numeric values
# are inlined because these types aren't in ``QConnectMessageType``.
# ADD_RENDERER (83), REMOVE_RENDERER (85), and ACTIVE_RENDERER_CHANGED (86)
# are routed to the controller-role callbacks instead — see
# ``_HANDLER_TABLE`` — and are therefore no longer listed here.
# See ARCHITECTURE.md (Tier-3).
_KNOWN_IGNORED_MESSAGE_TYPES: frozenset[int] = frozenset(
    {
        84,  # SRVR_CTRL_UPDATE_RENDERER
        87,  # SRVR_CTRL_VOLUME_CHANGED
        97,  # SRVR_CTRL_LOOP_MODE_SET
        98,  # SRVR_CTRL_VOLUME_MUTED
        99,  # SRVR_CTRL_MAX_AUDIO_QUALITY_CHANGED
        100,  # SRVR_CTRL_FILE_AUDIO_QUALITY_CHANGED
    }
)


def _format_track_ref(ref: Any) -> str:
    if ref is None:
        return "-"
    return f"{ref.queue_item_id}:{ref.track_id}"


class InboundDispatcher:
    """Routes a decoded inner ``QConnectMessage`` to the right callback."""

    __slots__ = ("_cb", "_codec", "_logger", "_on_error_message")

    def __init__(
        self,
        codec: QobuzConnectCodec,
        callbacks: SessionCallbacks,
        *,
        label: str = "",
        on_error_message: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        """
        Hold the codec + callback bundle this dispatcher will fan out to.

        :param codec: Codec used to parse inner messages.
        :param callbacks: Provider callback bundle to fan out to.
        :param label: Optional connection label (e.g. the session role) used
            as a child-logger suffix so renderer and controller connection
            log lines are distinguishable.
        :param on_error_message: Optional hook invoked for message-level
            errors (``messageType`` 1) — the shape the cloud uses to reject
            frames from a deregistered renderer.
        """
        self._codec = codec
        self._cb = callbacks
        self._logger = LOGGER.getChild(label) if label else LOGGER
        self._on_error_message = on_error_message

    async def dispatch(self, msg: Any) -> None:
        """Parse and forward a single inner message."""
        msg_type = msg.messageType
        handler = self._HANDLER_TABLE.get(msg_type)
        if handler is not None:
            await handler(self, msg)
            return
        if msg_type in _KNOWN_IGNORED_MESSAGE_TYPES:
            self._logger.debug("Qobuz broadcast ignored: type=%s", msg_type)
            return
        self._logger.debug("Unhandled Qobuz Connect message type: %s", msg_type)

    # ---- per-type handlers (kept short; the table maps to each) ----------

    async def _on_error(self, msg: Any) -> None:
        # Message-level rejection — observed live when the cloud has
        # deregistered this renderer but the socket is still open.
        code = msg.error.code if msg.HasField("error") else "?"
        message = msg.error.message if msg.HasField("error") else ""
        self._logger.warning("Qobuz Connect message error %s: %s", code, message)
        if self._on_error_message is not None:
            await self._on_error_message(str(message))

    async def _on_set_state(self, msg: Any) -> None:
        if event := self._codec.parse_set_state(msg):
            self._logger.debug(
                "Qobuz SET_STATE state=%s pos=%s current=%s next=%s qv=%s",
                event.playing_state,
                event.position_ms,
                _format_track_ref(event.current_item),
                _format_track_ref(event.next_item),
                event.queue_version,
            )
            await self._cb.on_set_state(event)

    async def _on_set_volume(self, msg: Any) -> None:
        if not msg.HasField("srvrRndrSetVolume"):
            return
        vol = msg.srvrRndrSetVolume
        if vol.HasField("volume"):
            await self._cb.on_volume(vol.volume)
        elif vol.HasField("volumeDelta"):
            await self._cb.on_volume_delta(vol.volumeDelta)

    async def _on_set_max_quality(self, msg: Any) -> None:
        if msg.HasField("srvrRndrSetMaxAudioQuality"):
            await self._cb.on_quality(msg.srvrRndrSetMaxAudioQuality.maxAudioQuality)

    async def _on_set_active(self, msg: Any) -> None:
        if not msg.HasField("srvrRndrSetActive"):
            return
        active = bool(msg.srvrRndrSetActive.active)
        self._logger.debug("Qobuz SET_ACTIVE active=%s", active)
        await self._cb.on_set_active(active)

    async def _on_set_loop_mode(self, msg: Any) -> None:
        if (mode := self._codec.parse_set_loop_mode(msg)) is not None:
            self._logger.debug("Qobuz SET_LOOP_MODE mode=%s", mode)
            await self._cb.on_loop_mode(mode)

    async def _on_set_shuffle_mode(self, msg: Any) -> None:
        if (shuffle := self._codec.parse_set_shuffle_mode(msg)) is not None:
            self._logger.debug("Qobuz SET_SHUFFLE_MODE on=%s", shuffle)
            await self._cb.on_shuffle_mode(shuffle)

    async def _on_set_autoplay_mode(self, msg: Any) -> None:
        if (autoplay := self._codec.parse_set_autoplay_mode(msg)) is not None:
            self._logger.debug("Qobuz SET_AUTOPLAY_MODE on=%s", autoplay)
            await self._cb.on_autoplay_mode(autoplay)

    async def _on_queue_load_ack(self, msg: Any) -> None:
        if ack := self._codec.parse_queue_load_ack(msg):
            self._logger.debug(
                "Qobuz queue-load ACK qv=%s tracks=%s",
                ack.queue_version,
                [_format_track_ref(track) for track in ack.tracks],
            )
            await self._cb.on_queue_load_ack(ack)

    async def _on_autoplay_load_ack(self, msg: Any) -> None:
        if ack := self._codec.parse_autoplay_load_ack(msg):
            self._logger.debug(
                "Qobuz autoplay-load ACK qv=%s tracks=%s",
                ack.queue_version,
                [_format_track_ref(track) for track in ack.tracks],
            )
            await self._cb.on_queue_load_ack(ack)

    async def _on_queue_error(self, msg: Any) -> None:
        if error := self._codec.parse_queue_error(msg):
            await self._cb.on_queue_error(error)

    async def _on_queue_version_changed(self, msg: Any) -> None:
        if version := self._codec.parse_queue_version_changed(msg):
            await self._cb.on_queue_version(version)

    async def _on_queue_state(self, msg: Any) -> None:
        if snapshot := self._codec.parse_queue_state(msg):
            self._logger.debug(
                "Qobuz QUEUE_STATE qv=%s tracks=%d shuffle=%s autoplay=%s",
                snapshot.queue_version,
                len(snapshot.tracks),
                snapshot.shuffle_mode,
                snapshot.autoplay_mode,
            )
            await self._cb.on_queue_state(snapshot)

    async def _on_queue_tracks_added(self, msg: Any) -> None:
        if added := self._codec.parse_queue_tracks_added(msg):
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_ADDED qv=%s tracks=%s",
                added.queue_version,
                [_format_track_ref(track) for track in added.tracks],
            )
            await self._cb.on_queue_tracks_added(added)

    async def _on_queue_tracks_inserted(self, msg: Any) -> None:
        if inserted := self._codec.parse_queue_tracks_inserted(msg):
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_INSERTED qv=%s after=%s tracks=%s",
                inserted.queue_version,
                inserted.insert_after,
                [_format_track_ref(track) for track in inserted.tracks],
            )
            await self._cb.on_queue_tracks_inserted(inserted)

    async def _on_queue_tracks_removed(self, msg: Any) -> None:
        if removed := self._codec.parse_queue_tracks_removed(msg):
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_REMOVED qv=%s ids=%s",
                removed.queue_version,
                removed.queue_item_ids,
            )
            await self._cb.on_queue_tracks_removed(removed)

    async def _on_queue_tracks_reordered(self, msg: Any) -> None:
        if reordered := self._codec.parse_queue_tracks_reordered(msg):
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_REORDERED qv=%s after=%s ids=%s",
                reordered.queue_version,
                reordered.insert_after,
                reordered.queue_item_ids,
            )
            await self._cb.on_queue_tracks_reordered(reordered)

    async def _on_queue_cleared(self, msg: Any) -> None:
        if cleared := self._codec.parse_queue_cleared(msg):
            self._logger.debug("Qobuz QUEUE_CLEARED qv=%s", cleared.queue_version)
            await self._cb.on_queue_cleared(cleared)

    async def _on_state_request(self, _msg: Any) -> None:
        self._logger.debug("Qobuz requested renderer state")
        await self._cb.on_state_request()

    async def _on_session_state(self, msg: Any) -> None:
        if event := self._codec.parse_session_state(msg):
            self._logger.debug(
                "Qobuz SESSION_STATE sessionId=%s qv=%s.%s trackIndex=%s",
                event.session_id,
                event.queue_version.major,
                event.queue_version.minor,
                event.track_index,
            )
            await self._cb.on_session_state(event)

    async def _on_add_renderer(self, msg: Any) -> None:
        if self._cb.on_add_renderer is None:
            self._logger.debug("Qobuz broadcast ignored: type=%s", msg.messageType)
            return
        if record := self._codec.parse_add_renderer(msg):
            self._logger.debug(
                "Qobuz ADD_RENDERER id=%s name=%s", record.renderer_id, record.friendly_name
            )
            await self._cb.on_add_renderer(record)

    async def _on_remove_renderer(self, msg: Any) -> None:
        if self._cb.on_remove_renderer is None:
            self._logger.debug("Qobuz broadcast ignored: type=%s", msg.messageType)
            return
        if (renderer_id := self._codec.parse_remove_renderer(msg)) is not None:
            self._logger.debug("Qobuz REMOVE_RENDERER id=%s", renderer_id)
            await self._cb.on_remove_renderer(renderer_id)

    async def _on_active_renderer_changed(self, msg: Any) -> None:
        if self._cb.on_active_renderer_changed is None:
            self._logger.debug("Qobuz broadcast ignored: type=%s", msg.messageType)
            return
        if (renderer_id := self._codec.parse_active_renderer_changed(msg)) is not None:
            self._logger.debug("Qobuz ACTIVE_RENDERER_CHANGED id=%s", renderer_id)
            await self._cb.on_active_renderer_changed(renderer_id)

    async def _on_renderer_state_updated(self, msg: Any) -> None:
        if self._cb.on_renderer_state_updated is None:
            self._logger.debug("Qobuz broadcast ignored: type=%s", msg.messageType)
            return
        if update := self._codec.parse_renderer_state_updated(msg):
            self._logger.debug(
                "Qobuz RENDERER_STATE_UPDATED id=%s state=%s pos=%s idx=%s",
                update.renderer_id,
                update.playing_state,
                update.position_ms,
                update.current_queue_index,
            )
            await self._cb.on_renderer_state_updated(update)

    # Dispatch table — populated below at class scope (`__class_getitem__`
    # style with the methods just defined). Keeps each branch one line
    # long and makes adding a new type a single entry.
    _HANDLER_TABLE: ClassVar[dict[int, Any]] = {}


InboundDispatcher._HANDLER_TABLE = {
    QConnectMessageType.ERROR: InboundDispatcher._on_error,
    QConnectMessageType.SRVR_RNDR_SET_STATE: InboundDispatcher._on_set_state,
    QConnectMessageType.SRVR_RNDR_SET_VOLUME: InboundDispatcher._on_set_volume,
    QConnectMessageType.SRVR_RNDR_SET_MAX_AUDIO_QUALITY: InboundDispatcher._on_set_max_quality,
    QConnectMessageType.SRVR_RNDR_SET_ACTIVE: InboundDispatcher._on_set_active,
    QConnectMessageType.SRVR_RNDR_SET_LOOP_MODE: InboundDispatcher._on_set_loop_mode,
    QConnectMessageType.SRVR_RNDR_SET_SHUFFLE_MODE: InboundDispatcher._on_set_shuffle_mode,
    QConnectMessageType.SRVR_RNDR_SET_AUTOPLAY_MODE: InboundDispatcher._on_set_autoplay_mode,
    QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED: InboundDispatcher._on_queue_load_ack,
    QConnectMessageType.SRVR_CTRL_AUTOPLAY_TRACKS_LOADED: InboundDispatcher._on_autoplay_load_ack,
    QConnectMessageType.SRVR_CTRL_QUEUE_ERROR_MESSAGE: InboundDispatcher._on_queue_error,
    QConnectMessageType.SRVR_CTRL_QUEUE_VERSION_CHANGED: InboundDispatcher._on_queue_version_changed,
    QConnectMessageType.SRVR_CTRL_QUEUE_STATE: InboundDispatcher._on_queue_state,
    QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_ADDED: InboundDispatcher._on_queue_tracks_added,
    QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_INSERTED: InboundDispatcher._on_queue_tracks_inserted,
    QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_REMOVED: InboundDispatcher._on_queue_tracks_removed,
    QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_REORDERED: InboundDispatcher._on_queue_tracks_reordered,
    QConnectMessageType.SRVR_CTRL_QUEUE_CLEARED: InboundDispatcher._on_queue_cleared,
    QConnectMessageType.CTRL_SRVR_ASK_FOR_RENDERER_STATE: InboundDispatcher._on_state_request,
    QConnectMessageType.SRVR_CTRL_SESSION_STATE: InboundDispatcher._on_session_state,
    QConnectMessageType.SRVR_CTRL_ADD_RENDERER: InboundDispatcher._on_add_renderer,
    QConnectMessageType.SRVR_CTRL_REMOVE_RENDERER: InboundDispatcher._on_remove_renderer,
    QConnectMessageType.SRVR_CTRL_ACTIVE_RENDERER_CHANGED: (
        InboundDispatcher._on_active_renderer_changed
    ),
    QConnectMessageType.SRVR_CTRL_RENDERER_STATE_UPDATED: (
        InboundDispatcher._on_renderer_state_updated
    ),
}
