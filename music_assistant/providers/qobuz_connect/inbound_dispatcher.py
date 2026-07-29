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

    __slots__ = (
        "_cb",
        "_codec",
        "_logger",
        "_on_dispatch_error",
        "_on_error_message",
    )

    def __init__(
        self,
        codec: QobuzConnectCodec,
        callbacks: SessionCallbacks,
        *,
        label: str = "",
        on_error_message: Callable[[str], Awaitable[None]] | None = None,
        on_dispatch_error: Callable[[int, Exception], None] | None = None,
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
        :param on_dispatch_error: Optional observer for exceptions contained
            at the per-message boundary.
        """
        self._codec = codec
        self._cb = callbacks
        self._logger = LOGGER.getChild(label) if label else LOGGER
        self._on_error_message = on_error_message
        self._on_dispatch_error = on_dispatch_error

    async def dispatch(self, msg: Any) -> None:
        """Parse and forward a single inner message."""
        msg_type = msg.messageType
        handler = self._HANDLER_TABLE.get(msg_type)
        if handler is not None:
            # Containment boundary: dispatch runs inline in the session's
            # receive loop, so a raising parser/translator/reducer used to
            # tear down the whole websocket — and since the cloud re-pushes
            # current state on reconnect, one poison message meant reconnect
            # churn. One bad message must never cost the connection.
            try:
                await handler(self, msg)
            except Exception as err:
                self._logger.exception(
                    "Error handling Qobuz Connect message type %s; message skipped", msg_type
                )
                if self._on_dispatch_error is not None:
                    self._on_dispatch_error(msg_type, err)
            return
        if msg_type in _KNOWN_IGNORED_MESSAGE_TYPES:
            self._logger.debug("Qobuz broadcast ignored: type=%s", msg_type)
            return
        self._logger.debug("Unhandled Qobuz Connect message type: %s", msg_type)

    # ---- per-type handlers (kept short; the table maps to each) ----------

    async def _on_error(self, msg: Any) -> None:
        # Message-level rejection — observed live when the cloud has
        # deregistered this device but the socket is still open.
        code = msg.error.code if msg.HasField("error") else "?"
        message = msg.error.message if msg.HasField("error") else ""
        self._logger.warning("Qobuz Connect message error %s: %s", code, message)
        if self._on_error_message is not None:
            await self._on_error_message(str(message))

    async def _on_set_state(self, msg: Any) -> None:
        if (event := self._codec.parse_set_state(msg)) is not None:
            self._logger.debug(
                "Qobuz SET_STATE state=%s pos=%s current=%s qv=%s",
                event.playing,
                event.position_ms,
                _format_track_ref(event.current_ref),
                event.version,
            )
            await self._cb.submit(event)

    async def _on_set_volume(self, msg: Any) -> None:
        if (event := self._codec.parse_set_volume(msg)) is not None:
            await self._cb.submit(event)

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
        if (event := self._codec.parse_set_loop_mode(msg)) is not None:
            self._logger.debug("Qobuz SET_LOOP_MODE mode=%s", event.loop)
            await self._cb.submit(event)

    async def _on_set_shuffle_mode(self, msg: Any) -> None:
        if (event := self._codec.parse_set_shuffle_mode(msg)) is not None:
            self._logger.debug("Qobuz SET_SHUFFLE_MODE on=%s", event.shuffle)
            await self._cb.submit(event)

    async def _on_set_autoplay_mode(self, msg: Any) -> None:
        if (event := self._codec.parse_set_autoplay_mode(msg)) is not None:
            self._logger.debug("Qobuz SET_AUTOPLAY_MODE on=%s", event.autoplay)
            await self._cb.submit(event)

    async def _on_queue_load_ack(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_load_ack(msg)) is not None:
            self._logger.debug(
                "Qobuz queue-load ACK qv=%s tracks=%s",
                event.version,
                [_format_track_ref(track) for track in event.tracks],
            )
            await self._cb.submit(event)

    async def _on_autoplay_load_ack(self, msg: Any) -> None:
        if (event := self._codec.parse_autoplay_load_ack(msg)) is not None:
            self._logger.debug(
                "Qobuz autoplay-load ACK qv=%s tracks=%s",
                event.version,
                [_format_track_ref(track) for track in event.tracks],
            )
            await self._cb.submit(event)

    async def _on_queue_error(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_error(msg)) is not None:
            await self._cb.submit(event)

    async def _on_queue_version_changed(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_version_changed(msg)) is not None:
            await self._cb.submit(event)

    async def _on_queue_state(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_state(msg)) is not None:
            self._logger.debug(
                "Qobuz QUEUE_STATE qv=%s tracks=%d shuffle=%s autoplay=%s",
                event.version,
                len(event.tracks),
                event.shuffle,
                event.autoplay,
            )
            await self._cb.submit(event)

    async def _on_queue_tracks_added(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_tracks_added(msg)) is not None:
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_ADDED qv=%s tracks=%s",
                event.version,
                [_format_track_ref(track) for track in event.tracks],
            )
            await self._cb.submit(event)

    async def _on_queue_tracks_inserted(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_tracks_inserted(msg)) is not None:
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_INSERTED qv=%s after=%s tracks=%s",
                event.version,
                event.insert_after,
                [_format_track_ref(track) for track in event.tracks],
            )
            await self._cb.submit(event)

    async def _on_queue_tracks_removed(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_tracks_removed(msg)) is not None:
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_REMOVED qv=%s ids=%s",
                event.version,
                event.queue_item_ids,
            )
            await self._cb.submit(event)

    async def _on_queue_tracks_reordered(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_tracks_reordered(msg)) is not None:
            self._logger.debug(
                "Qobuz QUEUE_TRACKS_REORDERED qv=%s after=%s ids=%s",
                event.version,
                event.insert_after,
                event.queue_item_ids,
            )
            await self._cb.submit(event)

    async def _on_queue_cleared(self, msg: Any) -> None:
        if (event := self._codec.parse_queue_cleared(msg)) is not None:
            self._logger.debug("Qobuz QUEUE_CLEARED qv=%s", event.version)
            await self._cb.submit(event)

    async def _on_state_request(self, msg: Any) -> None:
        self._logger.debug("Qobuz requested renderer state")
        await self._cb.submit(self._codec.parse_state_request(msg))

    async def _on_session_state(self, msg: Any) -> None:
        if (event := self._codec.parse_session_state(msg)) is not None:
            self._logger.debug(
                "Qobuz SESSION_STATE qv=%s.%s trackIndex=%s",
                event.version.major,
                event.version.minor,
                event.track_index,
            )
            await self._cb.submit(event)

    async def _on_add_renderer(self, msg: Any) -> None:
        if (event := self._codec.parse_add_renderer(msg)) is not None:
            self._logger.debug("Qobuz ADD_RENDERER id=%s", event.renderer_id)
            await self._cb.submit(event)

    async def _on_remove_renderer(self, msg: Any) -> None:
        if (event := self._codec.parse_remove_renderer(msg)) is not None:
            self._logger.debug("Qobuz REMOVE_RENDERER id=%s", event.renderer_id)
            await self._cb.submit(event)

    async def _on_active_renderer_changed(self, msg: Any) -> None:
        if (event := self._codec.parse_active_renderer_changed(msg)) is not None:
            self._logger.debug("Qobuz ACTIVE_RENDERER_CHANGED id=%s", event.renderer_id)
            await self._cb.submit(event)

    async def _on_renderer_state_updated(self, msg: Any) -> None:
        if (event := self._codec.parse_renderer_state_updated(msg)) is not None:
            self._logger.debug(
                "Qobuz RENDERER_STATE_UPDATED id=%s state=%s pos=%s idx=%s",
                event.renderer_id,
                event.playing,
                event.position_ms,
                event.current_index,
            )
            await self._cb.submit(event)

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
