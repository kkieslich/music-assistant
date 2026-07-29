"""
Chrome DevTools Protocol WebSocket recorder for Qobuz Connect captures.

Records every WebSocket frame (both directions) seen by a Playwright Page,
preserving full binary bytes. Output goes to ``.runs/<scenario>__client_*.json``
in a shape compatible with a Chrome WS-export schema (``version``,
``exportDate``, ``statistics``, ``eventTypes``, ``messages[]``).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import datetime as dt
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from music_assistant.providers.qobuz_connect.models import (
    OuterMessageType,
    QConnectMessageType,
)
from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, CDPSession, Page

LOGGER = logging.getLogger(__name__)

DEFAULT_URL_FILTER = "qobuz.com/ws"


@dataclass(slots=True)
class _ConnInfo:
    """Per-WebSocket connection metadata gathered from CDP events."""

    connection_id: int
    url: str


@dataclass(slots=True)
class _Frame:
    """One captured WebSocket frame in the export-ready shape."""

    direction: str
    opcode: int
    connection_id: int
    url: str
    timestamp_ms: int
    payload: bytes

    def to_export(self) -> dict[str, Any]:
        """Serialize to the capture-*.json frame shape."""
        return {
            "type": "message",
            "eventType": "text" if self.opcode == 1 else "binary",
            "direction": self.direction,
            "url": self.url,
            "connectionId": self.connection_id,
            "timestamp": self.timestamp_ms,
            "data": {str(i): b for i, b in enumerate(self.payload)},
            "size": len(self.payload),
        }


@dataclass
class WsRecorder:
    """
    Attach to a Playwright Page and record WebSocket frames matching url_filter.

    Intentionally not ``slots=True``: Playwright's impl-to-api mapping does a
    ``setattr`` on the bound handler's ``__self__`` to memoize the wrapped
    callable, which requires an instance ``__dict__``.

    Usage::

        recorder = WsRecorder(page)
        await recorder.start()
        # ... drive scenario via page-object methods ...
        await recorder.stop()
        recorder.write(output_path)

    Filtering: only frames on URLs containing `url_filter` are recorded; the
    Qobuz Web Client also opens analytics/heartbeat sockets we don't care
    about. Defaults to "qobuz.com/ws" which matches wss://qws-eu-prod.qobuz.com/ws.
    """

    page: Page
    url_filter: str = DEFAULT_URL_FILTER
    _cdp: CDPSession | None = None
    _connections: dict[str, _ConnInfo] = field(default_factory=dict)
    _next_connection_id: int = 1
    _frames: list[_Frame] = field(default_factory=list)
    _started: bool = False

    async def start(self) -> None:
        """Attach a CDP session to the page and subscribe to WebSocket events."""
        if self._started:
            return
        context: BrowserContext = self.page.context
        self._cdp = await context.new_cdp_session(self.page)
        self._cdp.on("Network.webSocketCreated", self._on_created)
        self._cdp.on("Network.webSocketFrameSent", self._on_frame_sent)
        self._cdp.on("Network.webSocketFrameReceived", self._on_frame_received)
        self._cdp.on("Network.webSocketClosed", self._on_closed)
        await self._cdp.send("Network.enable")
        self._started = True
        LOGGER.info("WsRecorder attached (filter=%r)", self.url_filter)

    async def stop(self) -> None:
        """Detach the CDP session. Frames captured so far remain available."""
        if not self._started or self._cdp is None:
            return
        try:
            await self._cdp.detach()
        except Exception:
            LOGGER.debug("CDP detach failed", exc_info=True)
        self._cdp = None
        self._started = False

    async def set_network_conditions(
        self,
        *,
        latency_ms: float = 0.0,
        download_kbps: float = 0.0,
        upload_kbps: float = 0.0,
        offline: bool = False,
    ) -> None:
        """
        Throttle this page's network via Chrome DevTools Protocol.

        ``0`` for ``download_kbps`` / ``upload_kbps`` means unlimited.
        Call :meth:`reset_network_conditions` to restore unlimited speed.

        :param latency_ms: Additional RTT applied to every request, in ms.
        :param download_kbps: Maximum downstream throughput, in kilobits/s.
        :param upload_kbps: Maximum upstream throughput, in kilobits/s.
        :param offline: If True, the network is fully blocked.
        """
        if self._cdp is None:
            raise RuntimeError("WsRecorder must be started before throttling network")
        bytes_per_kbit = 1024 / 8
        await self._cdp.send(
            "Network.emulateNetworkConditions",
            {
                "offline": offline,
                "latency": float(latency_ms),
                "downloadThroughput": float(download_kbps) * bytes_per_kbit
                if download_kbps
                else -1,
                "uploadThroughput": float(upload_kbps) * bytes_per_kbit if upload_kbps else -1,
            },
        )
        LOGGER.info(
            "set network conditions offline=%s latency=%.0fms down=%skbps up=%skbps",
            offline,
            latency_ms,
            download_kbps or "unlimited",
            upload_kbps or "unlimited",
        )

    async def reset_network_conditions(self) -> None:
        """Restore unlimited network throughput on this page."""
        await self.set_network_conditions()

    @property
    def frames(self) -> list[_Frame]:
        """Return captured frames (mutable list — caller must not modify)."""
        return self._frames

    @property
    def has_active_connection(self) -> bool:
        """True if at least one matching WebSocket has been opened."""
        return bool(self._connections)

    def renderer_id(self, friendly_name: str) -> int | None:
        """Return the latest cloud renderer ID advertised for ``friendly_name``."""
        renderer_id: int | None = None
        for message in self._incoming_messages():
            if (
                message.messageType == QConnectMessageType.SRVR_CTRL_ADD_RENDERER
                and message.HasField("srvrCtrlAddRenderer")
                and message.srvrCtrlAddRenderer.renderer.friendlyName == friendly_name
            ):
                renderer_id = int(message.srvrCtrlAddRenderer.rendererId)
        return renderer_id

    def cloud_queue_track_ids(self) -> tuple[str, ...]:
        """Reconstruct the latest cloud queue observed on this client's WebSocket."""
        queue: list[tuple[int, str]] = []
        for message in self._incoming_messages():
            if message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_STATE:
                queue = _wire_track_pairs(message.srvrCtrlQueueState.tracks)
            elif message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED:
                queue = _wire_track_pairs(message.srvrCtrlQueueTracksLoaded.tracks)
            elif message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_ADDED:
                queue.extend(_wire_track_pairs(message.srvrCtrlQueueTracksAdded.tracks))
            elif message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_INSERTED:
                event = message.srvrCtrlQueueTracksInserted
                index = max(0, min(int(event.insertAfter), len(queue)))
                queue[index:index] = _wire_track_pairs(event.tracks)
            elif message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_REMOVED:
                removed = set(message.srvrCtrlQueueTracksRemoved.queueItemIds)
                queue = [item for item in queue if item[0] not in removed]
            elif message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_REORDERED:
                event = message.srvrCtrlQueueTracksReordered
                moving_ids = tuple(event.queueItemIds)
                moving_set = set(moving_ids)
                by_id = dict(queue)
                moving = [(item_id, by_id[item_id]) for item_id in moving_ids if item_id in by_id]
                remaining = [item for item in queue if item[0] not in moving_set]
                index = max(0, min(int(event.insertAfter), len(remaining)))
                remaining[index:index] = moving
                queue = remaining
            elif message.messageType == QConnectMessageType.SRVR_CTRL_QUEUE_CLEARED:
                queue = []
        return tuple(track_id for _, track_id in queue)

    def _incoming_messages(self) -> list[Any]:
        """Decode all QConnect messages received by the browser client."""
        codec = QobuzConnectCodec(uuid.uuid4().bytes)
        messages: list[Any] = []
        for frame in self._frames:
            if frame.direction != "incoming":
                continue
            decoded = codec.decode_frame(frame.payload)
            if (
                decoded is None
                or decoded.msg_type is not OuterMessageType.PAYLOAD
                or decoded.payload is None
            ):
                continue
            batch = codec.decode_qconnect_batch(decoded.payload)
            if batch is not None:
                messages.extend(batch.messages)
        return messages

    async def wait_for_first_frame(
        self, timeout_seconds: float, poll_interval_seconds: float = 0.5
    ) -> bool:
        """
        Block until at least one frame is captured, then return True.

        :param timeout_seconds: Give up after this long; returns False.
        :param poll_interval_seconds: How often to re-check.

        Used as a robust "the user finished logging in" signal: as soon as
        the Qobuz Web Client hits the player and opens its WebSocket, a
        frame appears here. Beats DOM-sniffing for a localized button.
        """
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self._frames:
                return True
            await asyncio.sleep(poll_interval_seconds)
        return False

    def write(self, path: str | Path) -> Path:
        """
        Write captured frames to ``path`` in the capture-*.json schema.

        :param path: Output file path. Parent directory must exist.
        :returns: Absolute path of the written file.
        """
        out = Path(path).resolve()
        document = self._build_export()
        out.write_text(json.dumps(document, indent=2))
        LOGGER.info(
            "Wrote %d frames (%d bytes payload) to %s",
            len(self._frames),
            sum(f.payload.__len__() for f in self._frames),
            out,
        )
        return out

    def _build_export(self) -> dict[str, Any]:
        sent = [f for f in self._frames if f.direction == "outgoing"]
        recv = [f for f in self._frames if f.direction == "incoming"]
        text_msgs = [f for f in self._frames if f.opcode == 1]
        binary_msgs = [f for f in self._frames if f.opcode != 1]
        return {
            "version": "1.0",
            "exportDate": dt.datetime.now(dt.UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "statistics": {
                "totalConnections": len({f.connection_id for f in self._frames}),
                "totalMessages": len(self._frames),
                "sentMessages": len(sent),
                "receivedMessages": len(recv),
                "bytesSent": sum(len(f.payload) for f in sent),
                "bytesReceived": sum(len(f.payload) for f in recv),
            },
            "eventTypes": {
                "text": {
                    "count": len(text_msgs),
                    "incoming": sum(1 for f in text_msgs if f.direction == "incoming"),
                    "outgoing": sum(1 for f in text_msgs if f.direction == "outgoing"),
                },
                "binary": {
                    "count": len(binary_msgs),
                    "incoming": sum(1 for f in binary_msgs if f.direction == "incoming"),
                    "outgoing": sum(1 for f in binary_msgs if f.direction == "outgoing"),
                },
            },
            "messages": [f.to_export() for f in self._frames],
        }

    def _on_created(self, params: dict[str, Any]) -> None:
        url: str = params.get("url", "")
        if self.url_filter not in url:
            return
        request_id: str = params["requestId"]
        self._connections[request_id] = _ConnInfo(connection_id=self._next_connection_id, url=url)
        self._next_connection_id += 1
        LOGGER.info("WS opened: %s", url)

    def _on_closed(self, params: dict[str, Any]) -> None:
        request_id: str = params.get("requestId", "")
        conn = self._connections.get(request_id)
        if conn is not None:
            LOGGER.info("WS closed: %s", conn.url)

    def _on_frame_sent(self, params: dict[str, Any]) -> None:
        self._record(params, direction="outgoing")

    def _on_frame_received(self, params: dict[str, Any]) -> None:
        self._record(params, direction="incoming")

    def _record(self, params: dict[str, Any], *, direction: str) -> None:
        request_id: str = params.get("requestId", "")
        conn = self._connections.get(request_id)
        if conn is None:
            # Frame on a connection we don't track (different URL).
            return
        response = params.get("response") or {}
        opcode = int(response.get("opcode", 2))
        payload = _decode_payload(response.get("payloadData", ""), opcode)
        # CDP timestamps are seconds since an arbitrary monotonic origin.
        # Use wall-clock epoch_ms here so timestamps are interpretable across
        # captures; ordering within one capture is preserved by capture time.
        self._frames.append(
            _Frame(
                direction=direction,
                opcode=opcode,
                connection_id=conn.connection_id,
                url=conn.url,
                timestamp_ms=int(time.time() * 1000),
                payload=payload,
            )
        )


def _decode_payload(payload_data: str, opcode: int) -> bytes:
    """
    Decode CDP payloadData to raw bytes.

    Per the CDP spec the field is always a string; for binary opcodes (>=2) it
    is base64-encoded, for text opcodes (1) it is the literal text. Real Qobuz
    traffic occasionally sends protobuf bytes over a text opcode, so we try
    base64 first and fall back to a latin1 round-trip that preserves arbitrary
    byte values.
    """
    if not payload_data:
        return b""
    if opcode != 1:
        try:
            return base64.b64decode(payload_data, validate=True)
        except binascii.Error, ValueError:
            return payload_data.encode("latin1", errors="replace")
    # opcode == 1 (text): the existing captures show binary-looking content
    # was sometimes preserved here. Try base64 first because some Chromium
    # versions encode any non-UTF-8 frame that way; otherwise fall back to
    # the bytes as-encoded.
    try:
        decoded = base64.b64decode(payload_data, validate=True)
    except binascii.Error, ValueError:
        decoded = None
    if decoded is not None and len(decoded) > 0:
        return decoded
    return payload_data.encode("latin1", errors="replace")


def _wire_track_pairs(tracks: Any) -> list[tuple[int, str]]:
    """Return cloud slot and Qobuz track IDs from wire queue refs."""
    return [(int(track.queueItemId), str(track.trackId)) for track in tracks]
