"""
Capture-file decoder for the Qobuz Connect protocol-capture harness.

Reads a ``.runs/*.json`` capture written by :mod:`.ws_recorder`, decodes the
outer Qobuz Connect frames and inner QConnectBatch protobuf payloads, and
returns one :class:`DecodedFrame` per binary frame.

The goal is to let scenarios, audit scripts, and ad-hoc REPL exploration
share a single decoder instead of re-implementing the same loop every time.
Codec errors are captured into the ``error`` field on the result rather than
raised — a single malformed frame should not abort iteration over a capture.

Example::

    from tests.providers.qobuz_connect.protocol_capture.decoder import decode_capture

    for f in decode_capture("tests/providers/qobuz_connect/protocol_capture/.runs/rapid_skip__client_b.json"):
        if f.direction == "incoming" and "RndrSetState" in (f.inner_name or ""):
            print(f.t_rel_ms, f.inner_fields)
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from music_assistant.providers.qobuz_connect.protocol import QobuzConnectCodec

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class DecodedFrame:
    """One binary frame from a capture, fully decoded."""

    index: int
    direction: str  # "incoming" / "outgoing"
    t_abs_ms: int
    t_rel_ms: int
    outer_kind: str | None
    inner_name: str | None
    inner_fields: dict[str, Any] = field(default_factory=dict)
    inner_raw_bytes: int = 0
    error: str | None = None


def _to_bytes(data: dict[str, int] | str | None) -> bytes | None:
    if not data:
        return None
    if isinstance(data, dict):
        try:
            return bytes(data[str(k)] for k in range(len(data)))
        except (KeyError, ValueError):
            return None
    return None


def _summarize_fields(message: Any) -> dict[str, Any]:
    """Build a dict of {field_name: value} for the populated fields on a proto message."""
    out: dict[str, Any] = {}
    try:
        for fld, val in message.ListFields():
            if isinstance(val, (int, float, bool, str)):
                out[fld.name] = val
            elif isinstance(val, bytes):
                out[fld.name] = f"<{len(val)}b>"
            else:
                out[fld.name] = "<msg>"
    except Exception as err:  # diagnostic helper — never reraise
        LOGGER.debug("Field summary failed: %s", err)
    return out


def decode_capture(path: str | Path) -> Iterator[DecodedFrame]:
    """
    Iterate decoded binary frames from a capture file.

    :param path: Path to a ``.runs/*.json`` file written by ``WsRecorder``.
    :returns: An iterator of :class:`DecodedFrame`, in capture order.
    """
    codec = QobuzConnectCodec(uuid.uuid4().bytes)
    doc = json.loads(Path(path).read_text())
    msgs = [m for m in doc.get("messages", []) if m.get("eventType") == "binary"]
    if not msgs:
        return
    t0 = msgs[0]["timestamp"]
    for idx, m in enumerate(msgs):
        raw = _to_bytes(m.get("data"))
        rel = m["timestamp"] - t0
        if raw is None:
            yield DecodedFrame(
                index=idx,
                direction=m.get("direction", "?"),
                t_abs_ms=m["timestamp"],
                t_rel_ms=rel,
                outer_kind=None,
                inner_name=None,
                error="empty payload",
            )
            continue
        try:
            outer = codec.decode_frame(raw)
        except Exception as err:
            yield DecodedFrame(
                index=idx,
                direction=m.get("direction", "?"),
                t_abs_ms=m["timestamp"],
                t_rel_ms=rel,
                outer_kind=None,
                inner_name=None,
                error=f"outer decode: {err}",
            )
            continue
        outer_kind = type(outer).__name__
        payload = getattr(outer, "payload", None)
        if not payload:
            yield DecodedFrame(
                index=idx,
                direction=m["direction"],
                t_abs_ms=m["timestamp"],
                t_rel_ms=rel,
                outer_kind=outer_kind,
                inner_name=None,
            )
            continue
        try:
            batch = codec.decode_qconnect_batch(payload)
        except Exception as err:
            yield DecodedFrame(
                index=idx,
                direction=m["direction"],
                t_abs_ms=m["timestamp"],
                t_rel_ms=rel,
                outer_kind=outer_kind,
                inner_name=None,
                inner_raw_bytes=len(payload),
                error=f"inner decode: {err}",
            )
            continue
        if batch is None:
            yield DecodedFrame(
                index=idx,
                direction=m["direction"],
                t_abs_ms=m["timestamp"],
                t_rel_ms=rel,
                outer_kind=outer_kind,
                inner_name=None,
                inner_raw_bytes=len(payload),
                error="empty batch",
            )
            continue
        for batch_msg in batch.messages:
            for fld, val in batch_msg.ListFields():
                if not fld.message_type:
                    continue
                yield DecodedFrame(
                    index=idx,
                    direction=m["direction"],
                    t_abs_ms=m["timestamp"],
                    t_rel_ms=rel,
                    outer_kind=outer_kind,
                    inner_name=fld.name,
                    inner_fields=_summarize_fields(val),
                    inner_raw_bytes=len(payload),
                )


def format_frame(frame: DecodedFrame) -> str:
    """Render one decoded frame as a compact single-line summary."""
    if frame.error:
        return f"[{frame.direction:8s}] t+{frame.t_rel_ms:6d}ms  ERROR: {frame.error}"
    fields = ",".join(f"{k}={v}" for k, v in frame.inner_fields.items())
    inner = f"{frame.inner_name}({fields})" if frame.inner_name else "(no inner)"
    return f"[{frame.direction:8s}] t+{frame.t_rel_ms:6d}ms  {inner}"
