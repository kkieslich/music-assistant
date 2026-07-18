"""
Persistent flight recorder for the qobuz_connect provider.

Keeps a bounded in-memory ring of everything relevant to diagnosing a sync
problem — every reduced event with its effects and resulting state digest,
plus every WARNING+ log record from the provider's module tree — and writes
JSON dumps to disk so a "it felt wonky yesterday evening" report can be
debugged after the fact without console access.

What lands on disk (under ``<storage_path>/qobuz_connect/<instance_id>/``):

- ``incidents/incident-<utc timestamp>-<reason>.json`` — full ring dump,
  written whenever an ERROR-level record is seen (debounced) or a component
  calls :meth:`FlightRecorder.incident` explicitly. Oldest incidents are
  pruned beyond ``MAX_INCIDENTS``.
- ``rolling.json`` — the current ring, rewritten every
  ``ROLLING_DUMP_INTERVAL_S`` and on provider unload, so the most recent
  window always survives a crash/restart even when nothing ever logged an
  error.

Never stores tokens or credentials — it only ever sees log messages and the
reducer's own event/effect/state values, none of which carry secrets.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import time
from collections import Counter, deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from .sync_types import CanonicalState, Event, ReduceResult

# Module loggers (coordinator/session/protocol/...) live under this subtree;
# the provider's own ``self.logger`` lives under ``music_assistant.<name>``
# and is attached separately.
_MODULE_LOGGER_ROOT = "music_assistant.providers.qobuz_connect"

RING_SIZE = 4000
MAX_INCIDENTS = 30
ROLLING_DUMP_INTERVAL_S = 600
# Minimum seconds between two automatic (log-triggered) incident dumps, so a
# repeating error can't turn the recorder into a disk-write loop.
INCIDENT_DEBOUNCE_S = 60

LOGGER = logging.getLogger(__name__)


def _utc_stamp(ms: float | None = None) -> str:
    """Compact UTC timestamp (``20260718T142501Z``) for filenames/entries."""
    t = time.gmtime((ms or time.time() * 1000) / 1000)
    return time.strftime("%Y%m%dT%H%M%SZ", t)


class _RecorderLogHandler(logging.Handler):
    """Feeds WARNING+ log records from the provider's loggers into the recorder."""

    def __init__(self, recorder: FlightRecorder) -> None:
        super().__init__(level=logging.WARNING)
        self._recorder = recorder

    def emit(self, record: logging.LogRecord) -> None:
        """Append the record to the ring; trigger an incident dump on ERROR."""
        # A broken recorder must never break logging itself.
        with contextlib.suppress(Exception):
            self._recorder.on_log_record(record)


class FlightRecorder:
    """
    Bounded in-memory diagnostic ring with persistent incident/rolling dumps.

    :param base_dir: Directory to persist dumps under (created on start).
    :param state_getter: Returns the coordinator's current ``CanonicalState``,
        included as a snapshot in every dump; ``None`` disables the snapshot.
    """

    def __init__(
        self,
        base_dir: Path | str,
        state_getter: Callable[[], CanonicalState] | None = None,
    ) -> None:
        """Bind the recorder to its dump directory and optional state source."""
        self._base_dir = Path(base_dir)
        self._state_getter = state_getter
        self._ring: deque[dict[str, Any]] = deque(maxlen=RING_SIZE)
        self._counters: Counter[str] = Counter()
        self._handler: _RecorderLogHandler | None = None
        self._attached_loggers: list[logging.Logger] = []
        self._rolling_task: asyncio.Task[None] | None = None
        self._dump_tasks: set[asyncio.Task[None]] = set()
        self._last_auto_incident: float = 0.0
        self._started_at = time.time()

    # ---- lifecycle ---------------------------------------------------------

    async def start(self, extra_logger: logging.Logger | None = None) -> None:
        """
        Attach the log tap and start the rolling-dump task.

        :param extra_logger: The provider instance's own logger (which lives
            outside the module subtree), also tapped when given.
        """
        await asyncio.to_thread(self._prepare_dirs)
        self._handler = _RecorderLogHandler(self)
        for logger in {logging.getLogger(_MODULE_LOGGER_ROOT), extra_logger or LOGGER}:
            logger.addHandler(self._handler)
            self._attached_loggers.append(logger)
        self._rolling_task = asyncio.get_running_loop().create_task(self._rolling_loop())
        self.record("recorder", msg="flight recorder started")

    async def stop(self) -> None:
        """Detach, cancel background work and write a final rolling dump."""
        self.record("recorder", msg="flight recorder stopping")
        if self._rolling_task is not None:
            self._rolling_task.cancel()
            self._rolling_task = None
        for task in list(self._dump_tasks):
            task.cancel()
        if self._handler is not None:
            for logger in self._attached_loggers:
                logger.removeHandler(self._handler)
            self._attached_loggers.clear()
            self._handler = None
        try:
            await asyncio.to_thread(self._write_json, self._base_dir / "rolling.json")
        except Exception:
            LOGGER.debug("Flight recorder final dump failed", exc_info=True)

    # ---- recording entry points -------------------------------------------

    def record(self, kind: str, **fields: Any) -> None:
        """Append one structured entry to the ring."""
        self._counters[kind] += 1
        entry = {"t": _utc_stamp(), "kind": kind, **fields}
        self._ring.append(entry)

    def record_reduce(self, event: Event, result: ReduceResult) -> None:
        """Record one reducer step: the event, its effects and the state digest."""
        state = result.state
        self._counters[f"event:{type(event).__name__}"] += 1
        for effect in result.effects:
            self._counters[f"effect:{type(effect).__name__}"] += 1
        self.record(
            "reduce",
            event=_summarize_value(event),
            effects=[_summarize_value(e) for e in result.effects],
            state=_state_digest(state),
        )

    def record_reduce_failure(self, event: Event, err: BaseException) -> None:
        """Record a reducer/intake exception and dump an incident."""
        self.record("reduce_error", event=_summarize_value(event), error=repr(err))
        self.incident("reduce-error")

    def on_log_record(self, record: logging.LogRecord) -> None:
        """Ring a WARNING+ log record; ERROR triggers a debounced incident dump."""
        entry: dict[str, Any] = {
            "t": _utc_stamp(record.created * 1000),
            "kind": "log",
            "level": record.levelname,
            "logger": record.name.removeprefix(f"{_MODULE_LOGGER_ROOT}."),
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            entry["exc"] = repr(record.exc_info[1])
        self._counters[f"log:{record.levelname}"] += 1
        self._ring.append(entry)
        if record.levelno >= logging.ERROR:
            now = time.monotonic()
            if now - self._last_auto_incident >= INCIDENT_DEBOUNCE_S:
                self._last_auto_incident = now
                self.incident("error-log")

    def incident(self, reason: str) -> None:
        """Write the current ring to a new incident file (async, best-effort)."""
        self._counters["incidents"] += 1
        path = self._base_dir / "incidents" / f"incident-{_utc_stamp()}-{reason}.json"
        self._schedule_dump(path, prune=True)

    # ---- internals ---------------------------------------------------------

    def _prepare_dirs(self) -> None:
        (self._base_dir / "incidents").mkdir(parents=True, exist_ok=True)

    async def _rolling_loop(self) -> None:
        while True:
            await asyncio.sleep(ROLLING_DUMP_INTERVAL_S)
            try:
                await asyncio.to_thread(self._write_json, self._base_dir / "rolling.json")
            except Exception:
                LOGGER.debug("Flight recorder rolling dump failed", exc_info=True)

    def _schedule_dump(self, path: Path, *, prune: bool = False) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._dump(path, prune=prune))
        self._dump_tasks.add(task)
        task.add_done_callback(self._dump_tasks.discard)

    async def _dump(self, path: Path, *, prune: bool) -> None:
        try:
            await asyncio.to_thread(self._write_json, path)
            if prune:
                await asyncio.to_thread(self._prune_incidents)
        except Exception:
            LOGGER.debug("Flight recorder dump to %s failed", path, exc_info=True)

    def _write_json(self, path: Path) -> None:
        state_snapshot: dict[str, Any] | None = None
        if self._state_getter is not None:
            try:
                state_snapshot = _state_digest(self._state_getter(), full=True)
            except Exception:
                state_snapshot = {"error": "state getter failed"}
        doc = {
            "written": _utc_stamp(),
            "recorder_uptime_s": int(time.time() - self._started_at),
            "counters": dict(sorted(self._counters.items())),
            "state": state_snapshot,
            "entries": list(self._ring),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, separators=(",", ":"), default=str)
        tmp.replace(path)

    def _prune_incidents(self) -> None:
        incidents = sorted((self._base_dir / "incidents").glob("incident-*.json"))
        for stale in incidents[:-MAX_INCIDENTS]:
            with contextlib.suppress(OSError):
                stale.unlink()


def _summarize_value(value: Any, _depth: int = 0) -> Any:
    """Render an event/effect dataclass as a compact JSON-friendly dict."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        out: dict[str, Any] = {"_": type(value).__name__}
        for f in dataclasses.fields(value):
            out[f.name] = _summarize_value(getattr(value, f.name), _depth + 1)
        return out
    if isinstance(value, bytes):
        return value.hex()[:16]
    if isinstance(value, (tuple, list, frozenset, set)):
        seq = list(value)
        if len(seq) > 12 and _depth > 0:
            return [_summarize_value(v, _depth + 1) for v in seq[:12]] + [
                f"...+{len(seq) - 12} more"
            ]
        return [_summarize_value(v, _depth + 1) for v in seq]
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def _state_digest(state: CanonicalState, *, full: bool = False) -> dict[str, Any]:
    """Compact digest of ``CanonicalState`` — full track list only when ``full``."""
    digest: dict[str, Any] = {
        "active": state.active,
        "playing": getattr(state.playing, "name", str(state.playing)),
        "current_id": state.current_id,
        "position_ms": state.position_ms,
        "settling": state.settling_position,
        "cloud_v": f"{state.cloud_version.major}.{state.cloud_version.minor}",
        "tracks": len(state.tracks),
        "pending": [p.kind.value for p in state.pending],
        "own_rid": state.own_rid,
        "active_rid": state.active_rid,
    }
    if full:
        digest["track_ids"] = [t.track_id for t in state.tracks]
        digest["queue_item_ids"] = [t.queue_item_id for t in state.tracks]
    return digest
