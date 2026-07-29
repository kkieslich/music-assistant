"""
Live Music Assistant probe for the Qobuz Connect integration harness.

Owns a real ``music_assistant`` server process (or attaches to one already
running) and turns its debug log into structured, assertable events:

- ``ReduceTrace`` — one per ``coordinator.reduce`` line: the inbound event,
  the emitted effect names, and the resulting canonical state snapshot
  (active / playing / current_id / cloud_version / tracks).
- ``StreamStart`` — one per ``streams.audio`` "Start Streaming queue track"
  line: the Qobuz track id MA actually began streaming (ground truth for
  "what did MA play").
- ``Report`` — one per ``Qobuz report`` line: the renderer state MA reported
  back to the cloud (item slot, playing state, queue version).

The log is the integration oracle: assertions are written against these
events rather than against MA internals, so a scenario reproduces exactly
what a phone/web controller would observe.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parents[4]

# The silent BlackHole player MA renders to during integration runs.
# NOTE: MA's player-id scheme drifted from the old ``up<hex>`` form to a
# dashed uuid; keep this in sync with what ``Local Audio Out`` registers.
_BLACKHOLE = "b97b9910-b8fe-5ff0-946c-ef06b0d44273"

# Strip terminal colour codes MA emits so the regexes match cleanly.
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

_REDUCE = re.compile(
    r"reduce (?P<event>\w+)\(v=(?P<version>[^)]*)\) -> \[(?P<effects>[^\]]*)\] \| "
    r"active=(?P<active>\w+) playing=(?P<playing>\w+) current_id=(?P<current>\S+) "
    r"cloud_v=(?P<cmaj>\d+)\.(?P<cmin>\d+) tracks=(?P<tracks>\d+) pending=(?P<pending>\d+)"
)
_STREAM = re.compile(
    # Title may itself contain parentheses ("(Skit)", "(Album Version)"), so
    # match greedily up to the trailing " for queue" rather than the first ")".
    r"Start Streaming queue track: \S+://track/(?P<track>\d+) \((?P<title>.*)\) for queue"
)
_REPORT = re.compile(
    # The reporter log line dropped the separate ``buffer=`` field (the
    # canonical buffer_state is now folded into wire_buffer); keep this in
    # sync with OutboundReporter.report_state's debug line.
    r"Qobuz report state=(?P<state>\d+) wire_buffer=\d+ pos=(?P<pos>\d+)ms "
    r"\(anchor ts=\d+\) item=(?P<slot>\d+):(?P<track>\d+) qv=(?P<qmaj>\d+)\.(?P<qmin>\d+)"
)


@dataclass(slots=True)
class ReduceTrace:
    """One coordinator.reduce log line, parsed."""

    event: str
    effects: tuple[str, ...]
    active: bool
    playing: str
    current_id: str
    cloud_version: tuple[int, int]
    tracks: int
    pending: int
    raw: str


@dataclass(slots=True)
class StreamStart:
    """MA began streaming a concrete Qobuz track (what it actually played)."""

    track_id: int
    title: str
    raw: str


@dataclass(slots=True)
class Report:
    """A renderer-state report MA sent to the cloud."""

    state: int
    position_ms: int
    slot: int
    track_id: int
    queue_version: tuple[int, int]
    raw: str


@dataclass(slots=True)
class ProbeEvents:
    """All parsed events from a slice of the MA log, in file order."""

    reduces: list[ReduceTrace] = field(default_factory=list)
    streams: list[StreamStart] = field(default_factory=list)
    reports: list[Report] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)

    def effects(self) -> list[str]:
        """Flatten all emitted effect names across every reduce, in order."""
        out: list[str] = []
        for r in self.reduces:
            out.extend(r.effects)
        return out

    def last_reduce(self) -> ReduceTrace | None:
        """Return the final canonical-state snapshot in this slice."""
        return self.reduces[-1] if self.reduces else None


def _parse_line(line: str) -> ReduceTrace | StreamStart | Report | None:
    clean = _ANSI.sub("", line)
    if (m := _REDUCE.search(clean)) is not None:
        return ReduceTrace(
            event=m["event"],
            effects=tuple(e for e in m["effects"].split(",") if e and e != "-"),
            active=m["active"] == "True",
            playing=m["playing"],
            current_id=m["current"],
            cloud_version=(int(m["cmaj"]), int(m["cmin"])),
            tracks=int(m["tracks"]),
            pending=int(m["pending"]),
            raw=clean.rstrip(),
        )
    if (m := _STREAM.search(clean)) is not None:
        return StreamStart(track_id=int(m["track"]), title=m["title"], raw=clean.rstrip())
    if (m := _REPORT.search(clean)) is not None:
        return Report(
            state=int(m["state"]),
            position_ms=int(m["pos"]),
            slot=int(m["slot"]),
            track_id=int(m["track"]),
            queue_version=(int(m["qmaj"]), int(m["qmin"])),
            raw=clean.rstrip(),
        )
    return None


class MAProbe:
    """
    Manage (or attach to) a live MA server and read its log as events.

    :param log_path: File the MA process writes stdout/stderr to.
    :param data_dir: MA ``--data-dir`` (must have qobuz + qobuz_connect
        configured and the Connect target pinned to a silent player).
    :param cache_dir: MA ``--cache-dir``.
    """

    def __init__(self, log_path: Path, data_dir: Path, cache_dir: Path) -> None:
        """Bind the probe to an MA log/data/cache location (does not start MA)."""
        self.log_path = log_path
        self.data_dir = data_dir
        self.cache_dir = cache_dir
        self._proc: subprocess.Popen[bytes] | None = None
        self._saved_target: str | None = None

    def start(self, *, connect_timeout: float = 90.0) -> None:
        """Launch MA (Connect target pinned to BlackHole) and block until it connects."""
        self._pin_target(_BLACKHOLE)
        self.log_path.write_bytes(b"")
        log_fh = self.log_path.open("wb")
        self._proc = subprocess.Popen(  # noqa: S603 - fixed argv launching our own venv python
            [
                str(REPO_ROOT / ".venv/bin/python"),
                "-m",
                "music_assistant",
                "--data-dir",
                str(self.data_dir),
                "--cache-dir",
                str(self.cache_dir),
                "--log-level",
                "debug",
            ],
            cwd=str(REPO_ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
        )
        self.wait_for("Qobuz Connect WebSocket connected", timeout=connect_timeout)

    @property
    def managed_pid(self) -> int | None:
        """Return the PID of the MA process started by this probe."""
        return self._proc.pid if self._proc is not None else None

    def stop(self) -> None:
        """Terminate MA if this probe started it, then restore the target player."""
        if self._proc is not None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            self._proc = None
        self._restore_target()

    def cursor(self) -> int:
        """Return the current byte length of the log; pass to :meth:`events_since`."""
        return self.log_path.stat().st_size if self.log_path.exists() else 0

    def events_since(self, cursor: int) -> ProbeEvents:
        """Parse every log line written after ``cursor`` into structured events."""
        events = ProbeEvents()
        with self.log_path.open("rb") as fh:
            fh.seek(cursor)
            chunk = fh.read().decode("utf-8", errors="replace")
        for line in chunk.splitlines():
            events.lines.append(line)
            parsed = _parse_line(line)
            if isinstance(parsed, ReduceTrace):
                events.reduces.append(parsed)
            elif isinstance(parsed, StreamStart):
                events.streams.append(parsed)
            elif isinstance(parsed, Report):
                events.reports.append(parsed)
        return events

    def wait_for(self, needle: str, *, timeout: float = 30.0, cursor: int = 0) -> bool:
        """Block until ``needle`` appears in the log (after ``cursor``) or timeout."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._has(needle, cursor):
                return True
            time.sleep(0.3)
        return False

    def wait_for_event(
        self,
        cursor: int,
        predicate: Callable[[ProbeEvents], bool],
        *,
        timeout: float = 25.0,
        poll: float = 0.5,
    ) -> ProbeEvents:
        """
        Poll parsed events since ``cursor`` until ``predicate`` holds or timeout.

        :param cursor: Byte offset from :meth:`cursor` taken before the action.
        :param predicate: Called with the accumulated events; return True to stop.
        :param timeout: Max seconds to wait.
        :param poll: Seconds between polls.
        :returns: The events slice when the predicate held, else the final slice
            at timeout.
        """
        deadline = time.monotonic() + timeout
        events = self.events_since(cursor)
        while time.monotonic() < deadline:
            events = self.events_since(cursor)
            if predicate(events):
                return events
            time.sleep(poll)
        return events

    def _has(self, needle: str, cursor: int) -> bool:
        if not self.log_path.exists():
            return False
        with self.log_path.open("rb") as fh:
            fh.seek(cursor)
            return needle in fh.read().decode("utf-8", errors="replace")

    def _pin_target(self, player_id: str) -> None:
        settings = self.data_dir / "settings.json"
        data = json.loads(settings.read_text())
        for value in data.get("providers", {}).values():
            if isinstance(value, dict) and value.get("domain") == "qobuz_connect":
                self._saved_target = value["values"].get("target_player")
                value["values"]["target_player"] = player_id
        settings.write_text(json.dumps(data, indent=1))

    def _restore_target(self) -> None:
        if self._saved_target is None:
            return
        settings = self.data_dir / "settings.json"
        data = json.loads(settings.read_text())
        for value in data.get("providers", {}).values():
            if isinstance(value, dict) and value.get("domain") == "qobuz_connect":
                value["values"]["target_player"] = self._saved_target
        settings.write_text(json.dumps(data, indent=1))
        self._saved_target = None
