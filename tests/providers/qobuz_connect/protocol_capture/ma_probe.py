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
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import psutil
from cryptography.fernet import Fernet

from music_assistant.constants import CONF_ENCRYPTION_KEY, ENCRYPT_SUFFIX

if TYPE_CHECKING:
    from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parents[4]

# The silent BlackHole player MA renders to during integration runs.
# NOTE: MA's player-id scheme drifted from the old ``up<hex>`` form to a
# dashed uuid; keep this in sync with what ``Local Audio Out`` registers.
_BLACKHOLE = "b97b9910-b8fe-5ff0-946c-ef06b0d44273"
MANAGED_CONNECT_TARGET = "Local Dev Hardening prnMvCkz"
_MANAGED_CONNECT_PORT = 8695
_MANAGED_PORTS = (8095, _MANAGED_CONNECT_PORT)

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
_FILE_QUALITY = re.compile(
    r"Qobuz file quality report quality=(?P<quality>\d+) "
    r"sample_rate=(?P<rate>\d+) bit_depth=(?P<depth>\d+) channels=(?P<channels>\d+)"
)
_MAX_QUALITY = re.compile(r"Qobuz maximum quality report quality=(?P<quality>\d+)")


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
class QualityReport:
    """One exact file-quality or configured-maximum report sent to Qobuz."""

    kind: str
    quality: int
    sample_rate: int | None
    bit_depth: int | None
    channels: int | None
    raw: str


@dataclass(slots=True)
class ProbeEvents:
    """All parsed events from a slice of the MA log, in file order."""

    reduces: list[ReduceTrace] = field(default_factory=list)
    streams: list[StreamStart] = field(default_factory=list)
    reports: list[Report] = field(default_factory=list)
    qualities: list[QualityReport] = field(default_factory=list)
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


def _parse_line(line: str) -> ReduceTrace | StreamStart | Report | QualityReport | None:
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
    if (m := _FILE_QUALITY.search(clean)) is not None:
        return QualityReport(
            kind="file",
            quality=int(m["quality"]),
            sample_rate=int(m["rate"]),
            bit_depth=int(m["depth"]),
            channels=int(m["channels"]),
            raw=clean.rstrip(),
        )
    if (m := _MAX_QUALITY.search(clean)) is not None:
        return QualityReport(
            kind="maximum",
            quality=int(m["quality"]),
            sample_rate=None,
            bit_depth=None,
            channels=None,
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

    def __init__(
        self,
        log_path: Path,
        data_dir: Path,
        cache_dir: Path,
        run_dir: tempfile.TemporaryDirectory[str] | None = None,
    ) -> None:
        """Bind the probe to an MA log/data/cache location (does not start MA)."""
        self.log_path = log_path
        self.data_dir = data_dir
        self.cache_dir = cache_dir
        self._run_dir = run_dir
        self._proc: subprocess.Popen[bytes] | None = None
        self._saved_connect_config: (
            tuple[str, dict[str, object], dict[str, object] | None] | None
        ) = None
        self._managed_connect_instance_id: str | None = None

    def start(self, *, connect_timeout: float = 90.0) -> None:
        """Launch MA (Connect target pinned to BlackHole) and block until it connects."""
        try:
            occupied = _listening_port_owners(_MANAGED_PORTS)
            if occupied:
                details = ", ".join(f"{port} (pid={pid})" for port, pid in sorted(occupied.items()))
                raise RuntimeError(f"Managed integration ports are occupied: {details}")
            self._pin_target(_BLACKHOLE)
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("wb") as log_fh:
                self._proc = subprocess.Popen(  # noqa: S603 - fixed argv
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
            self._wait_until_ready(connect_timeout)
            if not self.owns_managed_ports():
                raise RuntimeError("Managed MA process does not own ports 8095 and 8695")
        except BaseException:
            self.stop()
            raise

    @property
    def managed_pid(self) -> int | None:
        """Return the PID of the MA process started by this probe."""
        if self._proc is None or self._proc.poll() is not None:
            return None
        return self._proc.pid

    def owns_managed_ports(self) -> bool:
        """Return whether the live managed child owns both harness listeners."""
        pid = self.managed_pid
        if pid is None:
            return False
        owners = _process_listening_port_owners(pid, _MANAGED_PORTS)
        return all(owners.get(port) == pid for port in _MANAGED_PORTS)

    def stop(self) -> None:
        """Terminate MA if this probe started it, then restore the target player."""
        try:
            if self._proc is not None:
                if self._proc.poll() is None:
                    self._proc.terminate()
                    try:
                        self._proc.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        self._proc.kill()
                        self._proc.wait(timeout=20)
                self._proc = None
        finally:
            try:
                self._restore_target()
            finally:
                if self._run_dir is not None:
                    self._run_dir.cleanup()
                    self._run_dir = None

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
            elif isinstance(parsed, QualityReport):
                events.qualities.append(parsed)
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

    def managed_connect_config(self) -> dict[str, object] | None:
        """Return the non-secret managed setup fields currently pinned on disk."""
        if self._managed_connect_instance_id is None:
            return None
        settings = self.data_dir / "settings.json"
        data = json.loads(settings.read_text())
        provider = data.get("providers", {}).get(self._managed_connect_instance_id)
        if not isinstance(provider, dict):
            return None
        setup_data = provider.get("setup_data")
        if not isinstance(setup_data, dict):
            return None
        return {
            "instance_id": self._managed_connect_instance_id,
            "target_player": _decrypt_setup_string(data, setup_data.get("target_player")),
            "publish_name": _decrypt_setup_string(data, setup_data.get("publish_name")),
            "http_port": setup_data.get("http_port"),
            "qobuz_provider": _decrypt_setup_string(data, setup_data.get("qobuz_provider")),
            "initial_volume": setup_data.get("initial_volume"),
        }

    def _has(self, needle: str, cursor: int) -> bool:
        if not self.log_path.exists():
            return False
        with self.log_path.open("rb") as fh:
            fh.seek(cursor)
            return needle in fh.read().decode("utf-8", errors="replace")

    def _wait_until_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._proc is None:
                raise RuntimeError("Managed MA process was not started")
            if (status := self._proc.poll()) is not None:
                raise RuntimeError(f"Managed MA process exited with status {status}")
            if self._has("Qobuz Connect WebSocket connected", 0):
                return
            time.sleep(0.3)
        raise TimeoutError("Managed MA did not connect to Qobuz before startup timeout")

    def _pin_target(self, player_id: str) -> None:
        settings = self.data_dir / "settings.json"
        data = json.loads(settings.read_text())
        providers = data.get("providers", {})
        if not isinstance(providers, dict):
            raise TypeError("Managed integration data has no provider configuration")
        qobuz_instance = next(
            (
                key
                for key, value in providers.items()
                if isinstance(value, dict) and value.get("domain") == "qobuz"
            ),
            None,
        )
        if qobuz_instance is None:
            raise RuntimeError("Managed integration data has no native Qobuz provider")
        connect_providers = [
            (key, value)
            for key, value in providers.items()
            if isinstance(value, dict) and value.get("domain") == "qobuz_connect"
        ]
        if len(connect_providers) != 1:
            raise RuntimeError(
                "Managed integration data must have exactly one Qobuz Connect provider"
            )
        instance_id, provider = connect_providers[0]
        values = provider.get("values")
        if not isinstance(values, dict):
            raise TypeError("Managed Qobuz Connect provider has invalid option values")
        setup_data = provider.get("setup_data")
        if setup_data is not None and not isinstance(setup_data, dict):
            raise TypeError("Managed Qobuz Connect provider has invalid setup data")
        self._saved_connect_config = (
            instance_id,
            dict(values),
            dict(setup_data) if setup_data is not None else None,
        )
        managed_setup = dict(setup_data or {})
        managed_setup.update(
            {
                "target_player": _encrypt_setup_string(data, player_id),
                "publish_name": _encrypt_setup_string(data, MANAGED_CONNECT_TARGET),
                "http_port": _MANAGED_CONNECT_PORT,
                "qobuz_provider": _encrypt_setup_string(data, qobuz_instance),
                "initial_volume": 25,
            }
        )
        provider["setup_data"] = managed_setup
        values.setdefault("max_quality", "27")
        self._managed_connect_instance_id = instance_id
        settings.write_text(json.dumps(data, indent=1))

    def _restore_target(self) -> None:
        if self._saved_connect_config is None:
            return
        settings = self.data_dir / "settings.json"
        data = json.loads(settings.read_text())
        instance_id, saved_values, saved_setup_data = self._saved_connect_config
        provider = data.get("providers", {}).get(instance_id)
        if isinstance(provider, dict):
            provider["values"] = saved_values
            if saved_setup_data is None:
                provider.pop("setup_data", None)
            else:
                provider["setup_data"] = saved_setup_data
        settings.write_text(json.dumps(data, indent=1))
        self._saved_connect_config = None
        self._managed_connect_instance_id = None


def _encrypt_setup_string(data: dict[str, object], value: str) -> str:
    """Encrypt one managed setup string with the copied MA server key."""
    encryption_key = data.get(CONF_ENCRYPTION_KEY)
    if not isinstance(encryption_key, str) or not encryption_key:
        raise RuntimeError("Managed integration data has no encryption key")
    return ENCRYPT_SUFFIX + Fernet(encryption_key.encode()).encrypt(value.encode()).decode()


def _decrypt_setup_string(data: dict[str, object], value: object) -> object:
    """Decrypt one whitelisted managed setup string for preflight comparison."""
    if not isinstance(value, str) or not value.startswith(ENCRYPT_SUFFIX):
        return value
    encryption_key = data.get(CONF_ENCRYPTION_KEY)
    if not isinstance(encryption_key, str) or not encryption_key:
        raise RuntimeError("Managed integration data has no encryption key")
    token = value.removeprefix(ENCRYPT_SUFFIX)
    return Fernet(encryption_key.encode()).decrypt(token.encode()).decode()


def _listening_port_owners(ports: tuple[int, ...]) -> dict[int, int | None]:
    """Return each requested port accepting loopback TCP connections."""
    owners: dict[int, int | None] = {}
    for port in ports:
        for host in ("127.0.0.1", "::1"):
            try:
                with socket.create_connection((host, port), timeout=0.2):
                    owners[port] = None
                    break
            except OSError:
                continue
    return owners


def _process_listening_port_owners(
    process_id: int,
    ports: tuple[int, ...],
) -> dict[int, int]:
    """Return requested TCP listeners owned by one managed process."""
    requested = set(ports)
    try:
        connections = psutil.Process(process_id).net_connections(kind="tcp")
    except psutil.Error:
        return {}
    return {
        connection.laddr.port: process_id
        for connection in connections
        if (
            connection.status == psutil.CONN_LISTEN
            and connection.laddr
            and connection.laddr.port in requested
        )
    }
