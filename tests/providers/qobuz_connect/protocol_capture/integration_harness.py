"""
Live integration harness: two real Qobuz web clients + one real MA renderer.

Drives a genuine Qobuz Web Client (Playwright, the controller) against the
real Qobuz cloud, with a real Music Assistant instance joined as the
``Local Dev`` Connect renderer. Each scenario resets to a clean state, runs
a controller action, and asserts MA's observable behaviour from its debug
log (via :class:`MAProbe`).

This is the automated replacement for hand-testing on physical hardware:
the web client stands in for the phone app, MA is the real renderer, and
the cloud is real. Behaviour that must match "what a web client does" is
grounded against reference captures in ``.runs/`` where non-obvious.

Not pytest-collected (see conftest ``collect_ignore``); run via
``integration_run.py``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .audio_probe import AudioProbe
from .harness import AUTH_DIR, _open_client
from .ma_probe import MAProbe, ProbeEvents

if TYPE_CHECKING:
    from playwright.async_api import Browser, Playwright

    from .harness import ClientHandle
    from .qobuz_page import QobuzPage

LOGGER = logging.getLogger(__name__)

# Daft Punk — Discovery. Stable album; 16 tracks, ids 1065476..1065491.
ALBUM_URL = "https://play.qobuz.com/album/0724384260958"
CONNECT_TARGET = "Local Dev"

# The Connect target's silent MA player (BlackHole) — where MA renders audio
# during integration runs so nothing is played out loud. Must match
# ma_probe._BLACKHOLE (MA's player-id scheme drifted from ``up<hex>`` to a
# dashed uuid).
BLACKHOLE_PLAYER_ID = "b97b9910-b8fe-5ff0-946c-ef06b0d44273"
MA_WS_URL = "ws://localhost:8095/ws"


@dataclass(slots=True)
class Check:
    """One assertion outcome within a scenario."""

    name: str
    passed: bool
    detail: str = ""


class ScenarioStatus(StrEnum):
    """Authoritative outcome of one live integration scenario."""

    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(slots=True)
class ScenarioResult:
    """Aggregate outcome of a scenario run."""

    scenario: str
    checks: list[Check] = field(default_factory=list)
    skip_reason: str | None = None

    @property
    def status(self) -> ScenarioStatus:
        """Return the scenario's authoritative status."""
        if self.skip_reason is not None:
            return ScenarioStatus.SKIP
        if self.checks and all(check.passed for check in self.checks):
            return ScenarioStatus.PASS
        return ScenarioStatus.FAIL

    @property
    def passed(self) -> bool:
        """Return whether every check passed (and at least one ran)."""
        return self.status is ScenarioStatus.PASS

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        """Record one named assertion outcome on this result."""
        self.checks.append(Check(name=name, passed=passed, detail=detail))

    def skip(self, reason: str) -> None:
        """Mark the scenario as not run because a required prerequisite is absent."""
        self.skip_reason = reason


@dataclass(slots=True, frozen=True)
class PreflightResult:
    """Outcome of the live playback safety preflight."""

    checks: tuple[str, ...]
    failures: tuple[str, ...]

    @property
    def passed(self) -> bool:
        """Return whether every mandatory safety condition passed."""
        return not self.failures


class SafetyPreflight:
    """Validate the exact managed MA and silent target before live playback."""

    def __init__(
        self,
        *,
        qobuz: Any,
        qobuz_observer: Any | None = None,
        ma_query_func: Callable[[str, dict[str, object]], Awaitable[object]],
        connect_target: str,
        managed_pid: int | None,
    ) -> None:
        """Store preflight dependencies."""
        self._qobuz = qobuz
        self._qobuz_observer = qobuz_observer
        self._ma_query = ma_query_func
        self._connect_target = connect_target
        self._managed_pid = managed_pid

    async def run(self) -> PreflightResult:
        """Run every mandatory safety check."""
        checks: list[str] = []
        failures: list[str] = []
        if self._managed_pid is None:
            failures.append("No identified managed MA process is available")
        else:
            checks.append(f"managed MA process pid={self._managed_pid}")

        provider_result = await self._ma_query(
            "config/providers",
            {"provider_domain": "qobuz_connect", "include_values": True},
        )
        providers = provider_result if isinstance(provider_result, list) else []
        matching_providers = [
            provider
            for provider in providers
            if isinstance(provider, dict)
            and provider.get("domain") == "qobuz_connect"
            and isinstance(provider.get("values"), dict)
            and provider["values"].get("publish_name") == self._connect_target
        ]
        if len(matching_providers) != 1:
            failures.append(
                f"Expected exactly one loaded MA provider named {self._connect_target!r}"
            )
        else:
            provider = matching_providers[0]
            target = provider["values"].get("target_player")
            if target != BLACKHOLE_PLAYER_ID:
                failures.append(
                    f"Qobuz Connect target player is {target!r}, expected BlackHole "
                    f"{BLACKHOLE_PLAYER_ID!r}"
                )
            else:
                checks.append(f"provider target={BLACKHOLE_PLAYER_ID}")
            if provider.get("last_error"):
                failures.append(f"Qobuz Connect provider error: {provider['last_error']}")

        player_result = await self._ma_query(
            "players/all",
            {
                "return_unavailable": True,
                "return_disabled": True,
                "return_protocol_players": False,
            },
        )
        players = player_result if isinstance(player_result, list) else []
        blackholes = [
            player
            for player in players
            if isinstance(player, dict) and player.get("player_id") == BLACKHOLE_PLAYER_ID
        ]
        if len(blackholes) != 1:
            failures.append(f"Expected exactly one BlackHole player {BLACKHOLE_PLAYER_ID!r}")
        else:
            blackhole = blackholes[0]
            name = blackhole.get("name") or blackhole.get("display_name")
            if (
                name != "BlackHole 2ch"
                or blackhole.get("provider") != "local_audio"
                or blackhole.get("available") is not True
            ):
                failures.append(
                    "BlackHole target is not the available local_audio player "
                    f"(name={name!r}, provider={blackhole.get('provider')!r}, "
                    f"available={blackhole.get('available')!r})"
                )
            else:
                checks.append("BlackHole 2ch is available via local_audio")

        cloud_clients = [("A", self._qobuz)]
        if self._qobuz_observer is not None:
            cloud_clients.append(("B", self._qobuz_observer))
        for label, qobuz in cloud_clients:
            outputs = await qobuz.network_output_names()
            if outputs.count(self._connect_target) != 1:
                failures.append(
                    f"Cloud renderer {self._connect_target!r} must appear exactly once "
                    f"on client {label}; visible outputs={outputs!r}"
                )
            else:
                checks.append(f"client {label} cloud renderer={self._connect_target}")

        if not failures:
            for label, qobuz in cloud_clients:
                await qobuz.select_local_output(expected_name="Web Player Chrome")
                selected = await qobuz.selected_output_name()
                if selected != "Web Player Chrome":
                    failures.append(
                        f"Client {label} browser-local output selection did not stick: "
                        f"selected={selected!r}"
                    )
                else:
                    checks.append(f"client {label} browser-local output=Web Player Chrome")
        return PreflightResult(checks=tuple(checks), failures=tuple(failures))


class IntegrationSession:
    """
    A running web client + MA probe, with reset and observation helpers.

    :param client: The controller web client handle.
    :param ma: Probe attached to the live MA renderer.
    """

    def __init__(
        self,
        client: ClientHandle,
        ma: MAProbe,
        preflight: SafetyPreflight,
        observer: ClientHandle | None = None,
    ) -> None:
        """Bind a web-client controller handle to a live MA probe."""
        self.client = client
        self.observer = observer
        self.ma = ma
        self.audio = AudioProbe()
        self._preflight = preflight
        self._safe_for_playback = False

    @property
    def q(self) -> QobuzPage:
        """The web client's page-object controller API."""
        return self.client.qobuz

    @property
    def q2(self) -> QobuzPage | None:
        """Return the independent second Qobuz cloud observer."""
        return self.observer.qobuz if self.observer is not None else None

    async def reset_to_clean_state(self) -> None:
        """
        Return control to the web client and start a known queue.

        First routes playback back to the browser's local output (so any
        active MA Connect renderer deactivates and the web client is the
        active player again — otherwise "play album" would just command the
        remote renderer). Then plays the reference album so the cloud queue
        is freshly versioned at track index 0. Gives every scenario the same
        deterministic starting point, free of cross-run pollution.
        """
        await self.ensure_safe_for_playback()
        await self.q.select_local_output(expected_name="Web Player Chrome")
        await asyncio.sleep(3)
        await self.q.play_album_by_url(ALBUM_URL)
        await asyncio.sleep(4)

    async def handoff_to_ma(self) -> None:
        """Hand playback off to the MA renderer (``Local Dev``)."""
        await self.ensure_safe_for_playback()
        await self.q.select_connect_target(CONNECT_TARGET)

    async def ensure_safe_for_playback(self) -> None:
        """Abort unless the managed receiver is pinned to the silent BlackHole player."""
        if self._safe_for_playback:
            return
        result = await self._preflight.run()
        if not result.passed:
            raise RuntimeError(
                "Live playback safety preflight failed: " + "; ".join(result.failures)
            )
        LOGGER.info("Live playback safety preflight passed: %s", ", ".join(result.checks))
        self._safe_for_playback = True

    async def cleanup_playback(self) -> None:
        """Stop the verified BlackHole queue and return control to browser-local output."""
        if not self._safe_for_playback:
            return
        stop_error = await _ma_send("player_queues/stop", {"queue_id": BLACKHOLE_PLAYER_ID})
        clear_error = await _ma_send("player_queues/clear", {"queue_id": BLACKHOLE_PLAYER_ID})
        if stop_error or clear_error:
            LOGGER.warning(
                "BlackHole cleanup errors: stop=%s clear=%s",
                stop_error,
                clear_error,
            )
        await self.q.select_local_output(expected_name="Web Player Chrome")

    def observe(self, cursor: int) -> ProbeEvents:
        """Parse MA log events written since ``cursor``."""
        return self.ma.events_since(cursor)

    def wait_for_stream(self, cursor: int, *, timeout: float = 25.0) -> ProbeEvents:
        """Wait until MA logs at least one StreamStart after ``cursor``."""
        return self.ma.wait_for_event(cursor, lambda ev: bool(ev.streams), timeout=timeout)

    def wait_for_sound(self, timeout: float = 15.0) -> bool:
        """Wait until real audio is present on the BlackHole output."""
        return self.audio.wait_for_sound(timeout)

    def wait_for_silence(self, timeout: float = 15.0) -> bool:
        """Wait until the BlackHole output is silent."""
        return self.audio.wait_for_silence(timeout)

    def assert_sound(self, result: ScenarioResult, name: str, *, timeout: float = 15.0) -> None:
        """Record a check that real audio is flowing to the output device."""
        ok = self.wait_for_sound(timeout)
        level = self.audio.measure(1.0)
        result.check(name, ok, detail=f"mean_db={level.mean_db} max_db={level.max_db}")

    def assert_silence(self, result: ScenarioResult, name: str, *, timeout: float = 10.0) -> None:
        """Record a check that the output device is silent."""
        ok = self.wait_for_silence(timeout)
        level = self.audio.measure(1.0)
        result.check(name, ok, detail=f"mean_db={level.mean_db} max_db={level.max_db}")

    def ma_current_title(self) -> str:
        """
        Return the title of the track MA is actually *playing* (its reported current).

        Uses MA's renderer-state Report (``item=slot:trackid``) as the audible
        truth, NOT the last ``StreamStart`` — the flow stream buffers the next
        track read-ahead, so the newest StreamStart is what MA is loading next,
        not what is playing now. The title is resolved from the id->title pairs
        that StreamStart lines provide.
        """
        events = self.ma.events_since(0)
        if not events.reports:
            return ""
        current_id = events.reports[-1].track_id
        for stream in reversed(events.streams):
            if stream.track_id == current_id:
                return stream.title
        return ""

    def ma_current_track_id(self) -> str:
        """Return the exact Qobuz track ID most recently reported by MA."""
        reports = self.ma.events_since(0).reports
        return str(reports[-1].track_id) if reports else ""

    def wait_for_playing(self, cursor: int, *, timeout: float = 25.0) -> ProbeEvents:
        """Wait until MA reports playing (Report state=2) after ``cursor``."""
        return self.ma.wait_for_event(
            cursor, lambda ev: any(r.state == 2 for r in ev.reports), timeout=timeout
        )

    async def assert_in_sync(
        self, result: ScenarioResult, name: str, *, timeout: float = 12.0
    ) -> None:
        """
        Record a check that both apps' exact Qobuz track IDs match MA.

        Polls to tolerate brief cloud/browser update races.
        """
        deadline = time.monotonic() + timeout
        app_a_id = ""
        app_b_id = ""
        ma_id = ""
        while time.monotonic() < deadline:
            app_a_id = await self.q.current_track_id()
            app_b_id = await self.q2.current_track_id() if self.q2 is not None else app_a_id
            ma_id = self.ma_current_track_id()
            if app_a_id and app_a_id == app_b_id == ma_id:
                result.check(
                    name,
                    True,
                    detail=f"app_a={app_a_id} app_b={app_b_id} ma={ma_id}",
                )
                return
            await asyncio.sleep(1.0)
        result.check(
            name,
            False,
            detail=f"DRIFT app_a={app_a_id!r} app_b={app_b_id!r} ma={ma_id!r}",
        )

    async def assert_playing_and_synced(self, result: ScenarioResult, step: str) -> None:
        """Assert both real audio and app/MA track agreement for a step."""
        self.assert_sound(result, f"{step}: audio playing")
        await self.assert_in_sync(result, f"{step}: app and MA agree on track")

    async def ma_reorder(self, index: int, pos_shift: int) -> str | None:
        """
        Reorder MA's queue: move the item at ``index`` by ``pos_shift`` slots.

        Drives an MA-side queue edit (the reconciliation/proposal path), the
        counterpart to a controller-side reorder. Requires an MA token.

        :param index: 0-based position of the queue item to move.
        :param pos_shift: Positive moves it later, negative earlier.
        :returns: ``None`` on success or a short error string.
        """
        items = await ma_query(
            "player_queues/items", {"queue_id": BLACKHOLE_PLAYER_ID, "limit": 200}
        )
        if not isinstance(items, list) or not 0 <= index < len(items):
            return "no-items"
        item = items[index]
        if not isinstance(item, dict):
            return "bad-item"
        return await _ma_send(
            "player_queues/move_item",
            {
                "queue_id": BLACKHOLE_PLAYER_ID,
                "queue_item_id": item["queue_item_id"],
                "pos_shift": pos_shift,
            },
        )


def _resolve_ma_token() -> str | None:
    """Return the MA access token from ``MA_TOKEN`` or the ``.auth/ma_token`` file."""
    token = os.environ.get("MA_TOKEN")
    if token:
        return token
    token_file = AUTH_DIR / "ma_token"
    if token_file.exists():
        return token_file.read_text().strip() or None
    return None


async def _ma_call(command: str, args: dict[str, object]) -> tuple[str | None, object]:
    """
    Send an authenticated MA WebSocket command; return ``(error, result)``.

    MA authenticates a WS connection via an ``auth`` command sent as the first
    message (``args={"token": ...}``); we wait for it to succeed before issuing
    the real command. The token comes from ``MA_TOKEN`` or ``.auth/ma_token``.

    :param command: The MA API command (e.g. ``player_queues/play_media``).
    :param args: The command arguments.
    :returns: ``(None, result)`` on success; ``("no-token", None)`` when
        unauthenticated; ``(error_string, None)`` otherwise.
    """
    import aiohttp  # noqa: PLC0415

    token = _resolve_ma_token()
    if not token:
        return "no-token", None
    async with aiohttp.ClientSession() as session, session.ws_connect(MA_WS_URL) as ws:
        await ws.receive_json()  # server-info greeting
        await ws.send_json({"command": "auth", "message_id": "auth", "args": {"token": token}})
        await ws.send_json({"command": command, "message_id": "cmd", "args": args})
        auth_ok = False
        for _ in range(200):
            try:
                msg = await asyncio.wait_for(ws.receive_json(), timeout=8)
            except TimeoutError:
                return "timeout", None
            if not isinstance(msg, dict):
                continue
            if msg.get("message_id") == "auth" and msg.get("error_code"):
                return f"auth failed: {msg.get('details')}", None
            if msg.get("message_id") == "auth":
                auth_ok = True
            if msg.get("message_id") == "cmd":
                if msg.get("error_code"):
                    return str(msg.get("details") or msg.get("error_code")), None
                return None, msg.get("result")
        return ("no-response" if auth_ok else "auth-no-response"), None


async def _ma_send(command: str, args: dict[str, object]) -> str | None:
    """Send an authenticated MA command and return only the error (None on success)."""
    error, _ = await _ma_call(command, args)
    return error


async def ma_query(command: str, args: dict[str, object]) -> object:
    """Send an authenticated MA command and return its result payload (or None)."""
    _, result = await _ma_call(command, args)
    return result


async def ma_play_media(
    uri: str, *, queue_id: str = BLACKHOLE_PLAYER_ID, option: str | None = None
) -> str | None:
    """
    Start (or enqueue) playback of ``uri`` on an MA queue via the WebSocket API.

    :param uri: Media URI to play (e.g. ``qobuz://album/0060694932902``).
    :param queue_id: The MA queue/player to play on (default: BlackHole).
    :param option: Optional enqueue mode (e.g. ``"add"`` to append).
    :returns: ``None`` on success, ``"no-token"`` when unauthenticated, or an
        error string.
    """
    args: dict[str, object] = {"queue_id": queue_id, "media": uri}
    if option is not None:
        args["option"] = option
    return await _ma_send("player_queues/play_media", args)


async def ma_player_command(command: str, extra: dict[str, object] | None = None) -> str | None:
    """
    Issue a ``players/cmd/*`` command on the BlackHole player via the WS API.

    :param command: The MA API command (e.g. ``players/cmd/next``). These take
        ``player_id`` (not ``queue_id``); for the BlackHole target the two ids
        are identical.
    :param extra: Optional extra args merged into ``{"player_id": BlackHole}``.
    :returns: ``None`` on success, ``"no-token"`` when unauthenticated, or an
        error string.
    """
    args: dict[str, object] = {"player_id": BLACKHOLE_PLAYER_ID}
    if extra:
        args.update(extra)
    return await _ma_send(command, args)


async def open_session(
    ma: MAProbe,
) -> tuple[IntegrationSession, tuple[Playwright, Browser]]:
    """
    Launch Chromium + two logged-in web clients and bind them to ``ma``.

    :param ma: An already-started/attached MA probe.
    :returns: ``(session, (playwright, browser))`` — pass the second element
        to :func:`close_session` when finished.
    """
    from playwright.async_api import async_playwright  # noqa: PLC0415

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    client, observer = await asyncio.gather(
        _open_client(
            browser,
            label="A",
            storage_state_path=AUTH_DIR / "client_a.json",
            headed=False,
        ),
        _open_client(
            browser,
            label="B",
            storage_state_path=AUTH_DIR / "client_b.json",
            headed=False,
        ),
    )
    preflight = SafetyPreflight(
        qobuz=client.qobuz,
        qobuz_observer=observer.qobuz,
        ma_query_func=ma_query,
        connect_target=CONNECT_TARGET,
        managed_pid=ma.managed_pid,
    )
    session = IntegrationSession(client, ma, preflight, observer)
    return session, (pw, browser)


async def close_session(handle: tuple[Playwright, Browser]) -> None:
    """Close the Playwright browser + driver opened by :func:`open_session`."""
    pw, browser = handle
    await browser.close()
    await pw.stop()


def default_probe() -> MAProbe:
    """
    Build a probe that manages an MA process on the local playground dirs.

    Writes MA's log to a temp file. The ``.mass-data`` provider config must
    have qobuz + qobuz_connect set up and the Connect target pinned to a
    silent player (e.g. BlackHole) so integration runs make no audible sound.
    """
    log_path = Path(tempfile.gettempdir()) / "ma_integration_run.log"
    return MAProbe(
        log_path=log_path,
        data_dir=Path(".mass-data").resolve(),
        cache_dir=Path(".mass-cache").resolve(),
    )
