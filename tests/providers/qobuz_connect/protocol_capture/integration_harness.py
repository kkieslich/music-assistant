"""
Live integration harness: one real Qobuz web client + one real MA renderer.

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
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

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
# during integration runs so nothing is played out loud.
BLACKHOLE_PLAYER_ID = "upb97b9910b8fe5ff0946cef06b0d44273"
MA_WS_URL = "ws://localhost:8095/ws"


@dataclass(slots=True)
class Check:
    """One assertion outcome within a scenario."""

    name: str
    passed: bool
    detail: str = ""


@dataclass(slots=True)
class ScenarioResult:
    """Aggregate outcome of a scenario run."""

    scenario: str
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Return whether every check passed (and at least one ran)."""
        return all(c.passed for c in self.checks) and bool(self.checks)

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        """Record one named assertion outcome on this result."""
        self.checks.append(Check(name=name, passed=passed, detail=detail))


class IntegrationSession:
    """
    A running web client + MA probe, with reset and observation helpers.

    :param client: The controller web client handle.
    :param ma: Probe attached to the live MA renderer.
    """

    def __init__(self, client: ClientHandle, ma: MAProbe) -> None:
        """Bind a web-client controller handle to a live MA probe."""
        self.client = client
        self.ma = ma
        self.audio = AudioProbe()

    @property
    def q(self) -> QobuzPage:
        """The web client's page-object controller API."""
        return self.client.qobuz

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
        try:
            await self.q.select_local_output()
        except Exception as err:
            LOGGER.debug("select_local_output during reset failed (non-fatal): %s", err)
        await asyncio.sleep(3)
        await self.q.play_album_by_url(ALBUM_URL)
        await asyncio.sleep(4)

    async def handoff_to_ma(self) -> None:
        """Hand playback off to the MA renderer (``Local Dev``)."""
        await self.q.select_connect_target(CONNECT_TARGET)

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

    def wait_for_playing(self, cursor: int, *, timeout: float = 25.0) -> ProbeEvents:
        """Wait until MA reports playing (Report state=2) after ``cursor``."""
        return self.ma.wait_for_event(
            cursor, lambda ev: any(r.state == 2 for r in ev.reports), timeout=timeout
        )

    async def assert_in_sync(
        self, result: ScenarioResult, name: str, *, timeout: float = 12.0
    ) -> None:
        """
        Record a check that the app's shown track matches what MA is streaming.

        Polls to tolerate brief update races: passes as soon as the web
        client's ``current_track_name`` appears within MA's most-recently
        streamed title; fails if they stay divergent (the drift bug). Also
        records the observed pair in the detail.
        """
        deadline = time.monotonic() + timeout
        app_name = ""
        ma_title = ""
        while time.monotonic() < deadline:
            app_name = (await self.q.current_track_name()).lower()
            ma_title = self.ma_current_title().lower()
            if app_name and ma_title and app_name in ma_title:
                result.check(name, True, detail=f"app={app_name!r} ma={ma_title!r}")
                return
            await asyncio.sleep(1.0)
        result.check(name, False, detail=f"DRIFT app={app_name!r} ma={ma_title!r}")

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
    Launch Chromium + one logged-in web client and bind it to ``ma``.

    :param ma: An already-started/attached MA probe.
    :returns: ``(session, (playwright, browser))`` — pass the second element
        to :func:`close_session` when finished.
    """
    from playwright.async_api import async_playwright  # noqa: PLC0415

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    client = await _open_client(
        browser, label="A", storage_state_path=AUTH_DIR / "client_a.json", headed=False
    )
    session = IntegrationSession(client, ma)
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
