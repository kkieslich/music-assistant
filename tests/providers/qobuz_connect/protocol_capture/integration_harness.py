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


async def ma_play_media(uri: str, *, queue_id: str = BLACKHOLE_PLAYER_ID) -> str | None:
    """
    Start playback of ``uri`` on an MA queue via the MA WebSocket API.

    Used by the initiate-from-MA scenario to begin Qobuz playback on the MA
    side (rather than via the web client). Player commands require auth, so
    an MA access token must be supplied via the ``MA_TOKEN`` env var.

    :param uri: Media URI to play (e.g. ``qobuz://album/0724384260958``).
    :param queue_id: The MA queue/player to play on (default: BlackHole).
    :returns: ``None`` on success, or a short error string (including
        ``"no-token"`` when ``MA_TOKEN`` is unset).
    """
    import aiohttp  # noqa: PLC0415

    token = os.environ.get("MA_TOKEN")
    if not token:
        return "no-token"
    async with aiohttp.ClientSession() as session, session.ws_connect(MA_WS_URL) as ws:
        await ws.receive_json()  # server-info greeting
        await ws.send_json({"command": "auth", "message_id": "auth", "args": {"token": token}})
        await ws.send_json(
            {
                "command": "player_queues/play_media",
                "message_id": "play",
                "args": {"queue_id": queue_id, "media": uri},
            }
        )
        for _ in range(8):
            try:
                msg = await asyncio.wait_for(ws.receive_json(), timeout=8)
            except TimeoutError:
                return "timeout"
            if isinstance(msg, dict) and msg.get("message_id") == "play":
                return None if not msg.get("error_code") else str(msg.get("details"))
    return "no-response"


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
