"""
Playwright session manager for Qobuz Connect protocol captures.

Owns:
- launching a single Chromium instance,
- spawning two BrowserContexts ("A" and "B") each backed by a persisted
  storage_state.json so login only happens once per profile,
- attaching one WsRecorder per page,
- writing a per-client capture file when a scenario completes.

First-run login is *manual*: in headed mode, the operator logs in by hand in
each browser window and the harness saves storage_state once the player
appears. On subsequent runs the persisted storage_state means no human
interaction is required and the run can be headless.

This is *not* a pytest conftest — naming was chosen to keep the file out of
pytest's auto-discovery (see tests/providers/qobuz_connect/conftest.py
which adds protocol_capture to collect_ignore).
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .qobuz_page import QobuzPage
from .ws_recorder import WsRecorder

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from playwright.async_api import Browser, BrowserContext, Page, Playwright

LOGGER = logging.getLogger(__name__)

HARNESS_ROOT = Path(__file__).resolve().parent
AUTH_DIR = HARNESS_ROOT / ".auth"
RUNS_DIR = HARNESS_ROOT / ".runs"


@dataclass(slots=True)
class ClientHandle:
    """One running Qobuz Web Client browser session."""

    label: str
    context: BrowserContext
    page: Page
    qobuz: QobuzPage
    recorder: WsRecorder
    storage_state_path: Path


@dataclass(slots=True)
class CaptureSession:
    """Two-client capture session bound to a single Chromium instance."""

    playwright: Playwright
    browser: Browser
    a: ClientHandle
    b: ClientHandle
    out_dir: Path = field(default_factory=lambda: RUNS_DIR)

    async def write_captures(self, scenario_name: str) -> tuple[Path, Path]:
        """
        Flush both clients' recorders to disk.

        :param scenario_name: Used to compose the output filenames.
        :returns: Tuple of (client_a_capture_path, client_b_capture_path).
        """
        self.out_dir.mkdir(parents=True, exist_ok=True)
        path_a = self.out_dir / f"{scenario_name}__client_a.json"
        path_b = self.out_dir / f"{scenario_name}__client_b.json"
        self.a.recorder.write(path_a)
        self.b.recorder.write(path_b)
        return path_a, path_b


async def _open_client(
    browser: Browser,
    *,
    label: str,
    storage_state_path: Path,
    headed: bool,
) -> ClientHandle:
    storage_state_arg = str(storage_state_path) if storage_state_path.exists() else None
    context = await browser.new_context(storage_state=storage_state_arg)
    page = await context.new_page()
    qobuz = QobuzPage(page, label=label)
    recorder = WsRecorder(page)

    # Recorder attaches BEFORE navigation so we capture the AUTHENTICATE /
    # SUBSCRIBE frames Qobuz sends right after the WebSocket opens.
    await recorder.start()
    await qobuz.open()
    await qobuz.accept_cookies_if_present()

    # Login-complete signal = the Qobuz Web Client opened its WebSocket and
    # at least one frame arrived. This is far more reliable across locales
    # and DOM redesigns than waiting for a specific button to be visible.
    if await recorder.wait_for_first_frame(timeout_seconds=8):
        LOGGER.info("[%s] reusing persisted storage_state", label)
    else:
        if not headed:
            raise RuntimeError(
                f"[{label}] no Qobuz WebSocket activity within 8s and running "
                "headless — re-run with --headed and complete the Qobuz login "
                "manually so the harness can persist storage_state for "
                "future headless runs."
            )
        LOGGER.warning(
            "[%s] please complete the Qobuz login in this browser window. "
            "Waiting up to %ds for the player WebSocket to open...",
            label,
            MANUAL_LOGIN_TIMEOUT_SECONDS,
        )
        # Re-attempt cookie accept periodically while we wait — some banners
        # only appear after the login form submits.
        login_ok = await _wait_for_login_with_cookie_retries(qobuz, recorder)
        if not login_ok:
            raise RuntimeError(
                f"[{label}] login window of {MANUAL_LOGIN_TIMEOUT_SECONDS}s "
                "elapsed without seeing a Qobuz WebSocket frame. Either the "
                "login didn't complete or the WS endpoint changed — re-run "
                "with PWDEBUG=1 to drive the inspector."
            )
        LOGGER.info("[%s] login detected via WS handshake", label)
        await qobuz.save_storage_state(storage_state_path)

    return ClientHandle(
        label=label,
        context=context,
        page=page,
        qobuz=qobuz,
        recorder=recorder,
        storage_state_path=storage_state_path,
    )


MANUAL_LOGIN_TIMEOUT_SECONDS = 5 * 60


async def _wait_for_login_with_cookie_retries(qobuz: QobuzPage, recorder: WsRecorder) -> bool:
    """
    Poll for a captured WS frame while periodically retrying cookie accept.

    Qobuz sometimes shows a fresh consent banner after the login form
    submits (different category than the landing-page one), so this keeps
    trying to dismiss it during the wait window.
    """
    poll_step_seconds = 5.0
    elapsed = 0.0
    while elapsed < MANUAL_LOGIN_TIMEOUT_SECONDS:
        if await recorder.wait_for_first_frame(timeout_seconds=poll_step_seconds):
            return True
        elapsed += poll_step_seconds
        await qobuz.accept_cookies_if_present(per_try_timeout_ms=500)
    return False


@asynccontextmanager
async def capture_session(
    *,
    headed: bool = False,
    out_dir: Path | None = None,
    slow_mo_ms: int = 0,
) -> AsyncIterator[CaptureSession]:
    """
    Launch Chromium + two Qobuz Web Client sessions with recorders attached.

    Use as::

        async with capture_session(headed=True) as session:
            await scenario.run(session)
            await session.write_captures("scenario_name")

    :param headed: Show the Chromium window. Required for first-run logins
        because login is done manually in the browser; subsequent runs
        with a persisted ``.auth/client_*.json`` can be headless.
    :param out_dir: Where to write capture files; defaults to ``.runs/``.
    :param slow_mo_ms: Optional ms delay between Playwright actions for
        easier debugging when running headed.
    """
    # Local import: playwright is an optional dep installed only via
    # the ``[qobuz-connect-capture]`` extra, so it must not appear at
    # module top level.
    from playwright.async_api import async_playwright  # noqa: PLC0415

    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    storage_a = AUTH_DIR / "client_a.json"
    storage_b = AUTH_DIR / "client_b.json"

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=not headed, slow_mo=slow_mo_ms)
        try:
            a_handle, b_handle = await asyncio.gather(
                _open_client(
                    browser,
                    label="A",
                    storage_state_path=storage_a,
                    headed=headed,
                ),
                _open_client(
                    browser,
                    label="B",
                    storage_state_path=storage_b,
                    headed=headed,
                ),
            )
            session = CaptureSession(
                playwright=playwright,
                browser=browser,
                a=a_handle,
                b=b_handle,
                out_dir=out_dir or RUNS_DIR,
            )
            try:
                yield session
            finally:
                await a_handle.recorder.stop()
                await b_handle.recorder.stop()
        finally:
            await browser.close()
