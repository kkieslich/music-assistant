"""
One-shot DOM discovery against a logged-in Qobuz Web Client session.

Uses the storage_state saved by a previous capture run so it doesn't need a
manual login. Loads play.qobuz.com, lets the SPA settle, then dumps:

- The full accessibility tree (Chromium's a11y snapshot)
- Every visible button: text, accessible name, id, class, data-testid, title
- Every visible input/searchbox/textbox with the same attributes
- The outer HTML of the player footer + queue panel (scoped to small slices)

Output goes to ``.runs/discover/<timestamp>/`` so it can be inspected manually
and read back by whatever's iterating on selectors.

Run with::

    source .venv/bin/activate
    python -m tests.providers.qobuz_connect.protocol_capture.discover

Add ``--scroll-to-search`` to also open the search bar and dump its panel
state after typing a query — useful for finding the per-track play button
and right-click menu items.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tests.providers.qobuz_connect.protocol_capture.harness import AUTH_DIR, RUNS_DIR
from tests.providers.qobuz_connect.protocol_capture.qobuz_page import (
    COOKIE_ACCEPT_SELECTORS,
    QOBUZ_WEB_URL,
)

if TYPE_CHECKING:
    from playwright.async_api import Page

LOGGER = logging.getLogger("qobuz_discover")


async def _accept_cookies(page: Page) -> None:
    for selector in COOKIE_ACCEPT_SELECTORS:
        try:
            await page.locator(selector).first.click(timeout=1000)
            LOGGER.info("accepted cookie banner via %s", selector)
            return
        except Exception as err:
            LOGGER.debug("cookie selector %s did not match (%s)", selector, err)
            continue


# JS run inside the page to extract attributes for every element matching a
# CSS selector. We pull a small set of stable identifiers and the visible
# text so the dump stays under a few hundred KB even on busy pages.
_ENUMERATE_JS = """
(selector) => {
    const out = [];
    document.querySelectorAll(selector).forEach((el) => {
        const rect = el.getBoundingClientRect();
        const visible = rect.width > 0 && rect.height > 0
            && getComputedStyle(el).visibility !== 'hidden'
            && getComputedStyle(el).display !== 'none';
        if (!visible) return;
        out.push({
            tag: el.tagName.toLowerCase(),
            id: el.id || null,
            classes: (el.getAttribute('class') || '').trim() || null,
            role: el.getAttribute('role') || null,
            ariaLabel: el.getAttribute('aria-label') || null,
            ariaLabelledBy: el.getAttribute('aria-labelledby') || null,
            title: el.getAttribute('title') || null,
            dataTestId: el.getAttribute('data-testid') || null,
            type: el.getAttribute('type') || null,
            placeholder: el.getAttribute('placeholder') || null,
            text: (el.innerText || el.textContent || '').trim().slice(0, 120) || null,
            href: el.getAttribute('href') || null,
            rect: { x: rect.x|0, y: rect.y|0, w: rect.width|0, h: rect.height|0 },
        });
    });
    return out;
}
"""


async def _enumerate(page: Page, selector: str) -> list[dict[str, Any]]:
    """Return attribute dumps for every visible element matching ``selector``."""
    result: list[dict[str, Any]] = await page.evaluate(_ENUMERATE_JS, selector)
    return result


async def _dump_state(page: Page, out_dir: Path, label: str) -> None:
    """Capture the page's current state into ``out_dir/<label>/`` as JSON."""
    state_dir = out_dir / label
    state_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("[%s] url=%s title=%r", label, page.url, await page.title())
    (state_dir / "meta.json").write_text(
        json.dumps({"url": page.url, "title": await page.title()}, indent=2)
    )

    # Accessibility tree (Chromium's view of roles + names). Most useful for
    # finding semantic anchors like "main", "navigation", "complementary".
    try:
        # Playwright exposes Page.accessibility at runtime but the type stubs
        # don't declare it — the harness uses it for one-shot DOM dumps only.
        snapshot = await page.accessibility.snapshot(interesting_only=True)  # type: ignore[attr-defined]
    except Exception:
        snapshot = None
    (state_dir / "a11y_tree.json").write_text(json.dumps(snapshot, indent=2))

    # Every actionable / input element.
    inventories = {
        "buttons": "button, [role='button']",
        "links": "a[href], [role='link']",
        "inputs": "input, [role='textbox'], [role='searchbox'], [role='combobox']",
        "sliders": "[role='slider']",
        "menuitems": "[role='menuitem']",
        "tabs": "[role='tab']",
        "dialogs": "[role='dialog'], dialog",
    }
    for name, selector in inventories.items():
        items = await _enumerate(page, selector)
        (state_dir / f"{name}.json").write_text(json.dumps(items, indent=2))
        LOGGER.info("[%s] %s: %d visible elements", label, name, len(items))

    # Outer HTML of likely-interesting regions. Qobuz doesn't use semantic
    # HTML5 (footer/main/aside) so we target by class-prefix instead.
    regions: dict[str, str] = {}
    for name, selector in [
        ("player_root", "[class*='Player'], [class^='player']"),
        ("queue_root", "[class*='Queue'], [class*='queue']"),
        ("nav_root", "[class*='NavBar']"),
        ("search_root", "[class*='SearchBar']"),
        ("modal_root", "[class*='Modal'], [role='dialog']"),
    ]:
        try:
            html = await page.locator(selector).first.evaluate(
                "(el) => el.outerHTML.slice(0, 60000)"
            )
            regions[name] = html
        except Exception:
            regions[name] = ""
    (state_dir / "regions.html.json").write_text(json.dumps(regions, indent=2))

    # All visible elements in the bottom 200px of the viewport — this is the
    # player footer area and where play/pause, scrubber, queue button live.
    # Many of these are <div> with React click handlers, not <button>, so the
    # earlier role-based filters miss them.
    bottom_elements = await page.evaluate(
        """
        () => {
            const vh = window.innerHeight;
            const out = [];
            document.querySelectorAll('*').forEach((el) => {
                const r = el.getBoundingClientRect();
                if (r.top < vh - 220 || r.top > vh - 5) return;
                if (r.width === 0 || r.height === 0) return;
                // Skip pure text containers
                if (el.children.length > 0 && el.tagName.toLowerCase() !== 'button'
                    && el.getAttribute('role') !== 'button'
                    && el.getAttribute('role') !== 'slider'
                    && el.tagName.toLowerCase() !== 'input') {
                    // keep top-level only — skip generic wrappers
                    return;
                }
                out.push({
                    tag: el.tagName.toLowerCase(),
                    id: el.id || null,
                    classes: (el.getAttribute('class') || '').trim() || null,
                    role: el.getAttribute('role') || null,
                    ariaLabel: el.getAttribute('aria-label') || null,
                    title: el.getAttribute('title') || null,
                    dataTestId: el.getAttribute('data-testid') || null,
                    text: (el.innerText || el.textContent || '').trim().slice(0, 80) || null,
                    rect: { x: r.x|0, y: r.y|0, w: r.width|0, h: r.height|0 },
                });
            });
            return out;
        }
        """
    )
    (state_dir / "player_footer_area.json").write_text(json.dumps(bottom_elements, indent=2))
    LOGGER.info("[%s] player_footer_area: %d elements", label, len(bottom_elements))


async def _run(args: argparse.Namespace) -> int:
    # Local import: playwright is an optional dep installed only via
    # the ``[qobuz-connect-capture]`` extra, so it must not appear at
    # module top level.
    from playwright.async_api import async_playwright  # noqa: PLC0415

    storage = AUTH_DIR / "client_a.json"
    if not storage.exists():
        LOGGER.error("no storage_state found at %s — run a --headed scenario first", storage)
        return 1

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = RUNS_DIR / "discover" / timestamp
    out_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("dumping to %s", out_dir)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(storage_state=str(storage))
        page = await context.new_page()
        await page.goto(QOBUZ_WEB_URL, wait_until="domcontentloaded")
        await _accept_cookies(page)
        # Let the SPA hydrate.
        await page.wait_for_load_state("networkidle", timeout=20_000)
        await asyncio.sleep(2)

        await _dump_state(page, out_dir, "landing")

        if args.scroll_to_search:
            await _dump_search_state(page, out_dir, args.query)

        if args.full:
            # Open the Qobuz Connect picker — the audio-output button is the
            # confirmed entry point (see discovery from 20260517T093043Z).
            await _dump_after_click(
                page,
                out_dir,
                "connect_picker_open",
                ".pct-audio-output-button",
            )
            # Close it again so subsequent clicks don't compose state.
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)

            await _dump_after_click(
                page,
                out_dir,
                "queue_panel_open",
                ".player__settings-control-playqueue",
            )
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)

            await _dump_url(page, out_dir, "settings_page", f"{QOBUZ_WEB_URL}user/settings")
            # Inside the settings page, click the audio-quality tab to surface
            # the radio buttons that aren't visible on the initial /settings
            # landing.
            try:
                await page.get_by_text("Wiedergabe der Musik", exact=False).first.click(
                    timeout=3000
                )
                await asyncio.sleep(1.5)
                await _dump_state(page, out_dir, "settings_audio_tab")
            except Exception:
                LOGGER.warning("could not open Music Playback tab — skipping")

            await _dump_url(page, out_dir, "album_page", args.album_url)
            # Click the first track-row's more-actions button to open the
            # context menu, then dump it. This is where the "add to queue"
            # entry lives.
            try:
                await page.locator(".ListItem__actions.icon-more-vertical").first.click(
                    timeout=3000
                )
                await asyncio.sleep(1.2)
                await _dump_state(page, out_dir, "track_context_menu")
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.3)
            except Exception:
                LOGGER.warning("could not open track context menu — skipping")

        await browser.close()

    LOGGER.info("done — see %s", out_dir)
    return 0


async def _dump_search_state(page: Page, out_dir: Path, query: str) -> None:
    """Open the search UI, type a query, dump results state."""
    # Discovered selector from the first pass: Qobuz uses a custom-class
    # text input with locale-dependent placeholder.
    search_candidates = [
        "input.SearchBar__input",
        '[class*="SearchBar"] input',
        '[role="searchbox"]',
        'input[type="search"]',
    ]
    focused = False
    for sel in search_candidates:
        try:
            await page.locator(sel).first.click(timeout=1500)
            focused = True
            LOGGER.info("focused search via %s", sel)
            break
        except Exception as err:
            LOGGER.debug("search candidate %s did not match (%s)", sel, err)
            continue
    if not focused:
        LOGGER.warning("could not find a search input — skipping search dump")
        return

    await page.keyboard.type(query, delay=30)
    await page.keyboard.press("Enter")
    await page.wait_for_load_state("networkidle", timeout=15_000)
    await asyncio.sleep(2)
    await _dump_state(page, out_dir, "search_results")


async def _dump_after_click(page: Page, out_dir: Path, label: str, click_selector: str) -> None:
    """Click ``click_selector`` then dump state under ``label``."""
    try:
        await page.locator(click_selector).first.click(timeout=3000)
    except Exception:
        LOGGER.warning("[%s] could not click %r — skipping", label, click_selector)
        return
    await asyncio.sleep(1.5)
    await _dump_state(page, out_dir, label)


async def _dump_url(page: Page, out_dir: Path, label: str, url: str) -> None:
    """Navigate to ``url`` then dump state under ``label``."""
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle", timeout=15_000)
    await asyncio.sleep(1)
    await _dump_state(page, out_dir, label)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="discover.py")
    p.add_argument(
        "--scroll-to-search",
        action="store_true",
        help="Also focus the search bar, type a query, and dump that state.",
    )
    p.add_argument(
        "--query",
        default="Daft Punk Around the World",
        help="Search query used when --scroll-to-search is set.",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="Also dump connect picker, queue panel, settings page, album page.",
    )
    p.add_argument(
        "--album-url",
        default="https://play.qobuz.com/album/0724384260958",
        help="Album to navigate to when --full is set (default: Daft Punk Discovery).",
    )
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point used by ``python -m ...protocol_capture.discover``."""
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
