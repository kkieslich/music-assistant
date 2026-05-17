"""
Page-object model for the Qobuz Web Client (play.qobuz.com).

Every selector here was *verified* against a real logged-in session via
``discover.py`` — see ``.runs/discover/<timestamp>/`` for the underlying
DOM dumps. The Qobuz SPA uses CSS-class names (not roles/aria) for the
player controls, so we target by stable class prefixes (``player__action-*``,
``NetworkAudioOutputListItem*``, ``pct-*``).

A few methods rely on locale-dependent strings the UI offers (e.g. the
audio-quality settings tab label). Those accept an env-var override so a
user on a non-German Qobuz UI can still drive the harness without code
changes.

The API is stable: scenarios call these methods, the methods own the
DOM details.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, Locator, Page

LOGGER = logging.getLogger(__name__)

QOBUZ_WEB_URL = "https://play.qobuz.com/"

# Cookie-consent accept selectors, tried in order. Qobuz uses Didomi; the
# rest are fallbacks for the major consent providers.
COOKIE_ACCEPT_SELECTORS = (
    "#didomi-notice-agree-button",
    "#onetrust-accept-btn-handler",
    "#truste-consent-button",
    "button#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
    "button#qc-cmp2-ui button[mode='primary']",
    'button[id*="cookie" i][id*="accept" i]',
)

# Locale-dependent labels for the settings page. Defaults are German since
# that's the locale of the verified session; override via env var when
# running with another Qobuz UI language.
AUDIO_SETTINGS_TAB_LABEL = os.environ.get(
    "QOBUZ_CAPTURE_AUDIO_SETTINGS_TAB_LABEL", "Wiedergabe der Musik"
)
# Track context-menu item that adds the track to the play queue. The menu is
# rendered as plain ``<a>`` elements without classes, so we match by text.
ADD_TO_QUEUE_MENU_ITEM_LABEL = os.environ.get(
    "QOBUZ_CAPTURE_ADD_TO_QUEUE_LABEL", "Zur Wiedergabeliste hinzufügen"
)


class QobuzPage:
    """Drives the Qobuz Web Client to reproduce protocol scenarios."""

    def __init__(self, page: Page, label: str = "client") -> None:
        """
        Initialize a page-object bound to a Playwright Page.

        :param page: Playwright page already navigated (or about to be) to Qobuz.
        :param label: Human-readable name for logging — e.g. "A" or "B" when
            two clients are running side by side.
        """
        self.page = page
        self.label = label

    # ---- lifecycle -------------------------------------------------------

    async def open(self) -> None:
        """Navigate to the Qobuz Web Client root."""
        await self.page.goto(QOBUZ_WEB_URL, wait_until="domcontentloaded")

    async def accept_cookies_if_present(self, per_try_timeout_ms: int = 1500) -> None:
        """
        Best-effort click the cookie-consent accept button if one is present.

        Idempotent and silent: if no dialog matches, just returns. Login
        detection no longer depends on this (the harness watches the
        recorder), so failure here is non-fatal.
        """
        for selector in COOKIE_ACCEPT_SELECTORS:
            try:
                await self.page.locator(selector).first.click(timeout=per_try_timeout_ms)
                LOGGER.info("[%s] accepted cookie banner via %r", self.label, selector)
                return
            except Exception as err:
                LOGGER.debug(
                    "[%s] cookie selector %s did not match (%s)", self.label, selector, err
                )
                continue

    async def save_storage_state(self, path: Path) -> None:
        """Persist cookies/localStorage so future runs skip login."""
        path.parent.mkdir(parents=True, exist_ok=True)
        context: BrowserContext = self.page.context
        await context.storage_state(path=str(path))
        LOGGER.info("[%s] storage_state saved to %s", self.label, path)

    # ---- transport controls (verified) ----------------------------------

    async def play(self) -> None:
        """Press the player's play/pause button (toggles state)."""
        await self._play_pause_locator().click()

    pause = play  # The same element is clicked to pause when playing.

    async def skip_next(self) -> None:
        """Skip to the next track."""
        await self.page.locator(".player__action-next").first.click()

    async def skip_previous(self) -> None:
        """Skip to the previous track."""
        await self.page.locator(".player__action-previous").first.click()

    async def toggle_shuffle(self) -> None:
        """Toggle shuffle mode."""
        await self.page.locator(".player__action-shuffle").first.click()

    async def toggle_repeat(self) -> None:
        """Cycle through repeat modes."""
        await self.page.locator(".player__action-repeat").first.click()

    async def toggle_mute(self) -> None:
        """Click the mute toggle button."""
        await self.page.locator(".pct-volume").first.click()

    async def seek_to_fraction(self, fraction: float) -> None:
        """
        Seek the player to a position by clicking the progress bar.

        :param fraction: 0.0-1.0 fraction of the current track to seek to.
        """
        scrubber = self.page.locator(".player__progressbar input[type='range']").first
        box = await scrubber.bounding_box()
        if box is None:
            raise RuntimeError("Could not locate the player progress slider")
        target_x = box["x"] + max(0.0, min(1.0, fraction)) * box["width"]
        target_y = box["y"] + box["height"] / 2
        await self.page.mouse.click(target_x, target_y)

    async def set_volume_percent(self, percent: int) -> None:
        """Set the volume by clicking on the volume rangeslider track."""
        slider = self.page.locator(".player__settings-volume-slider .rangeslider").first
        box = await slider.bounding_box()
        if box is None:
            raise RuntimeError("Could not locate the player volume slider")
        clamped = max(0.0, min(100.0, percent)) / 100.0
        target_x = box["x"] + clamped * box["width"]
        target_y = box["y"] + box["height"] / 2
        await self.page.mouse.click(target_x, target_y)

    # ---- track loading via deterministic URL navigation ------------------

    async def play_album_by_url(self, album_url: str) -> None:
        """
        Navigate to ``album_url`` and click the album's main play button.

        Far more reliable than driving the search bar: the album URL is a
        stable identifier, and the album page has one large play button
        whose class (``ButtonRoundPrimary--large.icon-play-``) is locale-
        and DOM-redesign-tolerant.
        """
        if not album_url.startswith("http"):
            album_url = f"{QOBUZ_WEB_URL.rstrip('/')}{album_url}"
        await self.page.goto(album_url, wait_until="domcontentloaded")
        await self.page.wait_for_load_state("networkidle", timeout=15_000)
        # The exact icon class is ``icon-play-arrow`` today, but Qobuz has
        # historically renamed icon classes (icon-play-circle, etc.) — the
        # substring match keeps us insulated from that without sacrificing
        # specificity, because ``.ButtonRoundPrimary--large`` is unique to
        # the album-header play button on this page.
        await self.page.locator(
            'button.ButtonRoundPrimary--large[class*="icon-play"]'
        ).first.click()

    async def play_track_on_open_album(self, track_index: int) -> None:
        """
        Play the n-th track on the currently open album page.

        :param track_index: 1-based row index in the album track list.
        """
        await self.page.locator(".ListItem__number").nth(track_index - 1).click()

    async def add_track_to_queue_on_open_album(self, track_index: int) -> None:
        """
        Add the n-th track on the open album page to the play queue.

        Opens the per-track context menu (...) and clicks the "Add to queue"
        entry. The menu items have no stable classes, so we match the entry
        by its visible text (locale-configurable).

        :param track_index: 1-based row index in the album track list.
        """
        more_buttons = self.page.locator(".ListItem__actions.icon-more-vertical")
        await more_buttons.nth(track_index - 1).click()
        await self.page.get_by_text(ADD_TO_QUEUE_MENU_ITEM_LABEL, exact=False).first.click()

    # ---- queue panel (verified) -----------------------------------------

    async def open_queue_panel(self) -> None:
        """Open the side queue panel (no-op if already open)."""
        await self.page.locator(".player__settings-control-playqueue").first.click()

    async def clear_queue(self) -> None:
        """Click the trash-can button on the queue panel."""
        await self.open_queue_panel()
        # ButtonRoundSecondary--isDisabled suffix is appended when the queue
        # is already empty; click() will still target the non-disabled state
        # by class match. We deliberately don't filter on enabled-ness.
        await self.page.locator("button.ButtonRoundSecondary.icon-delete").first.click()

    async def reorder_current_forward(self, positions: int) -> None:
        """
        Drag the currently playing track N rows downward in the queue panel.

        Qobuz's queue uses a React-virtualized list with a draggable row
        per track (``div.ListItem`` with ``.isPlaying`` on the current
        one) and no separate drag-handle element — the whole row is the
        target. We simulate the drag with low-level mouse events so the
        React DnD library can register move ticks; ``steps=15`` slows the
        drag enough for ``dragover`` listeners to fire on each row.

        :param positions: Number of queue positions to move the current
            track forward by.
        """
        await self.open_queue_panel()
        current_row = self.page.locator(".ListItem.isPlaying").first
        box = await current_row.bounding_box()
        if box is None:
            raise RuntimeError("Could not locate the currently playing queue row")
        center_x = box["x"] + box["width"] / 2
        center_y = box["y"] + box["height"] / 2
        target_y = center_y + positions * box["height"]
        await self.page.mouse.move(center_x, center_y)
        await self.page.mouse.down()
        await self.page.mouse.move(center_x, target_y, steps=15)
        await self.page.mouse.up()

    # ---- audio quality (settings page, locale-dependent label) ----------

    async def set_max_quality(self, label: str) -> None:
        """
        Set the maximum audio quality in Settings → Music Playback.

        :param label: Visible label of the desired option in the Qobuz UI
            (locale-dependent). Verified set on German: "Hi-Res 24-Bit / bis
            192 kHz", "Hi-Res 24-Bit / bis 96 kHz", "CD 16-Bit / 44,1 kHz",
            "MP3 320 kbps".

        Quality options are rendered as ``span.bt3`` elements with click
        handlers — selecting one toggles the ``bt3 active`` class on it.
        """
        await self.page.goto(f"{QOBUZ_WEB_URL}user/settings", wait_until="domcontentloaded")
        await self.page.get_by_text(AUDIO_SETTINGS_TAB_LABEL, exact=False).first.click()
        # Quality options share the same span class — disambiguate by text.
        await self.page.locator("span.bt3", has_text=label).first.click()

    # ---- Qobuz Connect picker (verified) --------------------------------

    async def open_connect_picker(self) -> None:
        """Open the Qobuz Connect device-picker popover."""
        await self.page.locator(".pct-audio-output-button").first.click()

    async def select_connect_target(self, name: str) -> None:
        """
        Hand off playback to a Qobuz Connect target by displayed name.

        :param name: Substring match against the target's visible name
            (e.g. "Web Player Chrome" or your MA-advertised device name).
        """
        await self.open_connect_picker()
        await self.page.locator(
            ".NetworkAudioOutputListItem",
            has=self.page.locator(
                ".NetworkAudioOutputListItem__content__name",
                has_text=name,
            ),
        ).first.click()

    async def select_connect_target_other_web_player(self) -> None:
        """
        Pick a Qobuz Connect target by "the other laptop entry".

        Both Web Clients show up as ``Web Player Chrome``; the active one
        carries the ``.NetworkAudioOutputListItem__current`` checkmark
        child. This selects the first laptop-icon entry that does *not*
        have that child — i.e. the other browser session.
        """
        await self.open_connect_picker()
        candidates = self.page.locator("li.NetworkAudioOutputListItem.icon-laptop")
        count = await candidates.count()
        for i in range(count):
            candidate = candidates.nth(i)
            has_current = await candidate.locator(".NetworkAudioOutputListItem__current").count()
            if has_current == 0:
                await candidate.click()
                return
        raise RuntimeError(
            "Could not find an inactive 'Web Player Chrome' entry in the "
            "Connect picker — only the current session was visible."
        )

    # ---- search (verified) ----------------------------------------------

    async def search(self, query: str) -> None:
        """
        Type a query into the search bar and submit it.

        :param query: Free-text search string.
        """
        await self.page.locator("input.SearchBar__input").first.fill(query)
        await self.page.keyboard.press("Enter")
        await self.page.wait_for_load_state("networkidle", timeout=15_000)

    # ---- private locators -----------------------------------------------

    def _play_pause_locator(self) -> Locator:
        """
        Locate the play/pause toggle.

        The class flips between ``player__action-pause`` (currently
        playing) and ``player__action-play`` (currently paused). We
        accept either via a comma-selector — first match wins.
        """
        return self.page.locator(".player__action-pause, .player__action-play").first
