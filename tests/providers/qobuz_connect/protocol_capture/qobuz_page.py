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
    resume = play  # ...and to resume when paused.

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
        """
        Click the volume/mute button in the player bar.

        WARNING: empirically, while a Qobuz Connect renderer is active, the
        button that carries ``aria-label="Mute"`` (the ``.pct-volume`` speaker
        icon) toggles AUTOPLAY on the cloud rather than muting the renderer —
        two cursor-correlated runs on 2026-07-10 each produced
        ``SET_AUTOPLAY_MODE`` and the renderer's audio stayed unmuted. There is
        no reliably drivable controller→renderer mute in the current web UI, so
        the mute scenario is omitted. This helper is retained for completeness
        but should not be relied on as a renderer mute.
        """
        await self.page.locator(".player__settings-volume").first.hover()
        await self.page.wait_for_timeout(300)
        await self.page.locator('.player__settings-volume [aria-label="Mute"]').first.click()

    async def seek_to_fraction(self, fraction: float) -> None:
        """
        Seek the player by simulating a real mouse drag on the progress bar.

        Direct-value approaches (native setter + ``input``/``change``
        dispatch) do not stick: Qobuz's ``pointerdown`` handler reads
        the current playback time and overwrites the input ``value``
        back to that anchor in the same event tick. Diagnostics showed
        ``before=9384 set=41140 after=82280`` — our 41140 was clobbered
        to 82280 (= the live audio position) before our read.

        The reliable path is to simulate the actual user gesture:
        ``mousedown`` at the slider's current thumb position,
        ``mousemove`` along the bar to the target x-coordinate (in
        several steps so React processes intermediate values), then
        ``mouseup``. The browser updates the input value organically
        during the drag and Qobuz's ``pointerup`` handler reads that
        final position and commits the seek.

        :param fraction: 0.0-1.0 fraction of the current track to seek to.
        """
        target = max(0.0, min(1.0, fraction))
        slider = self.page.locator(".player__progressbar input[type='range']").first
        box = await slider.bounding_box()
        if box is None:
            raise RuntimeError("Could not locate the player progress slider")
        # Anchor the drag at the current thumb position rather than the
        # bar's far left — some implementations only treat motion *from*
        # the thumb as a real drag.
        before = await slider.evaluate(
            "(el) => ({min: parseFloat(el.min || '0'), "
            "max: parseFloat(el.max || '100'), value: parseFloat(el.value)})"
        )
        v_min = float(before["min"])
        v_max = float(before["max"])
        v_now = float(before["value"])
        span = max(1.0, v_max - v_min)
        start_frac = max(0.0, min(1.0, (v_now - v_min) / span))
        start_x = box["x"] + start_frac * box["width"]
        target_x = box["x"] + target * box["width"]
        mid_y = box["y"] + box["height"] / 2
        await self.page.mouse.move(start_x, mid_y)
        await self.page.mouse.down()
        steps = 12
        for i in range(1, steps + 1):
            x = start_x + (target_x - start_x) * (i / steps)
            await self.page.mouse.move(x, mid_y)
        await self.page.mouse.up()
        after = await slider.evaluate("(el) => parseFloat(el.value)")
        LOGGER.info(
            "[%s] seek_to_fraction(%.2f) min=%s max=%s before=%s after=%s "
            "(drag %.1fpx → %.1fpx at y=%.1f)",
            self.label,
            target,
            v_min,
            v_max,
            v_now,
            after,
            start_x,
            target_x,
            mid_y,
        )

    async def jump_media_to_remaining(self, remaining_seconds: float) -> None:
        """
        Fast-forward the HTML5 media element to ``remaining_seconds`` before end.

        Bypasses the player UI entirely — finds the underlying
        ``<audio>``/``<video>`` element (searching shadow roots and
        same-origin iframes) and sets ``currentTime`` directly. The Web
        Client's media-event listeners drive the cloud-sync, so this
        trips the same outbound traffic a real scrub would.

        Must be called on the *renderer*'s page (the client that owns the
        playing audio). After a Connect handoff the controller's page has
        no media element, so calling this on the controller will fail
        with ``no media element``.

        :param remaining_seconds: Seconds of playback to leave between the
            jump target and the end-of-track.
        """
        result = await self.page.evaluate(
            """
            (remainingSeconds) => {
                // Depth-first search through a document, including
                // every shadow root we can reach. Returns the first
                // <audio> or <video> with a finite duration.
                function findMediaIn(root) {
                    if (!root) return null;
                    const direct = root.querySelectorAll
                        ? root.querySelectorAll('audio, video')
                        : [];
                    for (const m of direct) {
                        if (isFinite(m.duration) && m.duration > 0) return m;
                    }
                    // Fall back: pick first match even without duration —
                    // useful for diagnostics, the caller checks duration.
                    if (direct.length > 0) return direct[0];
                    // Walk shadow roots of every element under this root.
                    const all = root.querySelectorAll
                        ? root.querySelectorAll('*')
                        : [];
                    for (const el of all) {
                        if (el.shadowRoot) {
                            const found = findMediaIn(el.shadowRoot);
                            if (found) return found;
                        }
                    }
                    return null;
                }
                function findEverywhere() {
                    let media = findMediaIn(document);
                    if (media) return media;
                    const iframes = document.querySelectorAll('iframe');
                    for (const f of iframes) {
                        try {
                            const idoc = f.contentDocument;
                            if (idoc) {
                                media = findMediaIn(idoc);
                                if (media) return media;
                            }
                        } catch (_) { /* cross-origin, skip */ }
                    }
                    return null;
                }
                const media = findEverywhere();
                if (!media) {
                    const iframeSrcs = Array.from(
                        document.querySelectorAll('iframe')
                    ).map(f => f.src);
                    const hasWebAudio = typeof window.AudioContext !== 'undefined'
                        || typeof window.webkitAudioContext !== 'undefined';
                    return {ok: false, error: 'no media element', diag: {
                        audiosAtRoot: document.querySelectorAll('audio').length,
                        videosAtRoot: document.querySelectorAll('video').length,
                        iframeCount: iframeSrcs.length,
                        iframeSrcs,
                        hasWebAudio,
                        readyState: document.readyState,
                        url: location.href,
                    }};
                }
                if (!isFinite(media.duration) || media.duration <= 0) {
                    return {ok: false, error: 'duration unknown',
                            duration: media.duration,
                            tagName: media.tagName,
                            readyState: media.readyState,
                            networkState: media.networkState,
                            currentSrc: media.currentSrc};
                }
                const target = Math.max(0, media.duration - remainingSeconds);
                media.currentTime = target;
                return {ok: true, duration: media.duration,
                        currentTime: media.currentTime, requested: target,
                        tagName: media.tagName};
            }
            """,
            remaining_seconds,
        )
        if not result.get("ok"):
            raise RuntimeError(f"Could not jump media element on [{self.label}]: {result}")
        LOGGER.info(
            "[%s] jumped <%s> to %.2fs (duration=%.2fs, requested=%.2fs)",
            self.label,
            result["tagName"],
            result["currentTime"],
            result["duration"],
            result["requested"],
        )

    async def set_volume_percent(self, percent: int) -> None:
        """Set the volume by clicking on the volume rangeslider track."""
        # The slider is collapsed until the volume control is hovered; hover
        # first so it expands to a real, clickable width. The track is only a
        # couple of pixels tall, so an absolute-coordinate mouse click is
        # unreliable — click the element itself at a relative position with
        # force=True, which lands on the track regardless of its thinness.
        await self.page.locator(".player__settings-volume").first.hover()
        await self.page.wait_for_timeout(400)
        slider = self.page.locator(".player__settings-volume-slider .rangeslider").first
        box = await slider.bounding_box()
        if box is None or box["width"] < 2:
            raise RuntimeError("Could not locate an expanded player volume slider")
        clamped = max(0.0, min(100.0, percent)) / 100.0
        await slider.click(
            position={"x": clamped * box["width"], "y": max(1.0, box["height"] / 2)},
            force=True,
        )

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

    async def play_track_by_index_on_album(self, album_url: str, track_index: int) -> None:
        """
        Open ``album_url`` and start its 1-based ``track_index`` track.

        :param album_url: The album page URL (absolute or site-relative).
        :param track_index: 1-based row index to start.
        """
        if not album_url.startswith("http"):
            album_url = f"{QOBUZ_WEB_URL.rstrip('/')}{album_url}"
        await self.page.goto(album_url, wait_until="domcontentloaded")
        await self.page.wait_for_load_state("networkidle", timeout=15_000)
        await self.play_track_on_open_album(track_index)

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

    async def select_local_output(self) -> None:
        """
        Route playback back to this browser's local audio output.

        Picks the "Default audio output" entry under the picker's
        *Direct devices* list, which deactivates any active Qobuz Connect
        renderer and makes this web client the active player again — the
        clean-state reset for handoff scenarios.
        """
        await self.open_connect_picker()
        await self.page.locator(".DirectAudioOutputListItem").first.click()

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
