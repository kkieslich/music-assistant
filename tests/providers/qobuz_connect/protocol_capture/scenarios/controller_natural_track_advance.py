"""
Controller natural-track-advance — A controls, B renders, wait through track-end.

Captures what the Qobuz cloud emits to a renderer (B) around a natural
end-of-track + auto-advance. Specifically targets the protocol question
behind the production "stops after 2 tracks" bug:

- Does the cloud ever push a bare position-only ``srvrRndrSetState``
  (``currentPosition`` set, but no ``playingState`` / ``currentQueueItem``
  / ``nextQueueItem`` / ``queueVersion``) during a healthy session?
- If yes, when does it arrive relative to the track-change events the
  reference renderer would naturally handle?

This scenario does **not** scrub or fast-forward — the Qobuz Web Client
uses the Web Audio API (no HTML5 ``<audio>``/``<video>`` element with
``currentTime``), so external time manipulation isn't possible. Instead
it plays a hand-picked album whose first track is short (~30s), then
idles long enough for the natural track-1 → track-2 transition to
land in the recording.

The album URL is hard-coded rather than env-var-driven on purpose: the
shared :func:`setup_a_controller_b_renderer` reads ``ALBUM_URL`` at
import time, so a per-scenario override has to bypass the helper. Both
``ALBUM_URL`` and ``WAIT_S`` can be tweaked at the top of the file.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

LOGGER = logging.getLogger(__name__)

# Album whose first track is ~30s long. Picked specifically so the
# scenario can capture a natural end-of-track + auto-advance within a
# short, repeatable window.
ALBUM_URL = "https://play.qobuz.com/album/0060694932902"

# Total idle window after the album-load + handoff. Covers track-1
# play-through (~30s) plus the auto-advance and a few seconds into
# track 2 (so we record any post-advance cloud messages).
WAIT_S = 55.0


async def run(session: CaptureSession) -> None:
    """Execute the natural-advance scenario against ``session``."""
    a = session.a.qobuz
    LOGGER.info("[controller_natural_track_advance] A loads album %s", ALBUM_URL)
    await a.play_album_by_url(ALBUM_URL)
    await asyncio.sleep(4)
    LOGGER.info("[controller_natural_track_advance] A hands off renderer to B")
    await a.select_connect_target_other_web_player()
    await asyncio.sleep(6)
    LOGGER.info(
        "[controller_natural_track_advance] Idling %.0fs to record the natural "
        "end-of-track + auto-advance traffic on B",
        WAIT_S,
    )
    await asyncio.sleep(WAIT_S)
