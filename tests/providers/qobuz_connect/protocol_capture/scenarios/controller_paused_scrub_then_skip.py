"""
Paused-scrub-then-skip scenario — exercises the paused-seek-storage edge case.

The hypothesis behind MA's paused-seek-storage code is that when a controller
scrubs while paused and then changes track *before* resuming, the renderer must
remember which track the scrub belonged to. This scenario reproduces that flow
so we can observe whether the reference renderer actually receives a separate
"paused-seek" command, or whether the controller buffers the scrub and only
sends it on resume.

Steps:
1. A controls, B renders (set up via helper).
2. A pauses.
3. A scrubs the progress bar while paused.
4. A skips to the next track (before resuming).
5. A resumes playback on the new track.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from tests.providers.qobuz_connect.protocol_capture.scenarios._controller_helpers import (
    setup_a_controller_b_renderer,
)

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

LOGGER = logging.getLogger(__name__)


async def run(session: CaptureSession) -> None:
    """Execute the paused-scrub-then-skip scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz
    LOGGER.info("[controller_paused_scrub_then_skip] A pauses")
    await a.pause()
    await asyncio.sleep(1.5)
    LOGGER.info("[controller_paused_scrub_then_skip] A scrubs to 0.6 while paused")
    await a.seek_to_fraction(0.6)
    await asyncio.sleep(1.5)
    LOGGER.info("[controller_paused_scrub_then_skip] A skips next BEFORE resuming")
    await a.skip_next()
    await asyncio.sleep(2)
    LOGGER.info("[controller_paused_scrub_then_skip] A resumes playback")
    await a.play()
    await asyncio.sleep(5)
