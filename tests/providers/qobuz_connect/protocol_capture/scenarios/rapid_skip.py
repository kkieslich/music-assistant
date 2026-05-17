"""
Rapid-skip scenario — reproduces capture-3's burst-stress flow.

Steps:
1. Client B starts a track.
2. Client B presses skip-next five times in quick succession.

Purpose: produce the burst of overlapping state updates that exposed the
"burst commands" reconciliation issues the recent commits tried to patch
(see commit 71f0af0a8 "Reconciling mechanism to handle burst commands").
We want full incoming-binary traffic for this so the reworked dispatcher
in Phase C can be exercised against real cloud behavior.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

LOGGER = logging.getLogger(__name__)

ALBUM_URL = os.environ.get("QOBUZ_CAPTURE_ALBUM_URL", "https://play.qobuz.com/album/0724384260958")
SKIP_COUNT = int(os.environ.get("QOBUZ_CAPTURE_SKIP_COUNT", "5"))
SKIP_INTERVAL_MS = int(os.environ.get("QOBUZ_CAPTURE_SKIP_INTERVAL_MS", "250"))


async def run(session: CaptureSession) -> None:
    """Execute the rapid-skip scenario against ``session``."""
    b = session.b.qobuz

    LOGGER.info("[rapid_skip] B starts album %s", ALBUM_URL)
    await b.play_album_by_url(ALBUM_URL)
    await asyncio.sleep(4)

    LOGGER.info(
        "[rapid_skip] B presses next %d times at %dms intervals",
        SKIP_COUNT,
        SKIP_INTERVAL_MS,
    )
    for _ in range(SKIP_COUNT):
        await b.skip_next()
        await asyncio.sleep(SKIP_INTERVAL_MS / 1000.0)

    await asyncio.sleep(5)
