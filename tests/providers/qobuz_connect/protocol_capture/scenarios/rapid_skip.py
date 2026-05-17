"""
Rapid-skip scenario — B self-controls and presses skip-next five times rapidly.

When a Web Client controls itself, it short-circuits — skip-next
mutates local state and broadcasts ``rndrSrvrStateUpdated`` instead of
round-tripping through ``ctrlSrvrSetPlayerState → cloud → srvrRndrSetState``.
So this scenario does NOT exercise our renderer-side burst-reconcile code.
For that, see ``controller_burst_skip`` where A controls and B renders.

Kept as a baseline for self-controlling burst behavior.
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
