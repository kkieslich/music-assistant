"""
Shared setup for controller→renderer scenarios.

Every "A controls B" scenario starts the same way: A picks an album, starts
playback, then hands off to B. Only after the handoff does the actual stress
behavior (burst skip, scrub, etc.) begin — and only then is it visible as
``srvrRndrSetState`` traffic on B. This module owns that warm-up.
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


async def setup_a_controller_b_renderer(session: CaptureSession) -> None:
    """
    Bring the session into the "A controls, B renders" state.

    1. A loads the album and starts playback (A is currently both controller
       and renderer).
    2. A hands off via the Connect picker to the other Web Player (B), so B
       becomes the active renderer.
    3. Sleep briefly so B's renderer-side handshake settles before the
       scenario starts issuing controller commands.
    """
    a = session.a.qobuz
    LOGGER.info("[setup] A loads album %s", ALBUM_URL)
    await a.play_album_by_url(ALBUM_URL)
    await asyncio.sleep(4)
    LOGGER.info("[setup] A hands off renderer to B")
    await a.select_connect_target_other_web_player()
    await asyncio.sleep(6)
