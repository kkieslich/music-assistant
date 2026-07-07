"""
Controller-full-lifecycle scenario — one authoritative end-to-end controller trace.

A controls, B renders. After the handoff, A walks through every controller
verb in a single recording: pause, resume, seek, volume set (twice), mute
toggle, skip, and — most importantly — loading a *new* album mid-session
while B stays the active renderer. That last step is the reference behavior
for "a controller replaces the queue during an active session", which is the
exact flow an MA-side controller needs to reproduce for MA-origin playback.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

from tests.providers.qobuz_connect.protocol_capture.scenarios._controller_helpers import (
    setup_a_controller_b_renderer,
)

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

LOGGER = logging.getLogger(__name__)

SECOND_ALBUM_URL = os.environ.get(
    "QOBUZ_CAPTURE_SECOND_ALBUM_URL", "https://play.qobuz.com/album/0060694932902"
)


async def run(session: CaptureSession) -> None:
    """Execute the controller-full-lifecycle scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz

    LOGGER.info("[lifecycle] pause")
    await a.play()
    await asyncio.sleep(2)

    LOGGER.info("[lifecycle] resume")
    await a.play()
    await asyncio.sleep(2)

    LOGGER.info("[lifecycle] seek to 0.5")
    await a.seek_to_fraction(0.5)
    await asyncio.sleep(2)

    LOGGER.info("[lifecycle] volume to 30")
    await a.set_volume_percent(30)
    await asyncio.sleep(2)

    LOGGER.info("[lifecycle] volume to 80")
    await a.set_volume_percent(80)
    await asyncio.sleep(2)

    LOGGER.info("[lifecycle] mute toggle on/off")
    await a.toggle_mute()
    await asyncio.sleep(1.5)
    await a.toggle_mute()
    await asyncio.sleep(1.5)

    LOGGER.info("[lifecycle] skip next")
    await a.skip_next()
    await asyncio.sleep(3)

    LOGGER.info("[lifecycle] load NEW album mid-session (queue replacement)")
    await a.play_album_by_url(SECOND_ALBUM_URL)
    await asyncio.sleep(5)

    LOGGER.info("[lifecycle] final pause")
    await a.play()
    await asyncio.sleep(4)
