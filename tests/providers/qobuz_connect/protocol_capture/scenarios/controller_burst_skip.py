"""
Controller-burst-skip scenario — A controls, B renders, A presses skip 5x rapidly.

Unlike the self-controlling ``rapid_skip`` scenario, this one isolates the
controller and the renderer onto two separate Web Clients so the
``ctrlSrvrSetPlayerState → srvrRndrSetState`` round-trip is actually exercised.
Use this to observe how the reference renderer handles a burst of inbound
``srvrRndrSetState`` frames in quick succession — which is the situation the
MA-side reconcile / generation-counter code claims to defend against.
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

SKIP_COUNT = int(os.environ.get("QOBUZ_CAPTURE_SKIP_COUNT", "5"))
SKIP_INTERVAL_MS = int(os.environ.get("QOBUZ_CAPTURE_SKIP_INTERVAL_MS", "250"))


async def run(session: CaptureSession) -> None:
    """Execute the controller-burst-skip scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz
    LOGGER.info(
        "[controller_burst_skip] A presses next %d times at %dms intervals",
        SKIP_COUNT,
        SKIP_INTERVAL_MS,
    )
    for _ in range(SKIP_COUNT):
        await a.skip_next()
        await asyncio.sleep(SKIP_INTERVAL_MS / 1000.0)
    await asyncio.sleep(6)
