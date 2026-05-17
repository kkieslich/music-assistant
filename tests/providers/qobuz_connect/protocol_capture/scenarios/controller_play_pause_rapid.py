"""
Rapid play/pause toggle scenario — A controls, B renders, 6 toggles in a row.

Tests state-flapping reconciliation: the renderer must keep its playing/paused
mirror consistent under fast oscillation. Useful for evaluating whether MA's
buffer-state / immediate-report code matches what the reference renderer does.
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

TOGGLE_COUNT = int(os.environ.get("QOBUZ_CAPTURE_TOGGLE_COUNT", "6"))
TOGGLE_INTERVAL_MS = int(os.environ.get("QOBUZ_CAPTURE_TOGGLE_INTERVAL_MS", "400"))


async def run(session: CaptureSession) -> None:
    """Execute the rapid play/pause scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz
    LOGGER.info(
        "[controller_play_pause_rapid] toggling play/pause %d times at %dms intervals",
        TOGGLE_COUNT,
        TOGGLE_INTERVAL_MS,
    )
    for _ in range(TOGGLE_COUNT):
        await a.play()
        await asyncio.sleep(TOGGLE_INTERVAL_MS / 1000.0)
    await asyncio.sleep(6)
