"""
Controller skip-then-immediate-seek — A controls, B renders, A skips and scrubs.

Captures the protocol traffic for the "play next + immediate seek" sequence
that has been observed to leave an MA renderer stopped on a real Raspberry Pi
deployment. A skips to the next track and, before the renderer's track-change
reconcile completes, A seeks within the new track. The capture answers two
questions: how rapidly does Qobuz emit the second ``srvrRndrSetState`` after
the first, and does it travel as a stand-alone position-only event or as a
combined state event with both ``currentItem`` and ``position``.
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

SEEK_FRACTION = float(os.environ.get("QOBUZ_CAPTURE_SKIP_THEN_SEEK_FRACTION", "0.6"))
GAP_AFTER_SKIP_MS = int(os.environ.get("QOBUZ_CAPTURE_SKIP_THEN_SEEK_GAP_MS", "150"))


async def run(session: CaptureSession) -> None:
    """Execute the skip-then-seek scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz
    LOGGER.info(
        "[controller_skip_then_seek] A presses next, then seeks to %.2f after %dms",
        SEEK_FRACTION,
        GAP_AFTER_SKIP_MS,
    )
    await a.skip_next()
    await asyncio.sleep(GAP_AFTER_SKIP_MS / 1000.0)
    await a.seek_to_fraction(SEEK_FRACTION)
    await asyncio.sleep(6)
