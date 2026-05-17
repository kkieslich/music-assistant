"""
Throttled controller-burst-skip — burst skip with slow + lossy renderer.

Same intent as :mod:`.controller_burst_skip`, but B's network is throttled
via Chrome DevTools Protocol before A starts the burst. This is the closest
we can get to "Qobuz mobile app on cellular skipping tracks while the renderer
is on a slow link" — the situation under which we'd expect the reference
renderer to also have to reconcile out-of-order arrivals.
"""

from __future__ import annotations

import asyncio
import contextlib
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
LATENCY_MS = float(os.environ.get("QOBUZ_CAPTURE_THROTTLE_LATENCY_MS", "200"))
DOWNLOAD_KBPS = float(os.environ.get("QOBUZ_CAPTURE_THROTTLE_DOWN_KBPS", "100"))
UPLOAD_KBPS = float(os.environ.get("QOBUZ_CAPTURE_THROTTLE_UP_KBPS", "100"))


async def run(session: CaptureSession) -> None:
    """Execute the throttled controller-burst-skip scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz
    b_recorder = session.b.recorder
    LOGGER.info(
        "[controller_burst_skip_throttled] throttling B: latency=%.0fms down=%skbps up=%skbps",
        LATENCY_MS,
        DOWNLOAD_KBPS,
        UPLOAD_KBPS,
    )
    await b_recorder.set_network_conditions(
        latency_ms=LATENCY_MS,
        download_kbps=DOWNLOAD_KBPS,
        upload_kbps=UPLOAD_KBPS,
    )
    try:
        LOGGER.info(
            "[controller_burst_skip_throttled] A presses next %d times at %dms intervals",
            SKIP_COUNT,
            SKIP_INTERVAL_MS,
        )
        for _ in range(SKIP_COUNT):
            await a.skip_next()
            await asyncio.sleep(SKIP_INTERVAL_MS / 1000.0)
        await asyncio.sleep(8)
    finally:
        with contextlib.suppress(Exception):
            await b_recorder.reset_network_conditions()
