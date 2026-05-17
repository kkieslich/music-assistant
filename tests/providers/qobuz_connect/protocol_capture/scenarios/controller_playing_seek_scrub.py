"""
Controller scrub-while-playing scenario — A controls, B renders, A scrubs around.

Drives the scrubber to multiple positions in quick succession to capture how
the reference renderer handles a stream of seek commands while playing — the
condition the MA-side debounce / pending-position state machine claims to
defend against.
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
    """Execute the playing-scrub scenario against ``session``."""
    await setup_a_controller_b_renderer(session)
    a = session.a.qobuz
    LOGGER.info(
        "[controller_playing_seek_scrub] A scrubs through positions 0.25 → 0.7 → 0.4 → 0.85"
    )
    for fraction in (0.25, 0.7, 0.4, 0.85):
        await a.seek_to_fraction(fraction)
        await asyncio.sleep(0.4)
    await asyncio.sleep(6)
