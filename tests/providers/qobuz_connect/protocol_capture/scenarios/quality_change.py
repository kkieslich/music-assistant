"""
Quality-change scenario — exercises SRVR_RNDR_SET_MAX_AUDIO_QUALITY.

Steps:
1. Client B starts a track at default quality.
2. Client B changes max quality to a different tier in Settings.
3. Client B keeps playing for a few seconds to capture the file-quality
   round-trip that follows.

Why: the provider has to mirror the user's quality choice back into both
its own config and the underlying qobuz music provider's CONF_QUALITY
(see qobuz_connect/__init__.py:291-322). The exact wire-level form of the
SRVR_RNDR_SET_MAX_AUDIO_QUALITY frame is not covered by the existing
captures because it never fired during those scenarios.
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
QUALITY_LABEL = os.environ.get("QOBUZ_CAPTURE_QUALITY_LABEL", "CD 16-Bit / 44,1 kHz")


async def run(session: CaptureSession) -> None:
    """Execute the quality-change scenario against ``session``."""
    b = session.b.qobuz

    LOGGER.info("[quality_change] B starts album %s", ALBUM_URL)
    await b.play_album_by_url(ALBUM_URL)
    await asyncio.sleep(4)

    LOGGER.info("[quality_change] B sets max quality to %r", QUALITY_LABEL)
    await b.set_max_quality(QUALITY_LABEL)
    await asyncio.sleep(5)
