"""
Queue-mutations scenario — exercises clear / add / pause / resume / reorder.

Steps (favorite/unfavorite intentionally omitted — those round-trip via REST,
not QConnect):
1. Client B clears its queue.
2. Client B starts a track.
3. Client A adds a track to B's queue (cross-client controller).
4. Client B adds a track to its own queue.
5. Client B pauses.
6. Client B resumes.
7. Client B reorders the currently playing item +3 positions in the queue.

What this exercises on the wire that the current implementation does NOT
handle yet (these are the protocol gaps the plan calls out):
- CTRL_SRVR_CLEAR_QUEUE outbound, SRVR_CTRL_QUEUE_CLEARED inbound.
- CTRL_SRVR_QUEUE_ADD_TRACKS outbound, SRVR_CTRL_QUEUE_TRACKS_ADDED inbound.
- CTRL_SRVR_QUEUE_REORDER_TRACKS outbound, SRVR_CTRL_QUEUE_TRACKS_REORDERED
  inbound.
- Plus the regular RNDR_SRVR_STATE_UPDATED stream that lets us verify the
  current decoder against full bidirectional traffic.
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


async def run(session: CaptureSession) -> None:
    """Execute the queue-mutations scenario against ``session``."""
    b = session.b.qobuz

    LOGGER.info("[queue_mutations] B clears its queue")
    await b.clear_queue()
    await asyncio.sleep(3)

    LOGGER.info("[queue_mutations] B starts album %s", ALBUM_URL)
    await b.play_album_by_url(ALBUM_URL)
    await asyncio.sleep(5)

    LOGGER.info("[queue_mutations] B adds track 3 from same album to queue")
    await b.add_track_to_queue_on_open_album(3)
    await asyncio.sleep(3)

    LOGGER.info("[queue_mutations] B adds track 5 to queue")
    await b.add_track_to_queue_on_open_album(5)
    await asyncio.sleep(3)

    LOGGER.info("[queue_mutations] B pauses")
    await b.pause()
    await asyncio.sleep(3)

    LOGGER.info("[queue_mutations] B resumes")
    await b.play()
    await asyncio.sleep(3)

    LOGGER.info("[queue_mutations] B reorders current item +3")
    await b.reorder_current_forward(3)
    await asyncio.sleep(5)
