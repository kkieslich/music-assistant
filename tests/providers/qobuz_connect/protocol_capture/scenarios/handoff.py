"""
Handoff scenario — reproduces capture-1's flow with two Web Clients.

Steps:
1. Client A starts a track.
2. Client A opens the Connect picker and hands off to Client B.
3. Client B advances to the next track.

What this exercises on the wire:
- AUTHENTICATE / SUBSCRIBE on both clients.
- RNDR_SRVR_JOIN_SESSION on B when it becomes the active renderer.
- SRVR_RNDR_SET_ACTIVE inbound on A (deactivated) and B (activated).
- SRVR_RNDR_SET_STATE inbound on B with the in-flight track.
- CTRL_SRVR_QUEUE_LOAD_TRACKS outbound from A on initial play.
- CTRL_SRVR_SET_PLAYER_STATE outbound from B on skip-next.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

LOGGER = logging.getLogger(__name__)

# Daft Punk — Discovery (verified in discovery.py dump). Each web client can
# also accept a different URL via env var if regional licensing differs.
DEFAULT_ALBUM_URL = os.environ.get(
    "QOBUZ_CAPTURE_ALBUM_URL", "https://play.qobuz.com/album/0724384260958"
)


async def run(session: CaptureSession) -> None:
    """Execute the handoff scenario against ``session``."""
    a = session.a.qobuz

    LOGGER.info("[handoff] A starts album %s", DEFAULT_ALBUM_URL)
    await a.play_album_by_url(DEFAULT_ALBUM_URL)
    await asyncio.sleep(5)

    LOGGER.info("[handoff] A hands off to the other Web Player entry")
    await a.select_connect_target_other_web_player()
    await asyncio.sleep(6)

    # After handoff, the *other* client (B) becomes the active renderer.
    # Driving the skip from A would no-op (A no longer owns the player);
    # send it to B instead.
    b = session.b.qobuz
    LOGGER.info("[handoff] B skips to next track")
    await b.skip_next()
    await asyncio.sleep(5)
