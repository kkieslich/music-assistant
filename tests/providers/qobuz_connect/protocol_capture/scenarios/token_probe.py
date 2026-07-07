"""
Token-probe scenario — capture the Qobuz connect-token HTTP endpoint(s).

The Qobuz Web Client fetches its cloud ``jwt_qconnect`` / ``jwt_api`` tokens
over HTTP as the SPA boots and registers itself as an available Connect
renderer. This scenario does the minimum needed to trigger those fetches:
open both clients (the harness already navigates + waits for the first WS
frame) and idle so the HTTP recorder can capture the token-issuing calls.

No UI automation, so it does not depend on any fragile Qobuz DOM selectors —
its whole purpose is the ``*_http.json`` companion capture, which is grepped
for ``jwt_qconnect`` / ``endpoint`` to find the endpoint (and the auth header)
our device must call to refresh its own token.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

LOGGER = logging.getLogger(__name__)


async def run(session: CaptureSession) -> None:  # noqa: ARG001
    """Idle both clients so the HTTP recorder captures the token fetches."""
    LOGGER.info("[token_probe] both clients up; idling to capture token HTTP calls")
    # The connect token is fetched during SPA boot (already done by the time
    # the harness saw the first WS frame). Idle a little longer in case the
    # client lazily fetches/refreshes it shortly after the socket opens.
    await asyncio.sleep(20)
