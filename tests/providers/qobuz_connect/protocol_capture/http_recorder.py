"""
HTTP (XHR/fetch) recorder for Qobuz Connect protocol captures.

The :class:`WsRecorder` only sees WebSocket frames, but the Qobuz Connect
cloud tokens (``jwt_qconnect`` / ``jwt_api``) are minted and refreshed over
plain HTTP against the Qobuz web API. This recorder attaches to a Playwright
Page and records every XHR/fetch request to a Qobuz host together with its
request headers, request body and response body, so the token-issuing /
token-refreshing endpoint (and the auth it requires) can be identified.

Output goes to ``.runs/<scenario>__client_*_http.json`` as a flat list of
request records. It is a diagnostic companion to the WS capture, not part of
the wire-schema the decoder consumes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from playwright.async_api import Page, Response

LOGGER = logging.getLogger(__name__)

# Qobuz web API hosts serve the connect-token endpoints; the play.qobuz.com
# SPA host mostly serves static assets, so filtering on "qobuz.com" keeps the
# API calls while dropping analytics/CDN noise on other domains.
DEFAULT_HOST_SUBSTRING = "qobuz.com"

# Only these resource types carry API JSON; page/script/image/font are noise.
_API_RESOURCE_TYPES = frozenset({"xhr", "fetch"})


@dataclass
class HttpRecorder:
    """
    Attach to a Playwright Page and record Qobuz API XHR/fetch traffic.

    Intentionally not ``slots=True`` for the same reason as
    :class:`WsRecorder`: Playwright memoizes bound handlers via ``setattr``.

    Usage::

        rec = HttpRecorder(page)
        await rec.start()
        # ... drive scenario ...
        await rec.stop()
        rec.write(path)
    """

    page: Page
    host_substring: str = DEFAULT_HOST_SUBSTRING
    _records: list[dict[str, Any]] = field(default_factory=list)
    _pending: set[asyncio.Task[None]] = field(default_factory=set)
    _started: bool = False

    async def start(self) -> None:
        """Subscribe to the page's response stream."""
        if self._started:
            return
        self.page.on("response", self._on_response)
        self._started = True
        LOGGER.info("HttpRecorder attached (host=%r)", self.host_substring)

    async def stop(self) -> None:
        """Stop listening and drain any in-flight body reads."""
        if not self._started:
            return
        self.page.remove_listener("response", self._on_response)
        self._started = False
        if self._pending:
            await asyncio.gather(*list(self._pending), return_exceptions=True)

    @property
    def records(self) -> list[dict[str, Any]]:
        """Captured request records (order of completion)."""
        return self._records

    def write(self, path: str | Path) -> Path:
        """
        Write captured records to ``path`` as a JSON list.

        :param path: Output file path. Parent directory must exist.
        :returns: Absolute path of the written file.
        """
        out = Path(path).resolve()
        out.write_text(json.dumps({"records": self._records}, indent=2))
        LOGGER.info("Wrote %d HTTP records to %s", len(self._records), out)
        return out

    def _on_response(self, response: Response) -> None:
        # The handler must stay sync; body reads are async, so defer them.
        if self.host_substring not in response.url:
            return
        if response.request.resource_type not in _API_RESOURCE_TYPES:
            return
        task = asyncio.create_task(self._capture(response))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _capture(self, response: Response) -> None:
        request = response.request
        record: dict[str, Any] = {
            "timestamp_ms": int(time.time() * 1000),
            "method": request.method,
            "url": response.url,
            "status": response.status,
            "request_headers": await _safe(request.all_headers()),
            "request_post_data": request.post_data,
            "response_body": None,
        }
        try:
            record["response_body"] = await response.text()
        except Exception as err:  # body may be unavailable (redirect, evicted)
            record["response_body_error"] = str(err)
        self._records.append(record)


async def _safe(awaitable: Any) -> Any:
    try:
        return await awaitable
    except Exception:
        return None
