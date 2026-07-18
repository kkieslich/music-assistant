"""
Mint an MA access token for the integration suite (one-time).

Run once, with MA running, against the local instance::

    MA_USER=<user> MA_PASS=<pass> .venv/bin/python -m \
        tests.providers.qobuz_connect.protocol_capture.mint_ma_token

Writes the token to ``.auth/ma_token`` (gitignored). The suite's MA-initiated
scenarios read it; without it they skip cleanly. Uses MA's own ``auth/login``
command (no direct database writes).
"""

from __future__ import annotations

import asyncio
import os
import sys

import aiohttp

from .harness import AUTH_DIR

# CLI helper: print is the intended output channel (T201 suppressed inline).

MA_WS_URL = "ws://localhost:8095/ws"


async def _mint() -> int:
    user = os.environ.get("MA_USER")
    password = os.environ.get("MA_PASS")
    if not user or not password:
        print("Set MA_USER and MA_PASS (your local MA login) and retry.", file=sys.stderr)  # noqa: T201
        return 2
    async with aiohttp.ClientSession() as session, session.ws_connect(MA_WS_URL) as ws:
        await ws.receive_json()  # server-info greeting
        await ws.send_json(
            {
                "command": "auth/login",
                "message_id": "login",
                "args": {
                    "username": user,
                    "password": password,
                    "device_name": "qobuz-connect-integration",
                },
            }
        )
        for _ in range(8):
            msg = await asyncio.wait_for(ws.receive_json(), timeout=8)
            if isinstance(msg, dict) and msg.get("message_id") == "login":
                result = msg.get("result")
                if (
                    msg.get("error_code")
                    or not isinstance(result, dict)
                    or not result.get("success")
                ):
                    detail = result.get("error") if isinstance(result, dict) else msg.get("details")
                    print(f"login failed: {detail}", file=sys.stderr)  # noqa: T201
                    return 1
                token = result["access_token"]
                AUTH_DIR.mkdir(parents=True, exist_ok=True)
                (AUTH_DIR / "ma_token").write_text(token)
                print(f"wrote {AUTH_DIR / 'ma_token'}")  # noqa: T201
                return 0
    print("no login response", file=sys.stderr)  # noqa: T201
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_mint()))
