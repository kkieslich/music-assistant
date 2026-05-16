"""Provider-local Qobuz Connect mDNS and HTTP discovery."""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from collections.abc import Callable
from typing import Any

from aiohttp import web
from zeroconf import ServiceInfo, Zeroconf

from .models import (
    OAUTH_APP_ID,
    QUALITY_TO_HTTP,
    ConnectTokens,
    DeviceConfig,
    JWTApiToken,
    JWTConnectToken,
)

LOGGER = logging.getLogger(__name__)

MDNS_SERVICE_TYPE = "_qobuz-connect._tcp.local."
SDK_VERSION = "ma-qobuz-connect"


class QobuzConnectDiscovery:
    """Expose this MA instance to Qobuz apps and receive connect tokens."""

    def __init__(
        self,
        device: DeviceConfig,
        on_connect: Callable[[ConnectTokens], None],
        quality_getter: Callable[[], int],
    ) -> None:
        """Initialize discovery service."""
        self.device = device
        self.on_connect = on_connect
        self.quality_getter = quality_getter
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._zeroconf: Zeroconf | None = None
        self._service_info: ServiceInfo | None = None
        self._current_session_id = ""
        self._received_tokens: ConnectTokens | None = None

    async def start(self) -> None:
        """Start HTTP endpoints and register mDNS service."""
        self._app = web.Application()
        self._app.router.add_get("/", self._handle_root)
        self._app.router.add_get("/streamcore/get-display-info", self._handle_display_info)
        self._app.router.add_get("/streamcore/get-connect-info", self._handle_connect_info)
        self._app.router.add_post("/streamcore/connect-to-qconnect", self._handle_connect)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.device.bind_address, self.device.http_port)
        await self._site.start()
        await self._register_mdns()

    async def stop(self) -> None:
        """Stop discovery."""
        await self._unregister_mdns()
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()

    async def _handle_root(self, request: web.Request) -> web.Response:
        return web.Response(text=f"Music Assistant Qobuz Connect - {self.device.name}")

    async def _handle_display_info(self, request: web.Request) -> web.Response:
        quality_id = self.quality_getter()
        return web.json_response(
            {
                "type": "SPEAKER",
                "friendly_name": self.device.name,
                "model_display_name": "Music Assistant",
                "brand_display_name": "Music Assistant",
                "serial_number": self.device.uuid,
                "max_audio_quality": QUALITY_TO_HTTP.get(quality_id, "HIRES_L3"),
            }
        )

    async def _handle_connect_info(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "current_session_id": self._current_session_id,
                "app_id": OAUTH_APP_ID,
            }
        )

    async def _handle_connect(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            tokens = self._parse_connect_request(data)
            if not tokens.is_valid():
                return web.json_response({"error": "Invalid tokens"}, status=400)
            self._received_tokens = tokens
            self._current_session_id = tokens.session_id
            self.on_connect(tokens)
            return web.json_response({})
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except Exception as err:
            LOGGER.exception("Error handling Qobuz connect request")
            return web.json_response({"error": str(err)}, status=500)

    @staticmethod
    def _parse_connect_request(data: dict[str, Any]) -> ConnectTokens:
        tokens = ConnectTokens(session_id=data.get("session_id", ""))
        if jwt_qconnect := data.get("jwt_qconnect", {}):
            tokens.ws_token = JWTConnectToken(
                jwt=jwt_qconnect.get("jwt", ""),
                exp=jwt_qconnect.get("exp", 0),
                endpoint=jwt_qconnect.get("endpoint", ""),
            )
        if jwt_api := data.get("jwt_api", {}):
            tokens.api_token = JWTApiToken(
                jwt=jwt_api.get("jwt", ""),
                exp=jwt_api.get("exp", 0),
            )
        return tokens

    async def _register_mdns(self) -> None:
        local_ip = self._get_local_ip()
        if not local_ip:
            LOGGER.warning("Could not determine local IP for Qobuz Connect mDNS")
            return
        service_name = f"{_sanitize_service_name(self.device.name)}.{MDNS_SERVICE_TYPE}"
        self._service_info = ServiceInfo(
            MDNS_SERVICE_TYPE,
            service_name,
            addresses=[socket.inet_aton(local_ip)],
            port=self.device.http_port,
            properties={
                "path": "/streamcore",
                "type": "SPEAKER",
                "sdk_version": SDK_VERSION,
                "Name": self.device.name,
                "device_uuid": self.device.uuid,
            },
        )
        zeroconf = self._zeroconf = Zeroconf()
        service_info = self._service_info
        assert service_info is not None
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, zeroconf.register_service, service_info)
        except Exception:
            await loop.run_in_executor(
                None,
                lambda: zeroconf.register_service(
                    service_info,
                    cooperating_responders=True,
                ),
            )

    async def _unregister_mdns(self) -> None:
        if self._zeroconf and self._service_info:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._zeroconf.unregister_service, self._service_info)
            await loop.run_in_executor(None, self._zeroconf.close)

    @staticmethod
    def _get_local_ip() -> str | None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(0)
            try:
                sock.connect(("8.8.8.8", 80))
                return str(sock.getsockname()[0])
            finally:
                sock.close()
        except Exception:
            LOGGER.exception("Failed to determine local IP")
            return None


def _sanitize_service_name(name: str) -> str:
    sanitized = "".join(c if c.isalnum() or c in "-_" else "-" for c in name.replace(" ", "-"))
    while "--" in sanitized:
        sanitized = sanitized.replace("--", "-")
    return sanitized.strip("-")
