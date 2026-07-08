"""
Controller-role connection to the Qobuz cloud.

Owns the second (persistent) websocket the provider holds: a
``QobuzConnectSession`` in CONTROLLER role that joins with the SAME
deviceUuid as the renderer, so the cloud merges the two connections into
one picker entry (verified live 2026-07-07). All MA-origin actions are
sent through this connection as controller verbs; the cloud loops them
back to our renderer as ``SRVR_RNDR_SET_STATE``, which the existing
follower path executes.

Exposes:
- ``QobuzConnectController`` — lifecycle (start/stop), renderer-registry
  tracking (which ``rendererId`` is ours, which is active) and the verb
  API used by the sync engine.

Depends on:
- :mod:`.session` / :mod:`.models`. **No MA imports.**
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .models import PlayingState, SessionRole

if TYPE_CHECKING:
    import logging
    from collections.abc import Awaitable, Callable

    from .models import (
        DeviceConfig,
        JWTConnectToken,
        QueueVersion,
        RendererRecord,
    )
    from .session import QobuzConnectSession, SessionCallbacks


class QobuzConnectController:
    """Persistent controller-role cloud connection + renderer-registry state."""

    def __init__(
        self,
        device: DeviceConfig,
        device_uuid: bytes,
        token_refresher: Callable[[], Awaitable[JWTConnectToken | None]],
        logger: logging.Logger,
        base_callbacks: SessionCallbacks,
    ) -> None:
        """
        Initialize controller.

        :param device: Shared device identity (same values the renderer uses).
        :param device_uuid: The renderer's 16-byte device uuid; joining with
            it makes the cloud merge this connection into the renderer entry.
        :param token_refresher: Coroutine minting a websocket JWT via the
            native Qobuz login (same one the renderer session uses).
        :param logger: Provider logger.
        :param base_callbacks: The provider's renderer callback bundle. The
            cloud routes renderer-directed unicast frames to the most
            recently joined connection with a given deviceUuid, so this
            connection must handle them with the same handlers as the
            renderer session.
        """
        self._device = device
        self._device_uuid = device_uuid
        self._token_refresher = token_refresher
        self._logger = logger
        self._base_callbacks = base_callbacks
        self._session: QobuzConnectSession | None = None
        self._own_renderer_id: int | None = None
        self._active_renderer_id: int | None = None

    @property
    def session(self) -> QobuzConnectSession | None:
        """Return the underlying controller session, if started."""
        return self._session

    @property
    def is_connected(self) -> bool:
        """Return whether the controller can currently issue verbs."""
        return (
            self._session is not None
            and self._session.is_connected
            and self._own_renderer_id is not None
        )

    @property
    def own_renderer_id(self) -> int | None:
        """Cloud rendererId of OUR renderer entry (None until discovered)."""
        return self._own_renderer_id

    @property
    def active_renderer_id(self) -> int | None:
        """RendererId the cloud currently routes playback to."""
        return self._active_renderer_id

    async def start(self) -> None:
        """Open the controller websocket (idempotent)."""
        if self._session is not None:
            return
        # Local import: session imports models, controller is imported by
        # __init__ — keep import-time cycles impossible.
        import dataclasses  # noqa: PLC0415

        from .session import QobuzConnectSession  # noqa: PLC0415

        async def _noop(*_args: object, **_kwargs: object) -> None:
            return None

        # Renderer-directed unicast frames (SET_STATE, SET_ACTIVE, volume,
        # quality, modes, state requests) follow the most recently joined
        # same-deviceUuid connection — i.e. this one — so they run through
        # the same provider handlers as on the renderer socket. Queue DELTA
        # broadcasts fan out to every session socket and mutate the mirror
        # non-idempotently, so those stay noop here; the renderer connection
        # applies them exactly once. Load acks / queue errors are idempotent
        # and may be delivered to the issuing socket only — shared.
        self._session = QobuzConnectSession(
            self._device,
            dataclasses.replace(
                self._base_callbacks,
                on_queue_version=_noop,
                on_queue_state=_noop,
                on_queue_tracks_added=_noop,
                on_queue_tracks_inserted=_noop,
                on_queue_tracks_removed=_noop,
                on_queue_tracks_reordered=_noop,
                on_queue_cleared=_noop,
                on_add_renderer=self._on_add_renderer,
                on_remove_renderer=self._on_remove_renderer,
                on_active_renderer_changed=self._on_active_renderer_changed,
                on_disconnected=self._on_disconnected,
            ),
            token_refresher=self._token_refresher,
            role=SessionRole.CONTROLLER,
        )
        await self._session.start()

    async def stop(self) -> None:
        """Close the controller websocket."""
        if self._session is not None:
            await self._session.stop()
            self._session = None
        self._own_renderer_id = None
        self._active_renderer_id = None

    async def activate_self(self) -> bool:
        """Make our renderer the session's active playback target."""
        if self._session is None or self._own_renderer_id is None:
            self._logger.debug("Controller activate_self skipped: not ready")
            return False
        if self._active_renderer_id == self._own_renderer_id:
            return True
        return await self._session.send_set_active_renderer(self._own_renderer_id)

    async def load_queue(
        self,
        *,
        action_uuid: bytes,
        track_ids: list[int],
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
    ) -> bool:
        """Replace the session queue with ``track_ids`` (MA-origin load)."""
        if self._session is None or self._own_renderer_id is None:
            self._logger.debug("Controller load_queue skipped: not ready")
            return False
        return await self._session.send_queue_load_tracks(
            action_uuid=action_uuid,
            track_id="",
            queue_version=queue_version,
            track_ids=track_ids,
            context_uuid=context_uuid,
        )

    async def play_item(self, queue_version: QueueVersion, queue_item_id: int) -> bool:
        """Start playback of a specific queue item (skip / start-at-index)."""
        if self._session is None or self._own_renderer_id is None:
            return False
        return await self._session.send_ctrl_player_state(
            playing_state=PlayingState.PLAYING,
            position_ms=0,
            queue_version=queue_version,
            queue_item_id=queue_item_id,
        )

    async def set_playing(self, *, playing: bool) -> bool:
        """Pause/resume via controller verb."""
        if self._session is None or self._own_renderer_id is None:
            return False
        state = PlayingState.PLAYING if playing else PlayingState.PAUSED
        return await self._session.send_ctrl_player_state(playing_state=state)

    async def seek(self, position_ms: int) -> bool:
        """Seek via controller verb (position-only SET_PLAYER_STATE)."""
        if self._session is None or self._own_renderer_id is None:
            return False
        return await self._session.send_ctrl_player_state(position_ms=position_ms)

    async def set_volume(self, volume: int) -> bool:
        """Command our renderer's volume through the cloud."""
        if self._session is None or self._own_renderer_id is None:
            return False
        return await self._session.send_ctrl_set_volume(self._own_renderer_id, volume)

    async def set_mute(self, *, muted: bool) -> bool:
        """Command our renderer's mute state through the cloud."""
        if self._session is None or self._own_renderer_id is None:
            return False
        return await self._session.send_ctrl_mute_volume(self._own_renderer_id, muted=muted)

    # ---- session callbacks ------------------------------------------------

    async def _on_add_renderer(self, record: RendererRecord) -> None:
        if record.device_uuid == self._device_uuid:
            self._logger.debug(
                "Controller discovered own rendererId=%s (%s)",
                record.renderer_id,
                record.friendly_name,
            )
            self._own_renderer_id = record.renderer_id

    async def _on_remove_renderer(self, renderer_id: int) -> None:
        if renderer_id == self._own_renderer_id:
            self._logger.debug("Controller lost own rendererId=%s", renderer_id)
            self._own_renderer_id = None
        if renderer_id == self._active_renderer_id:
            self._active_renderer_id = None

    async def _on_active_renderer_changed(self, renderer_id: int) -> None:
        self._active_renderer_id = renderer_id

    async def _on_disconnected(self) -> None:
        if self._own_renderer_id is not None or self._active_renderer_id is not None:
            self._logger.debug("Controller connection lost; clearing renderer-registry state")
        self._own_renderer_id = None
        self._active_renderer_id = None
