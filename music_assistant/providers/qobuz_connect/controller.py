"""
Controller-verb API and renderer-registry state for the Qobuz Connect provider.

Since the single-socket consolidation the provider holds ONE cloud
websocket (a ``QobuzConnectSession`` in CONTROLLER role, joined via
``CtrlSrvrJoinSession`` with our deviceUuid) that carries both roles —
it reports renderer state AND sends controller verbs, exactly like the
reference Qobuz web client. This module does not own that socket; it
tracks the renderer registry the cloud pushes on it (which ``rendererId``
is ours, which is active) and exposes the verb API the sync engine uses
for MA-origin actions.

Exposes:
- ``QobuzConnectController`` — renderer-registry tracking + verbs.

Depends on:
- :mod:`.models` only at runtime. **No MA imports.**
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .models import PlayingState

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable

    from .models import QueueVersion, RendererRecord


class QobuzConnectController:
    """Renderer-registry state + controller verbs over the shared cloud socket."""

    def __init__(
        self,
        device_uuid: bytes,
        logger: logging.Logger,
        session_getter: Callable[[], Any],
    ) -> None:
        """
        Initialize controller state.

        :param device_uuid: Our 16-byte device uuid, used to recognize our own
            entry in the cloud's renderer-registry bootstrap.
        :param logger: Provider logger.
        :param session_getter: Callable returning the provider's (single)
            cloud session, or None while it doesn't exist yet.
        """
        self._device_uuid = device_uuid
        self._logger = logger
        self._session_getter = session_getter
        self._own_renderer_id: int | None = None
        self._active_renderer_id: int | None = None

    @property
    def session(self) -> Any:
        """Return the shared cloud session, or None if not created yet."""
        return self._session_getter()

    @property
    def is_connected(self) -> bool:
        """Return whether controller verbs can currently be issued."""
        session = self.session
        return session is not None and session.is_connected and self._own_renderer_id is not None

    @property
    def own_renderer_id(self) -> int | None:
        """Cloud rendererId of OUR renderer entry (None until discovered)."""
        return self._own_renderer_id

    @property
    def active_renderer_id(self) -> int | None:
        """RendererId the cloud currently routes playback to."""
        return self._active_renderer_id

    async def activate_self(self) -> bool:
        """Make our renderer the session's active playback target."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            self._logger.debug("Controller activate_self skipped: not ready")
            return False
        if self._active_renderer_id == self._own_renderer_id:
            return True
        return bool(await session.send_set_active_renderer(self._own_renderer_id))

    async def load_queue(
        self,
        *,
        action_uuid: bytes,
        track_ids: list[int],
        queue_version: QueueVersion,
        context_uuid: bytes | None = None,
    ) -> bool:
        """Replace the session queue with ``track_ids`` (MA-origin load)."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            self._logger.debug("Controller load_queue skipped: not ready")
            return False
        return bool(
            await session.send_queue_load_tracks(
                action_uuid=action_uuid,
                track_id="",
                queue_version=queue_version,
                track_ids=track_ids,
                context_uuid=context_uuid,
            )
        )

    async def play_item(self, queue_version: QueueVersion, queue_item_id: int) -> bool:
        """Start playback of a specific queue item (skip / start-at-index)."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            return False
        return bool(
            await session.send_ctrl_player_state(
                playing_state=PlayingState.PLAYING,
                position_ms=0,
                queue_version=queue_version,
                queue_item_id=queue_item_id,
            )
        )

    async def set_playing(self, *, playing: bool) -> bool:
        """Pause/resume via controller verb."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            return False
        state = PlayingState.PLAYING if playing else PlayingState.PAUSED
        return bool(await session.send_ctrl_player_state(playing_state=state))

    async def seek(self, position_ms: int) -> bool:
        """Seek via controller verb (position-only SET_PLAYER_STATE)."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            return False
        return bool(await session.send_ctrl_player_state(position_ms=position_ms))

    async def set_volume(self, volume: int) -> bool:
        """Command our renderer's volume through the cloud."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            return False
        return bool(await session.send_ctrl_set_volume(self._own_renderer_id, volume))

    async def set_mute(self, *, muted: bool) -> bool:
        """Command our renderer's mute state through the cloud."""
        session = self.session
        if session is None or self._own_renderer_id is None:
            return False
        return bool(await session.send_ctrl_mute_volume(self._own_renderer_id, muted=muted))

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
            self._logger.debug("Cloud connection lost; clearing renderer-registry state")
        self._own_renderer_id = None
        self._active_renderer_id = None
