"""
MA-origin queue-load orchestration for the Qobuz Connect sync engine.

When MA's player decides to play a track that doesn't match the Qobuz
queue (e.g. because the user picked a track in the MA UI), we mirror
that decision *back* to the Qobuz cloud with a
``CTRL_SRVR_QUEUE_LOAD_TRACKS`` command and wait for the cloud's
acknowledgement. This module owns that exchange:

- mints an action UUID,
- registers a future in the engine's ``_pending_queue_loads`` map,
- builds the context UUID + sends the load command,
- awaits the ack with a 3s timeout,
- on ack: flips ``qobuz_state.playing_state`` to PLAYING/PAUSED based
  on MA's current state and triggers a renderer-state report.

A small pure helper (``try_parse_qobuz_id``) lives here too because
it's only used in this path — the Qobuz cloud's queue-load command
needs a numeric track id, and we silently skip non-numeric ids (Qobuz
radio etc.).
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

from music_assistant_models.enums import PlaybackState as MAPlaybackState

from .models import (
    PlayingState,
    QueueError,
    QueueLoadAck,
    QueueVersion,
)

if TYPE_CHECKING:
    from .sync import QobuzConnectSyncEngine


MA_QUEUE_LOAD_ACK_TIMEOUT_S = 3.0


def try_parse_qobuz_id(value: Any) -> int | None:
    """Return an integer Qobuz id when the protocol can represent the value."""
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


class QueueLoader:
    """MA-origin queue-load round-trip with the Qobuz cloud."""

    __slots__ = ("_engine",)

    def __init__(self, engine: QobuzConnectSyncEngine) -> None:
        """Hold the engine for state + session access."""
        self._engine = engine

    async def send_ma_origin_load(self, track_id: str, queue: Any) -> None:
        """Send a ``CTRL_SRVR_QUEUE_LOAD_TRACKS`` for an MA-picked track."""
        engine = self._engine
        session = engine.bridge.session
        if session is None:
            return
        queue_version = QueueVersion(
            engine.qobuz_state.queue_version.major,
            engine.qobuz_state.queue_version.minor,
        )
        action_uuid = uuid.uuid4().bytes
        future: asyncio.Future[QueueLoadAck | QueueError | None] = (
            asyncio.get_running_loop().create_future()
        )
        engine._pending_queue_loads[action_uuid] = future
        if try_parse_qobuz_id(track_id) is None:
            engine._pending_queue_loads.pop(action_uuid, None)
            engine._last_ma_origin_track_id = None
            engine.bridge.logger.debug(
                "MA-to-Qobuz queue load unsupported for track %s: no numeric Qobuz track id",
                track_id,
            )
            return
        engine.bridge.logger.debug(
            "Trying MA-to-Qobuz QWeb-style queue load for track %s with queue version %s.%s",
            track_id,
            queue_version.major,
            queue_version.minor,
        )
        await session.send_queue_load_tracks(
            action_uuid=action_uuid,
            track_id=track_id,
            queue_version=queue_version,
            context_uuid=self._context_uuid_for_ma_origin_load(),
            qweb_track_session=True,
        )
        try:
            result = await asyncio.wait_for(future, timeout=MA_QUEUE_LOAD_ACK_TIMEOUT_S)
        except TimeoutError:
            engine._pending_queue_loads.pop(action_uuid, None)
            engine._last_ma_origin_track_id = None
            engine.bridge.logger.warning(
                "MA-to-Qobuz queue load unsupported by current session for track %s",
                track_id,
            )
            result = None
        if isinstance(result, QueueLoadAck):
            engine.qobuz_state.playing_state = (
                PlayingState.PLAYING
                if queue.state == MAPlaybackState.PLAYING
                else PlayingState.PAUSED
            )
            engine.reporter.set_buffer_ok()
            await engine.report_state()
        elif isinstance(result, QueueError):
            engine._last_ma_origin_track_id = None

    # ---- helpers --------------------------------------------------------

    def _context_uuid_for_ma_origin_load(self) -> bytes:
        """Return a valid 16-byte context UUID for the load command."""
        mirror = self._engine.qobuz_state
        current_item = mirror.current_item
        if current_item and current_item.context_uuid and len(current_item.context_uuid) == 16:
            return current_item.context_uuid
        next_item = mirror.next_item
        if next_item and next_item.context_uuid and len(next_item.context_uuid) == 16:
            return next_item.context_uuid
        return uuid.uuid4().bytes
