"""
Single seam between the sync engine and the Music Assistant world.

After Phase C, every MA-world touch in :mod:`.sync` goes through this
class — both the provider-facing accessors (``provider.logger``,
``provider.qobuz_session``, ``provider.get_target_player_id()`` …) and
the MA-core operations (``mass.player_queues.play_index``,
``mass.players.cmd_volume_set`` …). The sync engine no longer holds a
direct ``self.mass`` reference and reaches into the provider only via
``self.bridge``.

That seam pays off in two places:

- tests no longer stand up a fake provider mirroring every method-name
  in MA's surface; a single ``FakeMABridge`` fixture covers it;
- changes to MA's API only need to be reflected in one file.

Stays MA-free at the type level — runtime accesses are typed ``Any`` so
adding this module doesn't pull MA-specific imports into the sync
graph.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import logging


class MABridge:
    """Provider-facing facade for :class:`.sync.QobuzConnectSyncEngine`."""

    __slots__ = ("_provider",)

    def __init__(self, provider: Any) -> None:
        """Wrap a ``QobuzConnectProvider`` instance."""
        self._provider = provider

    @property
    def provider(self) -> Any:
        """Return the underlying provider — escape hatch for the few sites that need it."""
        return self._provider

    @property
    def mass(self) -> Any:
        """Return the Music Assistant instance, for direct ``mass.*`` use."""
        return self._provider.mass

    @property
    def logger(self) -> logging.Logger:
        """Provider logger."""
        return cast("logging.Logger", self._provider.logger)

    @property
    def session(self) -> Any:
        """
        Current Qobuz cloud websocket session, or ``None`` if not connected.

        Returns the session via the provider's ``qobuz_session`` property
        so callers always see the latest instance (reconnects may
        replace it).
        """
        return self._provider.qobuz_session

    def target_player_id(self) -> str | None:
        """Resolve the configured Music Assistant target player id."""
        return cast("str | None", self._provider.get_target_player_id())

    def qobuz_music_provider(self) -> Any:
        """Return the native MA Qobuz music provider (used for track metadata)."""
        return self._provider.get_qobuz_provider()

    def qobuz_track_id_for(self, queue_item: Any) -> str | None:
        """Extract the Qobuz track id from an MA queue item, if present."""
        return cast("str | None", self._provider.get_qobuz_track_id_from_queue_item(queue_item))

    # ---- MA player-queue operations -------------------------------------

    def get_queue(self, player_id: str | None) -> Any | None:
        """Return the MA player-queue for ``player_id`` (or ``None``)."""
        if not player_id:
            return None
        return self._provider.mass.player_queues.get(player_id)

    def queue_items(self, player_id: str) -> list[Any]:
        """Return the full ordered ``QueueItem`` list for ``player_id``."""
        return cast("list[Any]", self._provider.mass.player_queues.items(player_id))

    async def play(self, player_id: str) -> None:
        """Resume playback on the target player."""
        await self._provider.mass.player_queues.play(player_id)

    async def pause(self, player_id: str) -> None:
        """Pause playback on the target player."""
        await self._provider.mass.player_queues.pause(player_id)

    async def stop_queue(self, player_id: str) -> None:
        """Stop playback on the target player."""
        await self._provider.mass.player_queues.stop(player_id)

    async def seek(self, player_id: str, position: int) -> None:
        """Seek the target player to a given position (in seconds)."""
        await self._provider.mass.player_queues.seek(player_id, position)

    async def play_index(self, player_id: str, index: int, **kwargs: Any) -> None:
        """Start a specific queue index on the target player."""
        await self._provider.mass.player_queues.play_index(player_id, index, **kwargs)

    async def play_media(self, player_id: str, media: Any, **kwargs: Any) -> None:
        """Play / enqueue media on the target player."""
        await self._provider.mass.player_queues.play_media(
            queue_id=player_id, media=media, **kwargs
        )

    async def load_queue(self, player_id: str, queue_items: list[Any], **kwargs: Any) -> None:
        """Replace the target player's queue with the given items."""
        await self._provider.mass.player_queues.load(player_id, queue_items, **kwargs)

    def clear_queue(self, player_id: str, *, skip_stop: bool = False) -> None:
        """Clear the target player's queue."""
        self._provider.mass.player_queues.clear(player_id, skip_stop=skip_stop)

    # ---- MA player operations -------------------------------------------

    def get_player(self, player_id: str) -> Any | None:
        """Return the MA player object for ``player_id`` (or ``None``)."""
        return self._provider.mass.players.get_player(player_id)

    async def cmd_volume_set(self, player_id: str, volume: int) -> None:
        """Set the player's volume."""
        await self._provider.mass.players.cmd_volume_set(player_id, volume)
