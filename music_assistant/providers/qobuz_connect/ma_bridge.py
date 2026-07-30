"""
Single seam between the sync core and the Music Assistant world.

Every MA-world touch in the sync shell (``coordinator`` / ``effect_runner``
and the reporter/metadata helpers) goes through this class — both the
provider-facing accessors (``provider.logger``, ``provider.qobuz_session``,
``provider.get_target_player_id()`` …) and the MA-core operations
(``mass.player_queues.play_index``, ``mass.players.cmd_volume_set`` …).
The sync shell holds no direct ``self.mass`` reference and reaches into the
provider only via the bridge.

That seam pays off in two places:

- tests no longer stand up a fake provider mirroring every method-name
  in MA's surface; a single ``FakeMABridge`` fixture covers it;
- changes to MA's API only need to be reflected in one file.

Stays MA-free at the type level — runtime accesses are typed ``Any`` so
adding this module doesn't pull MA-specific imports into the sync graph.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import logging


class MABridge:
    """Provider-facing facade over MA's player/queue APIs for the sync core."""

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

    def qobuz_track_ids_for(self, queue_item: Any) -> tuple[str, ...]:
        """Extract every Qobuz track id mapped to an MA queue item."""
        return cast(
            "tuple[str, ...]",
            self._provider.get_qobuz_track_ids_from_queue_item(queue_item),
        )

    # ---- MA player-queue operations -------------------------------------

    def get_queue(self, player_id: str | None) -> Any | None:
        """Return the MA player-queue for ``player_id`` (or ``None``)."""
        if not player_id:
            return None
        return self._provider.mass.player_queues.get(player_id)

    def set_autoplay(self, player_id: str, enabled: bool) -> None:
        """Set autoplay on the target player's queue."""
        self._provider.mass.player_queues.set_autoplay(player_id, enabled)

    def queue_items(self, player_id: str) -> list[Any]:
        """Return the full ordered ``QueueItem`` list for ``player_id``."""
        # items() defaults to limit=500; the sync core diffs MA's queue
        # against the cloud's canonical list, so a truncated read on a large
        # queue (>500 tracks) would be misread as a user edit and pushed
        # back to the cloud as a truncating queue load.
        return cast("list[Any]", self._provider.mass.player_queues.items(player_id, limit=100_000))

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

    async def insert_items(
        self,
        player_id: str,
        items: list[Any],
        *,
        insert_at_index: int,
        keep_played: bool = True,
        keep_remaining: bool = True,
    ) -> None:
        """
        Insert ``items`` at ``insert_at_index`` without disrupting playback.

        Thin wrapper over :py:meth:`mass.player_queues.load` with the defaults
        the reconciler always wants — keeping played items above the insert
        point and the remainder below.
        """
        await self._provider.mass.player_queues.load(
            player_id,
            items,
            insert_at_index=insert_at_index,
            keep_played=keep_played,
            keep_remaining=keep_remaining,
        )

    def delete_item(self, player_id: str, queue_item_id_or_index: int | str) -> None:
        """Delete a single item from the target player's queue."""
        self._provider.mass.player_queues.delete_item(player_id, queue_item_id_or_index)

    def update_items(self, player_id: str, items: list[Any]) -> None:
        """Replace the ordered queue-items list for a player without restarting playback."""
        self._provider.mass.player_queues.update_items(player_id, items)

    def set_current_index(self, player_id: str, index: int) -> None:
        """
        Set ``PlayerQueue.current_index`` directly to ``index``.

        Required after :py:meth:`update_items` when an external authority
        (the Qobuz cloud) reorders items across the currently-playing
        position — ``update_items`` keeps the integer ``current_index``
        unchanged, so the playing anchor would otherwise point at the
        wrong item. Mirrors the direct-assignment pattern used internally
        by ``mass.player_queues.next/previous/play_index/clear``.
        """
        queue = self._provider.mass.player_queues.get(player_id)
        if queue is not None:
            queue.current_index = index

    def clear_queue(self, player_id: str, *, skip_stop: bool = False) -> None:
        """Clear the target player's queue."""
        self._provider.mass.player_queues.clear(player_id, skip_stop=skip_stop)

    # ---- MA player operations -------------------------------------------

    def get_player(self, player_id: str) -> Any | None:
        """Return the MA player object for ``player_id`` (or ``None``)."""
        return self._provider.mass.players.get_player(player_id)

    async def set_shuffle(self, player_id: str, shuffle_enabled: bool) -> None:
        """Toggle MA's queue shuffle from a cloud command."""
        await self._provider.mass.player_queues.set_shuffle(player_id, shuffle_enabled)

    def set_shuffle_flag(self, player_id: str, shuffle_enabled: bool) -> None:
        """
        Set ``PlayerQueue.shuffle_enabled`` without re-shuffling MA's items.

        Used when applying a Qobuz ``QUEUE_STATE`` snapshot — the reconciler
        rebuilds queue order to match the mirror immediately after, so all
        we need is to flip the UI flag. Going through
        :py:meth:`mass.player_queues.set_shuffle` would smart-shuffle MA's
        items locally, producing an order that's instantly overwritten by
        the reconciler.

        :param player_id: Target MA player id.
        :param shuffle_enabled: New value for the flag.
        """
        queue = self._provider.mass.player_queues.get(player_id)
        if queue is not None:
            queue.shuffle_enabled = shuffle_enabled

    def set_repeat(self, player_id: str, repeat_mode_value: str) -> None:
        """
        Set MA's queue repeat mode from a cloud command.

        Accepts MA's ``RepeatMode`` string value (``"off"`` / ``"one"`` /
        ``"all"``) and constructs the enum at the bridge boundary so the
        sync core stays free of MA type imports.
        """
        from music_assistant_models.enums import RepeatMode  # noqa: PLC0415

        self._provider.mass.player_queues.set_repeat(player_id, RepeatMode(repeat_mode_value))

    async def cmd_volume_set(self, player_id: str, volume: int) -> None:
        """Set the player's volume."""
        await self._provider.mass.players.cmd_volume_set(player_id, volume)

    async def adjust_volume(self, player_id: str, delta: int) -> None:
        """Adjust the player's current volume and clamp it to MA's valid range."""
        player = self.get_player(player_id)
        if player is None or player.volume_level is None:
            return
        await self.cmd_volume_set(player_id, max(0, min(100, player.volume_level + delta)))

    async def set_muted(self, player_id: str, muted: bool) -> None:
        """Set the player's mute state."""
        await self._provider.mass.players.cmd_volume_mute(player_id, muted)
