"""
Single seam between the sync engine and the Music Assistant world.

Why this exists: the sync engine used to reach into the provider object
~40 times directly — ``provider.get_target_player_id()``,
``provider.get_qobuz_track_id_from_queue_item()``,
``provider.qobuz_session``, ``provider.logger``, etc. — across hundreds
of lines. Tests had to stand up a fake provider whose surface matched
every one of those calls.

The bridge collects the provider-facing accessors behind a small typed
interface. Future stages of the Phase C redesign will:

- expand the bridge to wrap MA-core operations
  (``mass.player_queues.play_index``, ``mass.players.cmd_volume_set``, ...)
  so sync.py stops touching ``mass`` directly;
- replace it with an abstract base in tests so a single ``FakeMABridge``
  fixture covers every sync handler.

For now it is intentionally a thin delegating layer: same behavior as
before, just one place to find every provider-level call.

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
