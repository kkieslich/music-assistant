"""
Ephemeral pending-action state for the Qobuz Connect sync engine.

Splits out from :mod:`.sync` what used to be a flat set of ~20 scattered
``_pending_*`` instance fields on ``QobuzConnectSyncEngine``. Each
logical concern now owns its own dataclass — paused-seek vs.
playing-seek-debounce vs. position-confirmation — so adding fields or
zeroing the group is one operation rather than a multi-line
clear-and-renumber dance.

``Origin`` (the in-flight-change source marker) lives next door in
:mod:`.models`; the ``origin_scope`` async context manager here keeps
assignments exception-safe so a missed ``finally`` can't leak the
marker across handlers.

This module is pure DTOs + asyncio glue; no MA imports, no protobuf
imports. Same framework-free rule as :mod:`.models`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .models import Origin, QueueTrackRef

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator


@dataclass(slots=True, frozen=True)
class TrackRefKey:
    """
    Composite key identifying a Qobuz queue item.

    Replaces the legacy ``f"{queue_item_id}:{track_id}"`` string format —
    that was collision-prone (track_ids can contain colons) and allocated
    on every comparison. Tuple equality on this dataclass is hash-stable
    and free at lookup time.
    """

    queue_item_id: int
    track_id: str

    @classmethod
    def from_ref(cls, ref: QueueTrackRef | None) -> TrackRefKey | None:
        """Build a key from a ``QueueTrackRef``, or ``None`` if ref is ``None``."""
        if ref is None:
            return None
        return cls(queue_item_id=ref.queue_item_id, track_id=ref.track_id)


@dataclass(slots=True)
class PausedSeek:
    """
    A scrub the user performed while playback was paused.

    Held until the next PLAYING command, when ``position_ms`` is consumed
    as the resume position. The command handler clears this whenever the
    current Qobuz queue item changes, so no per-track ref is needed here.
    """

    position_ms: int


@dataclass(slots=True)
class PendingPlayingSeek:
    """
    A scrub the user performed while playback was playing.

    Coalesced through a debounce window so rapid scrubs from the Qobuz
    app result in a single MA seek; ``generation`` ties this pending
    seek to a specific Qobuz command so a later command can invalidate
    it. ``ref`` is optional because some position-only commands from
    Qobuz don't carry a queue-item identifier.
    """

    position_ms: int
    ref: TrackRefKey | None
    generation: int
    task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class PendingQobuzPosition:
    """
    Target position we sent to MA, waiting for confirmation.

    Holds Qobuz's reported state frozen at ``target_ms`` until MA's own
    progress report crosses it (within tolerance, allowing for elapsed
    interpolation since ``timestamp_ms``). The command handler clears
    this on track change, so no per-track ref is needed here.
    """

    target_ms: int
    timestamp_ms: int


@asynccontextmanager
async def origin_scope(holder: Any, origin: Origin) -> AsyncIterator[None]:
    """
    Tag the in-flight change with ``origin`` for the duration of the block.

    Replaces the manual ``try/finally`` dance that used to wrap each
    handler. ``holder`` is the sync engine (or any object exposing a
    settable ``origin`` attribute); on exit the attribute is restored to
    its previous value (``None`` in steady state) even if the body
    raises.
    """
    previous = getattr(holder, "origin", None)
    holder.origin = origin
    try:
        yield
    finally:
        holder.origin = previous
