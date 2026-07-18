"""
MA track-metadata lookups for the Qobuz Connect sync core.

Every place that needs the duration or full ``Track`` for a Qobuz queue
item asks the native MA qobuz provider for ``get_track(id)``, caches the
``track_id`` if it fails, and surfaces a typed ``Track | None``. This module
pulls that into one place and gives the "this track is unresolvable, don't
keep retrying" fail-cache its own home.

The effect runner uses it (via a small duck-typed host exposing ``bridge``)
to resolve the Qobuz ids in ``MaPlayTrack`` / ``MaResyncQueue`` effects into
MA ``Track`` objects before driving the player queue.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from music_assistant_models.errors import MediaNotFoundError

if TYPE_CHECKING:
    from music_assistant_models.media_items import Track

    from .models import QueueTrackRef


class MetadataResolver:
    """Resolves Qobuz track ids to MA ``Track`` objects, with a fail-cache."""

    __slots__ = ("_engine", "_unresolvable_track_ids")

    def __init__(self, engine: Any) -> None:
        """Bind the resolver to the host engine for bridge + state access."""
        self._engine = engine
        self._unresolvable_track_ids: set[str] = set()

    def clear_unresolvable_cache(self) -> None:
        """Drop the "track lookups previously failed" memo set."""
        self._unresolvable_track_ids.clear()

    @property
    def unresolvable_track_ids(self) -> frozenset[str]:
        """Snapshot of track ids the metadata fetch has marked unfetchable."""
        return frozenset(self._unresolvable_track_ids)

    async def get_track(self, track_id: str) -> Track:
        """Fetch an MA ``Track`` for a Qobuz track id; raises on cloud failure."""
        return cast(
            "Track",
            await self._engine.bridge.qobuz_music_provider().get_track(track_id),
        )

    async def get_track_or_none(self, track_id: str) -> Track | None:
        """
        Fetch an MA ``Track`` for a Qobuz track id, or ``None`` if it fails.

        A definitive not-found adds the id to the fail-cache so we don't keep
        retrying it; transient failures (network blips, rate limits, the qobuz
        provider briefly reloading) are NOT cached — blacklisting those made
        tracks silently unplayable for the rest of a long-running session.
        """
        if track_id in self._unresolvable_track_ids:
            return None
        try:
            return await self.get_track(track_id)
        except MediaNotFoundError:
            self._unresolvable_track_ids.add(track_id)
            self._engine.bridge.logger.warning(
                "Ignoring unresolved Qobuz Connect cloud track %s", track_id
            )
            return None
        except Exception as err:
            self._engine.bridge.logger.warning(
                "Qobuz Connect track %s temporarily unavailable (%s); will retry",
                track_id,
                err,
            )
            return None

    async def ensure_track_duration(self, item: QueueTrackRef) -> None:
        """Resolve track metadata + write duration_ms into the mirror."""
        ma_track = await self.get_track(item.track_id)
        self._engine.qobuz_state.duration_ms = (ma_track.duration or 0) * 1000

    async def try_ensure_track_duration(self, item: QueueTrackRef) -> bool:
        """
        Try to resolve track metadata for the mirror; cache failures.

        :returns: ``True`` if the lookup succeeded and the mirror duration
            was updated, ``False`` if the track was already known
            unresolvable or the lookup raised.
        """
        if item.track_id in self._unresolvable_track_ids:
            self._engine.qobuz_state.duration_ms = 0
            return False
        try:
            await self.ensure_track_duration(item)
        except MediaNotFoundError:
            self._unresolvable_track_ids.add(item.track_id)
            self._engine.qobuz_state.duration_ms = 0
            self._engine.bridge.logger.warning(
                "Ignoring unresolved Qobuz Connect cloud track %s",
                item.track_id,
            )
            return False
        except Exception as err:
            self._engine.qobuz_state.duration_ms = 0
            self._engine.bridge.logger.warning(
                "Qobuz Connect track %s temporarily unavailable (%s); will retry",
                item.track_id,
                err,
            )
            return False
        return True
