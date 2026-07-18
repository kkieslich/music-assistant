"""
MA track-metadata lookups for the Qobuz Connect sync core.

Every place that needs the full ``Track`` for a Qobuz queue item asks the
native MA qobuz provider for ``get_track(id)``, caches the ``track_id`` if it
fails, and surfaces a typed ``Track | None``. This module pulls that into one
place and gives the "this track is unresolvable, don't keep retrying"
fail-cache its own home.

The effect runner uses it to resolve the Qobuz ids in ``MaPlayTrack`` /
``MaResyncQueue`` effects into MA ``Track`` objects before driving the player
queue.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from music_assistant_models.errors import MediaNotFoundError

if TYPE_CHECKING:
    from collections.abc import Callable

    from music_assistant_models.media_items import Track


class MetadataResolver:
    """Resolves Qobuz track ids to MA ``Track`` objects, with a fail-cache."""

    __slots__ = ("_logger", "_qobuz_provider_getter", "_unresolvable_track_ids")

    def __init__(
        self,
        *,
        qobuz_provider_getter: Callable[[], object],
        logger: logging.Logger,
    ) -> None:
        """
        Bind the resolver to the native Qobuz provider accessor.

        :param qobuz_provider_getter: Returns the native MA Qobuz music provider
            (looked up lazily so a briefly-absent provider raises at call time).
        :param logger: Logger for unresolved-track diagnostics.
        """
        self._qobuz_provider_getter = qobuz_provider_getter
        self._logger = logger
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
        provider = self._qobuz_provider_getter()
        return cast("Track", await provider.get_track(track_id))  # type: ignore[attr-defined]

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
            self._logger.warning("Ignoring unresolved Qobuz Connect cloud track %s", track_id)
            return None
        except Exception as err:
            self._logger.warning(
                "Qobuz Connect track %s temporarily unavailable (%s); will retry",
                track_id,
                err,
            )
            return None
