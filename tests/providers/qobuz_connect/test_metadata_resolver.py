"""Fail-cache semantics of the metadata resolver."""

from __future__ import annotations

import logging
from typing import Any, cast

from music_assistant_models.errors import MediaNotFoundError

from music_assistant.providers.qobuz_connect.metadata_resolver import MetadataResolver


class _FakeQobuzProvider:
    """Raises a configurable error on ``get_track`` and counts calls."""

    def __init__(self, error: Exception) -> None:
        """Hold the error to raise and start the call counter."""
        self.error = error
        self.calls = 0

    async def get_track(self, track_id: str) -> Any:
        """Raise the configured error."""
        self.calls += 1
        raise self.error


class _FakeHost:
    """Duck-typed engine host exposing only what the resolver reads."""

    def __init__(self, provider: _FakeQobuzProvider) -> None:
        """Wrap the fake qobuz provider behind a bridge-shaped object."""
        self.bridge = _FakeBridge(provider)


class _FakeBridge:
    """Bridge stub: qobuz provider + logger."""

    def __init__(self, provider: _FakeQobuzProvider) -> None:
        """Hold the fake provider and a real (silent) logger."""
        self._provider = provider
        self.logger = logging.getLogger("test.metadata_resolver")

    def qobuz_music_provider(self) -> _FakeQobuzProvider:
        """Return the fake qobuz provider."""
        return self._provider


async def test_transient_errors_are_retried_not_blacklisted() -> None:
    """
    A network blip / 5xx must not permanently poison a track.

    The fail-cache used to catch every exception, so one transient outage
    made those tracks silently unplayable until provider reload — queue
    mismatches with no recovery on a long-running server.
    """
    provider = _FakeQobuzProvider(RuntimeError("qobuz api 502"))
    resolver = MetadataResolver(cast("Any", _FakeHost(provider)))

    assert await resolver.get_track_or_none("123") is None
    assert await resolver.get_track_or_none("123") is None

    assert provider.calls == 2, "transient failure must be retried"
    assert resolver.unresolvable_track_ids == frozenset()


async def test_media_not_found_is_cached() -> None:
    """A definitive 404 keeps the fail-cache behavior: never re-fetch."""
    provider = _FakeQobuzProvider(MediaNotFoundError("gone"))
    resolver = MetadataResolver(cast("Any", _FakeHost(provider)))

    assert await resolver.get_track_or_none("456") is None
    assert await resolver.get_track_or_none("456") is None

    assert provider.calls == 1
    assert resolver.unresolvable_track_ids == frozenset({"456"})
