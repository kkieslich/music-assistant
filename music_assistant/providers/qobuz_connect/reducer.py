"""
Pure Qobuz Connect sync reducer.

``reduce(state, event)`` is the single owner of MA↔Qobuz sync decisions.
It is pure: no I/O, no MA imports, no websocket. The cloud ``queue_version``
is the logical clock; MA-origin edits are optimistic proposals that become
canonical truth only when the cloud echoes them with a version bump.
"""

from __future__ import annotations

import dataclasses

from .models import QueueTrackRef, QueueVersion
from .sync_types import (
    CanonicalState,
    CloudAutoplayTracksLoaded,
    CloudCleared,
    CloudSnapshot,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudVersionChanged,
    Effect,
    Event,
    MaResyncQueue,
    ReduceResult,
)

_LIST_INBOUND = (
    CloudSnapshot,
    CloudVersionChanged,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudCleared,
    CloudAutoplayTracksLoaded,
)


def reduce(state: CanonicalState, event: Event) -> ReduceResult:
    """Compute the next canonical state and the effects an event produces."""
    version = getattr(event, "version", None)
    if version is not None and _version_le(version, state.cloud_version):
        return ReduceResult(state, ())
    if isinstance(event, _LIST_INBOUND):
        return _reduce_list_inbound(state, event)
    return ReduceResult(state, ())


def _reduce_list_inbound(state: CanonicalState, event: Event) -> ReduceResult:
    if isinstance(event, CloudSnapshot):
        tracks = tuple(event.tracks)
        current_id = _current_from_pointer(tracks, event.track_index)
        new = dataclasses.replace(
            state,
            cloud_version=event.version,
            tracks=tracks,
            autoplay_tracks=tuple(event.autoplay_tracks),
            current_id=current_id,
        )
        return _with_resync(new)
    if isinstance(event, CloudVersionChanged):
        return ReduceResult(dataclasses.replace(state, cloud_version=event.version), ())
    if isinstance(event, CloudCleared):
        new = dataclasses.replace(state, cloud_version=event.version, tracks=(), current_id=None)
        return _with_resync(new)
    if isinstance(event, CloudTracksAdded):
        tracks = state.tracks + tuple(event.tracks)
        return _with_resync(dataclasses.replace(state, cloud_version=event.version, tracks=tracks))
    if isinstance(event, CloudTracksInserted):
        idx = max(0, min(event.insert_after, len(state.tracks)))
        tracks = state.tracks[:idx] + tuple(event.tracks) + state.tracks[idx:]
        return _with_resync(dataclasses.replace(state, cloud_version=event.version, tracks=tracks))
    if isinstance(event, CloudTracksRemoved):
        removed = set(event.queue_item_ids)
        tracks = tuple(t for t in state.tracks if t.queue_item_id not in removed)
        # Current-change on removal only updates the anchor here; MaPlayTrack
        # (restarting audio) is wired in the transport lane (Task 4).
        current_id = _successor_if_removed(state, removed, tracks)
        return _with_resync(
            dataclasses.replace(
                state, cloud_version=event.version, tracks=tracks, current_id=current_id
            ),
        )
    if isinstance(event, CloudTracksReordered):
        tracks = _reorder(state.tracks, event.queue_item_ids, event.insert_after)
        return _with_resync(dataclasses.replace(state, cloud_version=event.version, tracks=tracks))
    if isinstance(event, CloudAutoplayTracksLoaded):
        return _with_resync(
            dataclasses.replace(
                state, cloud_version=event.version, autoplay_tracks=tuple(event.tracks)
            ),
        )
    return ReduceResult(state, ())


def _with_resync(new: CanonicalState) -> ReduceResult:
    """Emit MaResyncQueue only when active; list-lane never restarts audio."""
    if not new.active:
        return ReduceResult(new, ())
    effects: tuple[Effect, ...] = (
        MaResyncQueue(
            track_ids=tuple(t.queue_item_id for t in new.tracks), current_track_id=new.current_id
        ),
    )
    return ReduceResult(new, effects)


def _current_from_pointer(tracks: tuple[QueueTrackRef, ...], track_index: int) -> int | None:
    if not tracks:
        return None
    idx = max(0, min(track_index - 1, len(tracks) - 1))
    return tracks[idx].queue_item_id


def _successor_if_removed(
    state: CanonicalState, removed: set[int], tracks: tuple[QueueTrackRef, ...]
) -> int | None:
    if state.current_id not in removed:
        return state.current_id
    old_ids = [t.queue_item_id for t in state.tracks]
    if state.current_id not in old_ids:
        return tracks[0].queue_item_id if tracks else None
    start = old_ids.index(state.current_id)
    for qid in old_ids[start + 1 :]:
        if qid not in removed:
            return qid
    return tracks[0].queue_item_id if tracks else None


def _reorder(
    tracks: tuple[QueueTrackRef, ...], moving_ids: tuple[int, ...], insert_after: int
) -> tuple[QueueTrackRef, ...]:
    moving_set = set(moving_ids)
    by_id = {t.queue_item_id: t for t in tracks}
    moving = [by_id[i] for i in moving_ids if i in by_id]
    remaining = [t for t in tracks if t.queue_item_id not in moving_set]
    target = max(0, min(insert_after, len(remaining)))
    return tuple(remaining[:target] + moving + remaining[target:])


def _version_le(a: QueueVersion, b: QueueVersion) -> bool:
    return (a.major, a.minor) <= (b.major, b.minor)
