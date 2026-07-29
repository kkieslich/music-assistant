"""
Impure shell: execute reducer ``Effect``s against the Qobuz cloud + MA.

This is the first shell module in the reducer rework — everything above it
(:mod:`.reducer`, :mod:`.sync_types`) is pure and produces ``Effect`` values
instead of doing I/O directly. ``EffectRunner`` is the only place those
values turn into real ``session.send_*`` calls or ``ma_bridge`` mutations.

ID semantics matter here and are **not** re-translated by this module:

- ``PushLoad``/``PushAdd``/``PushInsert`` track ids are Qobuz track ids
  (``int``) — the cloud add/insert verbs want ``QueueTrackRef``s, so this
  module wraps each id with ``queue_item_id=0`` (the cloud assigns the real
  slot id and echoes it back on the list lane).
- ``PushRemove``/``PushReorder``/``PushPlayerState`` ids are already cloud
  slot ids (``int``) and pass straight through.
- ``MaPlayTrack``/``MaResyncQueue`` ids are Qobuz track ids and drive MA
  playback via :class:`.metadata_resolver.MetadataResolver` + the bridge.

Stays free of ``asyncio``-adjacent cleverness — every branch is a small,
readable ``await`` so a future protocol change (new effect, new session
verb) touches exactly one branch.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from music_assistant_models.enums import QueueOption
from music_assistant_models.queue_item import QueueItem

from .models import LoopMode, QueueTrackRef
from .sync_types import (
    AskSnapshot,
    Effect,
    MaPause,
    MaPlayTrack,
    MaReleasePlayer,
    MaResume,
    MaResyncQueue,
    MaSeek,
    MaSetLoop,
    MaSetShuffleFlag,
    MaSetVolume,
    PushAdd,
    PushClear,
    PushInsert,
    PushLoad,
    PushLoop,
    PushMute,
    PushPlayerState,
    PushQuality,
    PushRemove,
    PushReorder,
    PushSetActive,
    PushVolume,
    ReportState,
)

if TYPE_CHECKING:
    from .ma_bridge import MABridge
    from .metadata_resolver import MetadataResolver
    from .outbound_reporter import OutboundReporter
    from .session import QobuzConnectSession

LOGGER = logging.getLogger(__name__)

RESYNC_RESOLVE_CONCURRENCY = 5

# Qobuz LoopMode -> MA RepeatMode string value. Kept local to this shell
# module so it owns its own Qobuz→MA enum translation.
_LOOP_TO_MA_REPEAT: dict[LoopMode, str] = {
    LoopMode.OFF: "off",
    LoopMode.REPEAT_ONE: "one",
    LoopMode.REPEAT_ALL: "all",
}


class EffectRunner:
    """Executes a single reducer ``Effect`` against the cloud session or MA bridge."""

    def __init__(
        self,
        *,
        session: QobuzConnectSession,
        bridge: MABridge,
        metadata: MetadataResolver | None,
        reporter: OutboundReporter | None,
        own_rid_getter: Callable[[], int | None] = lambda: None,
        generation_getter: Callable[[], int] = lambda: 0,
    ) -> None:
        """
        Bind the runner to the collaborators it dispatches effects to.

        :param session: Qobuz cloud websocket session for ``Push*``/``AskSnapshot`` effects.
        :param bridge: MA-facing bridge for every ``Ma*`` effect.
        :param metadata: Resolver used to materialize Qobuz track ids into MA tracks;
            ``None`` if the caller never routes an effect that needs it.
        :param reporter: Outbound renderer-state reporter for ``ReportState``; ``None``
            if the caller never routes that effect.
        :param own_rid_getter: Returns this renderer's own Qobuz renderer id, or ``None``
            if not yet known — consulted by ``PushSetActive``.
        :param generation_getter: Returns the newest canonical queue generation.
        """
        self._session = session
        self._bridge = bridge
        self._metadata = metadata
        self._reporter = reporter
        self._own_rid_getter = own_rid_getter
        self._generation_getter = generation_getter

    async def run(self, effect: Effect) -> None:
        """Execute a single reducer effect."""
        LOGGER.debug("effect_runner.run %s", type(effect).__name__)
        if isinstance(
            effect,
            MaPlayTrack
            | MaPause
            | MaResume
            | MaSeek
            | MaResyncQueue
            | MaSetLoop
            | MaSetShuffleFlag
            | MaSetVolume
            | MaReleasePlayer,
        ):
            await self._run_ma_effect(effect)
        else:
            await self._run_cloud_effect(effect)

    # ---- cloud effect dispatch ---------------------------------------------

    async def _run_cloud_effect(self, effect: Effect) -> None:
        """Execute a ``Push*``/``AskSnapshot``/``ReportState`` effect against the session."""
        if isinstance(effect, PushLoad):
            await self._session.send_queue_load_tracks(
                action_uuid=effect.action_uuid,
                track_id="",
                queue_version=effect.base_version,
                track_ids=list(effect.track_ids),
                queue_position=effect.current_index,
                context_uuid=effect.context_uuid,
            )
        elif isinstance(effect, PushAdd):
            await self._session.send_queue_add_tracks(
                action_uuid=effect.action_uuid,
                tracks=_refs(effect.track_ids),
                queue_version=effect.base_version,
            )
        elif isinstance(effect, PushInsert):
            await self._session.send_queue_insert_tracks(
                action_uuid=effect.action_uuid,
                tracks=_refs(effect.track_ids),
                insert_after=effect.insert_after,
                queue_version=effect.base_version,
            )
        elif isinstance(effect, PushRemove):
            await self._session.send_queue_remove_tracks(
                action_uuid=effect.action_uuid,
                queue_item_ids=list(effect.queue_item_ids),
                queue_version=effect.base_version,
            )
        elif isinstance(effect, PushReorder):
            await self._session.send_queue_reorder_tracks(
                action_uuid=effect.action_uuid,
                queue_item_ids=list(effect.queue_item_ids),
                insert_after=effect.insert_after,
                queue_version=effect.base_version,
            )
        elif isinstance(effect, PushClear):
            await self._session.send_clear_queue(queue_version=effect.base_version)
        elif isinstance(effect, PushSetActive):
            await self._run_push_set_active()
        elif isinstance(effect, PushPlayerState):
            await self._session.send_ctrl_player_state(
                playing_state=effect.playing,
                position_ms=effect.position_ms,
                queue_version=effect.queue_version,
                queue_item_id=effect.queue_item_id,
            )
        elif isinstance(effect, PushLoop):
            await self._session.send_set_loop_mode(effect.loop)
        elif isinstance(effect, PushVolume):
            await self._session.send_volume_changed(effect.volume)
        elif isinstance(effect, PushMute):
            await self._session.send_volume_muted(effect.muted)
        elif isinstance(effect, PushQuality):
            await self._session.send_max_quality_report(effect.quality)
        elif isinstance(effect, AskSnapshot):
            await self._session.send_ask_for_queue_state(
                queue_version=effect.version,
                queue_uuid=uuid.uuid4().bytes,
            )
        elif isinstance(effect, ReportState) and self._reporter is not None:
            await self._reporter.report_state()

    # ---- MA effect dispatch -------------------------------------------------

    async def _run_ma_effect(self, effect: Effect) -> None:
        """Execute a ``Ma*`` effect against the MA bridge."""
        if isinstance(effect, MaPlayTrack):
            await self._run_ma_play_track(effect)
        elif isinstance(effect, MaPause):
            if (pid := self._target_player_id("MaPause")) is not None:
                await self._bridge.pause(pid)
        elif isinstance(effect, MaResume):
            if (pid := self._target_player_id("MaResume")) is not None:
                await self._bridge.play(pid)
        elif isinstance(effect, MaSeek):
            if (pid := self._target_player_id("MaSeek")) is not None:
                await self._bridge.seek(pid, effect.position_ms // 1000)
        elif isinstance(effect, MaResyncQueue):
            await self._run_ma_resync_queue(effect)
        elif isinstance(effect, MaSetLoop):
            await self._run_ma_set_loop(effect)
        elif isinstance(effect, MaSetShuffleFlag):
            if (pid := self._target_player_id("MaSetShuffleFlag")) is not None:
                self._bridge.set_shuffle_flag(pid, effect.shuffle)
        elif isinstance(effect, MaSetVolume):
            if (pid := self._target_player_id("MaSetVolume")) is not None:
                await self._bridge.cmd_volume_set(pid, effect.volume)
        elif isinstance(effect, MaReleasePlayer):
            await self._run_ma_release_player()

    # ---- MA effect helpers ------------------------------------------------

    async def _run_push_set_active(self) -> None:
        """Activate this renderer on the cloud, if its own renderer id is already known."""
        rid = self._own_rid_getter()
        if rid is not None:
            await self._session.send_set_active_renderer(rid)

    async def _run_ma_play_track(self, effect: MaPlayTrack) -> None:
        """
        Start Qobuz track ``effect.track_id`` on MA — the only audio-restarting effect.

        Fast path: if the track is already sitting in MA's queue (preloaded by a prior
        ``MaResyncQueue``), jump to it with ``play_index`` instead of tearing the queue
        down. Otherwise resolve it via the metadata resolver and start it fresh with
        ``play_media(option=REPLACE)`` — filling in the rest of the queue is left to the
        ``MaResyncQueue`` effect the reducer emits alongside/after this one, so this
        method never needs "current + next" context the way the old command handler did.
        """
        pid = self._target_player_id("MaPlayTrack")
        if pid is None:
            return
        track_id_str = str(effect.track_id)
        existing_idx = self._find_ma_queue_index(pid, track_id_str)
        if existing_idx is not None:
            await self._bridge.play_index(
                pid, existing_idx, seek_position=effect.position_ms // 1000
            )
            return
        if self._metadata is None:
            LOGGER.debug(
                "MaPlayTrack: no metadata resolver wired up; cannot resolve track %s",
                track_id_str,
            )
            return
        track = await self._metadata.get_track_or_none(track_id_str)
        if track is None:
            LOGGER.warning("MaPlayTrack: ignoring unresolved Qobuz track %s", track_id_str)
            return
        await self._bridge.play_media(pid, media=track, option=QueueOption.REPLACE)
        if effect.position_ms > 0:
            await self._bridge.seek(pid, effect.position_ms // 1000)

    async def _run_ma_resync_queue(self, effect: MaResyncQueue) -> None:
        """
        Bring MA's queue items in line with the Qobuz track-id list without restarting audio.

        Reuses existing MA ``QueueItem`` instances for tracks already present (preserving
        their ``queue_item_id`` so the stream buffer stays stable) and resolves metadata
        only for tracks MA doesn't have yet, then commits the whole list via one
        ``update_items``. Non-Qobuz MA queue items (e.g. a local/Spotify track a user
        manually queued during an active Connect session) are preserved, appended after
        the reconciled Qobuz block. This is the safe subset of full reconciliation;
        deferred: chunked metadata resolution and duplicate-count bucketing for
        repeated tracks.
        """
        pid = self._target_player_id("MaResyncQueue")
        if pid is None or self._metadata is None:
            return
        ma_by_track_id: dict[str, list[Any]] = {}
        non_qobuz_items: list[Any] = []
        for item in self._bridge.queue_items(pid):
            track_id = self._bridge.qobuz_track_id_for(item)
            if track_id is not None:
                ma_by_track_id.setdefault(track_id, []).append(item)
            else:
                non_qobuz_items.append(item)

        # First pass reuses existing MA items in order; unknown tracks get a
        # placeholder slot and are resolved concurrently below.
        slots: list[Any | None] = []
        missing: list[tuple[int, str]] = []
        for qid in effect.track_ids:
            track_id_str = str(qid)
            pool = ma_by_track_id.get(track_id_str)
            item = pool.pop(0) if pool else None
            if item is None:
                missing.append((len(slots), track_id_str))
            slots.append(item)

        if missing:
            batch = await self._metadata.resolve_batch(
                tuple(track_id_str for _slot, track_id_str in missing),
                concurrency=RESYNC_RESOLVE_CONCURRENCY,
            )
            if batch.transient_failed or effect.generation != self._generation_getter():
                return
            for (slot, _track_id_str), track in zip(missing, batch.items, strict=True):
                slots[slot] = None if track is None else QueueItem.from_media_item(pid, track)

        if effect.generation != self._generation_getter():
            return

        final_items: list[Any] = []
        current_index: int | None = None
        for qid, item in zip(effect.track_ids, slots, strict=True):
            if item is None:
                continue
            if effect.current_track_id == qid and current_index is None:
                current_index = len(final_items)
            final_items.append(item)

        final_items.extend(non_qobuz_items)
        if current_index is not None:
            self._bridge.set_current_index(pid, current_index)
        self._bridge.update_items(pid, final_items)

    async def _run_ma_set_loop(self, effect: MaSetLoop) -> None:
        """Map Qobuz ``LoopMode`` to MA's ``RepeatMode`` string value and apply it."""
        pid = self._target_player_id("MaSetLoop")
        if pid is None:
            return
        ma_value = _LOOP_TO_MA_REPEAT.get(effect.loop)
        if ma_value is None:
            LOGGER.debug("MaSetLoop: no MA RepeatMode mapping for %r", effect.loop)
            return
        self._bridge.set_repeat(pid, ma_value)

    async def _run_ma_release_player(self) -> None:
        """Stop and clear the target player's queue — mirrors the old ``release_target_player``."""
        pid = self._bridge.target_player_id()
        if pid is None:
            return
        try:
            await self._bridge.stop_queue(pid)
        except Exception:
            LOGGER.debug("MaReleasePlayer: stop_queue failed", exc_info=True)
        try:
            self._bridge.clear_queue(pid, skip_stop=True)
        except Exception:
            LOGGER.debug("MaReleasePlayer: clear_queue failed", exc_info=True)

    def _find_ma_queue_index(self, player_id: str, track_id: str) -> int | None:
        """Return the MA queue index already holding Qobuz track ``track_id``, if any."""
        for idx, item in enumerate(self._bridge.queue_items(player_id)):
            if self._bridge.qobuz_track_id_for(item) == track_id:
                return idx
        return None

    def _target_player_id(self, effect_name: str) -> str | None:
        """Resolve the MA target player id, logging and returning ``None`` if unset."""
        pid = self._bridge.target_player_id()
        if pid is None:
            LOGGER.debug("%s: no target player configured; dropping effect", effect_name)
        return pid


def _refs(track_ids: tuple[int, ...]) -> list[QueueTrackRef]:
    """Wrap Qobuz track ids as ``QueueTrackRef``s for the cloud add/insert verbs."""
    return [QueueTrackRef(queue_item_id=0, track_id=str(qid)) for qid in track_ids]
