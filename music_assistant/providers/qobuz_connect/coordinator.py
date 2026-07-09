"""
Impure shell: serialize inbound events, run the pure reducer, execute effects.

``QobuzConnectCoordinator`` owns the ONLY mutable :class:`.sync_types.CanonicalState`.
It translates raw inbound — the old parsed cloud messages the session hands to its
callbacks, and MA subscription events the provider hands to its ``on_ma_*`` entry
points — into pure ``Event`` values, then serializes them through :func:`.reducer.reduce`
one at a time under an ``asyncio.Lock``. Each event is fully processed (state stored,
every resulting effect awaited on the ``EffectRunner``, in order) before the next
event starts; that linear ordering is what makes the reducer's version-gated
decisions correct — nothing ever observes a half-applied state.

Owns:
- ``self._state`` (via the ``state`` property) — the single source of truth.
- ``build_session_callbacks()`` — translates :mod:`.session`'s old parsed DTOs
  into :mod:`.sync_types` events.
- ``on_ma_queue_event`` / ``on_ma_transport_event`` / ``on_ma_modes_event`` /
  ``on_ma_volume_event`` — MA-side entry points the provider wires to MA's event
  bus; each reads current MA state via ``bridge`` and submits the matching
  ``Ma*Changed`` event.
- The proposal-timeout timer: schedules a ``ProposalTimeout`` for every pending
  proposal that doesn't already have a live timer, and cancels a proposal's timer
  once it leaves ``pending``.

Depends on:
- :func:`.reducer.reduce` (pure) and :mod:`.sync_types` (pure event/effect/state
  types) for all sync decisions.
- :class:`.session.SessionCallbacks` for the shape the transport expects.
- the local ``try_parse_qobuz_id`` helper and the old parsed DTOs in :mod:`.models`
  for translating cloud/MA input.
- ``asyncio``/``time``/``uuid`` — this module is the impure shell.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from .models import (
    LoopMode,
    PlayingState,
    QueueClearedEvent,
    QueueError,
    QueueLoadAck,
    QueueStateSnapshot,
    QueueTracksAddedEvent,
    QueueTracksInsertedEvent,
    QueueTracksRemovedEvent,
    QueueTracksReorderedEvent,
    QueueVersion,
    RendererRecord,
    RendererStateUpdate,
    SessionStateEvent,
    SetStateEvent,
)
from .reducer import reduce
from .session import SessionCallbacks
from .sync_types import (
    CanonicalState,
    CloudActiveRendererChanged,
    CloudAddRenderer,
    CloudAutoplaySet,
    CloudAutoplayTracksLoaded,
    CloudCleared,
    CloudLoadAck,
    CloudLoopSet,
    CloudQuality,
    CloudQueueError,
    CloudRemoveRenderer,
    CloudRendererStateUpdated,
    CloudSessionState,
    CloudSetActive,
    CloudSetState,
    CloudShuffleSet,
    CloudSnapshot,
    CloudStateRequest,
    CloudTracksAdded,
    CloudTracksInserted,
    CloudTracksRemoved,
    CloudTracksReordered,
    CloudVersionChanged,
    CloudVolume,
    CloudVolumeDelta,
    Disconnected,
    Event,
    MaModesChanged,
    MaQueueChanged,
    MaTransportChanged,
    MaVolumeChanged,
    ProposalTimeout,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from .effect_runner import EffectRunner
    from .ma_bridge import MABridge

LOGGER = logging.getLogger(__name__)

# How long to wait for a cloud echo of an MA-origin proposal before giving up
# and converging MA back to canonical truth. The shell half of the
# lost-echo safety net; the reducer half (dropping the proposal on
# ProposalTimeout) lives in .reducer.
PROPOSAL_TIMEOUT_S = 5.0


def try_parse_qobuz_id(value: Any) -> int | None:
    """Return an integer Qobuz id when the protocol can represent the value."""
    if value is None:
        return None
    try:
        return int(str(value))
    except TypeError, ValueError:
        return None


# MA's str-enum RepeatMode (off/one/all) to Qobuz's int-enum LoopMode. Kept as a
# local copy so this shell module carries its own MA↔Qobuz enum translation.
_MA_REPEAT_TO_LOOP: dict[str, LoopMode] = {
    "off": LoopMode.OFF,
    "one": LoopMode.REPEAT_ONE,
    "all": LoopMode.REPEAT_ALL,
}

# MA's PlaybackState to Qobuz's PlayingState. IDLE/UNKNOWN both fold to
# STOPPED — Qobuz has no third "not yet started" state.
_MA_PLAYBACK_TO_QOBUZ: dict[str, PlayingState] = {
    "playing": PlayingState.PLAYING,
    "paused": PlayingState.PAUSED,
    "idle": PlayingState.STOPPED,
    "unknown": PlayingState.STOPPED,
}


class QobuzConnectCoordinator:
    """Serialize reducer intake and route its effects to the cloud session + MA."""

    def __init__(
        self,
        *,
        runner: EffectRunner,
        bridge: MABridge,
        device_uuid: bytes,
        controller_enabled: bool = True,
        now: Callable[[], int] = lambda: int(time.time() * 1000),
    ) -> None:
        """
        Bind the coordinator to its collaborators.

        :param runner: Executes the effects ``reduce()`` returns, in order.
        :param bridge: MA-facing bridge the ``on_ma_*`` entry points read state from.
        :param device_uuid: This renderer's own Qobuz device uuid, used to resolve
            own-ness on ``SRVR_CTRL_ADD_RENDERER``.
        :param controller_enabled: When ``False``, MA-side entry points no-op instead
            of emitting cloud pushes — inbound cloud events still process normally.
        :param now: Wall-clock-ms provider for event timestamps; injectable for tests.
        """
        self._runner = runner
        self._bridge = bridge
        self._device_uuid = device_uuid
        self._controller_enabled = controller_enabled
        self._now = now
        self._state = CanonicalState()
        self._lock = asyncio.Lock()
        self._last_track_index = 0
        self._timers: dict[bytes, asyncio.TimerHandle] = {}
        # Last (volume, muted) pair pushed to the cloud, so repeated
        # PLAYER_UPDATED events for an unchanged level don't spam
        # PushVolume/PushMute. ``None`` sentinel forces the first real value
        # through.
        self._last_volume_state: tuple[int, bool] | None = None

    @property
    def state(self) -> CanonicalState:
        """Current canonical state — read-only snapshot for callers."""
        return self._state

    def own_rid_getter(self) -> int | None:
        """Return this renderer's own Qobuz renderer id, for ``EffectRunner``'s ``own_rid_getter``."""
        return self._state.own_rid

    def build_session_callbacks(self) -> SessionCallbacks:
        """Build the ``SessionCallbacks`` bundle that translates cloud messages into events."""
        return SessionCallbacks(
            on_set_state=self._on_set_state,
            on_queue_load_ack=self._on_queue_load_ack,
            on_autoplay_tracks_loaded=self._on_autoplay_tracks_loaded,
            on_queue_error=self._on_queue_error,
            on_queue_version=self._on_queue_version,
            on_queue_state=self._on_queue_state,
            on_queue_tracks_added=self._on_queue_tracks_added,
            on_queue_tracks_inserted=self._on_queue_tracks_inserted,
            on_queue_tracks_removed=self._on_queue_tracks_removed,
            on_queue_tracks_reordered=self._on_queue_tracks_reordered,
            on_queue_cleared=self._on_queue_cleared,
            on_volume=self._on_volume,
            on_volume_delta=self._on_volume_delta,
            on_quality=self._on_quality,
            on_loop_mode=self._on_loop_mode,
            on_shuffle_mode=self._on_shuffle_mode,
            on_autoplay_mode=self._on_autoplay_mode,
            on_state_request=self._on_state_request,
            on_set_active=self._on_set_active,
            on_session_state=self._on_session_state,
            on_add_renderer=self._on_add_renderer,
            on_remove_renderer=self._on_remove_renderer,
            on_active_renderer_changed=self._on_active_renderer_changed,
            on_renderer_state_updated=self._on_renderer_state_updated,
            on_disconnected=self._on_disconnected,
        )

    # ---- MA-side entry points ----------------------------------------------

    async def on_ma_queue_event(self, player_id: str) -> None:
        """Translate MA's current queue contents into a ``MaQueueChanged`` event."""
        if not self._controller_enabled:
            return
        track_ids: list[int] = []
        resolvable: set[int] = set()
        for item in self._bridge.queue_items(player_id):
            qid = try_parse_qobuz_id(self._bridge.qobuz_track_id_for(item))
            if qid is None:
                continue
            track_ids.append(qid)
            resolvable.add(qid)
        current_track_id = None
        queue = self._bridge.get_queue(player_id)
        if queue is not None and queue.current_item is not None:
            current_track_id = try_parse_qobuz_id(
                self._bridge.qobuz_track_id_for(queue.current_item)
            )
        await self._submit(
            MaQueueChanged(
                now_ms=self._now(),
                action_uuid=uuid.uuid4().bytes,
                track_ids=tuple(track_ids),
                current_track_id=current_track_id,
                resolvable=frozenset(resolvable),
            )
        )

    async def on_ma_transport_event(self, player_id: str) -> None:
        """Translate MA's current transport state into a ``MaTransportChanged`` event."""
        if not self._controller_enabled:
            return
        queue = self._bridge.get_queue(player_id)
        if queue is None:
            return
        playing = _MA_PLAYBACK_TO_QOBUZ.get(
            getattr(queue.state, "value", queue.state), PlayingState.STOPPED
        )
        current_track_id = None
        if queue.current_item is not None:
            current_track_id = try_parse_qobuz_id(
                self._bridge.qobuz_track_id_for(queue.current_item)
            )
        await self._submit(
            MaTransportChanged(
                now_ms=self._now(),
                playing=playing,
                current_track_id=current_track_id,
                position_ms=int(queue.corrected_elapsed_time * 1000),
            )
        )

    async def on_ma_modes_event(self, player_id: str) -> None:
        """Translate MA's current repeat mode into a ``MaModesChanged`` event."""
        if not self._controller_enabled:
            return
        queue = self._bridge.get_queue(player_id)
        if queue is None:
            return
        repeat_value = getattr(queue.repeat_mode, "value", queue.repeat_mode)
        loop = _MA_REPEAT_TO_LOOP.get(repeat_value, self._state.loop)
        await self._submit(
            MaModesChanged(
                now_ms=self._now(),
                action_uuid=uuid.uuid4().bytes,
                loop=loop,
                # No MA-side autoplay concept exists to read here; carry the
                # current canonical value forward so this never spuriously
                # flips autoplay off.
                autoplay=self._state.autoplay,
            )
        )

    async def on_ma_volume_event(self, player_id: str) -> None:
        """Translate MA's current player volume into a ``MaVolumeChanged`` event."""
        if not self._controller_enabled:
            return
        player = self._bridge.get_player(player_id)
        if player is None:
            return
        volume = player.volume_level or 0
        muted = bool(player.volume_muted)
        if self._last_volume_state == (volume, muted):
            return
        self._last_volume_state = (volume, muted)
        await self._submit(
            MaVolumeChanged(
                now_ms=self._now(),
                volume=volume,
                muted=muted,
            )
        )

    # ---- core intake --------------------------------------------------------

    async def _submit(self, event: Event) -> None:
        """Serialize one event through ``reduce()``, store the result, run its effects in order."""
        async with self._lock:
            result = reduce(self._state, event)
            self._state = result.state
            if LOGGER.isEnabledFor(logging.DEBUG):
                state = result.state
                LOGGER.debug(
                    "reduce %s(v=%s) -> [%s] | active=%s playing=%s current_id=%s "
                    "cloud_v=%s.%s tracks=%d pending=%d",
                    type(event).__name__,
                    getattr(event, "version", None),
                    ",".join(type(e).__name__ for e in result.effects) or "-",
                    state.active,
                    getattr(state.playing, "name", state.playing),
                    state.current_id,
                    state.cloud_version.major,
                    state.cloud_version.minor,
                    len(state.tracks),
                    len(state.pending),
                )
            for effect in result.effects:
                try:
                    await self._runner.run(effect)
                except Exception:
                    LOGGER.exception("Qobuz Connect effect %s failed", type(effect).__name__)
            self._sync_proposal_timers()

    # ---- session callback translators ---------------------------------------

    async def _on_set_state(self, event: SetStateEvent) -> None:
        await self._submit(
            CloudSetState(
                now_ms=self._now(),
                version=event.queue_version,
                playing=event.playing_state,
                position_ms=event.position_ms,
                current_ref=event.current_item,
                next_ref=event.next_item,
            )
        )

    async def _on_queue_load_ack(self, ack: QueueLoadAck) -> None:
        await self._submit(
            CloudLoadAck(
                now_ms=self._now(),
                version=ack.queue_version,
                action_uuid=ack.action_uuid,
                tracks=tuple(ack.tracks),
                queue_position=ack.queue_position,
            )
        )

    async def _on_autoplay_tracks_loaded(self, ack: QueueLoadAck) -> None:
        await self._submit(
            CloudAutoplayTracksLoaded(
                now_ms=self._now(),
                version=ack.queue_version,
                action_uuid=ack.action_uuid,
                tracks=tuple(ack.tracks),
            )
        )

    async def _on_queue_error(self, error: QueueError) -> None:
        await self._submit(
            CloudQueueError(
                now_ms=self._now(),
                version=error.queue_version or self._state.cloud_version,
                action_uuid=error.action_uuid,
                code=str(error.code),
                message=error.message,
            )
        )

    async def _on_queue_version(self, version: QueueVersion) -> None:
        await self._submit(CloudVersionChanged(now_ms=self._now(), version=version))

    async def _on_queue_state(self, snapshot: QueueStateSnapshot) -> None:
        await self._submit(
            CloudSnapshot(
                now_ms=self._now(),
                version=snapshot.queue_version,
                tracks=tuple(snapshot.tracks),
                autoplay_tracks=tuple(snapshot.autoplay_tracks),
                shuffle=snapshot.shuffle_mode,
                autoplay=snapshot.autoplay_mode,
                # QueueStateSnapshot carries no track pointer of its own; fall
                # back to the last SESSION_STATE trackIndex the coordinator
                # has seen.
                track_index=self._last_track_index,
            )
        )

    async def _on_queue_tracks_added(self, event: QueueTracksAddedEvent) -> None:
        await self._submit(
            CloudTracksAdded(
                now_ms=self._now(),
                version=event.queue_version,
                action_uuid=event.action_uuid,
                tracks=tuple(event.tracks),
                # QueueTracksAddedEvent carries no explicit position — an ADD
                # always appends, so the pre-event track count is the anchor.
                after_index=len(self._state.tracks),
            )
        )

    async def _on_queue_tracks_inserted(self, event: QueueTracksInsertedEvent) -> None:
        await self._submit(
            CloudTracksInserted(
                now_ms=self._now(),
                version=event.queue_version,
                action_uuid=event.action_uuid,
                tracks=tuple(event.tracks),
                insert_after=event.insert_after,
            )
        )

    async def _on_queue_tracks_removed(self, event: QueueTracksRemovedEvent) -> None:
        await self._submit(
            CloudTracksRemoved(
                now_ms=self._now(),
                version=event.queue_version,
                action_uuid=event.action_uuid,
                queue_item_ids=tuple(event.queue_item_ids),
            )
        )

    async def _on_queue_tracks_reordered(self, event: QueueTracksReorderedEvent) -> None:
        await self._submit(
            CloudTracksReordered(
                now_ms=self._now(),
                version=event.queue_version,
                action_uuid=event.action_uuid,
                queue_item_ids=tuple(event.queue_item_ids),
                insert_after=event.insert_after,
            )
        )

    async def _on_queue_cleared(self, event: QueueClearedEvent) -> None:
        await self._submit(
            CloudCleared(
                now_ms=self._now(), version=event.queue_version, action_uuid=event.action_uuid
            )
        )

    async def _on_volume(self, volume: int) -> None:
        await self._submit(CloudVolume(now_ms=self._now(), volume=volume))

    async def _on_volume_delta(self, delta: int) -> None:
        await self._submit(CloudVolumeDelta(now_ms=self._now(), delta=delta))

    async def _on_quality(self, quality: int) -> None:
        await self._submit(CloudQuality(now_ms=self._now(), quality=quality))

    async def _on_loop_mode(self, mode: LoopMode) -> None:
        await self._submit(CloudLoopSet(now_ms=self._now(), action_uuid=None, loop=mode))

    async def _on_shuffle_mode(self, shuffle: bool) -> None:
        await self._submit(CloudShuffleSet(now_ms=self._now(), action_uuid=None, shuffle=shuffle))

    async def _on_autoplay_mode(self, autoplay: bool) -> None:
        await self._submit(
            CloudAutoplaySet(now_ms=self._now(), action_uuid=None, autoplay=autoplay)
        )

    async def _on_state_request(self) -> None:
        await self._submit(CloudStateRequest(now_ms=self._now()))

    async def _on_set_active(self, active: bool) -> None:
        await self._submit(CloudSetActive(now_ms=self._now(), active=active))

    async def _on_session_state(self, event: SessionStateEvent) -> None:
        self._last_track_index = event.track_index
        await self._submit(
            CloudSessionState(
                now_ms=self._now(), version=event.queue_version, track_index=event.track_index
            )
        )

    async def _on_add_renderer(self, record: RendererRecord) -> None:
        await self._submit(
            CloudAddRenderer(
                now_ms=self._now(),
                renderer_id=record.renderer_id,
                device_uuid=record.device_uuid,
                is_own=record.device_uuid == self._device_uuid,
            )
        )

    async def _on_remove_renderer(self, renderer_id: int) -> None:
        await self._submit(CloudRemoveRenderer(now_ms=self._now(), renderer_id=renderer_id))

    async def _on_active_renderer_changed(self, renderer_id: int) -> None:
        await self._submit(CloudActiveRendererChanged(now_ms=self._now(), renderer_id=renderer_id))

    async def _on_renderer_state_updated(self, update: RendererStateUpdate) -> None:
        await self._submit(
            CloudRendererStateUpdated(
                now_ms=self._now(),
                renderer_id=update.renderer_id,
                playing=update.playing_state,
                position_ms=update.position_ms,
                current_index=update.current_queue_index,
            )
        )

    async def _on_disconnected(self) -> None:
        await self._submit(Disconnected(now_ms=self._now()))

    # ---- proposal-timeout timer ----------------------------------------------

    def _sync_proposal_timers(self) -> None:
        """
        Schedule a ``ProposalTimeout`` for every newly-pending proposal.

        Cancels timers for proposals that left ``pending`` (confirmed, rejected past
        retries, or timed out already) and leaves existing live timers untouched, so a
        proposal only ever gets one timeout scheduled for its whole lifetime.
        """
        pending_uuids = {p.action_uuid for p in self._state.pending}
        for action_uuid in [uid for uid in self._timers if uid not in pending_uuids]:
            self._timers.pop(action_uuid).cancel()
        loop = asyncio.get_running_loop()
        for proposal in self._state.pending:
            if proposal.action_uuid in self._timers:
                continue
            self._timers[proposal.action_uuid] = loop.call_later(
                PROPOSAL_TIMEOUT_S, self._fire_proposal_timeout, proposal.action_uuid
            )

    def _fire_proposal_timeout(self, action_uuid: bytes) -> None:
        """``call_later`` callback: drop the timer and submit ``ProposalTimeout``."""
        self._timers.pop(action_uuid, None)
        asyncio.get_running_loop().create_task(
            self._submit(ProposalTimeout(now_ms=self._now(), action_uuid=action_uuid))
        )
