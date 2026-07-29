"""
Impure shell: serialize inbound events, run the pure reducer, execute effects.

``QobuzConnectCoordinator`` owns the ONLY mutable :class:`.sync_types.CanonicalState`.
Inbound cloud events are already ``sync_types`` events (the codec parses frames
straight into them); ``submit`` injects the little shell-only context they can't
carry (own-renderer verdict, the snapshot's remembered track pointer, an error's
version fallback) and MA subscription events are turned into ``Ma*Changed`` events
by the ``on_ma_*`` entry points. Reducer state transitions are serialized under
one ``asyncio.Lock`` and effect sequences under another. This lets newer canonical
queue events advance a generation while slow metadata resolution is in flight;
the stale result and any dependent effects are then discarded before MA is mutated.

Owns:
- ``self._state`` (via the ``state`` property) — the single source of truth.
- ``submit()`` — the single intake sink for codec-built cloud events (context
  injection, then reduce), plus ``build_session_callbacks()`` which hands it and
  the two provider hooks to the inbound dispatcher.
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
- the local ``try_parse_qobuz_id`` helper for translating MA input.
- ``asyncio``/``time``/``uuid`` — this module is the impure shell.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from .models import (
    LoopMode,
    PlayingState,
    QueueVersion,
)
from .reducer import reduce
from .session import SessionCallbacks
from .sync_types import (
    CanonicalState,
    CloudAddRenderer,
    CloudQuality,
    CloudQueueError,
    CloudSessionState,
    CloudSetActive,
    CloudSnapshot,
    Disconnected,
    Event,
    MaModesChanged,
    MaQueueChanged,
    MaReleasePlayer,
    MaResyncQueue,
    MaTransportChanged,
    MaVolumeChanged,
    ProposalTimeout,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from .effect_runner import EffectRunner
    from .flight_recorder import FlightRecorder
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
        now: Callable[[], int] = lambda: int(time.time() * 1000),
        recorder: FlightRecorder | None = None,
        unresolvable_getter: Callable[[], frozenset[str]] = frozenset,
    ) -> None:
        """
        Bind the coordinator to its collaborators.

        :param runner: Executes the effects ``reduce()`` returns, in order.
        :param bridge: MA-facing bridge the ``on_ma_*`` entry points read state from.
        :param device_uuid: This renderer's own Qobuz device uuid, used to resolve
            own-ness on ``SRVR_CTRL_ADD_RENDERER``.
        :param now: Wall-clock-ms provider for event timestamps; injectable for tests.
        :param recorder: Optional flight recorder fed one entry per reduced event.
        :param unresolvable_getter: Returns the track ids (as strings) the metadata
            resolver has marked unfetchable — everything else counts as resolvable
            when diffing MA's queue against canonical.
        """
        self._runner = runner
        self._bridge = bridge
        self._device_uuid = device_uuid
        self._now = now
        self._recorder = recorder
        self._unresolvable_getter = unresolvable_getter
        self._closed = False
        self._timeout_tasks: set[asyncio.Task[None]] = set()
        self._state = CanonicalState()
        self._lock = asyncio.Lock()
        self._effect_lock = asyncio.Lock()
        self._queue_generation = 0
        self._owned_target_player_id: str | None = None
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

    @property
    def queue_generation(self) -> int:
        """Return the newest canonical queue generation."""
        return self._queue_generation

    def close(self) -> None:
        """
        Stop all timer activity — call on provider unload.

        Cancels every live proposal timer and in-flight timeout submission and
        prevents new ones, so a zombie coordinator can never fire effects
        against MA or a dead session after the provider is gone.
        """
        self._closed = True
        for handle in self._timers.values():
            handle.cancel()
        self._timers.clear()
        for task in self._timeout_tasks:
            task.cancel()

    def build_session_callbacks(self) -> SessionCallbacks:
        """
        Build the ``SessionCallbacks`` bundle the inbound dispatcher fans out to.

        Every codec-built cloud event flows through ``submit``; ``on_set_active``
        / ``on_quality`` are the coordinator defaults the provider overrides to
        add the activation volume/quality broadcast and quality-config
        persistence.
        """
        return SessionCallbacks(
            submit=self.submit,
            on_set_active=self._on_set_active,
            on_quality=self._on_quality,
            on_disconnected=self._on_disconnected,
        )

    async def submit(self, event: Event) -> None:
        """
        Intake for codec-built cloud events: inject coordinator context, then reduce.

        A few events carry fields only the shell can fill: the last
        SESSION_STATE trackIndex (memoized here and stamped onto snapshots),
        this device's own-renderer verdict, and the version fallback for a
        wire error that carried none. Everything else passes straight through.
        """
        if isinstance(event, CloudSessionState):
            self._last_track_index = event.track_index
        elif isinstance(event, CloudSnapshot):
            # The snapshot carries no track pointer; use the last SESSION_STATE
            # trackIndex we saw.
            event = dataclasses.replace(event, track_index=self._last_track_index)
        elif isinstance(event, CloudAddRenderer):
            # Own-ness needs uuid-comparison context the pure reducer lacks.
            event = dataclasses.replace(event, is_own=event.device_uuid == self._device_uuid)
        elif isinstance(event, CloudQueueError) and event.version == QueueVersion():
            # A wire error with no queue_version parses to the (0,0) default;
            # fall back to our current cloud_version so a rejection still
            # rebases against the version we hold.
            event = dataclasses.replace(event, version=self._state.cloud_version)
        await self._submit(event)

    # ---- MA-side entry points ----------------------------------------------

    async def on_ma_queue_event(self, player_id: str) -> None:
        """Translate MA's current queue contents into a ``MaQueueChanged`` event."""
        track_ids: list[int] = []
        resolvable: set[int] = set()
        for item in self._bridge.queue_items(player_id):
            qid = try_parse_qobuz_id(self._bridge.qobuz_track_id_for(item))
            if qid is None:
                continue
            track_ids.append(qid)
            resolvable.add(qid)
        # Resolvability is a property of the TRACK (can MA materialize it?),
        # not of the current queue contents. Canonical tracks the metadata
        # resolver never failed on count as resolvable even when absent from
        # MA's queue — that absence is exactly what a user-driven removal
        # looks like, and deriving resolvable from the queue alone made
        # removals invisible to the differ (the cloud then re-added the
        # track on the next resync).
        unresolvable = {
            qid
            for raw in self._unresolvable_getter()
            if (qid := try_parse_qobuz_id(raw)) is not None
        }
        for ref in self._state.tracks:
            qid = try_parse_qobuz_id(ref.track_id)
            if qid is not None and qid not in unresolvable:
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
                context_uuid=uuid.uuid4().bytes,
            )
        )

    async def on_ma_transport_event(self, player_id: str) -> None:
        """Translate MA's current transport state into a ``MaTransportChanged`` event."""
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
                target_player_id=player_id,
            )
        )

    async def on_ma_modes_event(self, player_id: str) -> None:
        """Translate MA's current repeat mode into a ``MaModesChanged`` event."""
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
        player = self._bridge.get_player(player_id)
        if player is None:
            return
        # A player can transiently report volume_level=None (network players do
        # this while acking a volume change; BlackHole never does). The old
        # `volume_level or 0` reported that as 0, so the Qobuz app showed the
        # renderer as muted a short while after the user changed volume (live
        # 2026-07-11). Never report a spurious 0 — skip until a real level is
        # known.
        if player.volume_level is None:
            return
        volume = player.volume_level
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
        """Reduce one event, then execute its effects in serialized order."""
        async with self._lock:
            previous_tracks = self._state.tracks
            was_active = self._state.active
            if (
                isinstance(event, CloudSetActive)
                and event.active
                and self._owned_target_player_id is None
            ):
                target_getter = getattr(self._bridge, "target_player_id", None)
                if target_getter is not None:
                    self._owned_target_player_id = target_getter()
            elif (
                isinstance(event, MaTransportChanged)
                and event.playing is PlayingState.PLAYING
                and event.target_player_id is not None
                and self._owned_target_player_id is None
            ):
                self._owned_target_player_id = event.target_player_id
            try:
                result = reduce(self._state, event)
            except Exception as err:
                # The reducer is pure and should never raise; if it does, make
                # sure the flight recorder captures the trigger before the
                # exception propagates into the session's receive loop.
                if self._recorder is not None:
                    self._recorder.record_reduce_failure(event, err)
                raise
            self._state = result.state
            if result.state.tracks != previous_tracks:
                self._queue_generation += 1
            effects = tuple(
                dataclasses.replace(effect, generation=self._queue_generation)
                if isinstance(effect, MaResyncQueue)
                else dataclasses.replace(effect, player_id=self._owned_target_player_id)
                if isinstance(effect, MaReleasePlayer)
                else effect
                for effect in result.effects
            )
            releasing = any(isinstance(effect, MaReleasePlayer) for effect in effects)
            if releasing or (was_active and not result.state.active):
                self._owned_target_player_id = None
            if self._recorder is not None:
                self._recorder.record_reduce(event, result)
            if isinstance(event, Disconnected):
                # The cloud forgets our reported volume with the session;
                # drop the dedup so the level is re-sent after reconnect.
                self._last_volume_state = None
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
            self._sync_proposal_timers()

        # Metadata resolution may be slow. Keep reducer intake unlocked so a
        # newer queue event can advance the generation while this batch is in
        # flight; the runner discards the stale batch before committing it.
        async with self._effect_lock:
            resync_generation: int | None = None
            for effect in effects:
                if isinstance(effect, MaResyncQueue):
                    resync_generation = effect.generation
                try:
                    await self._runner.run(effect)
                except Exception:
                    LOGGER.exception("Qobuz Connect effect %s failed", type(effect).__name__)
                if resync_generation is not None and resync_generation != self._queue_generation:
                    break

    # ---- provider hooks (overridable) + lifecycle ---------------------------

    async def _on_set_active(self, active: bool) -> None:
        await self.submit(CloudSetActive(now_ms=self._now(), active=active))

    async def _on_quality(self, quality: int) -> None:
        await self.submit(CloudQuality(now_ms=self._now(), quality=quality))

    async def _on_disconnected(self) -> None:
        await self.submit(Disconnected(now_ms=self._now()))

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
        if self._closed:
            return
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
        if self._closed:
            return
        # Hold a strong reference: asyncio only weak-refs scheduled tasks, so
        # a bare create_task here could be garbage-collected mid-flight.
        task = asyncio.get_running_loop().create_task(
            self._submit(ProposalTimeout(now_ms=self._now(), action_uuid=action_uuid))
        )
        self._timeout_tasks.add(task)
        task.add_done_callback(self._timeout_tasks.discard)
