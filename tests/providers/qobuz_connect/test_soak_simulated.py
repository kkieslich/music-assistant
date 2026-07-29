"""
Simulated long-run soak: seeded random event storms through the sync core.

Compresses "days of homelab traffic" — reconnect churn, stale frames, rapid
skips, MA-origin edits, proposal timeouts — into seconds, asserting the
invariants that keep a long-lived provider healthy: the reducer never raises,
is deterministic, canonical state stays structurally sane, pending proposals
and timers never accumulate, and event intake never wedges.

Failures print the seed + step so a run can be replayed exactly.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
from typing import Any

import pytest

import music_assistant.providers.qobuz_connect.coordinator as coordinator_module
from music_assistant.providers.qobuz_connect.coordinator import QobuzConnectCoordinator
from music_assistant.providers.qobuz_connect.models import (
    LoopMode,
    PlayingState,
    QueueTrackRef,
    QueueVersion,
)
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudActiveRendererChanged,
    CloudAddRenderer,
    CloudAutoplaySet,
    CloudCleared,
    CloudLoadAck,
    CloudLoopSet,
    CloudMute,
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
    Disconnected,
    Event,
    MaModesChanged,
    MaQueueChanged,
    MaTransportChanged,
    MaVolumeChanged,
    ProposalTimeout,
)

MAX_PENDING = 32
QID_POOL = tuple(range(1000, 1030))


class _EventStorm:
    """Seeded generator of a plausible interleaving of cloud + MA events."""

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.now_ms = 1_000_000
        self.version = QueueVersion(self.rng.randint(1, 40), 1)
        self.next_item_id = 1

    def _bump_version(self) -> QueueVersion:
        self.version = QueueVersion(self.version.major + self.rng.randint(0, 2), 1)
        return self.version

    def _some_version(self) -> QueueVersion:
        # Mostly current/newer, sometimes strictly stale, rarely a regression
        # to a tiny value (cloud session reset).
        roll = self.rng.random()
        if roll < 0.65:
            return self._bump_version()
        if roll < 0.95:
            return QueueVersion(max(1, self.version.major - self.rng.randint(1, 3)), 1)
        return QueueVersion(self.rng.randint(1, 3), 1)

    def _refs(self, max_len: int = 25) -> tuple[QueueTrackRef, ...]:
        n = self.rng.randint(0, max_len)
        refs = []
        for _ in range(n):
            refs.append(
                QueueTrackRef(
                    queue_item_id=self.next_item_id,
                    track_id=str(self.rng.choice(QID_POOL)),
                )
            )
            self.next_item_id += 1
        return tuple(refs)

    def _uuid(self) -> bytes:
        return self.rng.randbytes(16)

    def next_event(self, state: CanonicalState) -> Event:
        """Produce the next random event, biased toward transport traffic."""
        self.now_ms += self.rng.randint(0, 3000)
        now = self.now_ms
        choice = self.rng.choice(
            # Weighted: transport + renderer updates dominate real traffic.
            ["set_state"] * 6
            + ["ma_transport"] * 6
            + ["renderer_update"] * 3
            + ["snapshot", "version", "added", "inserted", "removed", "reordered"]
            + ["cleared", "load_ack", "queue_error", "session_state"]
            + ["set_active", "add_renderer", "remove_renderer", "active_changed"]
            + ["loop", "shuffle", "autoplay", "volume", "mute", "quality"]
            + ["ma_queue", "ma_modes", "ma_volume"]
            + ["disconnected", "proposal_timeout", "state_request"]
        )
        event = (
            self._transport_session_event(choice, state, now)
            or self._list_event(choice, state, now)
            or self._modes_side_event(choice, now)
        )
        return event if event is not None else CloudStateRequest(now_ms=now)

    def _transport_session_event(
        self, choice: str, state: CanonicalState, now: int
    ) -> Event | None:
        if choice == "set_state":
            ref = None
            if state.tracks and self.rng.random() < 0.7:
                ref = self.rng.choice(state.tracks)
            return CloudSetState(
                now_ms=now,
                version=self._some_version() if self.rng.random() < 0.4 else None,
                playing=self.rng.choice([None, *PlayingState]),
                position_ms=self.rng.choice([None, self.rng.randint(0, 400_000)]),
                current_ref=ref,
            )
        if choice == "ma_transport":
            return MaTransportChanged(
                now_ms=now,
                playing=self.rng.choice(list(PlayingState)),
                current_track_id=self.rng.choice([None, state.current_id, *QID_POOL[:5]]),
                position_ms=self.rng.randint(0, 400_000),
            )
        if choice == "renderer_update":
            return CloudRendererStateUpdated(
                now_ms=now,
                renderer_id=self.rng.randint(1, 5),
                playing=self.rng.choice([None, *PlayingState]),
                position_ms=self.rng.choice([None, self.rng.randint(0, 400_000)]),
                current_index=self.rng.choice([None, self.rng.randint(0, 30)]),
            )
        if choice == "session_state":
            return CloudSessionState(
                now_ms=now, version=self._some_version(), track_index=self.rng.randint(0, 30)
            )
        if choice == "set_active":
            return CloudSetActive(now_ms=now, active=self.rng.random() < 0.6)
        if choice == "add_renderer":
            return CloudAddRenderer(
                now_ms=now,
                renderer_id=self.rng.randint(1, 5),
                device_uuid=self._uuid(),
                is_own=self.rng.random() < 0.3,
            )
        if choice == "remove_renderer":
            return CloudRemoveRenderer(now_ms=now, renderer_id=self.rng.randint(1, 5))
        if choice == "active_changed":
            return CloudActiveRendererChanged(now_ms=now, renderer_id=self.rng.randint(1, 5))
        if choice == "disconnected":
            return Disconnected(now_ms=now)
        if choice == "proposal_timeout":
            uuid_bytes = (
                state.pending[0].action_uuid
                if state.pending and self.rng.random() < 0.8
                else self._uuid()
            )
            return ProposalTimeout(now_ms=now, action_uuid=uuid_bytes)
        return None

    def _list_event(self, choice: str, state: CanonicalState, now: int) -> Event | None:
        known_item_ids = [t.queue_item_id for t in state.tracks]
        if choice == "snapshot":
            tracks = self._refs()
            return CloudSnapshot(
                now_ms=now,
                version=self._some_version(),
                tracks=tracks,
                autoplay_tracks=self._refs(5),
                shuffle=self.rng.random() < 0.2,
                autoplay=self.rng.random() < 0.3,
                track_index=self.rng.randint(0, max(1, len(tracks))),
            )
        if choice == "version":
            return CloudVersionChanged(now_ms=now, version=self._some_version())
        if choice == "added":
            return CloudTracksAdded(
                now_ms=now,
                version=self._some_version(),
                action_uuid=self._uuid(),
                tracks=self._refs(5),
            )
        if choice == "inserted":
            return CloudTracksInserted(
                now_ms=now,
                version=self._some_version(),
                action_uuid=self._uuid(),
                tracks=self._refs(5),
                insert_after=self.rng.randint(0, len(state.tracks) + 2),
            )
        if choice == "removed":
            ids = (
                tuple(
                    self.rng.sample(
                        known_item_ids, k=min(len(known_item_ids), self.rng.randint(1, 4))
                    )
                )
                if known_item_ids
                else (self.rng.randint(1, 100),)
            )
            return CloudTracksRemoved(
                now_ms=now,
                version=self._some_version(),
                action_uuid=self._uuid(),
                queue_item_ids=ids,
            )
        if choice == "reordered":
            ids = tuple(self.rng.sample(known_item_ids, k=len(known_item_ids)))
            return CloudTracksReordered(
                now_ms=now,
                version=self._some_version(),
                action_uuid=self._uuid(),
                queue_item_ids=ids,
                insert_after=self.rng.randint(0, len(known_item_ids)),
            )
        if choice == "cleared":
            return CloudCleared(now_ms=now, version=self._some_version(), action_uuid=self._uuid())
        if choice == "load_ack":
            tracks = self._refs()
            uuid_bytes = (
                state.pending[0].action_uuid
                if state.pending and self.rng.random() < 0.5
                else self._uuid()
            )
            return CloudLoadAck(
                now_ms=now,
                version=self._some_version(),
                action_uuid=uuid_bytes,
                tracks=tracks,
                queue_position=self.rng.randint(0, max(1, len(tracks))),
            )
        if choice == "queue_error":
            uuid_bytes = (
                state.pending[0].action_uuid
                if state.pending and self.rng.random() < 0.7
                else self._uuid()
            )
            return CloudQueueError(
                now_ms=now,
                version=self._some_version(),
                action_uuid=uuid_bytes,
                code="409",
                message="stale version",
            )
        if choice == "ma_queue":
            ids = tuple(self.rng.choices(QID_POOL, k=self.rng.randint(0, 20)))
            return MaQueueChanged(
                now_ms=now,
                action_uuid=self._uuid(),
                track_ids=ids,
                current_track_id=self.rng.choice([None, *ids]) if ids else None,
                resolvable=frozenset(ids),
            )
        return None

    def _modes_side_event(self, choice: str, now: int) -> Event | None:
        if choice == "loop":
            return CloudLoopSet(now_ms=now, action_uuid=None, loop=self.rng.choice(list(LoopMode)))
        if choice == "shuffle":
            return CloudShuffleSet(now_ms=now, action_uuid=None, shuffle=self.rng.random() < 0.5)
        if choice == "autoplay":
            return CloudAutoplaySet(now_ms=now, action_uuid=None, autoplay=self.rng.random() < 0.5)
        if choice == "volume":
            return CloudVolume(now_ms=now, volume=self.rng.randint(0, 100))
        if choice == "mute":
            return CloudMute(now_ms=now, muted=self.rng.random() < 0.5)
        if choice == "quality":
            return CloudQuality(now_ms=now, quality=self.rng.choice([5, 6, 7, 27]))
        if choice == "ma_modes":
            return MaModesChanged(
                now_ms=now,
                action_uuid=self._uuid(),
                loop=self.rng.choice(list(LoopMode)),
                autoplay=self.rng.random() < 0.5,
            )
        if choice == "ma_volume":
            return MaVolumeChanged(
                now_ms=now, volume=self.rng.randint(0, 100), muted=self.rng.random() < 0.5
            )
        return None


def _check_invariants(state: CanonicalState, seed: int, step: int, event: Event) -> None:
    context = f"seed={seed} step={step} event={event!r}"
    assert len(state.pending) <= MAX_PENDING, f"pending proposals unbounded: {context}"
    assert state.position_ms >= 0, f"negative position: {context}"
    assert state.position_anchor_ms >= 0, f"negative anchor: {context}"
    item_ids = [t.queue_item_id for t in state.tracks]
    # Cloud-assigned slot ids must stay unique (placeholder id 0 may repeat).
    real_ids = [i for i in item_ids if i != 0]
    assert len(real_ids) == len(set(real_ids)), f"duplicate queue_item_ids: {context}"


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_pure_reducer_storm_is_total_and_deterministic(seed: int) -> None:
    """Thousands of random events: reduce never raises and is deterministic."""
    storm = _EventStorm(seed)
    state = CanonicalState()
    for step in range(4000):
        event = storm.next_event(state)
        try:
            result = reduce(state, event)
        except Exception as err:
            pytest.fail(f"reduce raised {err!r} at seed={seed} step={step} event={event!r}")
        if step % 97 == 0:
            replay = reduce(state, event)
            assert replay == result, f"non-deterministic reduce: seed={seed} step={step}"
        state = result.state
        _check_invariants(state, seed, step, event)


class _CountingRunner:
    """Effect sink that only counts, so a long storm can't grow memory."""

    def __init__(self) -> None:
        self.count = 0

    async def run(self, effect: Any) -> None:
        await asyncio.sleep(0)
        self.count += 1


class _StormBridge:
    """Bridge stub whose queue snapshot the storm mutates as it goes."""

    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []
        self.queue: Any = None
        self.player: Any = None

    def queue_items(self, player_id: str) -> list[dict[str, str]]:
        return self.items

    def qobuz_track_id_for(self, item: Any) -> str | None:
        return item.get("track_id") if isinstance(item, dict) else None

    def get_queue(self, player_id: str) -> Any:
        return self.queue

    def get_player(self, player_id: str) -> Any:
        return self.player


@pytest.mark.parametrize("seed", [3, 11])
async def test_coordinator_storm_never_wedges_or_leaks_timers(
    seed: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Randomized intake through the real coordinator: no deadlock, timers drain."""
    monkeypatch.setattr(coordinator_module, "PROPOSAL_TIMEOUT_S", 0.005)
    runner = _CountingRunner()
    coordinator = QobuzConnectCoordinator(
        runner=runner,  # type: ignore[arg-type]
        bridge=_StormBridge(),  # type: ignore[arg-type]
        device_uuid=b"\x01" * 16,
    )
    storm = _EventStorm(seed)
    for step in range(1500):
        event = storm.next_event(coordinator.state)
        await asyncio.wait_for(coordinator._submit(event), timeout=2.0)
        _check_invariants(coordinator.state, seed, step, event)
        if step % 50 == 0:
            # Let proposal timers fire so timeouts interleave with traffic.
            await asyncio.sleep(0.01)
    # After the storm settles every pending proposal must time out and every
    # timer must be reaped — leftovers here are exactly the slow leak a
    # multi-day run would accumulate.
    async with asyncio.timeout(5.0):
        while coordinator.state.pending or coordinator._timers:
            await asyncio.sleep(0.01)
    assert runner.count > 0


def test_reducer_state_is_still_a_pure_value() -> None:
    """Guard the copy-on-write contract: reduce must never mutate its input."""
    storm = _EventStorm(99)
    state = CanonicalState()
    for _ in range(500):
        event = storm.next_event(state)
        before = dataclasses.replace(state)
        result = reduce(state, event)
        assert state == before, f"reduce mutated its input state on {event!r}"
        state = result.state
