"""Golden tests for the modes lane and side channels."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import LoopMode, QueueTrackRef, QueueVersion
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudAutoplaySet,
    CloudLoopSet,
    CloudShuffleSet,
    CloudVolume,
    MaModesChanged,
    MaSetLoop,
    MaSetShuffleFlag,
    MaSetVolume,
    MaVolumeChanged,
    PushLoop,
    PushVolume,
)


def test_cloud_loop_updates_canonical_and_ma() -> None:
    """An inbound loop change updates canonical and drives MA."""
    result = reduce(
        CanonicalState(active=True),
        CloudLoopSet(now_ms=1, action_uuid=None, loop=LoopMode.REPEAT_ALL),
    )
    assert result.state.loop is LoopMode.REPEAT_ALL
    assert any(isinstance(e, MaSetLoop) for e in result.effects)


def test_ma_modes_change_pushes_loop_and_tracks_autoplay() -> None:
    """An MA loop change pushes PushLoop; autoplay only updates canonical (no cloud verb)."""
    state = CanonicalState(loop=LoopMode.OFF, autoplay=False, active=True)
    result = reduce(
        state,
        MaModesChanged(now_ms=1, action_uuid=b"\xbb" * 16, loop=LoopMode.REPEAT_ONE, autoplay=True),
    )
    assert any(isinstance(e, PushLoop) for e in result.effects)
    assert result.state.autoplay is True
    assert all(isinstance(e, PushLoop) for e in result.effects)


def test_cloud_volume_drives_ma() -> None:
    """Inbound volume is applied to MA (side channel, ungated)."""
    result = reduce(CanonicalState(active=True), CloudVolume(now_ms=1, volume=40))
    assert any(isinstance(e, MaSetVolume) and e.volume == 40 for e in result.effects)


def test_ma_volume_pushes_to_cloud() -> None:
    """MA volume change pushes to the cloud."""
    result = reduce(CanonicalState(active=True), MaVolumeChanged(now_ms=1, volume=55, muted=False))
    assert any(isinstance(e, PushVolume) and e.volume == 55 for e in result.effects)


def test_ma_modes_change_pushes_only_the_changed_flag() -> None:
    """Only loop changed -> PushLoop present, autoplay unchanged."""
    state = CanonicalState(loop=LoopMode.OFF, autoplay=True, active=True)
    result = reduce(
        state,
        MaModesChanged(now_ms=1, action_uuid=b"\xbb" * 16, loop=LoopMode.REPEAT_ONE, autoplay=True),
    )
    assert any(isinstance(e, PushLoop) for e in result.effects)
    assert result.state.loop is LoopMode.REPEAT_ONE


def test_ma_modes_no_change_emits_nothing() -> None:
    """No flag changed -> no effects at all."""
    state = CanonicalState(loop=LoopMode.REPEAT_ALL, autoplay=True, active=True)
    result = reduce(
        state,
        MaModesChanged(now_ms=1, action_uuid=b"\xbb" * 16, loop=LoopMode.REPEAT_ALL, autoplay=True),
    )
    assert result.effects == ()


def test_cloud_shuffle_set_flags_without_reorder() -> None:
    """CloudShuffleSet emits MaSetShuffleFlag and does NOT reorder tracks."""
    tracks = tuple(QueueTrackRef(queue_item_id=i, track_id=str(100 + i)) for i in range(3))
    state = CanonicalState(cloud_version=QueueVersion(5, 1), tracks=tracks, active=True)
    result = reduce(state, CloudShuffleSet(now_ms=1, action_uuid=None, shuffle=True))
    assert any(isinstance(e, MaSetShuffleFlag) for e in result.effects)
    assert result.state.tracks == tracks  # order untouched


def test_cloud_autoplay_set_updates_canonical_only() -> None:
    """CloudAutoplaySet updates canonical autoplay with no MA effect."""
    result = reduce(
        CanonicalState(autoplay=False, active=True),
        CloudAutoplaySet(now_ms=1, action_uuid=None, autoplay=True),
    )
    assert result.state.autoplay is True
    assert result.effects == ()
