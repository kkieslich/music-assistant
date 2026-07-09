"""Golden tests for the modes lane and side channels."""

from __future__ import annotations

from music_assistant.providers.qobuz_connect.models import LoopMode
from music_assistant.providers.qobuz_connect.reducer import reduce
from music_assistant.providers.qobuz_connect.sync_types import (
    CanonicalState,
    CloudLoopSet,
    CloudVolume,
    MaModesChanged,
    MaSetLoop,
    MaSetVolume,
    MaVolumeChanged,
    PushAutoplay,
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


def test_ma_modes_change_pushes_only_changed_flags() -> None:
    """An MA loop+autoplay change pushes each flag that actually differs."""
    state = CanonicalState(loop=LoopMode.OFF, autoplay=False, active=True)
    result = reduce(
        state,
        MaModesChanged(now_ms=1, action_uuid=b"\xbb" * 16, loop=LoopMode.REPEAT_ONE, autoplay=True),
    )
    assert any(isinstance(e, PushLoop) for e in result.effects)
    assert any(isinstance(e, PushAutoplay) for e in result.effects)


def test_cloud_volume_drives_ma() -> None:
    """Inbound volume is applied to MA (side channel, ungated)."""
    result = reduce(CanonicalState(active=True), CloudVolume(now_ms=1, volume=40))
    assert any(isinstance(e, MaSetVolume) and e.volume == 40 for e in result.effects)


def test_ma_volume_pushes_to_cloud() -> None:
    """MA volume change pushes to the cloud."""
    result = reduce(CanonicalState(active=True), MaVolumeChanged(now_ms=1, volume=55, muted=False))
    assert any(isinstance(e, PushVolume) and e.volume == 55 for e in result.effects)
