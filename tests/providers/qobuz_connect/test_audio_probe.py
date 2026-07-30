"""Unit tests for the AudioProbe ffmpeg-output parser (no audio hardware)."""

from __future__ import annotations

import subprocess

import pytest

from tests.providers.qobuz_connect.protocol_capture.audio_probe import (
    AudioLevel,
    AudioProbe,
    parse_volumedetect,
)

_SILENCE = """
[Parsed_volumedetect_0 @ 0x145f76880] mean_volume: -91.0 dB
[Parsed_volumedetect_0 @ 0x145f76880] max_volume: -91.0 dB
"""

_MUSIC = """
[Parsed_volumedetect_0 @ 0x1] mean_volume: -24.3 dB
[Parsed_volumedetect_0 @ 0x1] max_volume: -3.1 dB
"""

_SUBPROCESS_RUN = "tests.providers.qobuz_connect.protocol_capture.audio_probe.subprocess.run"


def test_parse_silence() -> None:
    """A silent capture parses to the -91 dB silence floor."""
    lvl = parse_volumedetect(_SILENCE)
    assert lvl == AudioLevel(mean_db=-91.0, max_db=-91.0)


def test_parse_music() -> None:
    """A music capture parses its mean/max volume."""
    lvl = parse_volumedetect(_MUSIC)
    assert lvl.mean_db == -24.3
    assert lvl.max_db == -3.1


def test_parse_missing_metrics_raises_capture_failure() -> None:
    """Output with no volume metrics is invalid capture evidence, not silence."""
    with pytest.raises(RuntimeError, match="volume metrics"):
        parse_volumedetect("no volume lines here")


def test_device_index_requires_exact_blackhole_2ch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prefix competitor cannot shadow the exact silent loopback."""
    stderr = """
[AVFoundation indev @ 0x1] [0] BlackHole 16ch
[AVFoundation indev @ 0x1] [1] BlackHole 2ch
[AVFoundation indev @ 0x1] [2] BlackHole 2ch Clone
"""
    monkeypatch.setattr(
        _SUBPROCESS_RUN,
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", stderr),
    )

    assert AudioProbe().device_index() == "1"


def test_device_index_rejects_duplicate_exact_blackhole_2ch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ambiguous exact devices abort instead of selecting the first one."""
    stderr = """
[AVFoundation indev @ 0x1] [1] BlackHole 2ch
[AVFoundation indev @ 0x1] [2] BlackHole 2ch
"""
    monkeypatch.setattr(
        _SUBPROCESS_RUN,
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", stderr),
    )

    with pytest.raises(RuntimeError, match="exactly one"):
        AudioProbe().device_index()


def test_measure_raises_when_ffmpeg_capture_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A nonzero ffmpeg exit cannot satisfy a silence check."""
    monkeypatch.setattr(
        _SUBPROCESS_RUN,
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 1, "", "input failed"),
    )
    probe = AudioProbe()
    probe._device = "1"

    with pytest.raises(RuntimeError, match="capture failed"):
        probe.measure(0.1)


def test_measure_raises_when_capture_has_no_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero captured samples are not evidence of silence."""
    stderr = """
[Parsed_volumedetect_0 @ 0x1] n_samples: 0
[Parsed_volumedetect_0 @ 0x1] mean_volume: -91.0 dB
[Parsed_volumedetect_0 @ 0x1] max_volume: -91.0 dB
"""
    monkeypatch.setattr(
        _SUBPROCESS_RUN,
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", stderr),
    )
    probe = AudioProbe()
    probe._device = "1"

    with pytest.raises(RuntimeError, match="no audio samples"):
        probe.measure(0.1)


def test_measure_accepts_final_positive_sample_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ffmpeg's provisional zero-sample filter must not override its final count."""
    stderr = """
[Parsed_volumedetect_0 @ 0x1] n_samples: 0
[Parsed_volumedetect_0 @ 0x2] n_samples: 44032
[Parsed_volumedetect_0 @ 0x2] mean_volume: -91.0 dB
[Parsed_volumedetect_0 @ 0x2] max_volume: -91.0 dB
"""
    monkeypatch.setattr(
        _SUBPROCESS_RUN,
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", stderr),
    )
    probe = AudioProbe()
    probe._device = "1"

    assert probe.measure(0.1) == AudioLevel(mean_db=-91.0, max_db=-91.0)
