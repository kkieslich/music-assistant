"""Unit tests for the AudioProbe ffmpeg-output parser (no audio hardware)."""

from __future__ import annotations

import math

from tests.providers.qobuz_connect.protocol_capture.audio_probe import (
    AudioLevel,
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


def test_parse_silence() -> None:
    """A silent capture parses to the -91 dB silence floor."""
    lvl = parse_volumedetect(_SILENCE)
    assert lvl == AudioLevel(mean_db=-91.0, max_db=-91.0)


def test_parse_music() -> None:
    """A music capture parses its mean/max volume."""
    lvl = parse_volumedetect(_MUSIC)
    assert lvl.mean_db == -24.3
    assert lvl.max_db == -3.1


def test_parse_missing_returns_negative_inf() -> None:
    """Output with no volume lines yields -inf (treated as silence)."""
    lvl = parse_volumedetect("no volume lines here")
    assert lvl.mean_db == -math.inf
    assert lvl.max_db == -math.inf
