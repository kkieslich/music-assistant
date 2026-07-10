"""
Real audio-output verification via the BlackHole loopback device.

MA's Qobuz Connect target renders to the BlackHole 2ch virtual device; its
input carries exactly that signal, so capturing the input with ffmpeg and
measuring the level proves whether audio actually flowed — the definitive
check for "the UI says playing but there is no sound". There is no reliable
MA-native signal for this, so we measure the analog truth.

Assumes nothing else on the machine routes audio to BlackHole during a run.
"""

from __future__ import annotations

import math
import re
import shutil
import subprocess
import time
from dataclasses import dataclass

_MEAN = re.compile(r"mean_volume:\s*(-?\d+(?:\.\d+)?)\s*dB")
_MAX = re.compile(r"max_volume:\s*(-?\d+(?:\.\d+)?)\s*dB")
_AVF_DEVICE = re.compile(r"\[(\d+)\]\s*BlackHole", re.IGNORECASE)
# Resolve ffmpeg to an absolute path so subprocess uses a full executable path
# (a fixed, trusted binary — never user input).
_FFMPEG = shutil.which("ffmpeg") or "ffmpeg"

# Calibrated 2026-07-10: BlackHole reads exactly -91 dB when MA is idle
# (digital silence) and ~-44 dB during full-volume playback. The default
# -70 dB threshold sits 21 dB above the silence floor, so it catches even
# quiet/low-volume playback yet reports true silence (mute, or the "UI says
# playing but no sound" bug) as not-playing.


@dataclass(slots=True)
class AudioLevel:
    """Measured loudness of a capture window, in dBFS (-inf == silence/no data)."""

    mean_db: float
    max_db: float


def parse_volumedetect(stderr: str) -> AudioLevel:
    """
    Parse ffmpeg ``volumedetect`` output into an :class:`AudioLevel`.

    :param stderr: The combined ffmpeg stderr text from a volumedetect run.
    """
    mean = _MEAN.search(stderr)
    mx = _MAX.search(stderr)
    return AudioLevel(
        mean_db=float(mean.group(1)) if mean else -math.inf,
        max_db=float(mx.group(1)) if mx else -math.inf,
    )


class AudioProbe:
    """Capture and measure the BlackHole loopback to verify real audio output."""

    def __init__(self) -> None:
        """Create a probe; the BlackHole device index is resolved lazily."""
        self._device: str | None = None

    def device_index(self) -> str:
        """
        Return BlackHole's avfoundation input index (resolved once, cached).

        :raises RuntimeError: If no BlackHole device is present.
        """
        if self._device is not None:
            return self._device
        proc = subprocess.run(  # noqa: S603 - fixed ffmpeg argv, no user input
            [_FFMPEG, "-hide_banner", "-f", "avfoundation", "-list_devices", "true", "-i", ""],
            capture_output=True,
            text=True,
            check=False,
        )
        match = _AVF_DEVICE.search(proc.stderr)
        if match is None:
            raise RuntimeError("BlackHole audio device not found in avfoundation device list")
        self._device = match.group(1)
        return self._device

    def measure(self, seconds: float) -> AudioLevel:
        """
        Capture ``seconds`` of BlackHole audio and return its level.

        :param seconds: Capture window length.
        """
        proc = subprocess.run(  # noqa: S603 - fixed ffmpeg argv, no user input
            [
                _FFMPEG, "-hide_banner", "-f", "avfoundation",
                "-i", f":{self.device_index()}",
                "-t", f"{seconds}", "-af", "volumedetect", "-f", "null", "-",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return parse_volumedetect(proc.stderr)

    def is_playing(self, seconds: float = 2.0, threshold_db: float = -70.0) -> bool:
        """
        Return whether audio is clearly above the silence floor.

        :param seconds: Capture window length.
        :param threshold_db: Mean level above which we call it "playing".
        """
        return self.measure(seconds).mean_db > threshold_db

    def wait_for_sound(
        self, timeout: float, *, threshold_db: float = -70.0, poll: float = 2.0
    ) -> bool:
        """Poll until audio is present or ``timeout`` elapses."""
        return self._wait(timeout, threshold_db, poll, want_sound=True)

    def wait_for_silence(
        self, timeout: float, *, threshold_db: float = -70.0, poll: float = 2.0
    ) -> bool:
        """Poll until audio is absent or ``timeout`` elapses."""
        return self._wait(timeout, threshold_db, poll, want_sound=False)

    def _wait(self, timeout: float, threshold_db: float, poll: float, *, want_sound: bool) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            playing = self.measure(poll).mean_db > threshold_db
            if playing == want_sound:
                return True
        return False
