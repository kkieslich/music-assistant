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

import re
import shutil
import subprocess
import time
from dataclasses import dataclass

_VOLUME_VALUE = r"(-?(?:\d+(?:\.\d+)?|inf))"
_MEAN = re.compile(rf"mean_volume:\s*{_VOLUME_VALUE}\s*dB")
_MAX = re.compile(rf"max_volume:\s*{_VOLUME_VALUE}\s*dB")
_SAMPLES = re.compile(r"n_samples:\s*(\d+)")
_AVF_DEVICE = re.compile(r"\[(\d+)\]\s*BlackHole 2ch[ \t]*$", re.IGNORECASE | re.MULTILINE)
# Resolve ffmpeg to an absolute path so subprocess uses a full executable path
# (a fixed, trusted binary — never user input).
_FFMPEG = shutil.which("ffmpeg") or "ffmpeg"

# Calibrated 2026-07-10. We threshold on PEAK (max) level, not mean: real
# playback always has peaks well above the silence floor even when the mean is
# low (quiet skits/intros measured mean -79 but peak -61), whereas true silence
# — MA idle, paused, or the "UI says playing but no sound" bug — reads peak -91
# on BlackHole. The default -80 dB threshold sits 11 dB above the -91 floor and
# ~19 dB below the quietest real playback, cleanly separating the two. Mean is
# unreliable here (quiet content and brief transition gaps depress it).


@dataclass(slots=True)
class AudioLevel:
    """Measured loudness of a capture window in dBFS."""

    mean_db: float
    max_db: float


def parse_volumedetect(stderr: str) -> AudioLevel:
    """
    Parse ffmpeg ``volumedetect`` output into an :class:`AudioLevel`.

    :param stderr: The combined ffmpeg stderr text from a volumedetect run.
    """
    mean = _MEAN.search(stderr)
    mx = _MAX.search(stderr)
    if mean is None or mx is None:
        raise RuntimeError("ffmpeg capture did not produce mean/max volume metrics")
    return AudioLevel(mean_db=float(mean.group(1)), max_db=float(mx.group(1)))


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
        matches = _AVF_DEVICE.findall(proc.stderr)
        if len(matches) != 1:
            raise RuntimeError(
                f"Expected exactly one BlackHole 2ch avfoundation input, found {len(matches)}"
            )
        self._device = matches[0]
        return self._device

    def measure(self, seconds: float) -> AudioLevel:
        """
        Capture ``seconds`` of BlackHole audio and return its level.

        :param seconds: Capture window length.
        """
        proc = subprocess.run(  # noqa: S603 - fixed ffmpeg argv, no user input
            [
                _FFMPEG,
                "-hide_banner",
                "-f",
                "avfoundation",
                "-i",
                f":{self.device_index()}",
                "-t",
                f"{seconds}",
                "-af",
                "volumedetect",
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg BlackHole capture failed with exit code {proc.returncode}")
        if (samples := _SAMPLES.search(proc.stderr)) is not None and int(samples.group(1)) == 0:
            raise RuntimeError("ffmpeg BlackHole capture produced no audio samples")
        return parse_volumedetect(proc.stderr)

    def is_playing(self, seconds: float = 2.0, threshold_db: float = -80.0) -> bool:
        """
        Return whether real audio (a peak above the silence floor) is present.

        :param seconds: Capture window length.
        :param threshold_db: Peak level above which we call it "playing".
        """
        return self.measure(seconds).max_db > threshold_db

    def wait_for_sound(
        self, timeout: float, *, threshold_db: float = -80.0, poll: float = 2.0
    ) -> bool:
        """Poll until audio is present or ``timeout`` elapses."""
        return self._wait(timeout, threshold_db, poll, want_sound=True)

    def wait_for_silence(
        self, timeout: float, *, threshold_db: float = -80.0, poll: float = 2.0
    ) -> bool:
        """Poll until audio is absent or ``timeout`` elapses."""
        return self._wait(timeout, threshold_db, poll, want_sound=False)

    def _wait(self, timeout: float, threshold_db: float, poll: float, *, want_sound: bool) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            playing = self.measure(poll).max_db > threshold_db
            if playing == want_sound:
                return True
        return False
