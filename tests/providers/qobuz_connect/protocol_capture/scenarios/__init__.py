"""
Scenario registry for the Qobuz Connect capture harness.

A scenario is any callable ``async def run(session: CaptureSession) -> None``.
Each scenario drives the two Web Clients into a specific protocol interaction
so the recorder can capture the resulting bidirectional WebSocket traffic
under ``.runs/``. Use existing scenarios as references when answering
protocol-behavior questions; add new ones when a question can't be answered
from existing captures.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from tests.providers.qobuz_connect.protocol_capture.scenarios import (
    controller_burst_skip,
    controller_burst_skip_throttled,
    controller_natural_track_advance,
    controller_paused_scrub_then_skip,
    controller_play_pause_rapid,
    controller_playing_seek_scrub,
    controller_skip_then_seek,
    handoff,
    quality_change,
    queue_mutations,
    rapid_skip,
)

if TYPE_CHECKING:
    from tests.providers.qobuz_connect.protocol_capture.harness import CaptureSession

ScenarioFn = Callable[["CaptureSession"], Awaitable[None]]

SCENARIOS: dict[str, ScenarioFn] = {
    "handoff": handoff.run,
    "queue_mutations": queue_mutations.run,
    "rapid_skip": rapid_skip.run,
    "quality_change": quality_change.run,
    "controller_burst_skip": controller_burst_skip.run,
    "controller_playing_seek_scrub": controller_playing_seek_scrub.run,
    "controller_paused_scrub_then_skip": controller_paused_scrub_then_skip.run,
    "controller_burst_skip_throttled": controller_burst_skip_throttled.run,
    "controller_play_pause_rapid": controller_play_pause_rapid.run,
    "controller_skip_then_seek": controller_skip_then_seek.run,
    "controller_natural_track_advance": controller_natural_track_advance.run,
}
