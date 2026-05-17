"""
Scenario registry for the Qobuz Connect capture harness.

A scenario is any callable ``async def run(session: CaptureSession) -> None``.
Each scenario reproduces (or extends) one of the capture files in
proto/captured/, with the goal of covering at least the SRVR_* messages our
provider has to decode.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from tests.providers.qobuz_connect.protocol_capture.scenarios import (
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
}
