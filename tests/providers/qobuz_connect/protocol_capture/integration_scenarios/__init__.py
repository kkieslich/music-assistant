"""Live integration scenarios, grouped by direction/feature."""

from __future__ import annotations

from . import app_driven, ma_driven, modes_volume, quality, sessions

SCENARIOS = {
    **app_driven.SCENARIOS,
    **modes_volume.SCENARIOS,
    **ma_driven.SCENARIOS,
    **quality.SCENARIOS,
    **sessions.SCENARIOS,
}
