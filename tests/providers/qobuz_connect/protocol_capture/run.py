r"""
CLI entry point for the Qobuz Connect protocol-capture harness.

Usage::

    # First-time login (headed, manual UI in browsers):
    python -m tests.providers.qobuz_connect.protocol_capture.run \
        --scenario handoff --headed

    # Subsequent runs reuse the persisted storage_state (headless):
    python -m tests.providers.qobuz_connect.protocol_capture.run \
        --scenario queue_mutations

Outputs two JSON capture files (one per Web Client) under ``.runs/``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from .harness import RUNS_DIR, capture_session
from .scenarios import SCENARIOS

LOGGER = logging.getLogger("qobuz_capture")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tests.providers.qobuz_connect.protocol_capture.run",
        description="Drive two Qobuz Web Clients and record their WebSocket "
        "traffic for protocol reverse-engineering.",
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=sorted(SCENARIOS),
        help="Scenario name to run.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        default=_truthy(os.environ.get("CAPTURE_HEADED")),
        help="Show the Chromium window. Required for first-run logins; "
        "default toggled by CAPTURE_HEADED env var.",
    )
    parser.add_argument(
        "--slow-mo",
        type=int,
        default=int(os.environ.get("CAPTURE_SLOW_MO_MS", "0")),
        help="Milliseconds to wait between Playwright actions (debugging aid).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RUNS_DIR,
        help=f"Where to write capture JSON (default: {RUNS_DIR}).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def _truthy(value: str | None) -> bool:
    return (value or "").lower() in {"1", "true", "yes", "y", "on"}


async def _run(args: argparse.Namespace) -> int:
    scenario_fn = SCENARIOS[args.scenario]
    LOGGER.info("Starting scenario %r (headed=%s)", args.scenario, args.headed)
    async with capture_session(
        headed=args.headed, out_dir=args.out_dir, slow_mo_ms=args.slow_mo
    ) as session:
        await session.b.qobuz.select_local_output(expected_name="Web Player Chrome")
        try:
            await scenario_fn(session)
            path_a, path_b = await session.write_captures(args.scenario)
            LOGGER.info("Capture A: %s", path_a)
            LOGGER.info("Capture B: %s", path_b)
        finally:
            await session.b.qobuz.select_local_output(expected_name="Web Player Chrome")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Entry point used by both ``python -m ...`` and editor run configurations."""
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
