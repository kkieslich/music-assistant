"""
CLI runner for the live Qobuz Connect integration scenarios.

Attaches to (or starts) a real MA renderer and drives a real web client
through one or more scenarios, printing per-check pass/fail.

Usage (MA already running, attach to its log)::

    .venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.integration_run \
        --attach /path/to/ma_run.log --scenario handoff

Usage (harness manages MA lifecycle)::

    .venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.integration_run \
        --scenario all
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .integration_harness import (
    IntegrationSession,
    ScenarioResult,
    close_session,
    default_probe,
    open_session,
)
from .integration_scenarios import SCENARIOS
from .ma_probe import MAProbe

LOGGER = logging.getLogger("qobuz_integration")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=__spec__.name if __spec__ else "integration_run")
    parser.add_argument(
        "--scenario",
        required=True,
        help="Scenario name, or 'all'.",
        choices=[*sorted(SCENARIOS), "all"],
    )
    parser.add_argument(
        "--attach",
        type=Path,
        default=None,
        help="Attach to an already-running MA writing to this log file "
        "(default: start a fresh MA process).",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    if args.attach is not None:
        ma = MAProbe(
            log_path=args.attach,
            data_dir=Path(".mass-data").resolve(),
            cache_dir=Path(".mass-cache").resolve(),
        )
        started_here = False
    else:
        ma = default_probe()
        LOGGER.info("Starting MA renderer...")
        ma.start()
        started_here = True

    names = sorted(SCENARIOS) if args.scenario == "all" else [args.scenario]
    session_handle = None
    session = None
    failures = 0
    try:
        session, session_handle = await open_session(ma)
        assert session is not None
        assert session_handle is not None
        for name in names:
            LOGGER.info("=== scenario: %s ===", name)
            assert session is not None
            result, cleanup_ok = await _run_scenario(name, session)
            if not cleanup_ok:
                LOGGER.warning(
                    "Recycling both browser clients after Qobuz refused local-output cleanup"
                )
                try:
                    assert session_handle is not None
                    await close_session(session_handle)
                    session_handle = None
                    session = None
                    session, session_handle = await open_session(ma)
                    await session.ensure_safe_for_playback()
                    cleanup_ok = True
                except Exception:
                    LOGGER.exception("Browser-session isolation recovery failed")
            result.check("isolated cleanup completed", cleanup_ok)
            _print_result(result)
            if not result.passed:
                failures += 1
    finally:
        try:
            if session_handle is not None:
                await close_session(session_handle)
        except Exception:
            LOGGER.exception("Browser cleanup failed")
            failures += 1
        finally:
            if started_here:
                ma.stop()
    return 1 if failures else 0


async def _run_scenario(name: str, session: IntegrationSession) -> tuple[ScenarioResult, bool]:
    """Run, capture, and isolate one scenario without aborting the full suite."""
    try:
        result = await SCENARIOS[name](session)
    except Exception as err:
        LOGGER.exception("Scenario %s raised an exception", name)
        result = ScenarioResult(scenario=name)
        result.check(
            "scenario completed without exception",
            False,
            detail=f"{type(err).__name__}: {err}",
        )
    evidence = session.write_evidence(name)
    LOGGER.info("Saved live websocket evidence: %s", evidence)
    try:
        cleanup_ok = await session.cleanup_playback()
    except Exception:
        LOGGER.exception("Per-scenario cleanup failed")
        cleanup_ok = False
    return result, cleanup_ok


def _print_result(result: ScenarioResult) -> None:
    status = result.status.value.upper()
    print(f"\n[{status}] {result.scenario}")  # noqa: T201 - CLI result output
    if result.skip_reason is not None:
        print(f"  - {result.skip_reason}")  # noqa: T201 - CLI result output
    for c in result.checks:
        mark = "  ✓" if c.passed else "  ✗"
        line = f"{mark} {c.name}"
        if c.detail and not c.passed:
            line += f"\n       {c.detail}"
        print(line)  # noqa: T201 - CLI result output


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(name)s — %(message)s")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
