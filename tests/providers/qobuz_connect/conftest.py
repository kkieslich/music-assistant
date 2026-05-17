"""Pytest config for qobuz_connect tests."""

from __future__ import annotations

# The protocol_capture/ subpackage is a Playwright-driven harness, not a
# test suite. It drives real browser sessions against play.qobuz.com and
# must never run in CI or during normal `pytest` invocations.
collect_ignore_glob = ["protocol_capture/*", "protocol_capture/**/*"]
