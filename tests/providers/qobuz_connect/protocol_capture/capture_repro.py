"""
Capture a phone-app repro from a running MA's debug log.

Some Qobuz Connect bugs only appear when the native phone app is the
controller (its handoff differs from the web client the harness drives). This
helper snapshots a running MA's log, lets you reproduce the bug on your phone,
then extracts the relevant ``qobuz_connect`` reduce/stream/dispatcher lines
into a compact file to share for diagnosis.

Usage::

    # 1. Run MA with debug logging (your normal setup / real target player):
    #      python -m music_assistant --log-level debug > /tmp/ma.log 2>&1 &
    # 2. Start the capture, pointing at that log:
    .venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.capture_repro /tmp/ma.log
    # 3. Do the repro on your phone (e.g. play an album, hand off mid-track).
    # 4. Press Enter. The extracted slice is written next to the log.
"""

from __future__ import annotations

# CLI helper: print is the intended output channel.
import contextlib
import re
import sys
from pathlib import Path

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_KEEP = re.compile(
    r"qobuz_connect|Qobuz Connect|Start Streaming queue track|player_queues|"
    r"Handling command|active output protocol"
)


def main(argv: list[str]) -> int:
    """Snapshot the log, wait for a repro, then dump the relevant slice."""
    if not argv:
        print("usage: capture_repro <path-to-ma-log>", file=sys.stderr)  # noqa: T201
        return 2
    log = Path(argv[0])
    if not log.exists():
        print(f"log not found: {log}", file=sys.stderr)  # noqa: T201
        return 2
    start = log.stat().st_size
    print(f"Snapshotted {log} at {start} bytes.")  # noqa: T201
    print("Now reproduce the issue on your phone, then press Enter here...")  # noqa: T201
    with contextlib.suppress(EOFError, KeyboardInterrupt):
        input()
    with log.open("rb") as fh:
        fh.seek(start)
        chunk = fh.read().decode("utf-8", errors="replace")
    kept = [_ANSI.sub("", ln) for ln in chunk.splitlines() if _KEEP.search(ln)]
    out = log.with_suffix(".qobuz_repro.txt")
    out.write_text("\n".join(kept) + "\n")
    print(f"Wrote {len(kept)} relevant lines to {out}")  # noqa: T201
    print("Share that file (and say what you did) for diagnosis.")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
