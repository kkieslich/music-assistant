# Qobuz Connect Prototype

This is an experimental Qobuz Connect receiver for Music Assistant.

The target architecture is:

```text
Qobuz app -> Qobuz Connect protocol -> Music Assistant player queue -> native MA Qobuz provider
```

This intentionally avoids the tested DLNA bridge path:

```text
Qobuz app -> qobuz-proxy -> DLNA renderer -> Music Assistant player
```

The DLNA bridge proved discovery/control could work, but latency and seeking
were not acceptable for this use case.

## Current Shape

The provider reuses `qobuz-proxy` for the reverse-engineered Qobuz Connect
network/protocol layer:

- mDNS discovery
- local HTTP handshake endpoints
- Qobuz WebSocket session management
- protobuf command decoding
- queue state handling
- state/volume reporting helpers

Audio playback is handled by Music Assistant. The adapter maps Qobuz track IDs
to the configured MA Qobuz provider and sends playback commands to the target MA
player queue.

## Local Dev Setup

From the sibling worktree:

```bash
cd /Users/koljakieslich/Documents/dev/ha/server-qobuz-connect
uv pip install --python ../server/.venv/bin/python -e ../qobuz-proxy
ln -s ../server/.venv .venv
```

Then run the playground server:

```bash
.venv/bin/python -m music_assistant \
  --data-dir .mass-data \
  --cache-dir .mass-cache \
  --log-level debug
```

The local data/cache directories are ignored by git because they can contain
tokens and machine-specific state.

## Known Gaps

- It depends on `qobuz-proxy` internals instead of a small stable protocol
  library.
- Queue/shuffle/repeat/autoplay mapping is incomplete.
- Device identity should be made stable across restarts.
- State reporting should eventually sync from the MA player queue instead of
  only tracking local timestamps.
- Upstream acceptance is uncertain because Qobuz Connect is unofficial and
  reverse engineered here.
