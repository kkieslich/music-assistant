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

The provider now owns a small local Qobuz Connect layer:

- mDNS discovery
- local HTTP handshake endpoints
- Qobuz WebSocket session management
- vendored protobuf command encoding/decoding
- queue state reconciliation
- state/volume/quality reporting helpers

Audio playback is handled by Music Assistant. The adapter maps Qobuz track IDs
to an explicitly selected native MA Qobuz provider and sends playback commands
to the target MA player queue.

## Local Dev Setup

Run the playground server:

```bash
.venv/bin/python -m music_assistant \
  --data-dir .mass-data \
  --cache-dir .mass-cache \
  --log-level debug
```

The local data/cache directories are ignored by git because they can contain
tokens and machine-specific state.

## Validation

Deterministic tests live in `tests/providers/qobuz_connect/`. The live harness
uses two authenticated Qobuz Web Clients, the real Qobuz cloud, a managed local
MA instance, and a BlackHole output. It verifies exact track IDs, queue state,
audio flow, ownership, and actual-versus-maximum quality while refusing to run
playback unless the silent target and unique Connect receiver are proven.

Qobuz Connect remains unofficial and reverse engineered. Protocol behavior
must therefore be grounded in current web-client captures, and the protobuf
definitions and generated modules must stay synchronized.
