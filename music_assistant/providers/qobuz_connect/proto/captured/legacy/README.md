# Legacy captures — OBSOLETE, DO NOT USE FOR PROTOCOL ANALYSIS

These three files are early Chrome-extension exports of the Qobuz Connect
WebSocket. **They preserve only outgoing frames** — incoming binary is empty.
That makes them unusable for any question about what the cloud or another
client sends.

For real protocol-behavior questions use the Playwright capture harness at
[tests/providers/qobuz_connect/protocol_capture/](../../../../../tests/providers/qobuz_connect/protocol_capture/),
which records both directions and writes captures into `.runs/`.

Kept here purely as a historical record of the reverse-engineering work
that bootstrapped the proto definitions.

## capture-1.json
QMac → QWeb handoff and a single track change. **Incoming binary empty.**

## capture-2.json
Queue mutations (clear / add / pause / resume / reorder / favorite). **Incoming binary empty.**

## capture-3.json
Rapid track-skip from QMac with QWeb as renderer. **Incoming binary empty** —
the very frames that would have told us how QWeb processed the burst are gone.
