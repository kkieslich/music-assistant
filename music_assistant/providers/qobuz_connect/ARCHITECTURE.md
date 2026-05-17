# Qobuz Connect Provider — Architecture

This document describes the **current** shape of the `qobuz_connect`
provider. It's the prerequisite for the redesign work in Phase B / Phase C
of the plan: you should be able to read this and modify any one module
without spelunking the others. Pair it with [`README.md`](README.md) for the
user-facing rationale (why this provider exists, what it replaces).

## What this provider does

Music Assistant becomes a **Qobuz Connect receiver**: the Qobuz mobile /
desktop / web client sees MA's instance as a Connect-capable device,
hands off playback to it, and continues to control the queue / position /
volume / quality from the app. The receiver re-implements the Qobuz
Connect protocol locally (`qws-eu-prod.qobuz.com/ws`) instead of bridging
through DLNA — that path was abandoned earlier for latency / seeking
reasons.

Streaming is **not** done by this provider. Once the Qobuz app says "play
track X at position Y", the receiver translates that into MA queue
operations against the native `qobuz` music provider, which is what
actually fetches the audio.

```
┌──────────────┐   mDNS    ┌─────────────────────────┐  enqueue+play  ┌──────────────┐
│  Qobuz app   │──────────▶│    qobuz_connect        │───────────────▶│ MA player    │
│ (controller) │           │ (this provider)         │                │ queue        │
└──────────────┘           │                         │                └──────┬───────┘
       ▲                   │                         │                       │
       │  WebSocket        │                         │   stream URL          │
       │  protobuf         │                         │   from MA's native    │
       ▼                   │                         │   qobuz provider      ▼
┌──────────────┐   QConnect┌─────────────────────────┐               ┌──────────────┐
│ Qobuz cloud  │◀─────────▶│                         │               │ MA's native  │
│ qws-eu-prod  │           │                         │◀──────────────│ qobuz prov.  │
└──────────────┘           └─────────────────────────┘               └──────────────┘
```

## End-to-end flow

```
1.  Provider loads
    ├─ __init__.py:QobuzConnectProvider.loaded_in_mass()
    ├─ derives a stable device UUID from instance_id (so the mDNS serial
    │  and Qobuz cloud device id survive restarts — otherwise the Qobuz
    │  app shows duplicates)
    ├─ subscribes to MA QUEUE_UPDATED events
    └─ starts QobuzConnectDiscovery (mDNS + local aiohttp endpoints)

2.  Qobuz app discovers MA
    ├─ discovery.py advertises _qobuz-connect._tcp.local. with the device
    │  serial / friendly name / max-quality
    └─ exposes /streamcore/* aiohttp handlers; the Qobuz app POSTs the
       JWT auth tokens + session_id to /streamcore/connect-to-qconnect

3.  Provider opens the cloud WebSocket
    ├─ on_connect callback in __init__.py creates QobuzConnectSession
    ├─ session.py sends OuterMessageType.AUTHENTICATE (JWT) ┐
    ├─ session.py sends OuterMessageType.SUBSCRIBE (QConnect proto)
    └─ session.py sends RNDR_SRVR_JOIN_SESSION (renderer joins)

4.  Steady state: bidirectional message loop
    ├─ session.py decodes outer envelopes via QobuzConnectCodec.decode_frame
    ├─ batched inner messages parsed → typed events fired via callbacks
    ├─ sync.py's handlers reconcile each event against QobuzMirror + MA
    └─ sync.py emits state reports back via session.send_renderer_state
       on a 5s heartbeat plus ad-hoc after every command
```

## Module map

| File                           | Owns                                                      | MA?  | Proto? |
|--------------------------------|-----------------------------------------------------------|------|--------|
| [`discovery.py`](discovery.py) | mDNS service + local HTTP handshake endpoints             | ❌    | ❌      |
| [`protocol.py`](protocol.py)   | Outer-frame codec + protobuf encode/decode                | ❌    | ✅      |
| [`session.py`](session.py)     | WebSocket lifecycle, token refresh, frame dispatch        | ❌    | via proto |
| [`models.py`](models.py)       | DTOs + enums shared across all of the above               | ❌    | enum refs only |
| [`sync.py`](sync.py)           | **Single owner of MA ↔ Qobuz reconciliation** (1.1k LOC)  | ✅    | indirect (via models enums) |
| [`__init__.py`](__init__.py)   | `QobuzConnectProvider`: config, lifecycle, wiring, callbacks | ✅ | ❌      |

The first three modules are intentionally pure: they could be lifted into a
standalone Qobuz Connect SDK and reused outside Music Assistant. The MA
coupling lives in `sync.py` and `__init__.py` — which is also the part the
plan calls out for restructuring in Phase C.

## Inbound messages (Qobuz → this provider)

All inbound traffic is one of these QConnect inner message types,
dispatched in [`session.py`](session.py) and handled in [`sync.py`](sync.py):

| Type ID | Message                              | Decoder (`protocol.py`)       | Handler (`sync.py`)                  | Purpose                                                |
|--------:|--------------------------------------|-------------------------------|--------------------------------------|--------------------------------------------------------|
|      41 | `SRVR_RNDR_SET_STATE`                | `parse_set_state`             | `handle_qobuz_set_state`             | Master command: play/pause/seek/load-track             |
|      42 | `SRVR_RNDR_SET_VOLUME`               | (inline in session.py)        | `set_volume` / `set_volume_delta`    | Volume command                                         |
|      43 | `SRVR_RNDR_SET_ACTIVE`               | (inline in session.py)        | `reset_for_deactivation` if `false`  | Cloud activates/deactivates this renderer              |
|      44 | `SRVR_RNDR_SET_MAX_AUDIO_QUALITY`    | (inline in session.py)        | `_on_quality_change` in `__init__.py`| User picked a new max quality in the Qobuz app         |
|      77 | `CTRL_SRVR_ASK_FOR_RENDERER_STATE`   | (no payload)                  | `report_state`                       | Cloud asks: "what's your current state?"               |
|      88 | `SRVR_CTRL_QUEUE_ERROR_MESSAGE`      | `parse_queue_error`           | `handle_queue_error`                 | Cloud rejected a queue-load we sent                    |
|      91 | `SRVR_CTRL_QUEUE_TRACKS_LOADED`      | `parse_queue_load_ack`        | `handle_queue_load_ack`              | Cloud ack'd a `CTRL_SRVR_QUEUE_LOAD_TRACKS`            |
|     103 | `SRVR_CTRL_AUTOPLAY_TRACKS_LOADED`   | `parse_autoplay_load_ack`     | `handle_queue_load_ack`              | Cloud ack'd a `CTRL_SRVR_AUTOPLAY_LOAD_TRACKS`         |
|     105 | `SRVR_CTRL_QUEUE_VERSION_CHANGED`    | `parse_queue_version_changed` | `handle_queue_version`               | Authoritative queue version bump                       |

### Known gaps (Phase B targets)

Validated against the full bidirectional captures under
[`tests/providers/qobuz_connect/protocol_capture/.runs/`](../../../tests/providers/qobuz_connect/protocol_capture/.runs/).
Tier 1 is the set the captures *prove* matter; Tier 2 are proto-defined
messages the Qobuz app *will* emit on flows we haven't captured yet
(insert/remove/reorder via specific UI paths, clear from another client).

**Tier 1 — directly observed in captures, must be handled:**

| Type ID | Name                                 | Direction | Symptom when ignored                                                          |
|--------:|--------------------------------------|-----------|-------------------------------------------------------------------------------|
|      90 | `SRVR_CTRL_QUEUE_STATE`              | inbound   | **Most critical.** Full queue snapshot the cloud pushes on every (re)connect and on `CTRL_SRVR_ASK_FOR_QUEUE_STATE`. Without it, MA cannot authoritatively rebuild queue state after the WS reconnects. |
|      93 | `SRVR_CTRL_QUEUE_TRACKS_ADDED`       | inbound   | Tracks added in the Qobuz app silently drift                                  |
|      45 | `SRVR_RNDR_SET_LOOP_MODE`            | inbound   | Repeat-mode toggles in the Qobuz app never reach MA                           |
|      46 | `SRVR_RNDR_SET_SHUFFLE_MODE`         | inbound   | Shuffle toggles in the Qobuz app never reach MA                               |
|      47 | `SRVR_RNDR_SET_AUTOPLAY_MODE`        | inbound   | Autoplay setting can't be controlled from the Qobuz app                       |
|      10 | `DISCONNECT` (outer envelope)        | inbound   | Already handled: the receive loop raises ``QobuzServerDisconnect`` and the outer loop reconnects after backoff. Phase B just upgrades the log level so the reconnect is user-visible at INFO. |
|      29 | `RNDR_SRVR_VOLUME_MUTED`             | outbound  | MA never tells the cloud when the target player gets muted                    |

**Tier 2 — defined in proto, expected on flows we haven't captured yet:**

| Type ID | Name                                  | Symptom when ignored                                  |
|--------:|---------------------------------------|-------------------------------------------------------|
|      92 | `SRVR_CTRL_QUEUE_TRACKS_INSERTED`     | Queue drift on insert-at-position                     |
|      94 | `SRVR_CTRL_QUEUE_TRACKS_REMOVED`      | Removals don't reflect in MA                          |
|      95 | `SRVR_CTRL_QUEUE_TRACKS_REORDERED`    | Reorders drift until the next full QUEUE_STATE        |
|      89 | `SRVR_CTRL_QUEUE_CLEARED`             | MA keeps playing the now-orphaned current track       |

**Tier 3 — explicitly benign, but must be acknowledged (not fall through unhandled):**

A class of `SRVR_CTRL_*` broadcasts shows up on our WebSocket because of
how Qobuz routes subscriptions: messages about *other* renderers'
state-changes get sent to us too. They require no MA action — the right
behavior is to log them at debug and move on:

`SRVR_CTRL_SESSION_STATE` (81), `SRVR_CTRL_RENDERER_STATE_UPDATED` (82),
`SRVR_CTRL_ADD_RENDERER` (83), `SRVR_CTRL_UPDATE_RENDERER` (84),
`SRVR_CTRL_REMOVE_RENDERER` (85), `SRVR_CTRL_ACTIVE_RENDERER_CHANGED`
(86), `SRVR_CTRL_VOLUME_CHANGED` (87), `SRVR_CTRL_VOLUME_MUTED` (98),
`SRVR_CTRL_MAX_AUDIO_QUALITY_CHANGED` (99),
`SRVR_CTRL_FILE_AUDIO_QUALITY_CHANGED` (100),
`SRVR_CTRL_LOOP_MODE_SET` (97).

**Known parse anomaly:** one 82-byte PAYLOAD frame in
`queue_mutations__client_b.json` (frame index 30) fails to parse as a
`QConnectBatch`. Possibly a partial transmission or non-batch control
message. Defer investigation to Phase C; for now the dispatcher should
log and continue rather than crash.

Phase B uses bytes from the captures as ground-truth fixtures for
decoder round-trip tests.

## Outbound messages (this provider → Qobuz)

All outbound traffic is constructed in [`protocol.py`](protocol.py) and
sent via [`session.py`](session.py). The triggering code lives in
[`sync.py`](sync.py) and [`__init__.py`](__init__.py).

| Type ID | Message                                  | Encoder (`protocol.py`)                  | Triggered from                                                                            |
|--------:|------------------------------------------|------------------------------------------|-------------------------------------------------------------------------------------------|
|       1 | `AUTHENTICATE` (outer envelope)          | `encode_authenticate`                    | `session.start()` after tokens arrive                                                     |
|       2 | `SUBSCRIBE` (outer envelope)             | `encode_subscribe`                       | `session.start()` after AUTHENTICATE                                                       |
|      23 | `RNDR_SRVR_STATE_UPDATED`                | `encode_renderer_state`                  | `sync.report_state()` on 5s heartbeat + after every command, plus 1s during buffering     |
|      25 | `RNDR_SRVR_VOLUME_CHANGED`               | `encode_volume_changed`                  | `_broadcast_current_volume` (on connect/activate) and `sync.set_volume`                   |
|      26 | `RNDR_SRVR_FILE_AUDIO_QUALITY_CHANGED`   | `encode_file_audio_quality_changed`      | `session.send_quality_reports` after connect / quality change                              |
|      27 | `RNDR_SRVR_DEVICE_AUDIO_QUALITY_CHANGED` | `encode_device_audio_quality_changed`    | same                                                                                       |
|      28 | `RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED`    | `encode_max_audio_quality_changed`       | same                                                                                       |
|      62 | `CTRL_SRVR_SET_PLAYER_STATE`             | `encode_player_state`                    | (method exists, no callsite — dead for now)                                                |
|      66 | `CTRL_SRVR_QUEUE_LOAD_TRACKS`            | `encode_queue_load_tracks`               | `sync._send_ma_origin_queue_load` when MA starts a track that diverges from Qobuz's state |
|      79 | `CTRL_SRVR_AUTOPLAY_LOAD_TRACKS`         | `encode_autoplay_load_tracks`            | (method exists, no callsite — dead for now)                                                |
|       — | `RNDR_SRVR_JOIN_SESSION`                 | `encode_join_session`                    | `session.start()` after SUBSCRIBE                                                          |

## The sync engine

[`sync.py`](sync.py) is the part that fuses two systems that don't speak
each other's language. The plan calls it out for restructuring — for now,
this section just names the concepts so the existing 1,100-line file can
be read.

### Canonical state

```python
class QobuzMirror:
    queue_version: QueueVersion      # major/minor from Qobuz
    current_item:  QueueTrackRef | None
    next_item:     QueueTrackRef | None
    playing_state: PlayingState      # STOPPED, PLAYING, PAUSED
    buffer_state:  BufferState       # UNKNOWN, BUFFERING, OK, ERROR, UNDERRUN
    position_ms:   int
    position_timestamp_ms: int        # interpolation anchor
    duration_ms:   int
```

`QobuzMirror` is the canonical "what does Qobuz think we're doing." Both
inbound events and outbound state reports flow through it.

### Ephemeral state (the band-aids the plan targets)

Scattered across `QobuzConnectSyncEngine.__init__`, currently a mix of
overlapping single fields:

- **Paused seek** (`pending_paused_seek_ms`, `_pending_paused_seek_ref`):
  if Qobuz seeks while paused, remember the position so the next play can
  resume there.
- **Playing seek, debounced** (`_pending_seek_position_ms`, `_ref`,
  `_generation`, `_pending_seek_task`): coalesce rapid scrubs from the
  Qobuz app before issuing a single MA seek.
- **Position confirmation** (`_pending_qobuz_position_ms`, `_ref`,
  `_source_ms`, `_timestamp_ms`): hold Qobuz state frozen until MA's
  reported position confirms a seek landed.
- **Queue-load acknowledgement** (`_pending_queue_loads: dict[bytes,
  asyncio.Future]`): map `action_uuid` to a future that resolves when
  Qobuz acks the load (or times out at 3s).
- **Prequeue** (`_prequeued_qobuz_items`, `_last_prequeued_next_ref`):
  optimistically place Qobuz's announced next_item into MA's queue ahead
  of time.

Phase C of the plan consolidates these into typed dataclasses in a new
`state.py`.

### Generation tracking

`_qobuz_command_generation` ticks up every time a *command* (vs. a
notification) arrives. Every handler checks `_is_current_command(gen)`
before issuing MA operations so a stale command doesn't undo a newer one.
The seek pipeline has its own `_pending_seek_generation` counter.

### Origin

`self.origin: Origin | None` is set to `QOBUZ` while a Qobuz command is
being processed, so the MA `QUEUE_UPDATED` listener doesn't echo the
change back. Currently a plain instance field — Phase C replaces it with
an async context manager so leaks become impossible.

### Tasks

| Task                          | Cadence                | Purpose                                                  |
|-------------------------------|------------------------|----------------------------------------------------------|
| `_heartbeat_task`             | every 5 s              | Periodic `RNDR_SRVR_STATE_UPDATED`                       |
| `_reconcile_task`             | latest-only            | Run async MA operations from the most recent Qobuz cmd   |
| `_metadata_task`              | latest-only            | Fetch track metadata for non-command updates             |
| `_buffering_report_task`      | every 1 s while loading| Faster state reports while audio is loading              |
| `_pending_seek_task`          | 350 ms after last scrub| Debounced playing-seek                                   |

## Glossary

- **Controller** — the role of a Qobuz client that *sends* commands to a
  renderer (e.g. the Qobuz mobile app driving MA).
- **Renderer** — the role of a Qobuz client that *plays* audio in
  response to controller commands (e.g. MA's `qobuz_connect`).
- **Origin** — internal tag for "who initiated the in-flight change"
  (`QOBUZ` / `MA` / `ACK`), used to short-circuit echo loops between the
  MA event bus and the Qobuz cloud.
- **Queue version** — `(major, minor)` integer pair the cloud uses to
  arbitrate concurrent edits to the play queue. Every load / insert /
  remove / reorder bumps `minor`; some operations bump `major`.
- **action_uuid** — a 16-byte UUID the controller mints when issuing a
  `CTRL_SRVR_QUEUE_LOAD_TRACKS`; the cloud echoes it in the
  corresponding `SRVR_CTRL_QUEUE_TRACKS_LOADED` ack so we can correlate
  request with response.
- **Pre-queue** — adding the cloud's announced `next_item` to MA's queue
  ahead of time so the player can switch tracks gaplessly.

## Out of scope / out of band

- **Favorites.** Adding or removing favorites uses the Qobuz REST API,
  *not* the QConnect WebSocket. The favorite/unfavorite steps in
  [`proto/captured/README.md`](proto/captured/README.md) `capture-2`
  produced no QConnect frames; that's why.
- **`ws.proto` is dead code.** It defines an older parallel
  protocol that nothing in `qobuz_connect` imports. Safe to delete
  whenever convenient.
- **Streaming audio.** Re-routes through MA's native `qobuz` music
  provider via `QobuzConnectProvider.get_qobuz_provider()`. If that
  provider isn't configured, `qobuz_connect` raises `InvalidDataError`
  on setup.

## Captured reference data

- [`proto/captured/`](proto/captured/) — original Chrome-extension exports
  (no incoming binary; useful only for outbound frame shapes).
- [`proto/captured/full/`](proto/captured/) — full bidirectional captures
  produced by the harness at
  [`tests/providers/qobuz_connect/protocol_capture/`](../../../tests/providers/qobuz_connect/protocol_capture/).
  Each scenario file is suitable as a Phase B test fixture once auth tokens are stripped.
