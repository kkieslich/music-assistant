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

3.  Provider opens the cloud WebSocket(s)
    ├─ on_connect callback in __init__.py creates the renderer
    │  QobuzConnectSession
    ├─ session.py sends OuterMessageType.AUTHENTICATE (JWT) ┐
    ├─ session.py sends OuterMessageType.SUBSCRIBE (QConnect proto)
    ├─ session.py sends RNDR_SRVR_JOIN_SESSION (renderer joins)
    └─ if `enable_controller` is on, controller.py opens a second,
       persistent session in CONTROLLER role (see "Controller connection
       (see "The single dual-role connection" below) — same JWT type, different JOIN
       message

4.  Steady state: bidirectional message loop
    ├─ session.py decodes outer envelopes via QobuzConnectCodec.decode_frame
    ├─ batched inner messages parsed → typed events fired via callbacks
    ├─ sync.py's handlers reconcile each event against QobuzMirror + MA
    └─ sync.py emits state reports back via session.send_renderer_state
       on a 5s heartbeat plus ad-hoc after every command
```

## Module map

The Phase C redesign split what used to be a single 1.1k-LOC `sync.py`
into a facade + six focused collaborators. Each collaborator owns one
concern and is reachable from the engine via a single attribute
(`engine.bridge`, `engine.reporter`, `engine.seek_pipeline`,
`engine.metadata`, `engine.queue_loader`, `engine.command_handler`).

| File                                                   | Owns                                                                                                                              | MA?       | Proto?         |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|----------------|
| [`discovery.py`](discovery.py)                         | mDNS service + local HTTP handshake endpoints                                                                                     | ❌         | ❌              |
| [`protocol.py`](protocol.py)                           | Outer-frame codec + protobuf encode/decode                                                                                        | ❌         | ✅              |
| [`session.py`](session.py)                             | WebSocket lifecycle, token refresh, hand-off to dispatcher                                                                        | ❌         | via proto      |
| [`inbound_dispatcher.py`](inbound_dispatcher.py)       | Routing table: decoded inner message → typed callback                                                                             | ❌         | via proto      |
| [`models.py`](models.py)                               | DTOs + enums shared across all of the above                                                                                       | ❌         | enum refs only |
| [`state.py`](state.py)                                 | Ephemeral pending-action dataclasses (`PausedSeek`, `PendingPlayingSeek`, `PendingQobuzPosition`, `TrackRefKey`, `origin_scope`)   | ❌         | ❌              |
| [`controller.py`](controller.py)                       | Renderer-registry tracking + the verb API over the shared single socket (`load_queue`, `play_item`, `seek`, `set_playing`, `set_volume`, `set_mute`, `activate_self`); the verb API is used by `queue_loader.py` (only). No MA imports.  | ❌         | via session   |
| [`ma_bridge.py`](ma_bridge.py)                         | The one place sync.py touches Music Assistant — provider accessors + `mass.player_queues.*` / `mass.players.*`                    | ✅         | ❌              |
| [`outbound_reporter.py`](outbound_reporter.py)         | Renderer→cloud emission: `report_state`, heartbeat, buffering reporter, wire-anchor logic                                         | via engine | ❌              |
| [`seek_pipeline.py`](seek_pipeline.py)                 | Paused-seek storage, playing-seek debouncing, Qobuz-position confirmation                                                         | via engine | ❌              |
| [`metadata_resolver.py`](metadata_resolver.py)         | MA track-metadata lookups + fail-cache                                                                                            | via engine | ❌              |
| [`queue_loader.py`](queue_loader.py)                   | MA→Qobuz queue-load round-trip (action_uuid futures, context UUID, ack/timeout)                                                   | via engine | indirect       |
| [`command_handler.py`](command_handler.py)             | `SRVR_RNDR_SET_STATE` reconciliation: mirror update, reconcile task, per-playing-state branches, MA-queue replacement, prequeue   | via engine | indirect       |
| [`sync.py`](sync.py)                                   | **Facade** holding QobuzMirror + cross-cutting helpers + Phase B mirror handlers; wires up the six collaborators above            | ✅ (via bridge) | indirect    |
| [`__init__.py`](__init__.py)                           | `QobuzConnectProvider`: config, lifecycle, MA event subscription, wiring                                                          | ✅         | ❌              |

The first six modules in the table (discovery → models → state →
inbound_dispatcher) are pure: they could be lifted into a standalone
Qobuz Connect SDK without MA. MA coupling lives behind `ma_bridge.py`
(and in `__init__.py` which constructs the provider). After Phase C
every MA access in the sync engine + its collaborators goes through
`self.bridge`.

### sync.py size, before vs. after Phase C

| Stage                                  | sync.py LOC |
|----------------------------------------|------------:|
| Pre-Phase-C                            |       1,255 |
| Stage 6 (outbound_reporter.py)         |       1,145 |
| Stage 7 (seek_pipeline.py)             |       1,002 |
| Stage 8 (metadata_resolver.py)         |         970 |
| Stage 9 (queue_loader.py + dead-code)  |         861 |
| Stage 10 (command_handler.py)          |         461 |

## The single dual-role connection

Everything in this provider speaks to the Qobuz cloud over **one**
websocket per instance — a `QobuzConnectSession` joined in **controller
role** (`CtrlSrvrJoinSession{deviceInfo}`), exactly like the reference
Qobuz Web Client. That single socket both *reports renderer state*
(`RNDR_SRVR_STATE_UPDATED`, volume/quality reports) and *sends controller
verbs* (`CTRL_SRVR_*`), and receives everything the cloud routes to the
device identity. The `enable_controller` config option (on by default)
falls the join back to the legacy renderer role
(`RndrSrvrJoinSession` + session-uuid subscribe), which restores the
pre-controller receive-only behavior.

### Why one connection (history)

The first controller design ran a *second*, persistent controller-role
socket next to the renderer socket, merged into one identity via a shared
deviceUuid. Live testing (2026-07-08) killed it: the cloud has **one
endpoint per device identity** and routes renderer-directed *unicast*
frames (`SRVR_RNDR_SET_STATE`, `SET_ACTIVE`, `SET_VOLUME`, quality/mode
commands, state requests) to the **most recently joined** same-uuid
connection — not to both. Two sockets under one identity therefore
produced routing steals (phone commands swallowed by the other socket),
cross-socket ordering races (one socket clearing the cloud queue while
the other reported the old track), and teardown hazards (either socket's
disconnect deregistered the device). One socket makes all of those
unrepresentable — reports and verbs are serialized on a single
connection, matching the reference client.

### Role is declared by the JOIN message, not the JWT

`POST qws/createToken` mints the same generic JWT for every connection
regardless of role; its claims are just `{quid, qaid}`. What makes a
connection a renderer or a controller is which JOIN message it sends
after `AUTHENTICATE` → `SUBSCRIBE`:

- `RndrSrvrJoinSession{deviceUuid, sessionUuid, ...}` → renderer
  (subscribe carries the session uuid channel).
- `CtrlSrvrJoinSession{deviceInfo}` → controller (subscribe carries no
  channels). `deviceInfo` is mandatory; joining without it is rejected
  with "Error while processing JoinSessionMessage". A controller-joined
  connection still registers a picker entry from its `deviceInfo` and
  receives renderer-directed frames — dual-role, like the web client.

Verified live against production (`wss://qws-eu-prod.qobuz.com/ws`,
2026-07-07/08) with the Playwright capture harness — see
`tests/providers/qobuz_connect/protocol_capture/.runs/controller_full_lifecycle__client_{a,b}.json`
and `docs/superpowers/specs/2026-07-07-qobuz-connect-controller-design.md`
("Verified protocol facts") for the full write-up.

### Controller verb map

`controller.py` holds the renderer-registry state and exposes the verb
API; every verb goes out on the shared socket. Only `queue_loader.py`
calls into it today (`load_queue`, `play_item`, `activate_self`) —
`set_playing`, `seek`, `set_volume` and `set_mute` are implemented but
have no production callsite yet:

| Verb                          | `controller.py` method | Wire message                                                        |
|--------------------------------|-------------------------|-----------------------------------------------------------------------|
| Pause / resume                 | `set_playing`           | `CTRL_SRVR_SET_PLAYER_STATE{playingState}` (no callsite yet)          |
| Seek                           | `seek`                  | `CTRL_SRVR_SET_PLAYER_STATE{currentPosition}` (position only) (no callsite yet) |
| Skip / play specific item      | `play_item`             | `CTRL_SRVR_SET_PLAYER_STATE{playingState, currentPosition: 0, currentQueueItem}` |
| Volume                         | `set_volume`            | `CTRL_SRVR_SET_VOLUME{rendererId, volume}` (no callsite yet)          |
| Mute                           | `set_mute`              | `CTRL_SRVR_MUTE_VOLUME{rendererId, value}` (no callsite yet)          |
| Become the active renderer     | `activate_self`         | `CTRL_SRVR_SET_ACTIVE_RENDERER{rendererId}`                           |
| Queue replacement (MA-origin)  | `load_queue`            | `CTRL_SRVR_QUEUE_LOAD_TRACKS` — packed little-endian uint32 track ids in the (misnamed) `sessionUuid` field, plus a mandatory fresh 16-byte `contextUuid` and explicitly-present `shufflePivotQueueItemId=0` / `shuffleMode=false`; the active renderer keeps rendering and switches to the new queue |

`load_queue` is the only queue-load verb `queue_loader.py` calls directly
(`_send_controller_load`); it falls back to the legacy
`_send_legacy_qweb_load` only when the controller is disabled via the
`enable_controller` config toggle. When the controller is enabled but not
currently connected, MA-origin loads are skipped (not mirrored to the
Qobuz app) rather than falling back, with one warning per outage.

### Own-rendererId discovery

The connection doesn't know its `rendererId` until the cloud tells it.
On join, the cloud bootstraps it with `SRVR_CTRL_ADD_RENDERER` for every
online renderer; `controller.py` matches the entry whose `device_uuid`
equals ours to learn `own_renderer_id`. Every verb method checks it and
is a no-op until it's known. `SRVR_CTRL_ACTIVE_RENDERER_CHANGED` keeps
`active_renderer_id` current; `activate_self()` only sends when we
aren't already active. On connection loss (`on_disconnected` callback,
fired from the session's connection loop) both ids are cleared and are
re-discovered from the next bootstrap.

### Playback takeover on activation

**A controller-joined connection never receives a renderer-directed
`SET_STATE` with track refs** (handoff capture `handoff__client_b.json` +
live 2026-07-08). When the user hands playback to us, the only
renderer-directed message is `SRVR_RNDR_SET_ACTIVE(active=true)` — the
cloud expects the new target to *continue the session by itself* from
controller-side knowledge, exactly like the reference web client (which
starts reporting `RNDR_SRVR_STATE_UPDATED` in the same millisecond it
receives SET_ACTIVE). Sources for that knowledge:

- `SRVR_CTRL_SESSION_STATE.trackIndex` (connect-time) — which queue index
  is current.
- `SRVR_CTRL_QUEUE_STATE` (asked-for snapshot) — the track list.
- `SRVR_CTRL_RENDERER_STATE_UPDATED` (type 82, ~1/s while another
  renderer plays) — live playing state, position, duration, and
  `currentQueueIndex` when the renderer reports one.

`sync.takeover_playback()` (called from `_on_set_active`) derives
`current_item = tracks[track_index]`, and if the previous renderer was
PLAYING, synthesizes the rich `SET_STATE` the renderer role used to
receive and feeds it through the normal command pipeline. Play commands
arriving later without track refs (`_handle_qobuz_play`) fall back to the
same derivation. Two guards:

- The queue loader calls `suppress_takeover_once()` before its own
  `activate_self()` — the activation echo of an MA-origin load must not
  resurrect the stale mirror queue.
- `current_item` stays `None` while we are not the target (it gates the
  heartbeat reporter — inactive renderers must stay silent, and
  deactivation clears it while preserving `tracks`/`track_index`, which
  the cloud never resends unprompted).

### Rejoin-on-error

The cloud can silently deregister a device while its socket stays open;
subsequent state reports are answered with **message-level type-1 errors
inside PAYLOAD batches** (not outer ERROR frames). Both error shapes
funnel into `session._maybe_rejoin_after_error` — a rate-limited re-send
of the role-appropriate `SUBSCRIBE` + JOIN on the same connection rather
than a full reconnect. Queued outbound frames older than ~2s are dropped
on reconnect instead of flushed: the cloud rejects stale envelope
timestamps ("Message too old") and the rejection can drop the connection.

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

A class of `SRVR_CTRL_*` broadcasts shows up on the **renderer** WebSocket
because of how Qobuz routes subscriptions: messages about *other*
renderers' state-changes get sent to us too. On the renderer connection
they require no MA action — the right behavior is to log them at debug
and move on:

`SRVR_CTRL_SESSION_STATE` (81), `SRVR_CTRL_RENDERER_STATE_UPDATED` (82),
`SRVR_CTRL_ADD_RENDERER` (83), `SRVR_CTRL_UPDATE_RENDERER` (84),
`SRVR_CTRL_REMOVE_RENDERER` (85), `SRVR_CTRL_ACTIVE_RENDERER_CHANGED`
(86), `SRVR_CTRL_VOLUME_CHANGED` (87), `SRVR_CTRL_VOLUME_MUTED` (98),
`SRVR_CTRL_MAX_AUDIO_QUALITY_CHANGED` (99),
`SRVR_CTRL_FILE_AUDIO_QUALITY_CHANGED` (100),
`SRVR_CTRL_LOOP_MODE_SET` (97).

On the **controller** connection three of these are load-bearing rather
than benign — `SRVR_CTRL_ADD_RENDERER`, `SRVR_CTRL_REMOVE_RENDERER` and
`SRVR_CTRL_ACTIVE_RENDERER_CHANGED` are how `controller.py` discovers our
own `rendererId` and tracks the session's active renderer (see
"The single dual-role connection" below). Same message types, different
meaning depending on which socket they arrive on.

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
[`sync.py`](sync.py) / [`queue_loader.py`](queue_loader.py) (renderer
connection) and [`controller.py`](controller.py) (controller connection —
see "The single dual-role connection" below).

| Type ID | Message                                  | Encoder (`protocol.py`)                  | Triggered from                                                                            |
|--------:|------------------------------------------|------------------------------------------|-------------------------------------------------------------------------------------------|
|       1 | `AUTHENTICATE` (outer envelope)          | `encode_authenticate`                    | `session.start()` after tokens arrive (both roles)                                        |
|       2 | `SUBSCRIBE` (outer envelope)             | `encode_subscribe`                       | `session.start()` after AUTHENTICATE (renderer: session-uuid channel; controller: empty)  |
|      23 | `RNDR_SRVR_STATE_UPDATED`                | `encode_renderer_state`                  | `sync.report_state()` on 5s heartbeat + after every command, plus 1s during buffering     |
|      25 | `RNDR_SRVR_VOLUME_CHANGED`               | `encode_volume_changed`                  | `_broadcast_current_volume` (on connect/activate) and `sync.set_volume`                   |
|      26 | `RNDR_SRVR_FILE_AUDIO_QUALITY_CHANGED`   | `encode_file_audio_quality_changed`      | `session.send_quality_reports` after connect / quality change                              |
|      27 | `RNDR_SRVR_DEVICE_AUDIO_QUALITY_CHANGED` | `encode_device_audio_quality_changed`    | same                                                                                       |
|      28 | `RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED`    | `encode_max_audio_quality_changed`       | same                                                                                       |
|      61 | `CTRL_SRVR_JOIN_SESSION`                 | `encode_ctrl_join_session`               | `controller.start()`, joining with the renderer's deviceUuid (shared-identity merge)      |
|      62 | `CTRL_SRVR_SET_PLAYER_STATE` (full)      | `encode_player_state`                    | (method exists, no callsite — dead for now)                                                |
|      62 | `CTRL_SRVR_SET_PLAYER_STATE` (partial)   | `encode_ctrl_set_player_state`           | `session.send_ctrl_player_state`, via `controller.play_item` (queue_loader.py, after MA-origin load ack); `set_playing`/`seek` (no callsite yet — verb available on QobuzConnectController) |
|      63 | `CTRL_SRVR_SET_ACTIVE_RENDERER`          | `encode_set_active_renderer`             | `controller.activate_self` before an MA-origin load if we aren't the active renderer      |
|      64 | `CTRL_SRVR_SET_VOLUME`                   | `encode_ctrl_set_volume`                 | `controller.set_volume` (no callsite yet — verb available on QobuzConnectController)       |
|      66 | `CTRL_SRVR_QUEUE_LOAD_TRACKS`            | `encode_queue_load_tracks`               | `queue_loader._send_controller_load` (controller connected) via `controller.load_queue`, else falls back to `queue_loader._send_legacy_qweb_load` (renderer socket, `enable_controller` off via config) |
|      73 | `CTRL_SRVR_MUTE_VOLUME`                  | `encode_ctrl_mute_volume`                | `controller.set_mute` (no callsite yet — verb available on QobuzConnectController)         |
|      79 | `CTRL_SRVR_AUTOPLAY_LOAD_TRACKS`         | `encode_autoplay_load_tracks`            | (method exists, no callsite — dead for now)                                                |
|       — | `RNDR_SRVR_JOIN_SESSION`                 | `encode_join_session`                    | `session.start()` after SUBSCRIBE (renderer role only)                                    |

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
- **Position confirmation** (`qobuz_position.target_ms`, `issued_ms`,
  `timestamp_ms`): hold Qobuz state frozen until MA's reported position
  confirms a seek landed. `issued_ms` is what we last asked MA to seek to
  via `bridge.seek` / `bridge.play_index` (or `None` when only the
  debounce path has reserved a target); `target_ms` is the latest target
  Qobuz wants. Rapid Qobuz seeks while MA is still buffering only update
  `target_ms`; when MA confirms `issued_ms`, the seek pipeline either
  clears the pending (if target unchanged) or issues a fresh MA seek for
  the deferred `target_ms` — never stacking expensive
  AirPlay-restart-level MA operations.
- **Queue-load acknowledgement** (`_pending_queue_loads: dict[bytes,
  asyncio.Future]`): map `action_uuid` to a future that resolves when
  Qobuz acks the load (or times out at 3s).
- **Prequeue** (`_prequeued_qobuz_items`, `_last_prequeued_next_ref`):
  optimistically place Qobuz's announced next_item into MA's queue ahead
  of time.

Phase C of the plan consolidates these into typed dataclasses in a new
`state.py`.

### Generation tracking

`_command_generation` ticks up every time a *play-state-changing*
command (a play/pause/stop, or a track change) arrives. Position-only
seek events deliberately do *not* bump the counter, and they run in a
separate task slot (`_position_only_task`) rather than through
`_schedule_reconcile`. The original conflation caused a position-only
event arriving mid-track-replace to cancel the in-flight reconcile
between its `stop_queue` and `play_index`, leaving the renderer stopped
(observed on a Pi, May 2026). Every handler still checks
`_is_current_command(gen)` before issuing MA operations so a stale
command doesn't undo a newer one.

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
| `_position_only_task`         | latest-only            | Apply position-only seeks without cancelling reconcile   |
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
  *not* the QConnect WebSocket — the favorite/unfavorite steps in early
  reverse-engineering captures produced no QConnect frames.
- **`ws.proto` is dead code.** It defines an older parallel
  protocol that nothing in `qobuz_connect` imports. Safe to delete
  whenever convenient.
- **Streaming audio.** Re-routes through MA's native `qobuz` music
  provider via `QobuzConnectProvider.get_qobuz_provider()`. If that
  provider isn't configured, `qobuz_connect` raises `InvalidDataError`
  on setup.

## Captured reference data

- [`tests/providers/qobuz_connect/protocol_capture/`](../../../tests/providers/qobuz_connect/protocol_capture/) — **the** source of reference data. A Playwright harness that drives two real Qobuz Web Clients via CDP, recording both directions of the WebSocket into `.runs/`. Add or extend a scenario whenever a protocol question can't be answered from existing captures. Scenarios can opt in to throttled-network conditions for "slow renderer" / "lossy link" tests.
  Each scenario file is suitable as a Phase B test fixture once auth tokens are stripped.
