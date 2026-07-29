# Qobuz Connect Provider — Architecture

This document describes the **current** shape of the `qobuz_connect`
provider. You should be able to read this and modify any one module without
spelunking the others. Pair it with [`README.md`](README.md) for the
user-facing rationale (why this provider exists, what it replaces).

The sync layer is a **pure reducer + impure shell**: `reduce(state, event)
-> (state, effects)` (`reducer.py` / `sync_types.py`) owns every MA↔Qobuz
decision without touching I/O, and a thin shell (`coordinator.py` /
`effect_runner.py`) feeds it events and executes the effects it returns. It
replaced the earlier stateful `QobuzConnectSyncEngine` facade + collaborators.

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
    ├─ __init__.py creates a single QobuzConnectSession
    ├─ session.py sends OuterMessageType.AUTHENTICATE (JWT) ┐
    ├─ session.py sends OuterMessageType.SUBSCRIBE (QConnect proto)
    ├─ session.py sends CtrlSrvrJoinSession (see "The single dual-role
    │  connection" below)
    └─ one websocket per instance carries both renderer reports and
       controller verbs

4.  Steady state: bidirectional message loop
    ├─ session.py decodes outer envelopes via QobuzConnectCodec.decode_frame
    ├─ inbound_dispatcher routes each inner message; the codec parses it
    │  straight into a sync_types Event
    ├─ coordinator.submit() injects shell-only context, runs it through
    │  reducer.reduce() under a lock, and awaits the resulting effects on
    │  effect_runner (in order) before the next event
    └─ effect_runner emits state reports back via outbound_reporter /
       session on a 5s heartbeat plus ad-hoc after every command
```

## Module map

The sync layer is a **functional core / imperative shell**. A pure reducer
(`reducer.py` over the value types in `sync_types.py`) makes every decision;
an impure shell (`coordinator.py` + `effect_runner.py`) does all the I/O.
Nothing outside the shell holds mutable sync state.

| File                                                   | Owns                                                                                                                              | MA?       | Proto?         |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------|----------------|
| [`discovery.py`](discovery.py)                         | mDNS service + local HTTP handshake endpoints                                                                                     | ❌         | ❌              |
| [`protocol.py`](protocol.py)                           | Outer-frame codec + protobuf encode/decode                                                                                        | ❌         | ✅              |
| [`session.py`](session.py)                             | WebSocket lifecycle, token refresh, JOIN role, `send_*` verbs, hand-off to dispatcher                                             | ❌         | via proto      |
| [`inbound_dispatcher.py`](inbound_dispatcher.py)       | Routing table: decoded inner message → codec-built `sync_types` Event → `coordinator.submit`                                       | ❌         | via proto      |
| [`models.py`](models.py)                               | Enums + wire value types shared across all of the above (`QueueTrackRef`, `QueueVersion`, quality maps, tokens)                    | ❌         | enum refs only |
| [`sync_types.py`](sync_types.py)                       | **Pure** value types: `CanonicalState`, the `Event` union (cloud + MA + timer), the `Effect` union (`Push*`/`Ma*`/`ReportState`), `Proposal`  | ❌         | ❌              |
| [`reducer.py`](reducer.py)                             | **Pure** `reduce(state, event) -> (state, effects)`: version gate, proposal confirm/reject/timeout, the four lanes               | ❌         | ❌              |
| [`coordinator.py`](coordinator.py)                     | **Shell**: owns the single `CanonicalState`; serializes inbound (cloud callbacks + MA `on_ma_*`) into `Event`s through `reduce` under a lock; runs the proposal-timeout timer | ✅ (via bridge) | via session |
| [`effect_runner.py`](effect_runner.py)                 | **Shell**: turns each `Effect` into a real `session.send_*` call or `ma_bridge` mutation; nothing else does I/O                  | ✅ (via bridge) | via session |
| [`ma_bridge.py`](ma_bridge.py)                         | The one place the sync core touches Music Assistant — provider accessors + `mass.player_queues.*` / `mass.players.*`             | ✅         | ❌              |
| [`metadata_resolver.py`](metadata_resolver.py)         | MA track-metadata lookups + fail-cache (used by `effect_runner` to resolve Qobuz ids → MA `Track`s)                              | via bridge | ❌              |
| [`outbound_reporter.py`](outbound_reporter.py)         | Renderer→cloud emission: `report_state`, heartbeat, wire-anchor logic (driven by the `ReportState` effect + heartbeat) | via host | ❌     |
| [`__init__.py`](__init__.py)                           | `QobuzConnectProvider`: config, lifecycle, MA event subscription, wiring the shell together                                       | ✅         | ❌              |

`discovery`, `protocol`, `inbound_dispatcher`, `models`, `sync_types` and
`reducer` are pure (no MA imports): they could be lifted into a standalone
Qobuz Connect SDK. MA coupling lives behind `ma_bridge.py` and in the two
shell modules (`coordinator` / `effect_runner`) plus `__init__.py`, which
constructs the provider and wires everything together.

`metadata_resolver` and `outbound_reporter` predate the reducer rework and
were built against the retired engine; the provider now hands each a small
duck-typed host (`_MetadataHost` / `_ReporterHost` in `__init__.py`) that
exposes just `bridge` (and, for the reporter, a live `QobuzMirror`
projection of the coordinator's `CanonicalState`) instead of the old engine.

## The single dual-role connection

Everything in this provider speaks to the Qobuz cloud over **one**
websocket per instance — a `QobuzConnectSession` joined in **controller
role** (`CtrlSrvrJoinSession{deviceInfo}`), exactly like the reference
Qobuz Web Client. That single socket both *reports renderer state*
(`RNDR_SRVR_STATE_UPDATED`, volume/quality reports) and *sends controller
verbs* (`CTRL_SRVR_*`), and receives everything the cloud routes to the
device identity. The controller join is the only mode the provider runs —
the legacy renderer-role join (`RndrSrvrJoinSession` + session-uuid
subscribe) was retired.

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
after `AUTHENTICATE` → `SUBSCRIBE`. The protocol has two; MA only ever
sends the controller one:

- `CtrlSrvrJoinSession{deviceInfo}` → controller (subscribe carries no
  channels). `deviceInfo` is mandatory; joining without it is rejected
  with "Error while processing JoinSessionMessage". A controller-joined
  connection still registers a picker entry from its `deviceInfo` and
  receives renderer-directed frames — dual-role, like the web client.
- `RndrSrvrJoinSession{deviceUuid, sessionUuid, ...}` → renderer
  (subscribe carries the session uuid channel). Retired — MA no longer
  builds or sends it.

Verified live against production (`wss://qws-eu-prod.qobuz.com/ws`,
2026-07-07/08) with the Playwright capture harness — see
`tests/providers/qobuz_connect/protocol_capture/.runs/controller_full_lifecycle__client_{a,b}.json`
and `docs/superpowers/specs/2026-07-07-qobuz-connect-controller-design.md`
("Verified protocol facts") for the full write-up.

### Controller verb map

The controller verbs are `Push*` **effects** the reducer emits; the shell
turns them into `session.send_*` calls via `effect_runner`. Every verb goes
out on the shared socket. MA-origin queue edits (`PushLoad` / `PushAdd` /
`PushInsert` / `PushRemove` / `PushReorder` / `PushClear`) and
`PushSetActive` are wired end-to-end today; `PushPlayerState` /
`PushVolume` / `PushMute` have `send_*` support but the reducer has no
callsite emitting them yet:

| Verb                          | Effect            | Wire message                                                        |
|--------------------------------|-------------------|-----------------------------------------------------------------------|
| Pause / resume                 | `PushPlayerState` | `CTRL_SRVR_SET_PLAYER_STATE{playingState}` (no callsite yet)          |
| Seek                           | `PushPlayerState` | `CTRL_SRVR_SET_PLAYER_STATE{currentPosition}` (no callsite yet)       |
| Skip / play specific item      | `PushPlayerState` | `CTRL_SRVR_SET_PLAYER_STATE{playingState, currentPosition: 0, currentQueueItem}` |
| Volume                         | `PushVolume`      | `CTRL_SRVR_SET_VOLUME{rendererId, volume}` (no callsite yet)          |
| Mute                           | `PushMute`        | `CTRL_SRVR_MUTE_VOLUME{rendererId, value}` (no callsite yet)          |
| Become the active renderer     | `PushSetActive`   | `CTRL_SRVR_SET_ACTIVE_RENDERER{rendererId}`                           |
| Queue replacement (MA-origin)  | `PushLoad`        | `CTRL_SRVR_QUEUE_LOAD_TRACKS` — packed little-endian uint32 track ids in the (misnamed) `sessionUuid` field, plus a mandatory fresh 16-byte `contextUuid` and explicitly-present `shufflePivotQueueItemId=0` / `shuffleMode=false`; the active renderer keeps rendering and switches to the new queue |

An MA-origin edit becomes a `Proposal` in `CanonicalState.pending`, and the
matching `Push*` effect carries the proposal's `action_uuid` + `base_version`
so the cloud echo can be correlated (see "The sync core" below). Cloud
pushes are only meaningful while the controller socket is connected; when it
isn't, the effect's `session.send_*` no-ops (the `_LiveSessionProxy` in
`__init__.py` forwards to whichever session is current and safely drops
sends before one exists).

### Own-rendererId discovery

The connection doesn't know its `rendererId` until the cloud tells it. On
join, the cloud bootstraps it with `SRVR_CTRL_ADD_RENDERER` for every online
renderer; the reducer's transport lane matches the entry whose `device_uuid`
equals ours (resolved by the coordinator against its `device_uuid`) and
stores it as `CanonicalState.own_rid`. `PushSetActive` is a no-op until
`own_rid` is known. `SRVR_CTRL_ACTIVE_RENDERER_CHANGED` keeps
`active_rid` current, and the reducer only emits `PushSetActive` when we
aren't already active. On connection loss (`Disconnected` event, from the
session's connection loop) both ids are cleared and re-discovered from the
next bootstrap.

### Eager connect

The connection is opened at provider load, self-minting the websocket
token via `qws/createToken` — NOT on the app's local handshake. Waiting
for the handshake loses the first handoff after a restart: the phone's
SET_ACTIVE races our connect+join and the app bounces playback back when
no renderer answers (live 2026-07-08). For the same reason, a handshake
must never swap tokens on an already-connected session — a token swap
closes and reopens the socket at the exact moment the cloud needs it up.

### Playback takeover on activation

**A controller-joined connection never receives a renderer-directed
`SET_STATE` with track refs** (handoff capture `handoff__client_b.json` +
live 2026-07-08). When the user hands playback to us, the only
renderer-directed message is `SRVR_RNDR_SET_ACTIVE(active=true)` — the
cloud expects the new target to *continue the session by itself* from
controller-side knowledge, exactly like the reference web client (which
starts reporting `RNDR_SRVR_STATE_UPDATED` in the same millisecond it
receives SET_ACTIVE). Sources for that knowledge:

- `SRVR_CTRL_SESSION_STATE.trackIndex` (connect-time) — the queue *read
  pointer*: the index of the NEXT track to pull, i.e. current + 1 (live
  2026-07-08: phone on index 2 → trackIndex 3; equivalently a 1-based
  current index). The reducer resolves `current_id` from `max(0,
  trackIndex - 1)` against the known track list.
- `SRVR_CTRL_QUEUE_STATE` (asked-for snapshot) — the track list.
- `SRVR_CTRL_RENDERER_STATE_UPDATED` (type 82, ~1/s while another
  renderer plays) — live playing state, position, duration, and
  `currentQueueIndex` when the renderer reports one.

The reducer's transport lane handles `CloudSetActive`: it flips
`state.active` and, when it transitions us *into* the active role while the
session was PLAYING, emits an `MaResyncQueue` / `MaPlayTrack` effect so MA
picks up the current track from local knowledge — the same continue-the-
session behavior as the reference web client. A takeover while paused only
records state (`test_reducer_transport.py`). Because those effects flow
through the ordinary transport lane, a later play arriving without track
refs converges the same way. Guards, now expressed as state rather than
side-effect flags:

- Audio is never restarted unless `current_id` actually *changes* (see
  "The sync core" below), so the activation echo of an MA-origin load —
  which leaves `current_id` unchanged — can't resurrect a stale queue.
- Reports are gated on `state.active`: inactive renderers stay silent, and
  a deactivation clears `active` while preserving `tracks`/`current_id`,
  which the cloud never resends unprompted.

### Rejoin-on-error

The cloud can silently deregister a device while its socket stays open;
subsequent state reports are answered with **message-level type-1 errors
inside PAYLOAD batches** (not outer ERROR frames). Both error shapes
funnel into `session._maybe_rejoin_after_error` — a rate-limited re-send
of the role-appropriate `SUBSCRIBE` + JOIN on the same connection rather
than a full reconnect. Queued outbound frames older than ~2s are dropped
on reconnect instead of flushed: the cloud rejects stale envelope
timestamps ("Message too old") and the rejection can drop the connection.

**Not every type-1 error means deregistration.** The cloud also rejects
individual *reports* on semantic grounds — "Current track not found in
queue nor autoplay" (stale current anchor after a queue clear) and
"Renderer state updated message received from non active renderer"
(reporting while not the target). Rejoining on those just churns the
session (live 2026-07-08 evening cascade); they're excluded via
`REPORT_SEMANTIC_ERRORS`. Related invariants that prevent the errors at
the source:

- Our own `QUEUE_CLEARED` echo clears `current_id` (report gate), and
  a queue snapshot prunes a `current_id` that's no longer in it.
- Losing the websocket drops `state.active` (a fresh connection is never
  the active renderer) and the heartbeat only reports while active.
- MA's `play_media` replaces a queue via a transient clear-then-load, so
  an empty MA queue only propagates to the cloud as `CLEAR_QUEUE` after
  a `CLEAR_EMIT_GRACE` debounce, and only if it's still empty then —
  emitting immediately wiped the phone's queue mid-replace.

## Inbound messages (Qobuz → this provider)

All inbound traffic is one of these QConnect inner message types. The codec
(`protocol.py`) parses each frame straight into a `sync_types` `Event`; the
[`inbound_dispatcher.py`](inbound_dispatcher.py) routes it to
`coordinator.submit()`, which injects the little shell-only context an event
can't carry (own-renderer verdict, snapshot track pointer, error version
fallback) before reducing it in [`reducer.py`](reducer.py). SET_ACTIVE and the
quality tap stay provider hooks (they broadcast/persist before submitting).
The reducer routes each event to one of four
lanes: **list** (queue snapshot/add/insert/remove/reorder/clear + load
acks/version), **transport/session** (set-state, set-active, session-state,
renderer registry), **modes** (loop/shuffle/autoplay), and **side-channels**
(volume/mute/quality). See "The sync core" below.

| Type ID | Message                              | Decoder (`protocol.py`)       | Event (`sync_types.py`)              | Lane      |
|--------:|--------------------------------------|-------------------------------|--------------------------------------|-----------|
|      41 | `SRVR_RNDR_SET_STATE`                | `parse_set_state`             | `CloudSetState`                      | transport |
|      42 | `SRVR_RNDR_SET_VOLUME`               | `parse_set_volume`            | `CloudVolume` / `CloudVolumeDelta`   | side      |
|      43 | `SRVR_RNDR_SET_ACTIVE`               | (inline bool → `on_set_active`)| `CloudSetActive`                    | transport |
|      44 | `SRVR_RNDR_SET_MAX_AUDIO_QUALITY`    | (inline int → `on_quality`)   | `_on_quality_change` in `__init__.py`| (provider)|
|      77 | `CTRL_SRVR_ASK_FOR_RENDERER_STATE`   | `parse_state_request`         | `CloudStateRequest` → `ReportState`  | transport |
|      88 | `SRVR_CTRL_QUEUE_ERROR_MESSAGE`      | `parse_queue_error`           | `CloudQueueError` (proposal reject)  | list      |
|      91 | `SRVR_CTRL_QUEUE_TRACKS_LOADED`      | `parse_queue_load_ack`        | `CloudLoadAck` (proposal confirm)    | list      |
|     103 | `SRVR_CTRL_AUTOPLAY_TRACKS_LOADED`   | `parse_autoplay_load_ack`     | `CloudAutoplayTracksLoaded`          | list      |
|     105 | `SRVR_CTRL_QUEUE_VERSION_CHANGED`    | `parse_queue_version_changed` | `CloudVersionChanged`                | list      |

### Inbound message catalog

Validated against the full bidirectional captures under
[`tests/providers/qobuz_connect/protocol_capture/.runs/`](../../../tests/providers/qobuz_connect/protocol_capture/.runs/).
The queue-mutation and mode messages below are now consumed by the reducer's
list and modes lanes; the tables catalog each message and the symptom it
prevents. Tier 2 messages are proto-defined and emitted on specific UI paths
(insert/remove/reorder, clear from another client).

**Tier 1 — directly observed in captures:**

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
`SRVR_CTRL_ACTIVE_RENDERER_CHANGED` become `CloudAddRenderer` /
`CloudRemoveRenderer` / `CloudActiveRendererChanged` events, and the
reducer's transport lane uses them to learn `own_rid` and track the
session's `active_rid` (see "Own-rendererId discovery" above). Same message
types, different meaning depending on which socket they arrive on.

**Known parse anomaly:** one 82-byte PAYLOAD frame in
`queue_mutations__client_b.json` (frame index 30) fails to parse as a
`QConnectBatch`. Possibly a partial transmission or non-batch control
message. Possibly a partial transmission or non-batch control message; for
now the dispatcher logs and continues rather than crashing.

Bytes from the captures serve as ground-truth fixtures for decoder
round-trip tests.

## Outbound messages (this provider → Qobuz)

All outbound traffic is constructed in [`protocol.py`](protocol.py) and
sent via [`session.py`](session.py)'s `send_*` verbs. Steady-state sends are
`Effect`s the reducer returns, executed by
[`effect_runner.py`](effect_runner.py); connection-level frames
(AUTHENTICATE / SUBSCRIBE / JOIN, quality reports) are still driven directly
by `session.py` / the provider.

| Type ID | Message                                  | Encoder (`protocol.py`)                  | Triggered from                                                                            |
|--------:|------------------------------------------|------------------------------------------|-------------------------------------------------------------------------------------------|
|       1 | `AUTHENTICATE` (outer envelope)          | `encode_authenticate`                    | `session.start()` after tokens arrive (both roles)                                        |
|       2 | `SUBSCRIBE` (outer envelope)             | `encode_subscribe`                       | `session.start()` after AUTHENTICATE (renderer: session-uuid channel; controller: empty)  |
|      23 | `RNDR_SRVR_STATE_UPDATED`                | `encode_renderer_state`                  | `ReportState` effect / `outbound_reporter` heartbeat (5s + after every command, 1s during buffering) |
|      25 | `RNDR_SRVR_VOLUME_CHANGED`               | `encode_volume_changed`                  | `PushVolume` effect; `_broadcast_current_volume` on connect/activate                       |
|      26 | `RNDR_SRVR_FILE_AUDIO_QUALITY_CHANGED`   | `encode_file_audio_quality_changed`      | `QualityReporter` after MA resolves current stream details                                  |
|      27 | `RNDR_SRVR_DEVICE_AUDIO_QUALITY_CHANGED` | `encode_device_audio_quality_changed`    | omitted until the renderer output format is known                                           |
|      28 | `RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED`    | `encode_max_audio_quality_changed`       | `QualityReporter` after connect / quality change                                            |
|      61 | `CTRL_SRVR_JOIN_SESSION`                 | `encode_ctrl_join_session`               | `session.start()` in controller role, joining with the device deviceUuid                   |
|      62 | `CTRL_SRVR_SET_PLAYER_STATE` (partial)   | `encode_ctrl_set_player_state`           | `PushPlayerState` effect via `session.send_ctrl_player_state` (no reducer callsite yet)    |
|      63 | `CTRL_SRVR_SET_ACTIVE_RENDERER`          | `encode_set_active_renderer`             | `PushSetActive` effect before an MA-origin load if we aren't the active renderer            |
|      64 | `CTRL_SRVR_SET_VOLUME`                   | `encode_ctrl_set_volume`                 | `PushVolume` effect (no reducer callsite yet)                                              |
|      66 | `CTRL_SRVR_QUEUE_LOAD_TRACKS`            | `encode_queue_load_tracks`               | `PushLoad` effect via `session.send_queue_load_tracks`; MA-origin loads are skipped (one warning per outage) while the controller socket is down |
|      73 | `CTRL_SRVR_MUTE_VOLUME`                  | `encode_ctrl_mute_volume`                | `PushMute` effect (no reducer callsite yet)                                                |
|       — | `CTRL_SRVR_QUEUE_ADD/INSERT/REMOVE/REORDER/CLEAR_TRACKS` | (per-verb encoders)       | `PushAdd` / `PushInsert` / `PushRemove` / `PushReorder` / `PushClear` effects              |

## The sync core

The part that fuses two systems that don't speak each other's language is a
**pure reducer + impure shell**. All MA↔Qobuz decisions live in one pure
function; everything with a side effect lives in a thin shell around it.

```
inbound (cloud callbacks + MA on_ma_*)
        │
        ▼
  coordinator.py  ── translate → Event ──▶ reduce(state, event) ──▶ (state', effects)
   (owns the one       serialize under          reducer.py            │
    CanonicalState)    an asyncio.Lock          (pure)                ▼
        ▲                                                       effect_runner.py
        └──────────────── stores state' ──────────────────────  runs each Effect
                                                                 (session.send_* / bridge)
```

### The pure reducer

`reduce(state, event) -> ReduceResult(state, effects)` in
[`reducer.py`](reducer.py) is pure: no I/O, no MA imports, no websocket. It
takes the current `CanonicalState` plus one `Event` and returns the next
state and a tuple of `Effect`s to run. It routes each event to one of four
lanes:

- **list** — queue snapshot / add / insert / remove / reorder / clear, plus
  load acks and version bumps. Owns `tracks` and `cloud_version`.
- **transport / session** — `CloudSetState`, `CloudSetActive`,
  `CloudSessionState`, renderer registry (`own_rid` / `active_rid`), and the
  MA-origin transport edits. Owns `current_id`, `playing`, `position_ms`,
  `active`.
- **modes** — loop / shuffle / autoplay flags.
- **side-channels** — volume / mute / quality; ungated fire-and-forget, since
  `CanonicalState` carries no volume/mute/quality fields.

### Canonical state

```python
@dataclass(slots=True)
class CanonicalState:
    cloud_version: QueueVersion       # the logical clock (major, minor)
    tracks: tuple[QueueTrackRef, ...]
    autoplay_tracks: tuple[QueueTrackRef, ...]
    current_id: int | None            # Qobuz track id of the current track
    playing: PlayingState             # STOPPED / PLAYING / PAUSED
    position_ms: int
    position_anchor_ms: int           # interpolation anchor
    loop: LoopMode
    autoplay: bool
    active: bool                      # are we the active renderer?
    own_rid: int | None               # our rendererId, learned from the cloud
    active_rid: int | None            # the session's active renderer
    pending: tuple[Proposal, ...]     # optimistic MA-origin edits awaiting echo
```

`CanonicalState` is the single source of truth: the one thing MA and Qobuz
agree on. The reducer never mutates it in place — it returns copies via
`dataclasses.replace()`. The coordinator holds the only live instance. The
provider projects a legacy `QobuzMirror` view from it for
`outbound_reporter`'s heartbeat.

### Cloud queue_version as the logical clock

The cloud's `queue_version` `(major, minor)` is the authoritative clock.
Every inbound event carrying a `version` that is `<=` the current
`cloud_version` is stale and no-ops (the version gate at the top of
`reduce`) — the one exception is `CloudQueueError`, a control event that must
always reach its proposal. This makes reordered or duplicate cloud frames
harmless.

### Optimistic proposals (emit / confirm / reject-rebase / timeout)

An MA-origin queue edit doesn't wait for a round-trip. The list lane diffs
the new MA queue against `tracks`, mints a `Proposal` (an `action_uuid`,
the `base_version` it was computed against, the mutation kind, and the
target Qobuz track ids), appends it to `state.pending`, and emits the
matching `Push*` effect to send it to the cloud. Then one of:

- **confirm** — the cloud echoes the edit (`CloudLoadAck` /
  `CloudTracksAdded` / …) carrying the same `action_uuid`; the proposal is
  dropped and the echo's new `cloud_version` becomes canonical.
- **reject → rebase** — the cloud rejects it (`CloudQueueError` with the
  `action_uuid`); the proposal is dropped and the reducer converges MA back
  to canonical truth (`MaResyncQueue`).
- **timeout** — no echo arrives in time; the coordinator's proposal-timeout
  timer fires a `ProposalTimeout`, the reducer drops the stale proposal and
  resyncs. This is the lost-echo safety net.

The `base_version` on each proposal is why concurrent edits stay correct:
the translation runs against the *pre-append* state, so an append comes out
as only the newly added tail rather than the whole list.

### Audio is never interrupted unless the track changes

The hard invariant: **only a `current_id` transition ever restarts audio.**
List-lane edits (add/insert/remove/reorder), version bumps, active-flag
flips, mode changes and position-only heartbeats all update state without
emitting a play/resync effect. `MaResyncQueue` / `MaPlayTrack` are emitted
only when `current_id` actually changes (or on a takeover into the active
role while playing). That's what lets the Qobuz app rearrange the queue, or
echo our own load, without the audio hiccuping.

### Track identity

A track's identity is its **Qobuz track id** (`current_id`, the diff match
key, the `Push*` payloads). The cloud slot id (`queue_item_id`) is only
meaningful on the wire: it arrives on cloud queue deltas and is required by
the remove / reorder verbs (which address existing slots), so the effect
runner passes those straight through, while add / insert / load verbs send
`queue_item_id=0` and let the cloud assign + echo the real slot.

### The impure shell

- [`coordinator.py`](coordinator.py) owns the single `CanonicalState`. Its
  `submit()` is the intake for codec-built cloud `Event`s (injecting shell-only
  context first), and its `on_ma_*` entry points build `Event`s for MA's event
  bus. Every event is fed through `reduce` under an `asyncio.Lock` and *fully*
  applied — state
  stored, every effect awaited in order — before the next event starts. That
  linear ordering is what makes the version-gated decisions correct: nothing
  ever observes a half-applied state. It also runs the proposal-timeout timer
  (schedule a `ProposalTimeout` per pending proposal, cancel it once the
  proposal leaves `pending`). Serializing under the lock is also what
  replaces the old engine's explicit `origin` echo-suppression flag — an
  MA-origin edit and its cloud echo can't interleave.
- [`effect_runner.py`](effect_runner.py) is the only place `Effect`s become
  real `session.send_*` calls or `ma_bridge` mutations. Each branch is a
  small `await`, so a new effect or session verb touches exactly one branch.
  `Ma*` effects resolve Qobuz ids to MA `Track`s via
  [`metadata_resolver.py`](metadata_resolver.py) and drive the player queue
  through [`ma_bridge.py`](ma_bridge.py).

## Glossary

- **Controller** — the role of a Qobuz client that *sends* commands to a
  renderer (e.g. the Qobuz mobile app driving MA).
- **Renderer** — the role of a Qobuz client that *plays* audio in
  response to controller commands (e.g. MA's `qobuz_connect`).
- **Queue version** — `(major, minor)` integer pair the cloud uses to
  arbitrate concurrent edits to the play queue, and the reducer's logical
  clock. Every load / insert / remove / reorder bumps `minor`; some
  operations bump `major`. Inbound events at or below the current version
  are stale and no-op.
- **action_uuid** — a 16-byte UUID minted for a `Proposal` when a `Push*`
  effect issues an MA-origin edit; the cloud echoes it in the corresponding
  ack (`SRVR_CTRL_QUEUE_TRACKS_LOADED`, etc.) so the reducer can correlate
  the echo with the proposal and confirm it.
- **Proposal** — an optimistic MA-origin queue edit held in
  `CanonicalState.pending` until the cloud confirms (echo), rejects
  (`CloudQueueError`), or the coordinator times it out.
- **Effect** — a value the pure reducer returns describing I/O to perform
  (`Push*` cloud verbs, `Ma*` player-queue operations, `ReportState`); the
  `effect_runner` is what actually performs it.

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
  Each scenario file is suitable as a decoder test fixture once auth tokens are stripped.
