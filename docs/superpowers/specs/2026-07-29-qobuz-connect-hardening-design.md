# Qobuz Connect hardening and live-validation design

Date: 2026-07-29

## Objective

Make the experimental `qobuz_connect` provider reliable in both directions:

```text
Qobuz application -> Qobuz cloud -> Music Assistant queue/player
Music Assistant queue/player -> Qobuz cloud -> every connected application
```

The work is complete only when the deterministic test suite passes and the
core user journeys pass against a disposable local Music Assistant instance,
the real Qobuz cloud, and two authenticated Qobuz Web Clients.

The implementation must fail closed around playback. A live test must never
select an arbitrary Music Assistant player or reuse an unrelated active Qobuz
renderer.

## Evidence and current root causes

### Quality drift

The Qobuz application can display `24-bit / 192 kHz` while Music Assistant
reports the actual stream as, for example, `24-bit / 44.1 kHz`.

The Connect provider currently passes its configured maximum-quality tier to
all three wire reports:

- maximum allowed quality;
- current file quality;
- current device/output quality.

For tier `27`, the codec fills the file report with the tier ceiling
`192000 Hz / 24-bit` even when the native Qobuz provider's
`StreamDetails.audio_format` says `44100 Hz / 24-bit`. Music Assistant is
describing the selected file; the application is displaying a fabricated
Connect report.

Reference captures support separating these concepts. A reference web
renderer advertises its maximum tier separately and emits file-quality frames
with the actual sample rate and bit depth when playback starts or the track
changes.

### Queue retry corruption

The saved flight-recorder data contains two real cloud failures:

1. an ADD was rejected because the action UUID was not 16 bytes;
2. the rebased retry then carried no tracks and was rejected as
   `No tracks to add`.

Focused reducer reproductions also show rejected LOAD and REMOVE proposals
being retried with empty payloads. Retry rebasing reconstructs payloads only
for ADD and uses set membership where duplicate occurrences matter.

### Unsafe integration target

The persistent playground configuration and the harness disagree about the
target player:

- the harness expects `b97b9910-b8fe-5ff0-946c-ef06b0d44273`,
  `BlackHole 2ch`;
- the saved provider configuration has contained obsolete player IDs;
- the provider falls back to automatic selection when the configured player
  is missing.

During startup, automatic selection can choose an audible player. A raw
two-client capture also starts playback without first verifying browser-local
output, so it can reuse whichever cloud renderer was already active.

## Invariants

### Renderer ownership

- MA-originated playback claims this receiver as the active Qobuz renderer
  before sending renderer-state reports.
- Renderer-state reports are sent only after cloud ownership is confirmed.
- Duplicate activation and deactivation commands are idempotent.
- Deactivation releases the exact MA player captured during activation; it
  never resolves a new automatic target after state becomes inactive.
- Disconnect clears connection-scoped renderer IDs, ownership, pending
  proposals, and queue-version state.

### Queue identity

- Queue operations are occurrence-aware. Duplicate Qobuz track IDs remain
  distinct through add, remove, reorder, retry, and confirmation.
- A proposal stores enough information to reconstruct its intended wire
  operation after a version conflict.
- LOAD always carries the requested full target.
- REMOVE carries the removed occurrences, not the survivors.
- ADD and INSERT preserve duplicate occurrences.
- Every wire action and queue context requiring a 16-byte UUID gets a fresh
  valid UUID.
- An authoritative empty cloud queue produces an empty Qobuz portion of the
  MA queue while preserving unrelated non-Qobuz items according to existing
  policy.

### Metadata safety

- A permanent Qobuz `MediaNotFoundError` may mark a track unresolvable.
- A transient lookup error never becomes an MA queue deletion and therefore
  never becomes a remote Qobuz removal.
- A partially resolved queue is not committed as an authoritative resync.
- Slow resolution does not block protocol intake indefinitely. Results carry
  a queue generation and are discarded or retried if the canonical target
  changed before application.

### Audio quality

- Maximum quality is a configured ceiling.
- File quality comes from the current queue item's resolved
  `StreamDetails.audio_format`.
- Device/output quality is reported only when MA exposes a trustworthy actual
  output format. Unknown output quality is omitted rather than inferred from
  the maximum tier.
- File-quality reports are emitted when actual stream details first become
  available and whenever the current stream format changes.
- Changing quality from Qobuz persists the Connect ceiling and native Qobuz
  stream quality independently, so failure of one save does not skip the
  other.
- Changing the Connect configuration from MA also synchronizes the selected
  native Qobuz provider.
- A Connect instance explicitly selects its native Qobuz provider instance
  when more than one is configured.
- Automatic quality resolves to the selected native provider's current
  setting; it is never encoded as an invented highest-quality report.

### Autoplay and side channels

- Autoplay continuation items participate in current-item lookup and MA queue
  materialization without being confused with the main queue.
- Autoplay replacement/removal updates the MA continuation without restarting
  an already playing main-queue item.
- Relative volume applies the requested signed delta to the target player.
- Mute commands change the target player's mute state.

## Architecture

### Pure reducer

The reducer remains the owner of canonical Qobuz/MA reconciliation. Changes
to proposal payloads, occurrence matching, ownership gating, idempotency,
autoplay state, and disconnect reset remain pure and receive unit tests before
implementation.

Occurrence-aware helpers consume matching items from left to right. They do
not use sets for ordered queue equality or subtraction.

The reducer emits target-bound effects. Effects that can act on the wrong
player after a state transition, especially release, carry the resolved
player ID captured at the coordinator boundary.

### Coordinator and effect runner

The coordinator continues serializing reducer transitions. Potentially slow
metadata materialization becomes a generation-tagged operation:

1. snapshot the canonical target and generation;
2. resolve metadata outside the reducer transition;
3. apply only if the generation still matches;
4. otherwise discard and schedule resolution for the newer target.

This preserves reducer ordering while allowing websocket intake and ownership
messages to remain responsive.

The effect runner uses explicit bridge methods for relative volume, mute,
queue clearing, and target-bound release.

### Quality reporter

Quality reporting is separated from renderer transport reporting:

- session join/activation/config change: send maximum-quality capability;
- current MA queue item gains stream details: send actual file quality;
- reliable output-format signal: send actual device quality;
- absent stream/output information: send no fabricated current-quality
  message.

The reporter deduplicates identical quality tuples while allowing a new track
with the same format to be reported when required by reference behavior.

### Multi-instance configuration

Each Connect instance stores:

- selected native Qobuz provider instance;
- target MA player;
- publish name;
- HTTP port;
- maximum-quality mode.

New-instance configuration proposes an unused local port. Setup still rejects
an explicit collision with a clear error; it does not silently bind a
different port.

## Fail-closed live harness

The live harness uses an isolated copy of the playground data and a unique
publish name for the test run. It does not attach to an arbitrary pre-existing
MA receiver.

Before any action that can start playback, preflight must prove:

1. the managed MA process is the process listening on the expected API port;
2. the expected Qobuz Connect instance is loaded with no provider error;
3. its configured target equals the BlackHole player ID;
4. BlackHole is available and identifies as the local-audio BlackHole device;
5. the configured audible MacBook/speaker IDs are not the target;
6. the Qobuz device picker contains the unique test receiver name;
7. browser-local output can be selected and the picker confirms it;
8. handoff selects the unique test receiver, never a fuzzy name match.

Failure of any preflight item aborts the scenario. Reset failures are fatal.
Every scenario ends by stopping the managed BlackHole queue and returning the
Qobuz client to verified browser-local output.

Raw two-client reference scenarios that call `play_*` also select and verify
browser-local output first.

## Test-suite hardening

### Deterministic tests

Add red-green regression coverage for:

- LOAD, ADD, INSERT, REMOVE, and duplicate-occurrence retries;
- exact 16-byte action UUIDs and fresh nonzero context UUIDs;
- duplicate reorder/removal/diff classification;
- cloud clear and removal of the final Qobuz item;
- partial transient metadata failure;
- autoplay materialization and current-item reporting;
- inactive MA playback claiming ownership before reporting;
- inactive report suppression;
- duplicate activation/deactivation;
- release of the captured player when automatic selection changes;
- relative volume and mute;
- disconnect reset;
- actual file-quality encoding and report timing;
- independent quality-config persistence failures;
- native Qobuz instance selection;
- generation-discard behavior for slow metadata resolution;
- dynamic multi-instance port defaults and collision errors.

Replay tests treat contained dispatch exceptions as failures by asserting that
no handler error was logged or recorded. Runtime dispatch containment remains
intact.

The seeded soak test gains duplicate-track queues and validates occurrence
counts, proposal payloads, UUID widths, and ownership/report invariants.

### Live tests

The authenticated suite uses two web clients plus the managed MA receiver.
Checks use Qobuz track IDs and queue item IDs rather than title substrings.

Required live scenarios:

- actual file quality for a known track whose real sample rate is below the
  configured maximum;
- quality ceiling change and persistence into the chosen native provider;
- Qobuz-to-MA handoff, pause, seek, skip, volume, and mute;
- MA-to-Qobuz initiation, pause, skip, and queue edits;
- cross-client propagation to the second Qobuz client;
- add, remove-last, clear, reorder, and duplicate-track edits;
- simultaneous client/MA edits that force a queue-version conflict and retry;
- deactivation while another MA player is also playing;
- reconnect and ownership restoration;
- autoplay continuation when the real client exposes a deterministic trigger.

Live results have three states: PASS, FAIL, and SKIP. Missing authentication,
missing BlackHole, or an unavailable cloud capability is SKIP or preflight
failure, never PASS. The `all` command exits unsuccessfully when a required
scenario did not run.

Transient upstream metadata failures cannot be induced safely against the real
service, so their end-to-end behavior is validated with the real coordinator,
effect runner, MA queue fakes, and a deterministic failing metadata provider.

## Existing harness defects included in scope

- `ma_queue_edit` currently checks only MA's local queue length.
- tokenless MA scenarios record a passing "skipped" check.
- MA track initiation accepts any track from the album.
- reset treats failure to select local output as nonfatal.
- app/MA synchronization compares title substrings.
- the live suite uses one Qobuz client despite the two-client capture harness.
- replay tests cannot see exceptions contained by the dispatcher.
- the soak generator excludes duplicates.

Each item is fixed as part of the test-suite hardening above.

## Additional implementation cleanups

- Change the premature `WebSocket connected` log to distinguish task start
  from confirmed connection.
- Keep the default multi-instance port deterministic but choose an unused
  proposal for new instances and reject explicit collisions clearly.
- Preserve controller-session serialization while preventing metadata I/O
  from monopolizing reducer intake.
- Update `ARCHITECTURE.md` wherever ownership, report gating, quality, retry,
  autoplay, disconnect, or effect execution semantics change.

## Delivery and verification

Implementation proceeds in small TDD commits on `qobuz-connect-hardening`.
Each behavior is introduced by a test that fails for the expected reason
before production code changes.

Final verification requires:

1. `pytest tests/providers/qobuz_connect/ --no-cov`;
2. focused branch coverage with no regression from the reviewed baseline;
3. `pre-commit run --all-files`;
4. managed MA boot and fail-closed preflight;
5. all required authenticated live scenarios;
6. inspection of MA logs, flight-recorder state, and both clients' captured
   websocket frames for quality, ownership, queue retries, and errors;
7. a clean diff review confirming generated protobuf files and unrelated
   provider code were not changed.

No claim of complete repair is made if required cloud/MA validation did not
run or if the Qobuz application and MA disagree on track, queue, ownership, or
actual file quality.
