# Qobuz Connect Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct Qobuz Connect quality, queue, ownership, and side-channel behavior and prove the core flows against a fail-closed local MA instance and the real Qobuz cloud.

**Architecture:** Keep the pure reducer as the reconciliation authority, make every impure MA/cloud action explicit through typed effects and `MABridge`, and separate configured quality ceilings from actual stream-quality observations. Harden the integration harness before allowing another playback command, then add deterministic red-green tests for every repair before live validation.

**Tech Stack:** Python 3.14, asyncio, pytest/pytest-asyncio, protobuf, aiohttp/websockets, Playwright, Music Assistant WebSocket API, Qobuz Web Client and Qobuz Connect cloud.

## Global Constraints

- Work only on branch `qobuz-connect-hardening` in the isolated worktree.
- Do not print authentication files, cookies, passwords, or MA bearer tokens.
- Generated protobuf modules in `music_assistant/providers/qobuz_connect/proto/` remain unchanged.
- Every production behavior starts with a test that fails for the expected reason.
- Run `pytest tests/providers/qobuz_connect/ --no-cov -q` after each task.
- Run `pre-commit run --all-files` after all code changes.
- Live playback is forbidden until the fail-closed preflight proves the exact target is `BlackHole 2ch`.
- A required live scenario that does not run is not a pass.
- Preserve unrelated user files and the original checkout.

## File map

- `music_assistant/providers/qobuz_connect/reducer.py`: pure ownership, transport, queue, retry, duplicate, autoplay, and side-channel decisions.
- `music_assistant/providers/qobuz_connect/sync_types.py`: immutable events/effects and proposal payloads.
- `music_assistant/providers/qobuz_connect/effect_runner.py`: execute typed cloud and MA effects.
- `music_assistant/providers/qobuz_connect/coordinator.py`: serialized intake, target capture, metadata generations, and event normalization.
- `music_assistant/providers/qobuz_connect/ma_bridge.py`: all MA player/queue/config operations needed by the sync shell.
- `music_assistant/providers/qobuz_connect/quality_reporter.py`: new actual/max quality observation and deduplicated wire reporting.
- `music_assistant/providers/qobuz_connect/outbound_reporter.py`: ownership-gated renderer-state reporting.
- `music_assistant/providers/qobuz_connect/session.py`: distinct maximum/file/device quality methods and accurate lifecycle logging.
- `music_assistant/providers/qobuz_connect/protocol.py`: encode actual quality tuples and existing wire commands.
- `music_assistant/providers/qobuz_connect/__init__.py`: configuration, provider selection, event wiring, and quality source getters.
- `tests/providers/qobuz_connect/protocol_capture/integration_harness.py`: fail-closed preflight, two clients, exact ID checks, and cleanup.
- `tests/providers/qobuz_connect/protocol_capture/ma_probe.py`: parse process identity, target, stream format, and provider errors.
- `tests/providers/qobuz_connect/protocol_capture/qobuz_page.py`: exact output selection and cloud queue/quality inspection.
- `tests/providers/qobuz_connect/protocol_capture/integration_scenarios/`: required real-cloud scenarios.

---

### Task 1: Fail-closed integration harness

**Files:**
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_harness.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/ma_probe.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/qobuz_page.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_run.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/harness.py`
- Create: `tests/providers/qobuz_connect/test_integration_harness_safety.py`

**Interfaces:**
- Produces: `SafetyPreflight.run() -> PreflightResult`
- Produces: `QobuzPage.selected_output_name() -> str`
- Produces: `QobuzPage.select_local_output(expected_name: str) -> None`
- Produces: `IntegrationSession.ensure_safe_for_playback() -> None`
- Produces: `IntegrationSession.cleanup_playback() -> None`

- [ ] **Step 1: Write failing preflight tests**

Use real `SafetyPreflight` logic with controlled MA/query and page fakes. Cover:

```python
async def test_preflight_rejects_wrong_target_player() -> None:
    preflight = safety_preflight(
        connect_target="Local Dev Hardening",
        configured_target=MACBOOK_PLAYER_ID,
        players=[blackhole_player(available=True), macbook_player(available=True)],
        selected_output="Web Player Chrome",
    )
    result = await preflight.run()
    assert not result.passed
    assert "BlackHole 2ch" in result.failures[0]


async def test_preflight_requires_exact_unique_cloud_renderer_name() -> None:
    preflight = safety_preflight(
        connect_target="Local Dev Hardening abc123",
        cloud_outputs=["Local Dev Hardening", "Music Assistant"],
    )
    result = await preflight.run()
    assert not result.passed


async def test_preflight_accepts_exact_blackhole_and_browser_output() -> None:
    preflight = safe_preflight_fixture()
    result = await preflight.run()
    assert result.passed
```

- [ ] **Step 2: Verify the tests fail for missing preflight behavior**

Run:

```bash
pytest tests/providers/qobuz_connect/test_integration_harness_safety.py -v
```

Expected: FAIL because `SafetyPreflight` and exact selected-output inspection do not exist.

- [ ] **Step 3: Implement safety types and exact page inspection**

Add:

```python
@dataclass(slots=True, frozen=True)
class PreflightResult:
    checks: tuple[str, ...]
    failures: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.failures
```

`SafetyPreflight.run()` must query MA provider config and players, validate the managed process log marker, require exact BlackHole ID/name/provider/availability, list Qobuz outputs, select browser-local output, and read it back. No substring matching is allowed.

- [ ] **Step 4: Make every playback entry point require preflight**

`IntegrationSession.reset_to_clean_state()`, `handoff_to_ma()`, and MA-driven play helpers call `ensure_safe_for_playback()`. Raw capture scenarios call a shared `ensure_local_output_before_play(qobuz)` helper. Remove the nonfatal reset exception path.

- [ ] **Step 5: Add deterministic cleanup**

`cleanup_playback()` sends stop/clear only to the exact BlackHole ID, selects verified browser-local output, and runs in `integration_run.py`'s `finally` block.

- [ ] **Step 6: Verify tests and baseline**

```bash
pytest tests/providers/qobuz_connect/test_integration_harness_safety.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
```

Expected: all pass, with no live playback.

- [ ] **Step 7: Commit**

```bash
git add tests/providers/qobuz_connect
git commit -m "qobuz_connect: make live harness fail closed"
```

---

### Task 2: Actual file-quality reporting and configuration synchronization

**Files:**
- Create: `music_assistant/providers/qobuz_connect/quality_reporter.py`
- Create: `tests/providers/qobuz_connect/test_quality_reporter.py`
- Modify: `music_assistant/providers/qobuz_connect/models.py`
- Modify: `music_assistant/providers/qobuz_connect/protocol.py`
- Modify: `music_assistant/providers/qobuz_connect/session.py`
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `tests/providers/qobuz_connect/test_protocol.py`
- Modify: `tests/providers/qobuz_connect/test_provider_wiring.py`

**Interfaces:**
- Produces: `AudioQualityReport(sample_rate: int, bit_depth: int, channels: int, quality: int)`
- Produces: `quality_id_for_format(content_type: str, sample_rate: int, bit_depth: int) -> int`
- Produces: `QualityReporter.report_max(quality: int) -> Awaitable[None]`
- Produces: `QualityReporter.report_file(report: AudioQualityReport | None) -> Awaitable[None]`
- Produces: `QobuzConnectProvider._current_file_quality() -> AudioQualityReport | None`

- [ ] **Step 1: Write protocol and reporter failures**

Hand-derived cases:

```python
@pytest.mark.parametrize(
    ("sample_rate", "bit_depth", "quality"),
    [(44100, 16, 6), (44100, 24, 7), (96000, 24, 7), (192000, 24, 27)],
)
def test_file_quality_encodes_actual_format(sample_rate, bit_depth, quality):
    report = AudioQualityReport(sample_rate, bit_depth, 2, quality)
    message = decode_single(codec.encode_file_audio_quality_changed(report))
    assert message.sampling_rate == sample_rate
    assert message.bit_depth == bit_depth
    assert message.nb_channels == 2
    assert message.audio_quality == QUALITY_TO_PROTOCOL[quality]


async def test_report_file_does_not_substitute_maximum_for_actual_format():
    reporter = quality_reporter(max_quality=27)
    await reporter.report_file(AudioQualityReport(44100, 24, 2, 7))
    assert decoded_file_report(reporter.sent[-1]) == (44100, 24, 2, 3)


async def test_unknown_file_format_emits_no_fabricated_report():
    await reporter.report_file(None)
    assert reporter.sent == []
```

- [ ] **Step 2: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_quality_reporter.py tests/providers/qobuz_connect/test_protocol.py -v
```

Expected: FAIL because quality reporting accepts only a ceiling integer.

- [ ] **Step 3: Implement quality value and protocol encoders**

Add the frozen dataclass to `models.py`. Change file/device encoders to require an `AudioQualityReport`; keep maximum encoding integer-only. Remove default substitution from current file/device messages.

- [ ] **Step 4: Implement `QualityReporter`**

It receives `session_getter`, `file_quality_getter`, and logger. Deduplicate identical maximum and file tuples. `report_current()` sends maximum and then an actual file report only when available. Device output is omitted until a trustworthy output getter is present.

- [ ] **Step 5: Wire actual MA stream details**

`_current_file_quality()` reads:

```python
queue.current_item.streamdetails.audio_format
```

and maps MPEG to tier 5, 16-bit lossless to 6, lossless up to 96 kHz to 7, and lossless above 96 kHz to 27. Trigger reporting on current queue/transport changes after stream details are populated.

- [ ] **Step 6: Write failing independent-persistence tests**

Tests must prove:

- Connect save failure still attempts native Qobuz save.
- Native Qobuz save failure still reports the selected maximum.
- MA-side Connect `update_config()` synchronizes native Qobuz quality.
- automatic quality resolves from native provider config.

- [ ] **Step 7: Implement independent synchronization**

Use two separately contained awaits, not one shared `try`. Store the resolved native quality in `_max_quality`; never send protocol quality for `AUTO_QUALITY`.

- [ ] **Step 8: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_quality_reporter.py tests/providers/qobuz_connect/test_protocol.py tests/providers/qobuz_connect/test_provider_wiring.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: report actual stream quality"
```

---

### Task 3: Valid UUIDs and lossless proposal retries

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/sync_types.py`
- Modify: `music_assistant/providers/qobuz_connect/reducer.py`
- Modify: `music_assistant/providers/qobuz_connect/coordinator.py`
- Modify: `tests/providers/qobuz_connect/test_reducer_proposals.py`
- Modify: `tests/providers/qobuz_connect/test_coordinator.py`

**Interfaces:**
- Changes: `MaQueueChanged` gains `context_uuid: bytes`
- Invariant: `action_uuid` and LOAD `context_uuid` are exactly 16 nonzero bytes
- Produces: `_retry_payload(state: CanonicalState, proposal: Proposal) -> tuple[int, ...]`

- [ ] **Step 1: Add failing UUID and retry tests**

Cover LOAD `(20, 21)`, REMOVE one occurrence, ADD duplicate, and INSERT. Assert exact emitted payloads after `CloudQueueError`. Assert coordinator-generated UUIDs have length 16 and LOAD contexts differ across two proposals.

- [ ] **Step 2: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_reducer_proposals.py tests/providers/qobuz_connect/test_coordinator.py -v
```

Expected: retry payload assertions fail and the LOAD context is all zero.

- [ ] **Step 3: Mint UUIDs at the coordinator boundary**

Use `uuid.uuid4().bytes` for both action and context UUIDs. The pure reducer receives them on the MA queue event and never generates randomness.

- [ ] **Step 4: Rebuild each retry from proposal intent**

Rules:

- LOAD payload equals `target_track_ids`.
- ADD/INSERT payload uses occurrence-aware difference between current canonical and target.
- REMOVE payload uses occurrence-aware removed IDs between current canonical and target.
- REORDER derives slot IDs only at `_emit_push`.
- if REMOVE/ADD intent cannot be represented after drift, rebase it as LOAD with the full target.

- [ ] **Step 5: Verify flight-recorder reproduction**

Add a fixture from the observed sequence: clear echo increments version, LOAD rejection retries with the original 20 IDs. Assert no empty LOAD/ADD retry.

- [ ] **Step 6: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_reducer_proposals.py tests/providers/qobuz_connect/test_coordinator.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: preserve proposal intent across retries"
```

---

### Task 4: Occurrence-aware duplicate queue identity

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/reducer.py`
- Modify: `music_assistant/providers/qobuz_connect/outbound_reporter.py`
- Modify: `tests/providers/qobuz_connect/test_reducer_diff.py`
- Modify: `tests/providers/qobuz_connect/test_reducer_proposals.py`
- Modify: `tests/providers/qobuz_connect/test_soak_simulated.py`

**Interfaces:**
- Produces: `_consume_subsequence(source: tuple[int, ...], target: tuple[int, ...]) -> tuple[int, ...] | None`
- Produces: `_match_occurrences(refs: tuple[QueueTrackRef, ...], ids: tuple[int, ...]) -> tuple[QueueTrackRef, ...] | None`

- [ ] **Step 1: Write failing multiset cases**

Assert:

- `(1, 1, 2) -> (1, 2, 2)` is LOAD, not REORDER.
- removing one duplicate sends exactly one distinct queue-item ID.
- reordering `(1a, 1b, 2)` preserves both occurrence IDs once each.
- current lookup prefers the canonical occurrence corresponding to MA's current queue index when available.

- [ ] **Step 2: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_reducer_diff.py tests/providers/qobuz_connect/test_reducer_proposals.py -v
```

- [ ] **Step 3: Replace set-based list logic**

Use left-to-right consuming counters/lists for subsequence, subtraction, and equality. Never translate duplicate qids with a first-match helper that can reuse the same slot.

- [ ] **Step 4: Expand soak generation**

Generate queue IDs with replacement and assert:

```python
Counter(ref.track_id for ref in state.tracks)
```

matches confirmed cloud state; every emitted REMOVE/REORDER slot ID is unique unless the protocol explicitly repeats it.

- [ ] **Step 5: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_reducer_diff.py tests/providers/qobuz_connect/test_reducer_proposals.py tests/providers/qobuz_connect/test_soak_simulated.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: reconcile duplicate track occurrences"
```

---

### Task 5: Empty and atomic metadata resynchronization

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/sync_types.py`
- Modify: `music_assistant/providers/qobuz_connect/coordinator.py`
- Modify: `music_assistant/providers/qobuz_connect/effect_runner.py`
- Modify: `music_assistant/providers/qobuz_connect/ma_bridge.py`
- Modify: `tests/providers/qobuz_connect/test_effect_runner.py`
- Modify: `tests/providers/qobuz_connect/test_metadata_resolver.py`
- Modify: `tests/providers/qobuz_connect/test_coordinator.py`

**Interfaces:**
- Changes: `MaResyncQueue` gains `generation: int`
- Produces: `MetadataBatchResult(items: tuple[object, ...], permanent_missing: frozenset[int], transient_failed: frozenset[int])`
- Produces: `MABridge.clear_qobuz_items(player_id: str) -> None`

- [ ] **Step 1: Write failing empty-queue test**

Start with only Qobuz MA items, run `MaResyncQueue(track_ids=())`, and assert the queue is cleared. Retain the existing mixed-provider preservation case.

- [ ] **Step 2: Write failing partial-transient test**

Resolve IDs `(10, 11)` where 10 succeeds and 11 raises a transient exception. Assert no partial `update_items()` call and no subsequent `PushRemove`.

- [ ] **Step 3: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_effect_runner.py tests/providers/qobuz_connect/test_metadata_resolver.py tests/providers/qobuz_connect/test_coordinator.py -v
```

- [ ] **Step 4: Implement authoritative empty resync**

Build `final_items` from preserved non-Qobuz items plus resolved Qobuz items. Call `update_items()` even when the result is empty; do not return early.

- [ ] **Step 5: Make metadata batches atomic**

`MetadataResolver.resolve_batch()` distinguishes permanent and transient misses. A transient miss aborts application of that generation. Permanent misses update the coordinator's unresolvable cache without pretending the user removed a track.

- [ ] **Step 6: Add generation checks**

Increment `_queue_generation` whenever canonical target tracks change. Resolve outside the reducer lock, then apply only when the generation and target tuple still match. A stale result is discarded and the newest generation remains scheduled.

- [ ] **Step 7: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_effect_runner.py tests/providers/qobuz_connect/test_metadata_resolver.py tests/providers/qobuz_connect/test_coordinator.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: make queue resync atomic and empty-safe"
```

---

### Task 6: Renderer ownership and target-bound release

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/sync_types.py`
- Modify: `music_assistant/providers/qobuz_connect/reducer.py`
- Modify: `music_assistant/providers/qobuz_connect/coordinator.py`
- Modify: `music_assistant/providers/qobuz_connect/effect_runner.py`
- Modify: `music_assistant/providers/qobuz_connect/outbound_reporter.py`
- Modify: `tests/providers/qobuz_connect/test_reducer_transport.py`
- Modify: `tests/providers/qobuz_connect/test_effect_runner.py`
- Modify: `tests/providers/qobuz_connect/test_provider_wiring.py`

**Interfaces:**
- Changes: `MaTransportChanged` gains `target_player_id: str | None`
- Changes: `MaReleasePlayer(player_id: str)`
- Produces: canonical `activation_requested: bool`

- [ ] **Step 1: Write failing ownership tests**

Assert MA PLAYING while inactive:

```python
effects == (PushSetActive(),)
```

and not `ReportState`. After `CloudActiveRendererChanged(own_rid)`, assert active state and a report/player-state effect. Duplicate active commands emit no second takeover/play. Inactive reporter calls send nothing.

- [ ] **Step 2: Write failing exact-release test**

Activate on `p1`, reorder available players so auto resolution would choose `p2`, deactivate, and assert stop/clear calls use `p1`.

- [ ] **Step 3: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_reducer_transport.py tests/providers/qobuz_connect/test_effect_runner.py tests/providers/qobuz_connect/test_provider_wiring.py -v
```

- [ ] **Step 4: Implement ownership handshake**

Track `activation_requested`. MA-origin playing emits one `PushSetActive`; renderer reports remain gated until `active_rid == own_rid`. Cloud confirmation clears the request and emits the current state. Losing ownership suppresses reports without stopping unrelated players.

- [ ] **Step 5: Capture target on release**

Coordinator stamps target ID onto activation/deactivation events or effects. `EffectRunner` never resolves a player for `MaReleasePlayer`; it consumes the embedded ID.

- [ ] **Step 6: Make activation idempotent**

Repeated `CloudSetActive(True)` while already active does not resync or restart. Repeated false does not release twice.

- [ ] **Step 7: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_reducer_transport.py tests/providers/qobuz_connect/test_effect_runner.py tests/providers/qobuz_connect/test_provider_wiring.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: gate reports on renderer ownership"
```

---

### Task 7: Autoplay, volume delta, mute, disconnect, and lifecycle accuracy

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/sync_types.py`
- Modify: `music_assistant/providers/qobuz_connect/reducer.py`
- Modify: `music_assistant/providers/qobuz_connect/effect_runner.py`
- Modify: `music_assistant/providers/qobuz_connect/ma_bridge.py`
- Modify: `music_assistant/providers/qobuz_connect/outbound_reporter.py`
- Modify: `music_assistant/providers/qobuz_connect/session.py`
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `tests/providers/qobuz_connect/test_reducer_modes.py`
- Modify: `tests/providers/qobuz_connect/test_effect_runner.py`
- Modify: `tests/providers/qobuz_connect/test_provider_wiring.py`

**Interfaces:**
- Produces: `MaAdjustVolume(delta: int)`
- Produces: `MaSetMuted(muted: bool)`
- Produces: `MABridge.adjust_volume(player_id: str, delta: int) -> Awaitable[None]`
- Produces: `MABridge.set_muted(player_id: str, muted: bool) -> Awaitable[None]`

- [ ] **Step 1: Write failing autoplay tests**

Assert `CloudAutoplayTracksLoaded` materializes `tracks + autoplay_tracks`, reporter resolves a current autoplay item, and replacing autoplay does not restart the current main item.

- [ ] **Step 2: Write failing volume/mute tests**

Cloud delta `-7` emits `MaAdjustVolume(-7)` and bridge clamps current volume to `[0, 100]`. Cloud mute emits `MaSetMuted(True)`.

- [ ] **Step 3: Write failing disconnect/lifecycle tests**

After disconnect, assert `own_rid`, `active_rid`, `activation_requested`, pending, and versions reset. Assert `_setup_websocket()` logs “session started” before connection and “connected” only from a confirmed session callback.

- [ ] **Step 4: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_reducer_modes.py tests/providers/qobuz_connect/test_effect_runner.py tests/providers/qobuz_connect/test_provider_wiring.py -v
```

- [ ] **Step 5: Implement autoplay combined view**

Use `(*state.tracks, *state.autoplay_tracks)` for materialization and current lookup, while list mutations continue to address the main queue separately.

- [ ] **Step 6: Implement volume/mute bridge effects**

Read current player volume at execution time, clamp the delta result, and call MA's absolute volume API. Call MA's mute command for `MaSetMuted`.

- [ ] **Step 7: Implement reset and log accuracy**

Clear all connection-scoped fields on `Disconnected`. Move connected logging to the websocket's confirmed-open path.

- [ ] **Step 8: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_reducer_modes.py tests/providers/qobuz_connect/test_effect_runner.py tests/providers/qobuz_connect/test_provider_wiring.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: complete autoplay and side channels"
```

---

### Task 8: Explicit native Qobuz instance and multi-instance ports

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `music_assistant/providers/qobuz_connect/strings.json`
- Modify: `tests/providers/qobuz_connect/test_provider_wiring.py`
- Modify: `tests/providers/qobuz_connect/test_models_controller.py`

**Interfaces:**
- Produces: `CONF_QOBUZ_PROVIDER = "qobuz_provider"`
- Produces: `_suggest_http_port(mass: MusicAssistant, instance_id: str | None) -> int`

- [ ] **Step 1: Write failing provider-selection tests**

With two loaded Qobuz instances, configure the second and assert metadata and quality saves use it. Missing selected instance raises `InvalidDataError` naming the instance.

- [ ] **Step 2: Write failing port tests**

With an existing Connect config on 8695, new config entries default to 8696. An explicit collision during setup produces a clear provider error and does not silently change the port.

- [ ] **Step 3: Verify red**

```bash
pytest tests/providers/qobuz_connect/test_provider_wiring.py tests/providers/qobuz_connect/test_models_controller.py -v
```

- [ ] **Step 4: Implement selection and dynamic default**

Build options from `mass.config.get_provider_configs(provider_domain="qobuz")`, store the instance ID, and use `mass.get_provider(instance_id)`. Suggest the first unused port at or above 8695 for new instances.

- [ ] **Step 5: Verify and commit**

```bash
pytest tests/providers/qobuz_connect/test_provider_wiring.py tests/providers/qobuz_connect/test_models_controller.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
git add music_assistant/providers/qobuz_connect tests/providers/qobuz_connect
git commit -m "qobuz_connect: select qobuz instance and unique port"
```

---

### Task 9: Replay, soak, and live assertion integrity

**Files:**
- Modify: `tests/providers/qobuz_connect/test_capture_replay.py`
- Modify: `tests/providers/qobuz_connect/test_soak_simulated.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_harness.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_run.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_scenarios/ma_driven.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/qobuz_page.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/INTEGRATION.md`

**Interfaces:**
- Produces: `ScenarioStatus(StrEnum): PASS, FAIL, SKIP`
- Produces: `QobuzPage.current_track_id() -> str`
- Produces: `QobuzPage.queue_track_ids() -> tuple[str, ...]`

- [ ] **Step 1: Make replay fail on contained errors**

Add a test dispatcher handler that raises; assert runtime `dispatch()` contains it, but replay's error collector records it and fails the replay. Real captures assert the collector remains empty.

- [ ] **Step 2: Make skips first-class**

Replace passing “skipped” checks with `ScenarioStatus.SKIP`. `all` returns nonzero if any required scenario is SKIP or FAIL.

- [ ] **Step 3: Replace title and local-only assertions**

Use exact Qobuz track IDs for app/MA sync. `ma_queue_edit` snapshots both clients' cloud queue IDs before/after and asserts the exact added occurrence. MA track initiation asserts the requested track ID.

- [ ] **Step 4: Open a second live client**

`open_session()` creates A and B from their persisted auth states. Integration assertions require both clients to converge on renderer, current track, queue version/order, and quality.

- [ ] **Step 5: Verify**

```bash
pytest tests/providers/qobuz_connect/test_capture_replay.py tests/providers/qobuz_connect/test_soak_simulated.py tests/providers/qobuz_connect/test_integration_harness_safety.py -v
pytest tests/providers/qobuz_connect/ --no-cov -q
```

- [ ] **Step 6: Commit**

```bash
git add tests/providers/qobuz_connect
git commit -m "qobuz_connect: make integration assertions authoritative"
```

---

### Task 10: Authenticated real-cloud scenarios, documentation, and final verification

**Files:**
- Create: `tests/providers/qobuz_connect/protocol_capture/integration_scenarios/quality.py`
- Create: `tests/providers/qobuz_connect/protocol_capture/integration_scenarios/queue_edge_cases.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_scenarios/__init__.py`
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_scenarios/sessions.py`
- Modify: `music_assistant/providers/qobuz_connect/ARCHITECTURE.md`
- Modify: `tests/providers/qobuz_connect/protocol_capture/INTEGRATION.md`

**Interfaces:**
- Produces live scenarios: `actual_quality`, `quality_change`, `queue_remove_last`, `queue_clear`, `duplicate_queue`, `version_conflict`, `ownership_two_players`, `reconnect`, and `autoplay_continuation`

- [ ] **Step 1: Prepare isolated live data**

Copy the ignored playground data into a worktree-local ignored directory. Change only the copy:

- unique publish name `Local Dev Hardening <short instance suffix>`;
- exact BlackHole target ID;
- noncolliding Connect HTTP port;
- disable or leave untargeted audible player providers;
- retain the encrypted Qobuz configuration and server encryption key.

Do not start MA until `SafetyPreflight` can inspect the copy and expected values.

- [ ] **Step 2: Add actual-quality scenario**

Select a Qobuz track whose API/MA stream resolves below the configured ceiling. Set maximum to 24/192, hand off to the unique receiver, then assert:

- MA stream log sample rate/bit depth;
- client A displayed file quality;
- client B displayed file quality;
- captured outgoing MA file-quality frame;
- all four values agree exactly;
- maximum remains 24/192 as a separate value.

- [ ] **Step 3: Add queue and conflict scenarios**

Use exact IDs to validate remove-last, clear, duplicate add/remove/reorder, and simultaneous MA/client edits. The conflict scenario must observe a rejected proposal followed by a nonempty retry and final identical queues in MA/A/B.

- [ ] **Step 4: Add ownership scenarios**

Start a second MA player only after confirming it is BlackHole or another silent sink. Initiate playback from MA, assert cloud ownership changes to the unique receiver before its first state report, then deactivate and prove only the captured BlackHole queue stopped.

- [ ] **Step 5: Add reconnect and conditional autoplay**

Restart the managed Connect websocket/MA process, assert ownership IDs are rebuilt and state reports resume only after ownership. Run autoplay only when the web client exposes a deterministic control; otherwise record a genuine SKIP and keep deterministic autoplay tests mandatory.

- [ ] **Step 6: Run deterministic verification**

```bash
pytest tests/providers/qobuz_connect/ --no-cov -q
pytest tests/providers/qobuz_connect/ -q -o addopts='' \
  --cov=music_assistant.providers.qobuz_connect \
  --cov-report=term-missing:skip-covered --cov-branch
pre-commit run --all-files
```

Expected: zero failures; focused coverage does not regress below the reviewed 75% overall baseline or below 95% for `reducer.py`.

- [ ] **Step 7: Run fail-closed live verification**

```bash
python -m tests.providers.qobuz_connect.protocol_capture.integration_run --scenario all
```

Expected: preflight names the managed PID, unique receiver, exact BlackHole ID, and browser-local reset. All required scenarios PASS; no required scenario SKIPs.

- [ ] **Step 8: Inspect evidence**

Review the managed MA log, rolling flight recorder, and both websocket capture files. Assert no:

- non-active-renderer state errors;
- empty ADD/LOAD retry;
- malformed UUID error;
- target fallback warning;
- app/MA quality drift;
- app/MA queue or current-track drift;
- unexpected audible-player command.

- [ ] **Step 9: Update architecture documentation**

Document actual/max/device quality separation, ownership gating, target-bound release, occurrence-aware proposals, atomic metadata generations, autoplay combined view, and fail-closed testing.

- [ ] **Step 10: Commit**

```bash
git add music_assistant/providers/qobuz_connect/ARCHITECTURE.md \
  tests/providers/qobuz_connect/protocol_capture
git commit -m "qobuz_connect: validate hardening against real cloud"
```

- [ ] **Step 11: Final branch verification**

```bash
git status --short
git log --oneline dev..HEAD
git diff --check dev...HEAD
```

Expected: clean tracked worktree, one reviewed commit per task, no generated protobuf changes, and no unrelated provider changes.
