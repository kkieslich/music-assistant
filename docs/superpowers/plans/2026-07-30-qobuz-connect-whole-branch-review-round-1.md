# Qobuz Connect Whole-Branch Review Round 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all ten verified review findings with transactional startup, coherent active ownership, deterministic native-quality reconciliation, and fail-safe live test tooling.

**Architecture:** Keep the existing reducer/coordinator split and make the provider's active target pin the authoritative lease. Move availability-critical work into awaited initialization, add explicit foreign-current transport state, and harden setup/capture helpers at their existing boundaries.

**Tech Stack:** Python 3.14, asyncio, aiohttp, zeroconf, pytest, unittest.mock, Playwright, ffmpeg, Ruff, mypy, pre-commit.

## Global Constraints

- Work only in `.worktrees/rebase-upstream-dev-20260730`.
- Verify each review claim before editing; all ten have been verified against current code.
- Use strict RED/GREEN TDD for every behavior change and commit logical groups.
- Preserve the source `.mass-data` and `.mass-cache`; real-cloud testing uses disposable copies.
- Live testing requires the exact unique `BlackHole 2ch` player and all 27 scenarios.
- Run full `pre-commit run --all-files` after code changes.
- Do not alter main `dev`, backup refs, remote refs, cleanup specification, or seed data.

---

### Task 1: Transactional Provider and Discovery Startup

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/manifest.json`
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `music_assistant/providers/qobuz_connect/discovery.py`
- Test: `tests/providers/qobuz_connect/test_provider_wiring.py`
- Test: `tests/providers/qobuz_connect/test_discovery.py`

**Interfaces:**
- Consumes: `MusicAssistant.get_provider(instance_id)`, `Provider.handle_async_init()`.
- Produces: `QobuzConnectProvider.handle_async_init()`, transactional `QobuzConnectDiscovery.start()` and `stop()`.

- [x] **Step 1: Write failing exact-dependency and lifecycle tests**

```python
async def test_handle_async_init_rejects_unavailable_selected_qobuz() -> None:
    provider, _mass = _make_provider()
    with pytest.raises(InvalidDataError):
        await provider.handle_async_init()

async def test_handle_async_init_rolls_back_after_discovery_failure() -> None:
    provider, mass = _make_provider_with_native_qobuz()
    mass.subscribe.side_effect = [MagicMock(), MagicMock(), MagicMock()]
    with patch.object(QobuzConnectDiscovery, "start", side_effect=OSError("occupied")):
        with pytest.raises(OSError):
            await provider.handle_async_init()
    assert provider._discovery is None
    assert provider._unsubscribe_queue_events is None
```

- [x] **Step 2: Run focused tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_provider_wiring.py tests/providers/qobuz_connect/test_discovery.py -q`

Expected: new validation/rollback assertions fail against `loaded_in_mass()` startup.

- [x] **Step 3: Implement awaited transactional startup**

```python
async def handle_async_init(self) -> None:
    self.get_qobuz_provider()
    try:
        await self._start_runtime()
    except BaseException:
        await self._stop_runtime()
        raise
```

Add `"depends_on": "qobuz"` to the manifest. Make discovery `start()` call
exception-safe `stop()` on failure, and make `stop()` attempt mDNS, TCP site,
runner, and private Zeroconf cleanup independently before clearing references.

- [x] **Step 4: Run focused tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_provider_wiring.py tests/providers/qobuz_connect/test_discovery.py -q`

Expected: all pass.

- [x] **Step 5: Commit**

```bash
git add music_assistant/providers/qobuz_connect/{manifest.json,__init__.py,discovery.py} tests/providers/qobuz_connect/{test_provider_wiring.py,test_discovery.py}
git commit -m "fix(qobuz_connect): make provider startup transactional"
```

### Task 2: Authoritative Active Target Lease

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `music_assistant/providers/qobuz_connect/coordinator.py`
- Test: `tests/providers/qobuz_connect/test_provider_wiring.py`
- Test: `tests/providers/qobuz_connect/test_coordinator.py`

**Interfaces:**
- Consumes: `QobuzConnectProvider.get_target_player_id()`.
- Produces: `QobuzConnectCoordinator.transfer_target(player_id)`, active pin/autoplay transfer.

- [x] **Step 1: Write failing migration/return/release tests**

```python
async def test_active_target_disappearance_transfers_release_and_autoplay() -> None:
    await provider._on_set_active(True)
    players.pop("p1")
    assert provider.get_target_player_id() == "p2"
    provider._suppress_ma_autoplay()
    await provider._on_set_active(False)
    mass.player_queues.stop.assert_awaited_once_with("p2")

def test_configured_target_return_waits_for_active_release() -> None:
    assert provider.get_target_player_id() == "fallback"
    provider._coordinator._state = CanonicalState(active=True)
    players["configured"] = configured
    assert provider.get_target_player_id() == "fallback"
```

- [x] **Step 2: Run target-focused tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_provider_wiring.py tests/providers/qobuz_connect/test_coordinator.py -q`

Expected: release/autoplay ownership remains on the vanished target or redirects on configured return.

- [x] **Step 3: Implement the single active lease**

```python
def transfer_target(self, player_id: str | None) -> None:
    """Move release ownership to the provider's authoritative active target."""
    self._owned_target_player_id = player_id
```

Resolve a valid active pin before consulting configured-target preference. When a
pin disappears, move the coordinator target and autoplay lease to the replacement.
Clear the pin and lease on deactivation, disconnect, ownership loss, and unload.

- [x] **Step 4: Run target-focused tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_provider_wiring.py tests/providers/qobuz_connect/test_coordinator.py -q`

Expected: all pass.

- [x] **Step 5: Commit**

```bash
git add music_assistant/providers/qobuz_connect/{__init__.py,coordinator.py} tests/providers/qobuz_connect/{test_provider_wiring.py,test_coordinator.py}
git commit -m "fix(qobuz_connect): keep active target ownership coherent"
```

### Task 3: Active Receiver Owns Native Quality

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `music_assistant/providers/qobuz_connect/ARCHITECTURE.md`
- Test: `tests/providers/qobuz_connect/test_provider_wiring.py`

**Interfaces:**
- Consumes: `_configured_max_quality`, `_update_qobuz_stream_quality()`.
- Produces: `_reconcile_active_quality()`.

- [ ] **Step 1: Write failing restart and shared-native tests**

```python
async def test_activation_reconciles_explicit_quality_after_restart() -> None:
    provider._configured_max_quality = 6
    native.config.get_value.return_value = "27"
    await provider._on_set_active(True)
    mass.config.save_provider_config.assert_awaited_with("qobuz", {"quality": "6"}, native.instance_id)

async def test_last_active_explicit_receiver_owns_shared_native_quality() -> None:
    await receiver_a._on_set_active(True)
    await receiver_b._on_set_active(True)
    assert saved_native_qualities == ["6", "27"]
```

- [ ] **Step 2: Run focused tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_provider_wiring.py -q`

Expected: activation never writes the persisted explicit ceiling.

- [ ] **Step 3: Implement and document last-active ownership**

```python
async def _reconcile_active_quality(self) -> None:
    if self._configured_max_quality != AUTO_QUALITY:
        await self._update_qobuz_stream_quality(self._max_quality)
```

Call it during activation with warning-only persistence failure. Document that AUTO
reads native and the last active explicit receiver writes shared native quality.

- [ ] **Step 4: Run focused tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_provider_wiring.py -q`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add music_assistant/providers/qobuz_connect/{__init__.py,ARCHITECTURE.md} tests/providers/qobuz_connect/test_provider_wiring.py
git commit -m "fix(qobuz_connect): reconcile active receiver quality"
```

### Task 4: Release Foreign Current Queue Items

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/sync_types.py`
- Modify: `music_assistant/providers/qobuz_connect/coordinator.py`
- Modify: `music_assistant/providers/qobuz_connect/reducer.py`
- Test: `tests/providers/qobuz_connect/test_coordinator.py`
- Test: `tests/providers/qobuz_connect/test_reducer_transport.py`
- Test: `tests/providers/qobuz_connect/test_provider_wiring.py`

**Interfaces:**
- Consumes: `MaTransportChanged`.
- Produces: `MaTransportChanged.current_item_unmappable: bool`.

- [ ] **Step 1: Write failing pure and coordinator tests**

```python
def test_foreign_current_releases_owned_renderer() -> None:
    result = reduce(active_state, MaTransportChanged(
        now_ms=2, playing=PlayingState.PLAYING, current_track_id=None,
        position_ms=90_000, current_item_unmappable=True,
    ))
    assert result.state.current_id is None
    assert result.state.playing is PlayingState.STOPPED
    assert result.state.position_ms == 0
    assert result.effects == (MaReleasePlayer(),)
```

- [ ] **Step 2: Run focused tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_reducer_transport.py tests/providers/qobuz_connect/test_coordinator.py tests/providers/qobuz_connect/test_provider_wiring.py -q`

Expected: the event has no unmappable marker and stale current id remains.

- [ ] **Step 3: Implement explicit foreign-current transition**

```python
if event.current_item_unmappable and state.active:
    return ReduceResult(
        dataclasses.replace(state, current_id=None, playing=PlayingState.STOPPED,
                            position_ms=0, position_anchor_ms=event.now_ms, active=False),
        (MaReleasePlayer(), ReportState()),
    )
```

Set the marker only when MA has a current item and selected-Qobuz mapping returns
`None`. Ensure duration/report getters cannot read a foreign item after ownership is
released.

- [ ] **Step 4: Run focused tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_reducer_transport.py tests/providers/qobuz_connect/test_coordinator.py tests/providers/qobuz_connect/test_provider_wiring.py -q`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add music_assistant/providers/qobuz_connect/{sync_types.py,coordinator.py,reducer.py,__init__.py} tests/providers/qobuz_connect/{test_coordinator.py,test_reducer_transport.py,test_provider_wiring.py}
git commit -m "fix(qobuz_connect): release foreign queue ownership"
```

### Task 5: Setup-Flow Storage Boundaries

**Files:**
- Modify: `music_assistant/providers/qobuz_connect/setup_flow.py`
- Test: `tests/providers/qobuz_connect/test_setup_flow.py`

**Interfaces:**
- Consumes: `ConfigController.get_raw_provider_config_value(instance_id, key)`.
- Produces: `_get_setup_or_legacy_value(mass, config, key)`.

- [ ] **Step 1: Write failing current/legacy port and finish-filter tests**

```python
async def test_finish_excludes_runtime_values() -> None:
    session.context.values = {"max_quality": "27", "log_level": "debug"}
    await run_setup(session)
    assert set(session.finished_values) == SETUP_KEYS

async def test_legacy_raw_sibling_port_is_reserved() -> None:
    mass.config.get_raw_provider_config_value.return_value = 8695
    assert await _suggest_http_port(mass, None) == 8696
```

- [ ] **Step 2: Run setup tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_setup_flow.py -q`

Expected: runtime values leak into finish and legacy raw port is ignored.

- [ ] **Step 3: Implement setup-only persistence and raw fallback**

```python
finish_data = {key: setup_data[key] for key in SETUP_KEYS}
await session.finish(finish_data)
```

Use sibling `setup_data` first, then
`mass.config.get_raw_provider_config_value(config.instance_id, key)`.

- [ ] **Step 4: Run setup tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_setup_flow.py -q`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add music_assistant/providers/qobuz_connect/setup_flow.py tests/providers/qobuz_connect/test_setup_flow.py
git commit -m "fix(qobuz_connect): isolate setup-owned configuration"
```

### Task 6: Fail-Safe BlackHole Audio Probe

**Files:**
- Modify: `tests/providers/qobuz_connect/protocol_capture/audio_probe.py`
- Test: `tests/providers/qobuz_connect/test_audio_probe.py`

**Interfaces:**
- Consumes: avfoundation device-list stderr and ffmpeg volumedetect stderr.
- Produces: exact unique device selection and exception-based capture failures.

- [ ] **Step 1: Write failing device/capture tests**

```python
def test_device_index_ignores_prefix_competitor(monkeypatch) -> None:
    stderr = "[0] BlackHole 16ch\\n[1] BlackHole 2ch\\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: CompletedProcess(a, 0, "", stderr))
    assert AudioProbe().device_index() == "1"

def test_missing_metrics_is_capture_failure() -> None:
    with pytest.raises(RuntimeError, match="volume metrics"):
        parse_volumedetect("n_samples: 0")
```

- [ ] **Step 2: Run audio-probe tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_audio_probe.py -q`

Expected: prefix can be selected and missing metrics become `-inf`.

- [ ] **Step 3: Implement exact unique selection and strict capture validation**

```python
_AVF_DEVICE = re.compile(r"^\\[(\\d+)\\]\\s*BlackHole 2ch\\s*$", re.IGNORECASE | re.MULTILINE)
matches = _AVF_DEVICE.findall(proc.stderr)
if len(matches) != 1:
    raise RuntimeError("Expected exactly one BlackHole 2ch avfoundation input")
```

Raise for nonzero capture return code, explicit zero samples, and absent mean/max
metrics.

- [ ] **Step 4: Run audio-probe tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_audio_probe.py -q`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add tests/providers/qobuz_connect/{protocol_capture/audio_probe.py,test_audio_probe.py}
git commit -m "test(qobuz_connect): fail closed on invalid audio capture"
```

### Task 7: Transactional Playwright Session Factory

**Files:**
- Modify: `tests/providers/qobuz_connect/protocol_capture/integration_harness.py`
- Test: `tests/providers/qobuz_connect/test_integration_harness_safety.py`

**Interfaces:**
- Consumes: `async_playwright().start()`, `_open_client()`.
- Produces: leak-free `open_session()` failure handling.

- [ ] **Step 1: Write failing launch and partial-client tests**

```python
async def test_open_session_stops_playwright_when_launch_fails() -> None:
    pw.chromium.launch.side_effect = RuntimeError("launch")
    with pytest.raises(RuntimeError, match="launch"):
        await open_session(ma)
    pw.stop.assert_awaited_once()

async def test_open_session_cancels_sibling_and_closes_all_owners() -> None:
    with pytest.raises(RuntimeError, match="client"):
        await open_session(ma)
    assert sibling.cancelled()
    browser.close.assert_awaited_once()
    pw.stop.assert_awaited_once()
```

- [ ] **Step 2: Run harness-safety tests and confirm RED**

Run: `pytest tests/providers/qobuz_connect/test_integration_harness_safety.py -q`

Expected: Playwright/browser/sibling client resources remain live on failures.

- [ ] **Step 3: Implement nested transactional cleanup**

```python
tasks = [asyncio.create_task(_open_client(...)), asyncio.create_task(_open_client(...))]
try:
    client, observer = await asyncio.gather(*tasks)
except BaseException:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    try:
        await browser.close()
    finally:
        await pw.stop()
    raise
```

Wrap browser launch separately so launch failure still stops Playwright.

- [ ] **Step 4: Run harness-safety tests and confirm GREEN**

Run: `pytest tests/providers/qobuz_connect/test_integration_harness_safety.py -q`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add tests/providers/qobuz_connect/{protocol_capture/integration_harness.py,test_integration_harness_safety.py}
git commit -m "test(qobuz_connect): clean partial browser sessions"
```

### Task 8: Deterministic and Live Verification

**Files:**
- Modify (ignored): `.superpowers/sdd/2026-07-30-qobuz-connect-upstream-compatibility/task-3-report.md`

**Interfaces:**
- Consumes: all completed implementation tasks.
- Produces: reproducible deterministic and real-cloud evidence.

- [ ] **Step 1: Run targeted provider suites**

Run: `pytest tests/providers/qobuz_connect tests/providers/qobuz tests/controllers/player_queues tests/controllers/players tests/providers/local_audio -q`

Expected: all pass.

- [ ] **Step 2: Run static validation**

Run: `.venv/bin/mypy music_assistant/providers/qobuz_connect tests/providers/qobuz_connect`

Expected: success with no errors.

- [ ] **Step 3: Run full pre-commit**

Run: `pre-commit run --all-files`

Expected: all hooks pass.

- [ ] **Step 4: Run the disposable 27-scenario real-cloud suite**

Record SHA-256 hashes of `.mass-data` and `.mass-cache`, verify the managed config
targets the exact unique BlackHole player, run the repository's integration command
for all scenarios, and verify the same hashes afterward.

Expected: 27/27 pass, seed hashes unchanged, no managed MA/Chromium/ffmpeg process,
owned port, or temporary run directory remains.

- [ ] **Step 5: Append evidence and commit any verification-only updates**

Append a `Whole-branch review fix round 1` section with commit range, focused RED/GREEN
commands, deterministic results, live result, before/after hashes, and cleanup
evidence. The report remains ignored and must not be force-added.
