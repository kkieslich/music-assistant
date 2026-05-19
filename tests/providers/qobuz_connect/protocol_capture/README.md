# Qobuz Connect protocol-capture harness

This is a reusable Playwright-based tool that drives two real Qobuz Web Client
sessions and records **every WebSocket frame in both directions, with full
binary bytes preserved**. It is the **only** authoritative source of protocol
behavior for the `qobuz_connect` provider. Each scenario writes a fresh
`.runs/<scenario>__client_a.json` and `…__client_b.json` pair containing both
directions.

The harness is **not** part of the normal test suite. It is excluded from
pytest collection (see `tests/providers/qobuz_connect/conftest.py`) so it
never runs accidentally in CI.

## When to use it

- You're working on the `qobuz_connect` provider and need to see what the
  Qobuz cloud actually sends back for a given user action.
- You're validating a decoder branch (Phase B of the redesign plan).
- You're investigating a protocol-level bug that only reproduces with the
  real Qobuz cloud.

## One-time setup

This harness depends on Playwright, which is a developer-only dep specific
to this tool — it is *not* a `qobuz_connect` runtime requirement and is
intentionally not declared in `pyproject.toml` or `manifest.json`. Install
it manually into your existing MA venv:

```bash
source .venv/bin/activate
uv pip install 'playwright==1.59.0'
playwright install chromium
```

If you ever need a clean reinstall, repeat both commands.

Qobuz Connect routes between two clients on the same account by design, so
both browser contexts use the same login — that's exactly the traffic we
want to capture. The harness keeps each context in its own `.auth/client_*.json`
storage-state file so cookies don't collide.

## First run (manual login)

```bash
source .venv/bin/activate
python -m tests.providers.qobuz_connect.protocol_capture.run \
    --scenario handoff --headed
```

`--headed` shows two Chromium windows. The harness handles cookie banners
automatically and then **waits up to 5 minutes for you to log in by hand**
in each window. Once it detects the player UI it persists
`.auth/client_a.json` / `.auth/client_b.json`. From that point on the
scenario continues automatically and subsequent runs are headless.

If a selector inside the scenario fails (the Qobuz web app's DOM isn't
publicly documented and the page-object guesses are marked
`# SELECTOR-NEEDS-VERIFICATION` in [qobuz_page.py](qobuz_page.py)), re-run
with `--slow-mo 500` and set `PWDEBUG=1` to open the Playwright Inspector
for verifying or tweaking the locator interactively.

## Subsequent runs (headless)

```bash
python -m tests.providers.qobuz_connect.protocol_capture.run \
    --scenario queue_mutations
```

Captures land in `.runs/<scenario>__client_a.json` and `…__client_b.json`.

## Scenarios

| Scenario                              | What it does                                                                       | Protocol coverage focus                                              |
|---------------------------------------|------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `handoff`                             | A plays → A hands off to B → B skips to next                                       | `srvrRndrSetActive` inbound on B                                     |
| `queue_mutations`                     | B clear → B play → +2 adds → pause → resume → reorder current +3                  | `srvrCtrl*` queue deltas inbound                                     |
| `rapid_skip`                          | B (self-controls) plays → 5× skip-next rapidly                                     | Baseline self-controlling burst (does NOT exercise renderer burst)   |
| `quality_change`                      | B plays → switches max quality mid-track                                           | `srvrRndrSetMaxAudioQuality` inbound                                 |
| `controller_burst_skip`               | A controls + B renders → A presses skip 5× rapidly                                 | True `srvrRndrSetState` burst on B — reconcile / staleness pressure  |
| `controller_playing_seek_scrub`       | A controls + B renders → A scrubs progress bar through 4 positions while playing  | Playing-seek debounce; do multiple `srvrRndrSetState` arrive at all? |
| `controller_paused_scrub_then_skip`   | A controls + B renders → pause → scrub-while-paused → skip-next BEFORE resume     | Does the reference renderer get a paused-seek command at all?        |
| `controller_burst_skip_throttled`     | Same as `controller_burst_skip` with B throttled (200ms RTT + 100kbps both ways)  | Burst under slow renderer (out-of-order arrivals; reconcile stress)  |
| `controller_play_pause_rapid`         | A controls + B renders → 6× rapid play/pause toggles                              | State-flapping reconciliation                                        |
| `controller_skip_then_seek`           | A controls + B renders → A skips next, waits ~150ms, then seeks to 60%           | Track-change reconcile vs. immediately-following playing seek        |
| `controller_natural_track_advance`    | A controls + B renders → plays a *short* album, waits through track 1 → 2 advance | What does the cloud emit on auto-advance? Are bare position-only updates real? |

`.runs/` is gitignored — captures contain personal Qobuz JWT auth tokens.
If you ever need to commit a capture as a fixture, strip the auth bytes
from the AUTHENTICATE frame (frame index 0 in most captures): zero out
the JWT payload bytes and update the recorded `size` field first.

## Schema

Output JSON shape — `version`, `exportDate`, `statistics`, `eventTypes`,
`messages[]` with each message holding a sparse byte-dict in `data`. Both
incoming and outgoing binary frames carry their full bytes.

## Layout

```
protocol_capture/
├── harness.py            Two-client browser session manager
├── ws_recorder.py        CDP listener that writes the capture JSON
├── qobuz_page.py         Page-object wrapping the Qobuz Web Client UI
├── scenarios/            One module per recorded user-flow
└── run.py                CLI entry point
```

## Env-var reference

| Variable                        | Purpose                                                              |
|---------------------------------|----------------------------------------------------------------------|
| `QOBUZ_CAPTURE_TRACK_QUERY`     | Search string used to pick a track (default: a Daft Punk track)      |
| `QOBUZ_CAPTURE_TRACK_QUERY_2/3` | Additional track queries for the multi-track scenarios               |
| `QOBUZ_CAPTURE_CONNECT_TARGET`  | Label of the Qobuz Connect handoff target (default: "this browser")  |
| `QOBUZ_CAPTURE_QUALITY_LABEL`   | Quality option label for the `quality_change` scenario               |
| `QOBUZ_CAPTURE_SKIP_COUNT`         | Number of skip-next presses in `rapid_skip` / `controller_burst_skip*` (default 5) |
| `QOBUZ_CAPTURE_SKIP_INTERVAL_MS`   | Delay between presses in the same scenarios (default 250)            |
| `QOBUZ_CAPTURE_TOGGLE_COUNT`       | Number of toggles in `controller_play_pause_rapid` (default 6)       |
| `QOBUZ_CAPTURE_TOGGLE_INTERVAL_MS` | Delay between toggles in the same scenario (default 400)             |
| `QOBUZ_CAPTURE_THROTTLE_LATENCY_MS`| Per-request latency injected on B in `controller_burst_skip_throttled` (default 200) |
| `QOBUZ_CAPTURE_THROTTLE_DOWN_KBPS` | Download cap on B in `controller_burst_skip_throttled` (default 100) |
| `QOBUZ_CAPTURE_THROTTLE_UP_KBPS`   | Upload cap on B in `controller_burst_skip_throttled` (default 100)   |
| `QOBUZ_CAPTURE_NATURAL_ADVANCE_REMAINING_S` | Seconds left on track when `controller_natural_track_advance` jumps near end (default 8) |
| `QOBUZ_CAPTURE_NATURAL_ADVANCE_WAIT_S`   | Idle wait after the jump (default 30s)                             |
| `CAPTURE_HEADED`                   | Same as `--headed`; convenience for shell aliases                    |
| `CAPTURE_SLOW_MO_MS`               | Same as `--slow-mo`                                                  |
| `PWDEBUG`                          | Standard Playwright; opens the Inspector for selector tweaking       |

Login itself is *not* env-var-driven — you log in manually in each browser
window on first run, and the harness saves storage_state for headless reuse.

## Legal & privacy

Captures contain your personal Qobuz JWT auth tokens (in the AUTHENTICATE
frame) and may contain track metadata personal to your account. **Strip
auth tokens before committing**, and do not redistribute captures that
include them. The `.auth/` directory is gitignored for the same reason.
