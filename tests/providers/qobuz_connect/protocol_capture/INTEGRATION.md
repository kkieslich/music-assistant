# Qobuz Connect live integration harness

Automated end-to-end tests that drive a **real Qobuz Web Client** (Playwright,
acting as the controller/phone) against the **real Qobuz cloud**, with a
**real Music Assistant** instance joined as the `Local Dev` Connect renderer.
This replaces hand-testing on physical hardware: each scenario resets to a
clean state, performs one controller action, and asserts MA's observable
behaviour from its debug log.

Behaviour that is non-obvious is **grounded against real reference captures**
(`.runs/`), not guessed — e.g. a newly-active renderer auto-plays the current
track, loop/shuffle are controller-managed and never reach the renderer,
volume does reach the renderer. See `integration_scenarios.py` docstrings for
the specific captures each assertion is based on.

## Pieces

- `ma_probe.py` — manages (or attaches to) a live MA process and parses its
  debug log into structured, assertable events (`ReduceTrace`, `StreamStart`,
  `Report`).
- `integration_harness.py` — one logged-in web client + the MA probe, with
  `reset_to_clean_state()` (routes playback back to the browser so MA
  deactivates, then loads a known queue) and `ma_play_media()` (initiate from
  MA via the MA WebSocket API).
- `integration_scenarios.py` — the scenarios and their assertions.
- `integration_run.py` — CLI runner.

## Prerequisites

1. Playwright + Chromium: `uv pip install --python .venv/bin/python playwright`
   then `.venv/bin/python -m playwright install chromium`.
2. A persisted Qobuz login at `.auth/client_a.json` (first run must be headed
   to log in once; see `harness.py`).
3. An MA data dir (`.mass-data`) with the `qobuz` and `qobuz_connect`
   providers configured, and the **Connect target pinned to a silent player**
   (e.g. `BlackHole 2ch`) so integration runs make no audible sound. Pin it in
   the `qobuz_connect` provider's `target_player` config value.

## Running

Attach to an already-running MA (fastest for iteration):

```bash
.venv/bin/python -m music_assistant --data-dir .mass-data --cache-dir .mass-cache --log-level debug > /tmp/ma.log 2>&1 &
.venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.integration_run \
    --attach /tmp/ma.log --scenario all
```

Let the harness manage the MA process itself:

```bash
.venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.integration_run --scenario all
```

## Scenarios

| Scenario | What it proves |
| --- | --- |
| `handoff` | Handoff makes MA the active renderer and it plays the current track. |
| `skip_next` | A controller skip advances MA to the next track (no play storm). |
| `skip_twice` | Handoff then two skips — MA follows both and stays playing. |
| `new_album_after_handoff` | Loading a different album on the app switches MA's playback to it. |
| `modes_dont_disturb` | Loop/shuffle toggles never restart MA's audio. |
| `volume` | A controller volume change reaches MA and is applied. |
| `initiate_from_ma` | Starting Qobuz playback on MA claims the cloud renderer role. Requires `MA_TOKEN` (an MA access token); skips otherwise. |

These scenarios also serve as regression protection against future MA and
Qobuz cloud/protocol changes: when the cloud behaviour shifts, extend or
re-capture the reference in `.runs/` and adjust the grounded assertions.
