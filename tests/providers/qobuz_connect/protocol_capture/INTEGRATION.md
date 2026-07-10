# Qobuz Connect live integration harness

Automated end-to-end tests that drive a **real Qobuz Web Client** (Playwright,
acting as the controller/phone) against the **real Qobuz cloud**, with a
**real Music Assistant** instance joined as the `Local Dev` Connect renderer.
This replaces hand-testing on physical hardware: each scenario resets to a
clean state, performs one controller action, and asserts MA's observable
behaviour — from its debug log **and from real audio output**.

Behaviour that is non-obvious is **grounded against real reference captures**
(`.runs/`), not guessed — e.g. a newly-active renderer auto-plays the current
track; loop/shuffle are controller-managed and never reach the renderer;
volume does reach the renderer.

## Real audio verification

The single most important capability: `audio_probe.py` captures the
**BlackHole loopback** with ffmpeg and measures its level, so a scenario can
prove audio *actually flowed* — the definitive check for "the UI says playing
but there is no sound". Calibrated on this machine: **-91 dB** when MA is idle
(digital silence) vs **~-44 dB** during playback; the default threshold is
**-70 dB**. Scenarios call `session.assert_sound(...)` where playback is
expected and `session.assert_silence(...)` for pause. Requires `ffmpeg` and the
`BlackHole 2ch` virtual device; assumes nothing else routes audio to BlackHole
during a run.

## Pieces

- `audio_probe.py` — captures/measures the BlackHole loopback (`AudioProbe`).
- `ma_probe.py` — manages (or attaches to) a live MA process, pins/restores the
  BlackHole target on managed start/stop, and parses the debug log into
  structured events (`ReduceTrace`, `StreamStart`, `Report`) with
  condition-based `wait_for_event`.
- `integration_harness.py` — one logged-in web client + the MA probe + an
  `AudioProbe`; `reset_to_clean_state()`, condition waits
  (`wait_for_stream`/`wait_for_sound`/`wait_for_silence`), audio assertions,
  and `ma_play_media`/`ma_player_command` (authenticated MA WS commands).
- `integration_scenarios/` — the scenario package: `app_driven`, `modes_volume`,
  `ma_driven`.
- `mint_ma_token.py` — one-time MA token minting for the MA-driven scenarios.
- `integration_run.py` — CLI runner.

## Prerequisites

1. Playwright + Chromium: `uv pip install --python .venv/bin/python playwright`
   then `.venv/bin/python -m playwright install chromium`.
2. `ffmpeg` on PATH and the `BlackHole 2ch` device installed.
3. A persisted Qobuz login at `.auth/client_a.json` (first run must be headed
   to log in once; see `harness.py`).
4. An MA data dir (`.mass-data`) with the `qobuz` and `qobuz_connect` providers
   configured. The managed runner pins the Connect target to BlackHole
   automatically and restores it on stop; when attaching to your own MA, pin
   `target_player` to BlackHole yourself so runs make no audible sound.
5. For MA-driven scenarios: an MA token —
   `MA_USER=<u> MA_PASS=<p> .venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.mint_ma_token`
   writes `.auth/ma_token`. Without it those scenarios skip cleanly.

## Running

Attach to an already-running MA (fastest for iteration):

```bash
.venv/bin/python -m music_assistant --data-dir .mass-data --cache-dir .mass-cache --log-level debug > /tmp/ma.log 2>&1 &
.venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.integration_run \
    --attach /tmp/ma.log --scenario all
```

Let the harness manage the MA process itself (pins/restores BlackHole):

```bash
.venv/bin/python -m tests.providers.qobuz_connect.protocol_capture.integration_run --scenario all
```

Run one scenario by name instead of `all`. Scenario runs are slow (real cloud
round-trips + 1-2 s audio captures); run individually while developing.

## Scenarios

Every scenario expecting playback also asserts **real audio**.

### App -> MA (web client drives, MA renders)

| Scenario | What it proves |
| --- | --- |
| `handoff_fresh` | Handoff makes MA active; it plays track 0 with sound. |
| `handoff_midtrack` | Handoff mid-track plays the SAME current track (not the next) with sound. |
| `handoff_paused` | Handoff while paused stays silent; resume produces sound. |
| `skip_next` / `skip_prev` | A skip/previous moves MA to the right track with sound; no play storm. |
| `play_new_album` / `play_new_track` | Loading new content on the app switches MA to it, with sound. |
| `seek_scrub` | An app seek keeps audio flowing and does not restart the track. |
| `queue_add` / `queue_reorder` | Queue edits reflect on MA without restarting audio. |
| `pause_resume` | Pause silences MA output; resume brings sound back. |

### Modes / volume (App -> MA)

| Scenario | What it proves |
| --- | --- |
| `shuffle_toggle` / `repeat_toggle` | Mode toggles never restart MA audio (modes don't reach the renderer). |
| `volume` | A controller volume change reaches MA (`MaSetVolume`); audio stays non-silent. |

Mute and autoplay-from-app are intentionally omitted: while a Connect renderer
is active, the Qobuz web "Mute"-labelled button toggles autoplay (not mute) and
there is no stable web autoplay control (see `QobuzPage.toggle_mute`).

### MA -> cloud (MA initiates, app follows; needs `MA_TOKEN`)

| Scenario | What it proves |
| --- | --- |
| `initiate_album_from_ma` / `initiate_track_from_ma` | Starting Qobuz content on MA plays it with sound and claims the cloud renderer role. |
| `ma_skip` / `ma_pause` | MA-side skip/pause behave (advance / silence-then-resume). |
| `ma_queue_edit` | An MA-side enqueue grows the cloud queue. |

These scenarios also serve as regression protection against future MA and
Qobuz cloud/protocol changes: when the cloud behaviour shifts, extend or
re-capture the reference in `.runs/` and adjust the grounded assertions.

## Note on phone-app-specific behaviour

The harness drives a real Qobuz *web client*. The native *phone app* can hand
off with a different message sequence; a bug seen only on the phone may not
reproduce here. When that happens, reproduce it once on the phone against a
debug-logging MA and analyse the `qobuz_connect` reduce/stream lines in the log
to ground a reducer test + fix.
