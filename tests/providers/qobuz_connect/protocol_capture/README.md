# Qobuz Connect protocol-capture harness

This is a reusable Playwright-based tool that drives two real Qobuz Web Client
sessions and records **every WebSocket frame in both directions, with full
binary bytes preserved**. It exists because the original capture files in
[../proto/captured/](../../../../music_assistant/providers/qobuz_connect/proto/captured/)
were exported by a Chrome extension that dropped the incoming binary payload
— so the entire `SRVR_*` side of the protocol is invisible there.

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

```bash
source .venv/bin/activate
uv pip install -e ".[qobuz-connect-capture]"
playwright install chromium
```

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

| Scenario          | What it does                                                                                       | Protocol coverage focus                                       |
|-------------------|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `handoff`         | A plays a track → A hands off to B → B skips to next                                              | `SRVR_RNDR_SET_ACTIVE`, `SRVR_RNDR_SET_STATE` inbound on B   |
| `queue_mutations` | B clear → B play → +2 adds → pause → resume → reorder current +3                                 | `SRVR_CTRL_QUEUE_*` (cleared/added/reordered) inbound        |
| `rapid_skip`      | B plays → 5× skip-next in fast succession                                                          | Burst-command reconciliation (overlapping `SET_STATE` frames) |
| `quality_change`  | B plays → switches max quality mid-track                                                           | `SRVR_RNDR_SET_MAX_AUDIO_QUALITY` inbound                    |

Promote a `.runs/*.json` capture into the committed reference set by moving
it to
[../proto/captured/full/](../../../../music_assistant/providers/qobuz_connect/proto/captured/full/) —
this folder is the ground-truth source for Phase B test fixtures. **Before
committing, strip Qobuz auth tokens** from the AUTHENTICATE frame (frame
index 0 in most captures): replace the JWT payload bytes with zeros and
update the recorded `size` field.

## Schema

Output JSON matches the existing
[capture-*.json](../../../../music_assistant/providers/qobuz_connect/proto/captured/)
shape — `version`, `exportDate`, `statistics`, `eventTypes`, `messages[]`
with each message holding a sparse byte-dict in `data`. The only meaningful
difference is that incoming binary frames are no longer empty.

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
| `QOBUZ_CAPTURE_SKIP_COUNT`      | Number of skip-next presses in `rapid_skip` (default 5)              |
| `QOBUZ_CAPTURE_SKIP_INTERVAL_MS`| Delay between presses in `rapid_skip` (default 250)                  |
| `CAPTURE_HEADED`                | Same as `--headed`; convenience for shell aliases                    |
| `CAPTURE_SLOW_MO_MS`            | Same as `--slow-mo`                                                  |
| `PWDEBUG`                       | Standard Playwright; opens the Inspector for selector tweaking       |

Login itself is *not* env-var-driven — you log in manually in each browser
window on first run, and the harness saves storage_state for headless reuse.

## Legal & privacy

Captures contain your personal Qobuz JWT auth tokens (in the AUTHENTICATE
frame) and may contain track metadata personal to your account. **Strip
auth tokens before committing**, and do not redistribute captures that
include them. The `.auth/` directory is gitignored for the same reason.
