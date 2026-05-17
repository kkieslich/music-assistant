# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Music Assistant is an async Python music library manager that connects to streaming services and speakers, integrating with Home Assistant.

## Fork Focus: `qobuz_connect` Provider

This fork's primary purpose is the experimental [qobuz_connect](music_assistant/providers/qobuz_connect/) plugin provider — a Qobuz Connect *receiver* that exposes MA as a Qobuz Connect target so that the Qobuz mobile/desktop apps can hand off playback to an MA player. Unlike the DLNA-bridge approach (rejected for latency/seeking), this provider re-implements the Connect protocol locally and routes playback through the native MA Qobuz music provider + MA player queue.

Data flow: `Qobuz app  →  Qobuz Connect protocol (this provider)  →  MA player queue  →  native MA qobuz provider`.

### qobuz_connect module layout

- [discovery.py](music_assistant/providers/qobuz_connect/discovery.py) — mDNS service (`_qobuz-connect._tcp.local.`) + local aiohttp endpoints the Qobuz app hits during handshake; produces a `ConnectTokens` to hand to the session.
- [session.py](music_assistant/providers/qobuz_connect/session.py) — Qobuz Connect WebSocket lifecycle (connect / auth / heartbeat / reconnect) and dispatch of decoded frames to provider callbacks.
- [protocol.py](music_assistant/providers/qobuz_connect/protocol.py) — `QobuzConnectCodec`: encode/decode outer frames and inner protobuf messages.
- [sync.py](music_assistant/providers/qobuz_connect/sync.py) — `QobuzConnectSyncEngine`, the single owner of bidirectional state reconciliation between the Qobuz cloud's queue/state and MA's player queue (set-state, queue load/ack, seek, volume, quality, origin tracking).
- [models.py](music_assistant/providers/qobuz_connect/models.py) — enums + dataclasses shared across the above (quality maps, queue refs, state events).
- [`__init__.py`](music_assistant/providers/qobuz_connect/__init__.py) — `QobuzConnectProvider` (PluginProvider): wires config, lifecycle, target-player resolution, and persists Qobuz app quality changes back into both this provider's config and the underlying qobuz music provider's `CONF_QUALITY`.

### qobuz_connect protobuf + reference captures

- `.proto` sources live in [proto/definition/](music_assistant/providers/qobuz_connect/proto/definition/); generated `*_pb2.py` are committed in [proto/](music_assistant/providers/qobuz_connect/proto/) and imported directly — do not delete the generated files.
- [proto/captured/](music_assistant/providers/qobuz_connect/proto/captured/) contains raw WebSocket traffic from two real Qobuz clients (QWeb + QMac) used to reverse-engineer queue/favorite/reorder behavior. Treat these JSON captures as authoritative reference data when interpreting unknown protocol fields; see [proto/captured/README.md](music_assistant/providers/qobuz_connect/proto/captured/README.md) for the scenarios each capture covers.

### qobuz_connect dev notes

- The provider is `stage: experimental` and `multi_instance: true`. The mDNS-advertised serial and Qobuz cloud device UUID are both derived from `instance_id` via a fixed namespace UUID so they stay stable across restarts (otherwise the Qobuz app shows duplicates).
- Target player resolution: `CONF_TARGET_PLAYER = "__auto__"` prefers any currently-playing player, else first available. A misconfigured/missing pinned player logs a warning rather than failing setup.
- The provider requires the native `qobuz` music provider to be configured — `get_qobuz_provider()` raises `InvalidDataError` otherwise.
- Tests: [tests/providers/qobuz_connect/](tests/providers/qobuz_connect/) — `test_protocol.py` covers codec round-trips, `test_sync.py` covers the sync engine state machine using fake MA player/queue doubles. Run with `pytest tests/providers/qobuz_connect/`.
- Local playground (from the provider README): `.venv/bin/python -m music_assistant --data-dir .mass-data --cache-dir .mass-cache --log-level debug`. The `.mass-data` / `.mass-cache` dirs are git-ignored because they hold tokens.

## Behaviour

- NEVER automatically reply on Github (PR's or Discussions) without explicit consent from the developer.

## Development Commands

- `scripts/setup.sh` - Initial setup (venv, dependencies, pre-commit hooks). Re-run after pulling latest code.
- `pytest` - Run all tests
- `pytest tests/specific_test.py` - Run a specific test file
- `pre-commit run --all-files` - Run all pre-commit hooks
- `python -m music_assistant --log-level debug` - Run server locally (localhost:8095)
- Requires ffmpeg v6.1+ and Python 3.14+ (see `.python-version` for the pinned runtime)

Always run `pre-commit run --all-files` after a code change to ensure the new code adheres to the project standards.

## Provider Development

Providers are modular: music (sources), player (speakers), metadata (art/lyrics), plugin (extras). See `_demo_*_provider` directories for annotated templates when creating new providers.

Each provider has at least `__init__.py` (logic) and `manifest.json` (metadata/config schema).

Check `helpers/` for reusable utilities before writing new ones.

When fixing sync/queue issues in a "connect"-style plugin provider, look first at sibling MA providers (e.g. `plex_connect`, `spotify_connect`) for established patterns before diving into third-party library internals.

## Code Style

### Comments

Only use comments to explain complex, multi-line blocks of code. Do not comment obvious operations.

### Docstring Format

Use Sphinx-style docstrings with `:param:` syntax. For simple functions, a single-line docstring is fine.
Don't explain inner workings of the code in the docstrings (you can use inline comments for that if/when needed). The docstring should provide clarity to the caller of the function/method, not explain how it works technically/internally.

```python
def my_function(param1: str, param2: int, param3: bool = False) -> str:
    """
    Brief one-line description of the function.

    :param param1: Description of what param1 is used for.
    :param param2: Description of what param2 is used for.
    :param param3: Description of what param3 is used for.
    """
```

Do **not** use Google-style (`Args:`) or bullet-style (`- param:`) docstrings.

## Branching and PRs

- This repo is a fork. `dev` is the working branch for local development of `qobuz_connect`; upstream PRs against `music-assistant/server` are uncertain since Qobuz Connect is unofficial/reverse-engineered.
- Upstream rule (for any PR that does target upstream): PRs target `dev` (primary development branch). `stable` is for production releases. PRs labeled `bugfix` + `backport-to-stable` are automatically backported to `stable` — use only for bugs also present in `stable`.

## Debugging

MA stores its data in `$HOME/.musicassistant/` by default — but for this fork the playground is typically run with `--data-dir .mass-data --cache-dir .mass-cache` (see qobuz_connect dev notes), so logs and the SQLite DB live under those local directories instead.

- **Logs:** `musicassistant.log` (current), `musicassistant.log.1`, `.log.2`, etc. for older rotated logs — inside whichever data-dir is active.
- **Database:** `library.db` in the active data-dir — query via `sqlite3`. **Only execute SELECT queries** — never write to a live database.
