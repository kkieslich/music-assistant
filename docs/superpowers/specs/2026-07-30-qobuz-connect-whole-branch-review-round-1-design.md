# Qobuz Connect Whole-Branch Review Round 1 Design

## Goal

Close all ten verified review findings without broad refactors, while preserving the
existing Qobuz Connect fallback behavior and making startup, target ownership,
quality ownership, live-audio verification, and capture-harness cleanup fail safe.

## Lifecycle and native Qobuz dependency

The provider manifest will declare `depends_on: "qobuz"` so Music Assistant delays
loading Connect receivers until at least one native Qobuz instance is available and
automatically retries dependants when Qobuz loads.

`QobuzConnectProvider.handle_async_init()` will validate the exact configured native
Qobuz instance and start all resources that must exist before the provider is marked
available: the flight recorder, heartbeat reporter, MA event subscriptions, HTTP
discovery endpoint, and mDNS registration. Missing, disabled, unavailable, or
wrong-domain selected instances fail initialization with `InvalidDataError`.
`loaded_in_mass()` will only start the eager cloud WebSocket after registration.

Initialization is transactional. If any step fails, already-started resources are
unsubscribed or stopped in reverse ownership order. Discovery startup and shutdown
are independently exception safe: a TCP listener cannot survive failed mDNS
registration, and one cleanup failure cannot skip later cleanup.

## Active target ownership

The provider's active target pin is the single target lease shared by MA effects,
coordinator release, event filtering, reporting, and autoplay suppression.

- On activation, the resolved configured/automatic target is pinned.
- Other players starting and configured targets returning do not redirect an active
  session.
- If the pinned player disappears, the provider resolves one replacement and moves
  coordinator ownership and the autoplay lease to it before further target-bound
  work.
- Deactivation, ownership loss, disconnect, and unload release the authoritative
  target, restore its captured autoplay setting, and clear the pin.

This retains the existing missing-player fallback instead of failing the whole
Connect session. It avoids the larger alternative of moving all player resolution
into the reducer/coordinator.

## Native quality ownership

An explicit Connect maximum-quality setting is reconciled to the selected native
Qobuz instance when that receiver becomes active. `AUTO` never writes native
quality; it adopts the selected native provider's current value.

Multiple Connect receivers may share a native Qobuz instance. The last explicitly
configured receiver to become active owns the native streaming-quality setting.
Inactive receivers retain their own advertised ceiling and reconcile it only on
their next activation. This makes ownership deterministic without global
cross-instance state.

## Foreign queue-item handling

The MA transport event will distinguish “no current item” from “a current item that
cannot map to the selected Qobuz provider.” While Connect owns the renderer, an
unmappable current item is a loss of queue ownership: canonical transport becomes
stopped with no current Qobuz id, phantom position/duration reporting ends, and the
owned player is released through the existing release effect. Transient MA stopped
events for a still-mapped Qobuz item keep their existing settling behavior.

## Setup-flow persistence and port selection

The setup form may merge runtime values with setup data for prefilling, but
`session.finish()` will receive only the five setup-owned keys:
`qobuz_provider`, `target_player`, `publish_name`, `http_port`, and
`initial_volume`.

Sibling port selection will prefer each sibling's current `setup_data`, then read
the upstream raw provider config for legacy top-level/`values` storage. It will not
depend on `ProviderConfig.get_value()` from a values-omitting list response.

## Live verification safety

The audio probe will accept exactly one avfoundation input whose complete device
name is `BlackHole 2ch`. Prefix matches and ambiguous duplicate exact matches are
errors. ffmpeg nonzero exit, no captured samples, or absent mean/max volume metrics
are capture failures, never silence.

The Playwright integration-session factory will be transactional. Failure to launch
Chromium stops Playwright. Failure in either client opener cancels and awaits its
sibling, closes the browser (including any partial contexts), and stops Playwright.
Cleanup attempts remain nested so one failure does not skip later owners.

## Testing and verification

Every behavior change starts with a focused failing test and is implemented only
after the failure is observed. Coverage includes:

- exact selected native instance: missing, disabled/unavailable, wrong domain, and
  delayed load order;
- occupied HTTP port and primary/fallback mDNS failure with no listeners, tasks,
  subscriptions, or partial discovery state;
- target disappearance, configured-target return, deactivation/disconnect,
  autoplay transfer/restoration, and exact release target;
- restart quality divergence and two receivers sharing one native instance;
- transition from an owned Qobuz item to a foreign queue tail;
- legacy and current sibling port storage plus setup finish key filtering;
- exact/ambiguous BlackHole discovery and every ffmpeg capture failure mode;
- Playwright launch and partial-client failure paths.

After focused suites, run the deterministic Qobuz/Qobuz Connect/player queue/player/
local-audio test set, mypy, and full pre-commit. Because the changes affect live
playback and safety, finish with the disposable 27-scenario real-cloud suite using
the exact BlackHole target, recording the seed-data hash before and after and
verifying process, port, and temporary-directory cleanup.
