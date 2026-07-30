# Qobuz Connect Upstream Compatibility Design

## Goal

Preserve Qobuz Connect behavior after rebasing onto the current Music Assistant
`dev` branch, including fresh provider setup, existing configurations, and queue
ownership under MA's new default-autoplay behavior.

## Provider configuration

Qobuz Connect will follow MA's setup-flow contract:

- The setup/reconfigure flow owns the selected native Qobuz instance, target
  player, advertised name, discovery port, and fallback initial volume.
- The loaded provider exposes maximum quality as its runtime option because that
  setting is already applied dynamically without a reload.
- Provider construction reads setup data first and falls back to legacy provider
  option values. Existing installations therefore retain their current settings;
  the next successful reconfigure naturally writes the new setup-data form.
- Setup and options use shared entry-building helpers so defaults, choices, and
  port allocation cannot drift.

The flow must reject an unusable configuration through MA's normal
`SetupFlowError` path and re-render the submitted values.

## Autoplay ownership

Qobuz cloud autoplay remains the only continuation authority while this renderer
is active. On activation, the provider records the target queue's current MA
autoplay value and disables MA autoplay before applying cloud playback effects.
On deactivation or unload, it restores that recorded value.

Restoration is tied to the exact player whose setting was captured. Repeated
activation is idempotent, and a missing queue/player is tolerated. A target that
vanishes cannot cause restoration to a different player.

## Queue compatibility

Existing queue commands remain API-compatible with upstream. Reordering continues
to reuse `QueueItem` identities so current audio is not restarted. The live
integration suite will cover app-side reorder and subsequent playback movement;
no queue-index implementation change will be made unless that evidence fails.

## Verification

Automated tests will cover:

- setup-flow fields and submitted-value preservation;
- loaded-provider runtime quality entries;
- legacy option fallback and setup-data precedence;
- autoplay capture, disable, idempotence, restoration, unload, and missing-player
  handling;
- the existing deterministic Qobuz Connect suite and relevant upstream provider,
  queue, and player suites.

The final verification will run formatting/static checks and the managed,
BlackHole-pinned two-client integration harness against the real Qobuz cloud and
a disposable copy of the MA data directory. The checked-out and remote `dev`
branches will not be rewritten until these checks pass.
