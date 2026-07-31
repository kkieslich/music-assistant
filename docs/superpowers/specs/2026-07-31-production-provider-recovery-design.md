# Production Provider Recovery Design

## Goal

Restore the deployed Qobuz Connect and AirPlay Receiver providers, close the
configuration-migration and image-packaging gaps that caused the outage, and
verify the rebuilt fork image on the real Music Assistant host.

## Confirmed root causes

The deployed Qobuz Connect instance predates provider setup flows. Its raw
configuration contains runtime quality and target values but no explicit native
Qobuz provider selection. The disposable integration harness supplied that
selection in its copied setup data, so strict startup validation passed locally
but rejected the real instance.

The fork image workflow independently pins Music Assistant base image `1.5.1`.
Current upstream `dev` uses base image `1.6.0`, which supplies the Linux
`shairport-sync` binary required by AirPlay Receiver. The fork image therefore
built successfully without the required Linux binary.

## Configuration recovery

Add Qobuz Connect's setup-owned legacy keys to MA's existing provider setup-data
migration: native Qobuz provider, target player, publish name, HTTP port, and
initial volume. Existing setup data always wins over legacy values.

For legacy configurations with no persisted native-provider selection, Qobuz
Connect will inspect configured native Qobuz instances during awaited startup.
If exactly one valid and available instance exists, it will select that exact
instance and persist the selection into encrypted setup data. Zero or multiple
valid candidates remain a hard failure with an actionable reconfigure message;
the provider must never silently select between multiple accounts.

Tests will use realistic raw provider configuration and cover migration,
single-instance recovery and persistence, zero candidates, multiple candidates,
disabled candidates, and exact-account selection.

## Image recovery

Remove the duplicated base-image version from the fork workflow. The workflow
will derive its build argument from `BASE_IMAGE_VERSION_NIGHTLY` in the checked-in
upstream release workflow, making that file the single version source for `dev`.
The build will fail closed unless `/usr/local/bin/shairport-sync` exists and is
executable in the final Linux image. Existing architecture-specific `cliairplay`
checks remain in place.

Tests or deterministic workflow checks will cover version extraction and the
required final-image assertion so another upstream base bump cannot silently
produce an incomplete fork image.

## Verification and rollout

Implementation follows RED/GREEN TDD, then the relevant Qobuz Connect/config
migration tests, mypy, and all pre-commit hooks. The rebuilt amd64 image must be
inspected before deployment to prove both `shairport-sync` and the expected code
are present.

After pushing `dev` and waiting for CI, update only the existing
`music-assistant-server` deployment on `kolja@192.168.1.20`. Preserve `/data`,
capture the current image digest for rollback, and verify after restart:

- the migrated Qobuz Connect instance loads against the exact native Qobuz
  instance and remains loaded;
- AirPlay Receiver reloads without a binary error;
- the container contains executable Linux `shairport-sync`;
- the web service is healthy and no new provider traceback appears;
- persisted setup data contains the recovered Qobuz selection and source
  credentials remain untouched.

If the new image fails these checks, restore the captured previous image digest
without changing persistent configuration.

## Scope

No queue, playback, quality, discovery, or AirPlay streaming behavior is being
redesigned. Changes are limited to backward-compatible setup-data migration,
single-instance legacy recovery, fork image version selection, packaging
validation, regression tests, and the authorized deployment verification.
