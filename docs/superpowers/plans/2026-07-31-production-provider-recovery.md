# Production Provider Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore legacy Qobuz Connect activation and package Linux `shairport-sync` in fork `dev` images, then deploy and verify both providers on the real host.

**Architecture:** Extend MA's existing setup-data migration for stored legacy fields and add a narrowly scoped single-native-Qobuz recovery path during awaited provider initialization. Make the fork workflow derive its base version from upstream's checked-in nightly version and make the Dockerfile fail closed when the receiver binary is absent.

**Tech Stack:** Python 3.14, pytest, Music Assistant setup/config APIs, GitHub Actions, Docker Buildx, SSH/Docker.

## Global Constraints

- Existing setup data always wins over legacy values.
- Automatically recover only when exactly one valid, available native Qobuz instance exists.
- Never silently choose between multiple Qobuz accounts.
- Do not alter queue, playback, quality, discovery, or AirPlay streaming behavior.
- Preserve the production `/data` volume and record the prior image digest for rollback.
- Do not expose Qobuz credentials, setup encryption keys, or tokens in logs or test output.

---

### Task 1: Recover legacy Qobuz Connect configuration

**Files:**
- Modify: `music_assistant/controllers/config/migrations.py`
- Modify: `music_assistant/providers/qobuz_connect/__init__.py`
- Modify: `tests/controllers/config/test_migrations.py`
- Modify: `tests/providers/qobuz_connect/test_provider_wiring.py`

**Interfaces:**
- Consumes: `Provider._update_setup_data(key, value, immediate=True)` and `mass.config.get_provider_configs(provider_domain="qobuz")`
- Produces: legacy setup-data migration and `_recover_legacy_qobuz_provider() -> None`

- [ ] **Step 1: Add failing raw-config migration tests**

Add a realistic `qobuz_connect` config containing legacy setup-owned values and assert that `migrate_provider_setup_data` encrypts strings, moves all five setup keys, preserves existing setup data, leaves `max_quality` in runtime values, and is idempotent.

- [ ] **Step 2: Run the migration tests and verify RED**

Run:

```bash
.venv/bin/pytest tests/controllers/config/test_migrations.py -q --no-cov
```

Expected: the new Qobuz Connect migration assertion fails because the domain is absent from `PROVIDER_SETUP_FLOW_KEYS`.

- [ ] **Step 3: Register the five setup-owned keys**

Add this exact mapping entry:

```python
"qobuz_connect": (
    "qobuz_provider",
    "target_player",
    "publish_name",
    "http_port",
    "initial_volume",
),
```

- [ ] **Step 4: Add failing single-instance recovery tests**

Add `test_init_recovers_and_persists_sole_legacy_qobuz_provider`, asserting
that an instance constructed without `CONF_QOBUZ_PROVIDER` selects
`qobuz--only` and persists it through
`_update_setup_data(CONF_QOBUZ_PROVIDER, "qobuz--only", immediate=True)`.
Add `test_init_rejects_ambiguous_legacy_qobuz_providers`, asserting an
`InvalidDataError` containing `reconfigure` when two exact available Qobuz
instances exist. Also assert zero candidates, disabled-only candidates, an
exact existing selection, and multiple configured candidates are handled
without fallback.

- [ ] **Step 5: Run focused recovery tests and verify RED**

Run only the new provider tests. Expected: the sole-provider case raises the existing “specific instance” error.

- [ ] **Step 6: Implement minimal legacy recovery**

Before strict validation in `handle_async_init`, call a private async helper only when `_qobuz_provider_id` is absent. Read configured Qobuz instances, resolve each exact loaded provider with `return_unavailable=True`, retain only exact-domain available instances, require exactly one, assign its instance ID, and persist it with `_update_setup_data(CONF_QOBUZ_PROVIDER, selected_instance_id, immediate=True)`. Raise an actionable `InvalidDataError` for zero or multiple candidates.

- [ ] **Step 7: Run Task 1 tests and commit**

Run:

```bash
.venv/bin/pytest tests/controllers/config/test_migrations.py \
  tests/providers/qobuz_connect/test_provider_wiring.py -q --no-cov
.venv/bin/mypy music_assistant/controllers/config/migrations.py \
  music_assistant/providers/qobuz_connect tests/controllers/config/test_migrations.py \
  tests/providers/qobuz_connect/test_provider_wiring.py
```

Commit only Task 1 files with `fix(qobuz_connect): recover legacy provider selection`.

### Task 2: Make the fork image packaging fail closed

**Files:**
- Create: `scripts/resolve_base_image_version.py`
- Create: `tests/scripts/test_resolve_base_image_version.py`
- Modify: `.github/workflows/fork-build.yml`
- Modify: `Dockerfile`

**Interfaces:**
- Produces: `resolve_base_image_version(path: Path, channel: str = "NIGHTLY") -> str`
- Consumes: `.github/workflows/release.yml` key `BASE_IMAGE_VERSION_NIGHTLY`

- [ ] **Step 1: Add failing version-resolution and Docker contract tests**

Tests must prove the helper extracts `1.6.0`, rejects missing/duplicate/malformed keys, the fork workflow calls the helper rather than containing a numeric base pin, and the Dockerfile asserts executable `/usr/local/bin/shairport-sync` in the final stage.

- [ ] **Step 2: Run the new script tests and verify RED**

Run:

```bash
.venv/bin/pytest tests/scripts/test_resolve_base_image_version.py -q --no-cov
```

Expected: import failure because the helper does not exist.

- [ ] **Step 3: Implement the resolver and workflow wiring**

The helper reads the release workflow as text, matches exactly one quoted `BASE_IMAGE_VERSION_<CHANNEL>` value, validates a three-component numeric version, and prints it from its CLI. The workflow writes its result to `GITHUB_ENV` before Buildx and retains `BASE_IMAGE_VERSION=${{ env.BASE_IMAGE_VERSION }}` as the build argument.

- [ ] **Step 4: Add the final-image binary assertion**

After the final `FROM`, add a build step that requires:

```dockerfile
RUN test -x /usr/local/bin/shairport-sync
```

This check must execute in each target architecture's final image.

- [ ] **Step 5: Run Task 2 tests and commit**

Run the new test module, Ruff, and `git diff --check`. Commit Task 2 files with `fix(build): track upstream base image requirements`.

### Task 3: Verify, publish, deploy, and prove recovery

**Files:**
- No source changes unless verification exposes a reproduced defect.

**Interfaces:**
- Consumes: fork `dev` workflow, GHCR image, `kolja@192.168.1.20`, existing `music-assistant-server` container and `/data` volume
- Produces: verified `origin/dev` commit and deployed image digest

- [ ] **Step 1: Run repository verification**

Run relevant Qobuz/config/script tests, focused mypy, and `pre-commit run --all-files`. Stop on any new failure.

- [ ] **Step 2: Push `dev` and monitor the exact workflow run**

Push normally after confirming remote `dev` still matches the local parent. Wait for the fork-build workflow for the pushed SHA to succeed. Resolve the immutable `dev-<short-sha>` image digest.

- [ ] **Step 3: Inspect the built amd64 image before deployment**

Pull the immutable tag on the Linux host and run a disposable container command proving `/usr/local/bin/shairport-sync` is executable and the installed Qobuz Connect source contains the legacy recovery logic.

- [ ] **Step 4: Capture rollback state and deploy**

Record the running container's exact image digest, mounts, network mode, ports, environment-key names, restart policy, command, and labels without printing secret values. Use the host's existing deployment mechanism when identifiable; otherwise recreate only `music-assistant-server` with the same inspected settings and immutable new image while preserving `/data`.

- [ ] **Step 5: Verify production behavior**

Confirm the server becomes healthy, Qobuz Connect loads and remains loaded across the retry interval, its encrypted setup data contains the exact native instance, AirPlay Receiver reloads without the binary error, and no new provider traceback appears. Confirm HTTP reachability and inspect only sanitized configuration keys.

- [ ] **Step 6: Roll back on failure or report success**

If any production check fails, restore the recorded previous digest with unchanged `/data` and report the exact blocker. Otherwise report the deployed commit/digest and evidence for both providers.
