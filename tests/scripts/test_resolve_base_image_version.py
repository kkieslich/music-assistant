"""Tests for upstream base-image version resolution."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.resolve_base_image_version import resolve_base_image_version

ROOT = Path(__file__).parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
FORK_BUILD_WORKFLOW = ROOT / ".github" / "workflows" / "fork-build.yml"
DOCKERFILE = ROOT / "Dockerfile"
RESOLVER_SCRIPT = ROOT / "scripts" / "resolve_base_image_version.py"


def test_resolves_nightly_version_from_release_workflow() -> None:
    """The nightly base-image version follows the upstream release workflow."""
    assert resolve_base_image_version(RELEASE_WORKFLOW) == "1.6.0"


def test_rejects_missing_base_image_version_key(tmp_path: Path) -> None:
    """A workflow without the requested base-image key cannot be packaged."""
    workflow = tmp_path / "release.yml"
    workflow.write_text('BASE_IMAGE_VERSION_STABLE: "1.5.4"\n')

    with pytest.raises(ValueError, match="exactly one"):
        resolve_base_image_version(workflow)


def test_rejects_duplicate_base_image_version_key(tmp_path: Path) -> None:
    """Ambiguous upstream base-image versions stop packaging."""
    workflow = tmp_path / "release.yml"
    workflow.write_text(
        'BASE_IMAGE_VERSION_NIGHTLY: "1.6.0"\nBASE_IMAGE_VERSION_NIGHTLY: "1.6.1"\n'
    )

    with pytest.raises(ValueError, match="exactly one"):
        resolve_base_image_version(workflow)


@pytest.mark.parametrize(
    "duplicate",
    ["BASE_IMAGE_VERSION_NIGHTLY: 1.6.1", 'BASE_IMAGE_VERSION_NIGHTLY: "1.6.1'],
    ids=["unquoted", "malformed"],
)
def test_rejects_duplicate_key_with_invalid_second_declaration(
    tmp_path: Path, duplicate: str
) -> None:
    """Every matching key declaration counts toward ambiguity before value parsing."""
    workflow = tmp_path / "release.yml"
    workflow.write_text(f'BASE_IMAGE_VERSION_NIGHTLY: "1.6.0"\n{duplicate}\n')

    with pytest.raises(ValueError, match="exactly one"):
        resolve_base_image_version(workflow)


def test_rejects_unquoted_base_image_version(tmp_path: Path) -> None:
    """A sole declaration must quote its semantic version value."""
    workflow = tmp_path / "release.yml"
    workflow.write_text("BASE_IMAGE_VERSION_NIGHTLY: 1.6.0\n")

    with pytest.raises(ValueError, match="must be quoted"):
        resolve_base_image_version(workflow)


@pytest.mark.parametrize("version", ["latest", "1.6", "1.6.0-rc1"])
def test_rejects_malformed_base_image_version(tmp_path: Path, version: str) -> None:
    """Only three-component numeric base-image versions are accepted."""
    workflow = tmp_path / "release.yml"
    workflow.write_text(f'BASE_IMAGE_VERSION_NIGHTLY: "{version}"\n')

    with pytest.raises(ValueError, match="three-component numeric"):
        resolve_base_image_version(workflow)


def test_cli_prints_resolved_base_image_version() -> None:
    """The resolver CLI emits the version for workflow environment wiring."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(RESOLVER_SCRIPT), str(RELEASE_WORKFLOW)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout == "1.6.0\n"


def test_fork_build_resolves_base_image_version_before_buildx() -> None:
    """Fork image builds resolve the upstream base version instead of pinning one."""
    workflow = FORK_BUILD_WORKFLOW.read_text()

    assert "resolve_base_image_version.py" in workflow
    assert not re.search(
        r'^[ \t]*BASE_IMAGE_VERSION:[ \t]*["\']?\d+\.\d+\.\d+',
        workflow,
        flags=re.MULTILINE,
    )
    assert "BASE_IMAGE_VERSION=${{ env.BASE_IMAGE_VERSION }}" in workflow
    assert workflow.index('>> "$GITHUB_ENV"') < workflow.index("Set up Buildx")


def test_fork_build_stops_before_export_when_resolver_fails(tmp_path: Path) -> None:
    """The workflow shell propagates resolver failure instead of exporting an empty value."""
    workflow = FORK_BUILD_WORKFLOW.read_text()
    match = re.search(
        r"(?m)^      - name: Resolve upstream base image version\n"
        r"        run: \|\n(?P<body>(?:          .*\n)+)",
        workflow,
    )
    assert match is not None
    script = "\n".join(line[10:] for line in match.group("body").splitlines())
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text("#!/bin/sh\nexit 23\n")
    fake_python.chmod(0o755)
    github_env = tmp_path / "github-env"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_ENV": str(github_env),
    }

    result = subprocess.run(  # noqa: S603
        ["/bin/bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=ROOT,
        env=env,
        check=False,
    )

    assert result.returncode == 23
    assert not github_env.exists()


def test_final_image_requires_shairport_sync_binary() -> None:
    """Every final target image fails to build without shairport-sync."""
    final_stage = DOCKERFILE.read_text().rsplit("\nFROM ", maxsplit=1)[1]

    assert "RUN test -x /usr/local/bin/shairport-sync" in final_stage
