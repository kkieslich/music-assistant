"""Resolve an upstream base-image version from its release workflow."""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import re
from pathlib import Path


def resolve_base_image_version(path: Path, channel: str = "NIGHTLY") -> str:
    """Return the validated base-image version for a release channel."""
    key = re.escape(f"BASE_IMAGE_VERSION_{channel.upper()}")
    matches = re.findall(
        rf'^[ \t]*{key}[ \t]*:[ \t]*(["\'])([^"\']+)\1[ \t]*(?:#.*)?$',
        path.read_text(),
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        msg = f"Expected exactly one quoted BASE_IMAGE_VERSION_{channel.upper()} value"
        raise ValueError(msg)

    version = matches[0][1]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        msg = f"BASE_IMAGE_VERSION_{channel.upper()} must be a three-component numeric version"
        raise ValueError(msg)
    return version


def main() -> None:
    """Print a base-image version resolved from a release workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--channel", default="NIGHTLY")
    args = parser.parse_args()
    print(resolve_base_image_version(args.path, args.channel))


if __name__ == "__main__":
    main()
