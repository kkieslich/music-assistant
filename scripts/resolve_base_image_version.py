"""Resolve an upstream base-image version from its release workflow."""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import re
from pathlib import Path


def resolve_base_image_version(path: Path, channel: str = "NIGHTLY") -> str:
    """Return the validated base-image version for a release channel."""
    key = re.escape(f"BASE_IMAGE_VERSION_{channel.upper()}")
    text = path.read_text()
    declarations = re.findall(rf"^[ \t]*{key}[ \t]*:", text, flags=re.MULTILINE)
    if len(declarations) != 1:
        msg = f"Expected exactly one BASE_IMAGE_VERSION_{channel.upper()} declaration"
        raise ValueError(msg)

    match = re.search(
        rf'^[ \t]*{key}[ \t]*:[ \t]*(["\'])([^"\']+)\1[ \t]*(?:#.*)?$',
        text,
        flags=re.MULTILINE,
    )
    if match is None:
        msg = f"BASE_IMAGE_VERSION_{channel.upper()} must be quoted"
        raise ValueError(msg)

    version = match.group(2)
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
