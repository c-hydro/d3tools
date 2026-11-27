"""Validate release metadata and extract notes from the changelog."""

from __future__ import annotations

import argparse
from pathlib import Path
import tomllib


def extract_release_notes(changelog: str, version: str) -> str:
    """Extract one version section from changelog text.

    Args:
        changelog: Complete changelog contents.
        version: Version heading to extract, without a leading ``v``.

    Returns:
        The release notes below the matching version heading.

    Raises:
        ValueError: If the version heading is missing or has no content.
    """
    heading = f"## [{version}]"
    lines = changelog.splitlines()

    try:
        start = next(
            index for index, line in enumerate(lines) if line.startswith(heading)
        ) + 1
    except StopIteration as error:
        raise ValueError(f"CHANGELOG.md has no {heading!r} section") from error

    end = next(
        (
            index
            for index in range(start, len(lines))
            if lines[index].startswith("## [")
        ),
        len(lines),
    )
    notes = "\n".join(lines[start:end]).strip()
    if not notes:
        raise ValueError(f"CHANGELOG.md section {heading!r} is empty")
    return notes + "\n"


def prepare_release(
    project_file: Path,
    changelog_file: Path,
    output_file: Path,
    tag: str | None = None,
) -> str:
    """Validate an optional tag and write notes for the project version.

    Args:
        project_file: Path to ``pyproject.toml``.
        changelog_file: Path to the changelog.
        output_file: Destination for the extracted release notes.
        tag: Optional Git tag, which must equal ``v<project version>``.

    Returns:
        The project version.

    Raises:
        ValueError: If the tag and project version differ or notes are absent.
    """
    with project_file.open("rb") as stream:
        version = tomllib.load(stream)["project"]["version"]

    expected_tag = f"v{version}"
    if tag is not None and tag != expected_tag:
        raise ValueError(
            f"release tag {tag!r} does not match project version {version!r}; "
            f"expected {expected_tag!r}"
        )

    notes = extract_release_notes(changelog_file.read_text(), version)
    output_file.write_text(notes)
    return version


def main() -> None:
    """Run release validation from the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("tag", nargs="?", help="Optional release tag to validate")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("release-notes.md"),
        help="Release-note output path",
    )
    args = parser.parse_args()

    version = prepare_release(
        Path("pyproject.toml"),
        Path("CHANGELOG.md"),
        args.output,
        args.tag,
    )
    print(f"Prepared release notes for {version}")


if __name__ == "__main__":
    main()
