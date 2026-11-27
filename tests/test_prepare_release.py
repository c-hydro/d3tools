from pathlib import Path

import pytest

from tools.prepare_release import extract_release_notes, prepare_release


def test_extract_release_notes_stops_at_next_version():
    changelog = """# Changelog

## [3.0.0] - Unreleased

First release notes.

## [2.3.0] - 2025-01-01

Earlier notes.
"""

    assert extract_release_notes(changelog, "3.0.0") == "First release notes.\n"


def test_prepare_release_validates_tag_and_writes_notes(tmp_path):
    project_file = tmp_path / "pyproject.toml"
    project_file.write_text('[project]\nname = "example"\nversion = "3.0.0"\n')
    changelog_file = tmp_path / "CHANGELOG.md"
    changelog_file.write_text("## [3.0.0] - Unreleased\n\nRelease notes.\n")
    output_file = tmp_path / "release-notes.md"

    version = prepare_release(
        project_file,
        changelog_file,
        output_file,
        tag="v3.0.0",
    )

    assert version == "3.0.0"
    assert output_file.read_text() == "Release notes.\n"


def test_prepare_release_rejects_mismatched_tag(tmp_path):
    project_file = tmp_path / "pyproject.toml"
    project_file.write_text('[project]\nname = "example"\nversion = "3.0.0"\n')
    changelog_file = tmp_path / "CHANGELOG.md"
    changelog_file.write_text("## [3.0.0] - Unreleased\n\nRelease notes.\n")

    with pytest.raises(ValueError, match="expected 'v3.0.0'"):
        prepare_release(
            project_file,
            changelog_file,
            tmp_path / "release-notes.md",
            tag="v2.4.0",
        )
