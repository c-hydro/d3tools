"""
Tests for workflow-section aliasing and WorkflowSection behavior.
"""

import pytest

from d3tools.config.workflow_section import (
    WORKFLOW_SECTION_ALIASES,
    WorkflowSection,
    normalize_section_alias,
    resolve_workflow_section_alias,
)


class TestWorkflowSectionAliases:
    """Test alias normalization and resolution for workflow sections."""

    def test_global_alias_mapping_contains_operational_keywords(self):
        """Requested operational aliases should map to expected engines."""
        assert WORKFLOW_SECTION_ALIASES["download"] == "door"
        assert WORKFLOW_SECTION_ALIASES["calculate"] == "dryes"
        assert WORKFLOW_SECTION_ALIASES["process"] == "dam"
        assert WORKFLOW_SECTION_ALIASES["publish"] == "dam"

    def test_normalize_section_alias_standardizes_tokens(self):
        """Alias normalization should ignore case, spaces and dashes."""
        assert normalize_section_alias("Door Downloader") == "door_downloader"
        assert normalize_section_alias("DOOR-DOWNLOADER") == "door_downloader"
        assert normalize_section_alias("  Process ") == "process"

    def test_resolve_workflow_section_alias(self):
        """Alias resolver should return known engines or None."""
        assert resolve_workflow_section_alias("Download") == "door"
        assert resolve_workflow_section_alias("Process") == "dam"
        assert resolve_workflow_section_alias("Calculate") == "dryes"
        assert resolve_workflow_section_alias("UnknownSection") is None


class TestWorkflowSection:
    """Test WorkflowSection dataclass construction behavior."""

    def test_from_config_populates_minimal_fields(self):
        """from_config should set name/engine/options/value fields."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={"source": "ERA5", "options": {"ts_per_year": 36}},
        )

        assert section.name == "Download"
        assert section.engine == "door"
        assert section.definition["source"] == "ERA5"
        assert section.value == section.definition

    def test_from_config_raises_for_non_workflow_key(self):
        """from_config should reject unrecognized top-level keys."""
        with pytest.raises(ValueError):
            WorkflowSection.from_config(name="TAGS", definition={"a": "b"})
