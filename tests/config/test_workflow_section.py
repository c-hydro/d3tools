"""
Tests for workflow-section aliasing and WorkflowSection behavior.
"""
import pytest
from d3tools.config.workflow_section import (
    WORKFLOW_SECTION_ALIASES,
    WorkflowSection,
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

    def test_from_config_with_engine_in_definition(self):
        """from_config should use engine from definition if provided."""
        section = WorkflowSection.from_config(
            name="not_an_alias",
            definition={"engine": "door", "source": "ERA5", "options": {"ts_per_year": 36}},
        )
        assert section.engine == "door"
        assert section.name   == "not_an_alias"

    def test_from_config_raises_with_invalid_engine(self):
        """from_config should raise ValueError for invalid engine."""
        with pytest.raises(ValueError):
            WorkflowSection.from_config(
                name="not_an_alias",
                definition={"engine": "invalid_engine", "source": "ERA5", "options": {"ts_per_year": 36}},
            )
