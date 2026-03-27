"""
Tests for workflow-section aliasing and WorkflowSection behavior.
"""

import pytest

from d3tools.errors import WorkflowEngineImportError
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


class TestWorkflowSectionBuildFlags:
    """Test WorkflowSection build/strict flags behavior."""

    def test_from_config_build_false_keeps_definition_as_value(self):
        """Default behavior should keep section value as raw definition."""
        definition = {"source": "ERA5"}
        section = WorkflowSection.from_config(name="Download", definition=definition)
        assert section.value is definition

    def test_from_config_build_true_non_strict_falls_back_on_import_error(self, monkeypatch):
        """build_object=True should fallback to definition when strict is False."""
        from d3tools.config import parsers

        def _raise_import(_):
            raise ModuleNotFoundError("door")

        monkeypatch.setitem(parsers._WORKFLOW_ENGINE_BUILDERS, "door", _raise_import)
        definition = {"source": "ERA5"}
        section = WorkflowSection.from_config(
            name="Download",
            definition=definition,
            build_object=True,
            strict_imports=False,
        )
        assert section.value is definition

    def test_from_config_build_true_strict_raises_on_import_error(self, monkeypatch):
        """build_object=True should raise when strict_imports is True."""
        from d3tools.config import parsers

        def _raise_import(_):
            raise ModuleNotFoundError("door")

        monkeypatch.setitem(parsers._WORKFLOW_ENGINE_BUILDERS, "door", _raise_import)

        with pytest.raises(WorkflowEngineImportError) as exc_info:
            WorkflowSection.from_config(
                name="Download",
                definition={"source": "ERA5"},
                build_object=True,
                strict_imports=True,
            )
        assert exc_info.value.engine == "door"
        assert isinstance(exc_info.value.original_error, ImportError)

    def test_from_config_non_import_errors_are_not_silenced(self, monkeypatch):
        """Non-import build errors should always propagate."""
        from d3tools.config import parsers

        def _raise_runtime(_):
            raise ValueError("invalid section payload")

        monkeypatch.setitem(parsers._WORKFLOW_ENGINE_BUILDERS, "door", _raise_runtime)

        with pytest.raises(ValueError, match="invalid section payload"):
            WorkflowSection.from_config(
                name="Download",
                definition={"source": "ERA5"},
                build_object=True,
                strict_imports=False,
            )
