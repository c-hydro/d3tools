"""
Tests for workflow-section aliasing and WorkflowSection behavior.
"""
import datetime as dt

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
        assert WORKFLOW_SECTION_ALIASES["ingest"] == "door"
        assert WORKFLOW_SECTION_ALIASES["calculate"] == "dryes"
        assert WORKFLOW_SECTION_ALIASES["process"] == "dam"
        assert WORKFLOW_SECTION_ALIASES["publish"] == "dam"

    def test_resolve_workflow_section_alias(self):
        """Alias resolver should return known engines or None."""
        assert resolve_workflow_section_alias("Download") == "door"
        assert resolve_workflow_section_alias("Ingest") == "door"
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

    def test_get_run_timerange_returns_missing_output_window(self):
        """get_run_timerange should span the timesteps still missing from output."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

            def __add__(self, n):
                next_day = self.start + dt.timedelta(days=n)
                return MockTimeStep(next_day.year, next_day.month, next_day.day)

            def __sub__(self, n):
                return self.__add__(-n)

        class MockProcess:
            def get_last_ts(self):
                return MockTimeStep(2024, 1, 4), MockTimeStep(2024, 1, 2)

        section = WorkflowSection("Download", "door", {}, MockProcess())

        time_range = section.get_run_timerange()

        assert time_range.start == dt.datetime(2024, 1, 3)
        assert time_range.end == dt.datetime(2024, 1, 4, 23, 59, 59)

    def test_get_run_timerange_raises_when_none_done(self):
        """get_run_timerange should use the latest available timestep when output is missing."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

            def __add__(self, n):
                next_day = self.start + dt.timedelta(days=n)
                return MockTimeStep(next_day.year, next_day.month, next_day.day)

            def __sub__(self, n):
                return self.__add__(-n)

        class MockProcess:
            def get_last_ts(self):
                return MockTimeStep(2024, 1, 4), None

        section = WorkflowSection("Download", "door", {}, MockProcess())

        with pytest.raises(ValueError, match="not enough available data"):
            section.get_run_timerange()

    def test_get_run_timerange_raises_without_available_data(self):
        """get_run_timerange should raise when no available timestep exists."""

        class MockProcess:
            def get_last_ts(self):
                return None, None

        section = WorkflowSection("Download", "door", {}, MockProcess())

        with pytest.raises(ValueError, match="not enough available data"):
            section.get_run_timerange()
