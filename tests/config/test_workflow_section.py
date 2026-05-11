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
            build_object=False,  # skip building workflow objects for this test
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
            build_object=False,  # skip building workflow objects for this test
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

    def test_from_config_extracts_exec_options_from_definition(self):
        """from_config should extract exec_options dict from definition."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={
                "source": "ERA5",
                "exec_options": {"repeat_window": "3d", "other_option": "value"}
            },
            build_object=False,  # skip building workflow objects for this test
        )

        assert section.exec_options == {"repeat_window": "3d", "other_option": "value"}

    def test_from_config_sets_exec_options_to_empty_dict_when_missing(self):
        """from_config should set exec_options to {} when not in definition."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={"source": "ERA5"},
            build_object=False,  # skip building workflow objects for this test
        )

        assert section.exec_options == {}

    def test_from_config_stores_empty_exec_options_dict_not_none(self):
        """from_config should store {} not None for easier dict operations."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={"engine": "door", "source": "ERA5"},
            build_object=False,  # skip building workflow objects for this test
        )

        # Should be able to call .get() without checking for None
        assert section.exec_options.get("repeat_window") is None

    def test_direct_construction_with_exec_options(self):
        """WorkflowSection can be constructed with exec_options dict."""
        section = WorkflowSection(
            name="Download",
            engine="door",
            definition={},
            value={},
            exec_options={"repeat_window": "2d", "param": "val"},
        )

        assert section.exec_options["repeat_window"] == "2d"
        assert section.exec_options["param"] == "val"

    def test_direct_construction_with_none_exec_options(self):
        """WorkflowSection can be constructed with exec_options=None."""
        section = WorkflowSection(
            name="Download",
            engine="door",
            definition={},
            value={},
            exec_options=None,
        )

        assert section.exec_options is None

    def test_get_exec_option_reads_from_exec_options_when_env_missing(self, monkeypatch):
        """get_exec_option should use section exec_options when env var is not set."""
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

        section = WorkflowSection(
            name="Download",
            engine="door",
            definition={},
            value={},
            exec_options={"repeat_window": "2d"},
        )

        assert section.get_exec_option("repeat_window") == "2d"

    def test_get_exec_option_env_overrides_exec_options(self, monkeypatch):
        """get_exec_option should prioritize environment variable over exec_options."""
        monkeypatch.setenv("REPEAT_WINDOW", "5d")

        section = WorkflowSection(
            name="Download",
            engine="door",
            definition={},
            value={},
            exec_options={"repeat_window": "2d"},
        )

        assert section.get_exec_option("repeat_window") == "5d"

    def test_get_exec_option_returns_default_when_missing(self, monkeypatch):
        """get_exec_option should return explicit default when env and config are missing."""
        monkeypatch.delenv("SPLIT", raising=False)

        section = WorkflowSection(
            name="Download",
            engine="door",
            definition={},
            value={},
            exec_options={},
        )

        assert section.get_exec_option("split", default="none") == "none"

    def test_get_run_timerange_uses_repeat_window_from_exec_options(self, monkeypatch):
        """get_run_timerange should use repeat_window from exec_options."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

            def __add__(self, n):
                next_day = self.start + dt.timedelta(days=n)
                return MockTimeStep(next_day.year, next_day.month, next_day.day)

            def __sub__(self, n):
                return self.__add__(-n)

            def __le__(self, other):
                return self.start <= other.start

        class MockProcess:
            def get_last_ts(self):
                # last_available=2024-01-04, last_done=2024-01-04 (no new work)
                ts = MockTimeStep(2024, 1, 4)
                return ts, ts

        # Clear environment
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

        # Without exec_options, should return None
        section1 = WorkflowSection("Download", "door", {}, MockProcess(), exec_options={})
        assert section1.get_run_timerange() is None

        # With exec_options repeat_window, should reopen work
        section2 = WorkflowSection(
            "Download", "door", {}, MockProcess(),
            exec_options={"repeat_window": "2d"}
        )
        time_range = section2.get_run_timerange()
        assert time_range is not None
        assert time_range.start == dt.datetime(2024, 1, 3)

    def test_get_run_timerange_falls_back_to_env_var_when_no_exec_options(self, monkeypatch):
        """get_run_timerange should fall back to REPEAT_WINDOW env var when exec_options doesn't have repeat_window."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

            def __add__(self, n):
                next_day = self.start + dt.timedelta(days=n)
                return MockTimeStep(next_day.year, next_day.month, next_day.day)

            def __sub__(self, n):
                return self.__add__(-n)

            def __le__(self, other):
                return self.start <= other.start

        class MockProcess:
            def get_last_ts(self):
                ts = MockTimeStep(2024, 1, 4)
                return ts, ts

        monkeypatch.setenv("REPEAT_WINDOW", "1d")

        # Section with empty exec_options should use env var
        section = WorkflowSection("Download", "door", {}, MockProcess(), exec_options={})
        time_range = section.get_run_timerange()

        assert time_range is not None
        assert time_range.start == dt.datetime(2024, 1, 4)

    def test_get_run_timerange_prefers_env_var_over_exec_options(self, monkeypatch):
        """get_run_timerange should prefer REPEAT_WINDOW env var over exec_options repeat_window."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

            def __add__(self, n):
                next_day = self.start + dt.timedelta(days=n)
                return MockTimeStep(next_day.year, next_day.month, next_day.day)

            def __sub__(self, n):
                return self.__add__(-n)

            def __le__(self, other):
                return self.start <= other.start

        class MockProcess:
            def get_last_ts(self):
                ts = MockTimeStep(2024, 1, 4)
                return ts, ts

        monkeypatch.setenv("REPEAT_WINDOW", "5d")

        # Section with exec_options should still use env var when present
        section = WorkflowSection(
            "Download", "door", {}, MockProcess(),
            exec_options={"repeat_window": "2d"}
        )
        time_range = section.get_run_timerange()

        assert time_range is not None
        # 5-day window from 2024-01-04 starts from 2023-12-31
        assert time_range.start == dt.datetime(2023, 12, 31)

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
