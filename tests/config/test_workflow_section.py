"""
Tests for workflow-section aliasing and WorkflowSection behavior.
"""
import datetime as dt

import pytest
from d3tools.timestepping import TimeWindow
from d3tools.config.workflow_section import (
    WORKFLOW_SECTION_ALIASES,
    WorkflowSection,
    WorkflowSectionRunResult,
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

    def test_from_config_extracts_exec_options_from_definition(self):
        """from_config should extract exec_options dict from definition."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={
                "source": "ERA5",
                "exec_options": {"repeat_window": "3d", "other_option": "value"}
            },
        )

        assert section.exec_options == {"repeat_window": "3d", "other_option": "value"}

    def test_from_config_sets_exec_options_to_empty_dict_when_missing(self):
        """from_config should set exec_options to {} when not in definition."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={"source": "ERA5"},
        )

        assert section.exec_options == {}

    def test_from_config_stores_empty_exec_options_dict_not_none(self):
        """from_config should store {} not None for easier dict operations."""
        section = WorkflowSection.from_config(
            name="Download",
            definition={"engine": "door", "source": "ERA5"},
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

    def test_get_run_timerange_prefers_exec_options_over_env_var(self, monkeypatch):
        """get_run_timerange should prefer repeat_window from exec_options over REPEAT_WINDOW env var."""

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

        # Section with exec_options should use that, not env var
        section = WorkflowSection(
            "Download", "door", {}, MockProcess(),
            exec_options={"repeat_window": "2d"}
        )
        time_range = section.get_run_timerange()

        assert time_range is not None
        # 5-day window would start from 2023-12-31, 2-day window starts from 2024-01-03
        assert time_range.start == dt.datetime(2024, 1, 3)

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

            def __le__(self, other):
                return self.start <= other.start

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

            def __le__(self, other):
                return self.start <= other.start

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

    def test_get_run_timerange_returns_none_when_same_timestep(self):
        """get_run_timerange should return None when available and done are the same timestep."""

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

        section = WorkflowSection("Download", "door", {}, MockProcess())

        assert section.get_run_timerange() is None

    def test_get_run_timerange_repeats_same_timestep_with_repeat_window(self):
        """get_run_timerange should reopen work when repeat_window is in exec_options."""

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

        section = WorkflowSection(
            "Download", "door", {}, MockProcess(),
            exec_options={"repeat_window": "3d"}
        )

        time_range = section.get_run_timerange()

        assert time_range is not None
        assert time_range.start == dt.datetime(2024, 1, 2)
        assert time_range.end == dt.datetime(2024, 1, 4, 23, 59, 59)

    def test_run_calls_door_get_data(self):
        """run() should call get_data() for door engine."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def get_data(self, time_range):
                calls.append(("get_data", time_range))

        time_range = TimeRange("2024-01-01", "2024-01-31")
        section = WorkflowSection("Download", "door", {}, MockProcess())

        result = section.run(time_range)

        assert len(calls) == 1
        assert calls[0][0] == "get_data"
        assert calls[0][1] == time_range
        assert isinstance(result, WorkflowSectionRunResult)
        assert result.executed is True
        assert result.section_name == "Download"
        assert result.engine == "door"
        assert result.time_range == time_range
        assert result.reason is None

    def test_run_calls_dam_run(self):
        """run() should call run() for dam engine."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def run(self, time_range):
                calls.append(("run", time_range))

        time_range = TimeRange("2024-01-01", "2024-01-31")
        section = WorkflowSection("Process", "dam", {}, MockProcess())

        result = section.run(time_range)

        assert len(calls) == 1
        assert calls[0][0] == "run"
        assert calls[0][1] == time_range
        assert isinstance(result, WorkflowSectionRunResult)
        assert result.executed is True

    def test_run_calls_dryes_compute(self):
        """run() should call compute() for dryes engine."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def compute(self, time_range):
                calls.append(("compute", time_range))

        time_range = TimeRange("2024-01-01", "2024-01-31")
        section = WorkflowSection("Calculate", "dryes", {}, MockProcess())

        result = section.run(time_range)

        assert len(calls) == 1
        assert calls[0][0] == "compute"
        assert calls[0][1] == time_range
        assert isinstance(result, WorkflowSectionRunResult)
        assert result.executed is True

    def test_run_returns_skipped_contract_when_no_timerange(self):
        """run() should return skipped result when no section range is available."""

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

            def get_data(self, _):
                raise AssertionError("get_data should not be called when section is skipped")

        section = WorkflowSection("Download", "door", {}, MockProcess())

        result = section.run(None)

        assert isinstance(result, WorkflowSectionRunResult)
        assert result.executed is False
        assert result.section_name == "Download"
        assert result.engine == "door"
        assert result.time_range is None
        assert result.reason == "nothing to do"

    def test_run_resolves_timerange_when_argument_is_none(self):
        """run(None) should resolve section timerange and execute with it."""

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

        calls = []

        class MockProcess:
            def get_last_ts(self):
                return MockTimeStep(2024, 1, 4), MockTimeStep(2024, 1, 2)

            def get_data(self, time_range):
                calls.append(time_range)

        section = WorkflowSection("Download", "door", {}, MockProcess())

        result = section.run(None)

        assert len(calls) == 1
        assert calls[0].start == dt.datetime(2024, 1, 3)
        assert calls[0].end == dt.datetime(2024, 1, 4, 23, 59, 59)
        assert result.executed is True
        assert result.time_range.start == dt.datetime(2024, 1, 3)

    def test_run_raises_for_unrecognized_engine(self):
        """run() should raise TypeError for unrecognized engine."""
        from d3tools.timestepping import TimeRange

        class MockProcess:
            pass

        time_range = TimeRange("2024-01-01", "2024-01-31")
        section = WorkflowSection("Unknown", "unknown_engine", {}, MockProcess())

        with pytest.raises(TypeError, match="unrecognized engine 'unknown_engine'"):
            section.run(time_range)
