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

    def test_resolve_workflow_section_alias_with_suffix_numbers(self):
        """Alias resolver should match when alias is contained in section name with suffix."""
        assert resolve_workflow_section_alias("Download_01") == "door"
        assert resolve_workflow_section_alias("Process_02") == "dam"
        assert resolve_workflow_section_alias("Calculate_03") == "dryes"
        assert resolve_workflow_section_alias("Ingest_99") == "door"
        assert resolve_workflow_section_alias("Publish_05") == "dam"

    def test_resolve_workflow_section_alias_with_custom_prefix(self):
        """Alias resolver should match when alias is contained in custom-prefixed name."""
        assert resolve_workflow_section_alias("MyDownload") == "door"
        assert resolve_workflow_section_alias("DataProcess") == "dam"
        assert resolve_workflow_section_alias("RiskCalculate") == "dryes"
        assert resolve_workflow_section_alias("MyIngest") == "door"
        assert resolve_workflow_section_alias("FinalPublish") == "dam"

    def test_resolve_workflow_section_alias_with_complex_names(self):
        """Alias resolver should match with complex naming schemes."""
        assert resolve_workflow_section_alias("Stage1_Download_01") == "door"
        assert resolve_workflow_section_alias("Data_Processing_Step_02") == "dam"
        assert resolve_workflow_section_alias("Final_Calculate_Index") == "dryes"

    def test_resolve_workflow_section_alias_case_insensitive(self):
        """Alias resolver should be case-insensitive."""
        assert resolve_workflow_section_alias("DOWNLOAD") == "door"
        assert resolve_workflow_section_alias("Process_01") == "dam"
        assert resolve_workflow_section_alias("MYDATAPROCESS") == "dam"
        assert resolve_workflow_section_alias("MyCalculate") == "dryes"

    def test_resolve_workflow_section_alias_no_match(self):
        """Alias resolver should return None for names without recognized aliases."""
        assert resolve_workflow_section_alias("Transform") is None
        assert resolve_workflow_section_alias("CustomStep_01") is None
        assert resolve_workflow_section_alias("UnknownOperation") is None


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

    def test_get_run_timerange_all_available_for_dam_uses_full_available_window(self):
        """all_available should run from first available to latest available for dam."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

        class MockProcess:
            def get_first_ts(self):
                return MockTimeStep(2023, 12, 29)

            def get_last_ts(self):
                # last_done is intentionally newer than first_ts to verify it is ignored.
                return MockTimeStep(2024, 1, 4), MockTimeStep(2024, 1, 3)

        section = WorkflowSection(
            "Process",
            "dam",
            {},
            MockProcess(),
            exec_options={"all_available": True},
        )

        time_range = section.get_run_timerange()

        assert time_range.start == dt.datetime(2023, 12, 29)
        assert time_range.end == dt.datetime(2024, 1, 4, 23, 59, 59)

    def test_get_run_timerange_all_available_raises_for_non_dam_engine(self):
        """all_available should fail fast for engines other than dam."""

        section = WorkflowSection(
            "Download",
            "door",
            {},
            object(),
            exec_options={"all_available": True},
        )

        with pytest.raises(ValueError, match="only supported for 'dam' sections"):
            section.get_run_timerange()

    def test_get_run_timerange_all_available_raises_when_first_ts_is_missing(self):
        """all_available should raise when no first available timestep can be resolved."""

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

        class MockProcess:
            def get_first_ts(self):
                return None

            def get_last_ts(self):
                return MockTimeStep(2024, 1, 4), MockTimeStep(2024, 1, 3)

        section = WorkflowSection(
            "Process",
            "dam",
            {},
            MockProcess(),
            exec_options={"all_available": True},
        )

        with pytest.raises(ValueError, match="has no available data"):
            section.get_run_timerange()

    def test_get_run_timerange_all_available_honors_env_override(self, monkeypatch):
        """ALL_AVAILABLE env var should override exec_options when resolving run timerange."""

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
            def get_first_ts(self):
                return MockTimeStep(2023, 12, 29)

            def get_last_ts(self):
                # If ALL_AVAILABLE is honored, end date should be 2024-01-04.
                return MockTimeStep(2024, 1, 4), MockTimeStep(2024, 1, 3)

        monkeypatch.setenv("ALL_AVAILABLE", "true")

        section = WorkflowSection(
            "Process",
            "dam",
            {},
            MockProcess(),
            exec_options={"all_available": False},
        )

        time_range = section.get_run_timerange()

        assert time_range.start == dt.datetime(2023, 12, 29)
        assert time_range.end == dt.datetime(2024, 1, 4, 23, 59, 59)

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

    def test_run_split_executes_month_subranges_when_enabled(self, monkeypatch):
        """run() should split long ranges into monthly chunks when split is enabled."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def get_data(self, tr):
                calls.append(tr)

        section = WorkflowSection(
            "Download",
            "door",
            {},
            MockProcess(),
            exec_options={"split": True},
        )

        result = section.run(TimeRange("2024-01-01", "2024-03-31"))

        assert len(calls) == 3
        assert calls[0].start == dt.datetime(2024, 1, 1)
        assert calls[-1].end == dt.datetime(2024, 3, 31, 23, 59, 59)
        assert result.executed is True
        assert result.reason == "split into 3 sub-ranges"

    def test_run_split_can_be_enabled_by_env_var(self, monkeypatch):
        """run() should honor SPLIT environment variable and override exec_options."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def get_data(self, tr):
                calls.append(tr)

        monkeypatch.setenv("SPLIT", "true")

        section = WorkflowSection(
            "Download",
            "door",
            {},
            MockProcess(),
            exec_options={"split": False},
        )

        result = section.run(TimeRange("2024-01-01", "2024-03-31"))

        assert len(calls) == 3
        assert result.reason == "split into 3 sub-ranges"

    def test_run_split_does_not_apply_to_short_ranges(self, monkeypatch):
        """run() should not split ranges with length <= 31 days even if split is enabled."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def get_data(self, time_range):
                calls.append(time_range)

        monkeypatch.setenv("SPLIT", "true")

        section = WorkflowSection("Download", "door", {}, MockProcess())

        result = section.run(TimeRange("2024-01-01", "2024-01-31"))

        assert len(calls) == 1
        assert calls[0].start == dt.datetime(2024, 1, 1)
        assert calls[0].end == dt.datetime(2024, 1, 31, 23, 59, 59)
        assert result.executed is True
        assert result.reason is None

    def test_run_split_preserves_original_timerange_boundaries(self, monkeypatch):
        """Split chunks should preserve original start on first chunk and original end on last chunk."""
        from d3tools.timestepping import TimeRange

        calls = []

        class MockProcess:
            def get_data(self, tr):
                calls.append(tr)

        original_range = TimeRange("2024-01-15", "2024-03-10")
        section = WorkflowSection(
            "Download",
            "door",
            {},
            MockProcess(),
            exec_options={"split": True},
        )

        section.run(original_range)

        assert len(calls) >= 2
        assert calls[0].start == original_range.start
        assert calls[-1].end == original_range.end

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

    def test_get_run_timerange_times_from_run_uses_stored_range(self, monkeypatch):
        """get_run_timerange should return the stored range when times_from_run resolves it."""
        from d3tools.timestepping import TimeRange

        stored_range = TimeRange("2024-01-05", "2024-01-10")
        monkeypatch.setattr(
            "d3tools.config.workflow_section.get_timerange_from_run_state",
            lambda _: stored_range,
        )
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

        section = WorkflowSection(
            "Download", "door", {}, object(),
            exec_options={"times_from_run": "some_run_ref"},
        )

        assert section.get_run_timerange() == stored_range

    def test_get_run_timerange_times_from_run_falls_through_to_normal_when_none(self, monkeypatch):
        """get_run_timerange should fall through to normal resolution when times_from_run returns None."""
        monkeypatch.setattr(
            "d3tools.config.workflow_section.get_timerange_from_run_state",
            lambda _: None,
        )
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

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

        section = WorkflowSection(
            "Download", "door", {}, MockProcess(),
            exec_options={"times_from_run": "some_run_ref"},
        )

        result = section.get_run_timerange()

        assert result is not None
        assert result.start == dt.datetime(2024, 1, 3)
        assert result.end == dt.datetime(2024, 1, 4, 23, 59, 59)

    def test_get_run_timerange_times_from_run_applies_repeat_window(self, monkeypatch):
        """repeat_window should extend the stored times_from_run range backwards."""
        from d3tools.timestepping import TimeRange

        stored_range = TimeRange("2024-01-05", "2024-01-10")
        monkeypatch.setattr(
            "d3tools.config.workflow_section.get_timerange_from_run_state",
            lambda _: stored_range,
        )
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

        section = WorkflowSection(
            "Download", "door", {}, object(),
            exec_options={"times_from_run": "some_run_ref", "repeat_window": "3d"},
        )

        result = section.get_run_timerange()

        assert result is not None
        assert result.start == dt.datetime(2024, 1, 2)  # 3 days before 2024-01-05
        assert result.end == stored_range.end

    def test_get_run_timerange_all_available_takes_priority_over_times_from_run(self, monkeypatch):
        """all_available should be resolved before times_from_run is ever consulted."""
        state_lookup_called = []
        monkeypatch.setattr(
            "d3tools.config.workflow_section.get_timerange_from_run_state",
            lambda _: state_lookup_called.append(True) or TimeRange("2024-01-01", "2024-01-07"),
        )

        class MockTimeStep:
            def __init__(self, year, month, day):
                self.start = dt.datetime(year, month, day)
                self.end = dt.datetime(year, month, day, 23, 59, 59)

        class MockProcess:
            def get_first_ts(self):
                return MockTimeStep(2023, 12, 1)

            def get_last_ts(self):
                return MockTimeStep(2024, 1, 4), MockTimeStep(2024, 1, 3)

        section = WorkflowSection(
            "Process", "dam", {}, MockProcess(),
            exec_options={"all_available": True, "times_from_run": "some_run_ref"},
        )

        result = section.get_run_timerange()

        assert not state_lookup_called, "get_timerange_from_run_state should not be called when all_available is set"
        assert result.start == dt.datetime(2023, 12, 1)
        assert result.end == dt.datetime(2024, 1, 4, 23, 59, 59)
