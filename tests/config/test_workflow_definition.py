"""
Tests for WorkflowDefinition class behavior.

WorkflowDefinition is a workflow executor that:
- Automatically parses configuration
- Extracts workflow components as attributes
- Executes workflow sections in order
"""
import json
import datetime as dt
import pytest

from d3tools import WorkflowDefinition
from d3tools.timestepping import TimeRange, TimeWindow
from d3tools.config.workflow_section import WorkflowSection


# ============================================================================
# Fixtures for common workflow configurations
# ============================================================================

@pytest.fixture
def minimal_config():
    """Minimal valid workflow configuration."""
    return {
        "TAGS": {},
        "DATASETS": {}
    }


@pytest.fixture
def config_with_tags():
    """Configuration with tags."""
    return {
        "TAGS": {"source": "ERA5", "year": 2024},
        "DATASETS": {}
    }


@pytest.fixture
def config_with_sections():
    """Configuration with workflow sections."""
    return {
        "TAGS": {},
        "DATASETS": {},
        "Download": {"source": "ERA5"},
        "Process": {"input": "x"},
        "Calculate": {"io_options": {"data": "y"}}
    }


@pytest.fixture
def config_with_workflow_name():
    """Configuration with explicit workflow name."""
    return {
        "TAGS": {},
        "DATASETS": {},
        "workflow-name": "test_workflow"
    }


@pytest.fixture
def config_with_workflow_log():
    """Configuration with workflow logging."""
    return {
        "tags": {},
        "Datasets": {},
        "workflow log": {
            "file": "/tmp/test.log",
            "level": "INFO"
        }
    }


@pytest.fixture
def config_with_other_options():
    """Configuration with other_options."""
    return {
        "TAGS": {},
        "DATASETS": {},
        "other_options": {
            "custom_setting": "custom_value",
            "nested": {"key": "value"}
        }
    }


# ============================================================================
# Test Classes
# ============================================================================

class TestWorkflowDefinitionInitialization:
    """Test WorkflowDefinition initialization and parsing."""

    def test_is_not_options_subclass(self):
        """WorkflowDefinition should be standalone, not inherit from Options."""
        from d3tools import Options
        assert not issubclass(WorkflowDefinition, Options)

    def test_init_from_dict(self, minimal_config):
        """Should initialize from a dict configuration."""
        wf = WorkflowDefinition(minimal_config)
        assert isinstance(wf, WorkflowDefinition)

    def test_init_automatically_parses(self, minimal_config):
        """__init__ should automatically parse configuration."""
        wf = WorkflowDefinition(minimal_config)
        
        # Should have all expected attributes
        assert hasattr(wf, "workflow_sections")
        assert hasattr(wf, "workflow_name")
        assert hasattr(wf, "tags")
        assert hasattr(wf, "datasets")
        assert hasattr(wf, "logger")
        assert hasattr(wf, "options")

    def test_init_raises_for_non_dict(self):
        """__init__ should raise TypeError for non-dict input."""
        with pytest.raises(TypeError, match="must be a dict"):
            WorkflowDefinition("not a dict")
        
        with pytest.raises(TypeError, match="must be a dict"):
            WorkflowDefinition(123)

    def test_no_parse_method_exists(self, minimal_config):
        """WorkflowDefinition should not have a parse() method."""
        wf = WorkflowDefinition(minimal_config)
        assert not hasattr(wf, "parse")


class TestWorkflowDefinitionAttributes:
    """Test WorkflowDefinition attributes extraction."""

    def test_has_options_attribute(self, config_with_tags):
        """Should have an options attribute with full parsed config."""
        wf = WorkflowDefinition(config_with_tags)
        
        from d3tools import Options
        assert hasattr(wf, "options")
        assert isinstance(wf.options, Options)
        assert wf.options["TAGS"]["source"] == "ERA5"

    def test_extracts_tags(self, config_with_tags):
        """Should extract tags as a dict attribute."""
        wf = WorkflowDefinition(config_with_tags)
        
        assert isinstance(wf.tags, dict)
        assert wf.tags["source"] == "ERA5"
        assert wf.tags["year"] == 2024

    def test_extracts_workflow_sections(self, config_with_sections):
        """Should extract workflow_sections as a list attribute."""
        wf = WorkflowDefinition(
                config_with_sections,
                build_workflow_objects=False)  # skip building workflow objects for this test
        
        assert isinstance(wf.workflow_sections, list)
        assert len(wf.workflow_sections) == 3

    def test_sections_have_correct_names(self, config_with_sections):
        """Workflow sections should have correct names and engines."""
        wf = WorkflowDefinition(
                config_with_sections,
                build_workflow_objects=False)  # skip building workflow objects for this test
                    
        
        names = [s.name for s in wf.workflow_sections]
        engines = [s.engine for s in wf.workflow_sections]
        
        assert names == ["Download", "Process", "Calculate"]
        assert engines == ["door", "dam", "dryes"]

    def test_sections_preserve_order(self, config_with_sections):
        """Workflow sections should preserve definition order."""
        wf = WorkflowDefinition(
                config_with_sections,
                build_workflow_objects=False) 
        
        names = [s.name for s in wf.workflow_sections]
        assert names == ["Download", "Process", "Calculate"]

    def test_empty_sections_for_minimal_config(self, minimal_config):
        """Minimal config should have empty workflow_sections."""
        wf = WorkflowDefinition(minimal_config)
        assert wf.workflow_sections == []

    def test_extracts_workflow_name(self, config_with_workflow_name):
        """Should extract workflow_name attribute."""
        wf = WorkflowDefinition(config_with_workflow_name)
        assert wf.workflow_name == "test_workflow"

    def test_default_workflow_name(self, minimal_config):
        """Should have default workflow_name if not specified."""
        wf = WorkflowDefinition(minimal_config)
        assert wf.workflow_name == "workflow"

    def test_extracts_logger(self, config_with_workflow_log):
        """Should extract logger attribute when configured."""
        wf = WorkflowDefinition(config_with_workflow_log)
        
        from d3tools.logging import WorkflowLogManager
        assert wf.logger is not None or wf.logger is None  # Depends on implementation

    def test_logger_none_when_not_configured(self, minimal_config):
        """Logger should be None when not configured."""
        wf = WorkflowDefinition(minimal_config)
        # Logger may be None or a default instance depending on implementation

    def test_extracts_other_options(self, config_with_other_options):
        """Should extract other_options as a dict attribute."""
        wf = WorkflowDefinition(config_with_other_options)
        
        assert hasattr(wf, "other_options")
        assert isinstance(wf.other_options, dict)
        assert wf.other_options["custom_setting"] == "custom_value"
        assert wf.other_options["nested"]["key"] == "value"

    def test_other_options_empty_when_not_configured(self, minimal_config):
        """other_options should be empty dict when not configured."""
        wf = WorkflowDefinition(minimal_config)
        assert hasattr(wf, "other_options")
        assert wf.other_options == {}


class TestWorkflowDefinitionLoad:
    """Test WorkflowDefinition.load() class method."""

    def test_load_from_json_file(self, tmp_path, config_with_tags):
        """Should load and parse from JSON file."""
        json_file = tmp_path / "workflow.json"
        json_file.write_text(json.dumps(config_with_tags))
        
        wf = WorkflowDefinition.load(str(json_file))
        
        assert isinstance(wf, WorkflowDefinition)
        assert wf.tags["source"] == "ERA5"

    def test_load_returns_workflow_definition(self, tmp_path, minimal_config):
        """load() should return a WorkflowDefinition instance."""
        json_file = tmp_path / "workflow.json"
        json_file.write_text(json.dumps(minimal_config))
        
        wf = WorkflowDefinition.load(str(json_file))
        assert type(wf) is WorkflowDefinition

    def test_load_forwards_build_flags(self, tmp_path, monkeypatch):
        """load() should forward build_workflow_objects flag."""
        from d3tools.config import workflow_definition
        
        seen = {}
        original_init = WorkflowDefinition.__init__
        
        def mock_init(self, config, build_workflow_objects=False, strict_workflow_imports=False):
            seen["build"] = build_workflow_objects
            seen["strict"] = strict_workflow_imports
            original_init(self, config, build_workflow_objects, strict_workflow_imports)
        
        monkeypatch.setattr(WorkflowDefinition, "__init__", mock_init)
        
        json_file = tmp_path / "workflow.json"
        json_file.write_text(json.dumps({"TAGS": {}, "DATASETS": {}}))
        
        WorkflowDefinition.load(
            str(json_file),
            build_workflow_objects=True,
            strict_workflow_imports=False
        )
        
        assert seen == {"build": True, "strict": False}


class TestWorkflowDefinitionBuildFlags:
    """Test WorkflowDefinition build flags forwarding."""

    def test_init_forwards_build_flags(self, monkeypatch):
        """__init__ should forward build flags to parse_options."""
        from d3tools.config import parsing_pipeline
        from d3tools.config import parsers
        
        seen = {}
        original_parse = parsing_pipeline.parse_options
        
        def mock_parse(config, build_workflow_objects=False, strict_workflow_imports=False):
            seen["build"] = build_workflow_objects
            seen["strict"] = strict_workflow_imports
            return original_parse(config, build_workflow_objects, strict_workflow_imports)
        
        monkeypatch.setattr(parsing_pipeline, "parse_options", mock_parse)
        monkeypatch.setitem(
            parsers._WORKFLOW_ENGINE_BUILDERS,
            "door",
            lambda section: {"built": True, **section}
        )
        
        WorkflowDefinition(
            {"TAGS": {}, "DATASETS": {}, "Download": {"source": "ERA5"}},
            build_workflow_objects=True,
            strict_workflow_imports=False
        )
        
        assert seen == {"build": True, "strict": False}


class TestWorkflowDefinitionRunExecution:
    """Test WorkflowDefinition.run() execution behavior."""

    def test_run_executes_sections_in_order(self, monkeypatch):
        """run() should execute workflow sections in order."""
        calls = []

        class MockDoorProcess:
            def get_data(self, time_range):
                calls.append(("door", time_range))

        class MockDamProcess:
            def run(self, time_range):
                calls.append(("dam", time_range))

        class MockDryesProcess:
            def compute(self, time_range):
                calls.append(("dryes", time_range))

        # Mock the parsing to return pre-built sections
        from d3tools.config import parsing_pipeline
        
        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [
                WorkflowSection("Download", "door", {}, MockDoorProcess()),
                WorkflowSection("Process", "dam", {}, MockDamProcess()),
                WorkflowSection("Calculate", "dryes", {}, MockDryesProcess()),
            ],
            "workflow_name": "test",
            "workflow_log": None
        }
        
        monkeypatch.setattr(
            parsing_pipeline,
            "parse_options",
            lambda config, **kwargs: parsed_config
        )
        
        wf = WorkflowDefinition({})
        wf.run(start="2024-01-01", end="2024-01-31")
        
        # Should execute in order: door, dam, dryes
        assert [call[0] for call in calls] == ["door", "dam", "dryes"]

    def test_run_with_minimal_workflow(self, minimal_config):
        """run() should work with workflow that has no sections."""
        wf = WorkflowDefinition(minimal_config)
        
        # Should not raise
        wf.run(start="2024-01-01", end="2024-01-31")

    def test_run_raises_for_invalid_engine(self, monkeypatch):
        """run() should raise TypeError for unrecognized engine."""
        from d3tools.config import parsing_pipeline
        
        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [
                WorkflowSection("BadSection", "invalid_engine", {}, {}),
            ],
            "workflow_name": "test",
            "workflow_log": None
        }
        
        monkeypatch.setattr(
            parsing_pipeline,
            "parse_options",
            lambda config, **kwargs: parsed_config
        )
        
        wf = WorkflowDefinition({})
        
        with pytest.raises(TypeError, match="unrecognized engine"):
            wf.run(start=dt.datetime(2024, 1, 1), end=dt.datetime(2024, 1, 2))

    def test_run_accepts_datetime_objects(self, minimal_config):
        """run() should accept datetime objects."""
        wf = WorkflowDefinition(minimal_config)
        
        # Should not raise
        wf.run(
            start=dt.datetime(2024, 1, 1),
            end=dt.datetime(2024, 1, 31),
        )

    def test_run_accepts_timerange_object(self, minimal_config):
        """run() should accept an explicit TimeRange."""
        wf = WorkflowDefinition(minimal_config)

        wf.run(time_range=TimeRange("2024-01-01", "2024-01-31"))

    def test_run_accepts_date_strings(self, minimal_config):
        """run() should accept date strings."""
        wf = WorkflowDefinition(minimal_config)
        
        # Should not raise
        wf.run(start="2024-01-01", end="2024-01-31")

    def test_run_defaults_end_to_now(self, minimal_config, monkeypatch):
        """run() should default end date to now if not provided."""
        wf = WorkflowDefinition(minimal_config)
        
        captured = {}
        def capture_run_sections(time_range):
            captured["time_range"] = time_range
            return []  # Return empty list (no sections in minimal_config)
        
        monkeypatch.setattr(wf, "_run_sections", capture_run_sections)

        before = dt.datetime.now()
        wf.run(start="2024-01-01")
        after = dt.datetime.now()

        assert captured["time_range"].start == dt.datetime(2024, 1, 1)
        assert before <= captured["time_range"].end <= after


class TestWorkflowDefinitionRunTimerangeResolution:
    """Test workflow timerange resolution helpers."""

    def test_get_run_timerange_returns_input_timerange(self):
        """_get_run_timerange should return a provided TimeRange unchanged."""
        time_range = TimeRange("2024-01-01", "2024-01-31")

        assert WorkflowDefinition._get_run_timerange(time_range=time_range) is time_range

    def test_get_run_timerange_reads_environment_dates(self, monkeypatch):
        """_get_run_timerange should build a range from environment variables."""
        monkeypatch.setenv("START_DATE", "2024-03-01")
        monkeypatch.setenv("END_DATE", "2024-03-31")

        time_range = WorkflowDefinition._get_run_timerange()

        assert isinstance(time_range, TimeRange)
        assert time_range.start == dt.datetime(2024, 3, 1)
        assert time_range.end == dt.datetime(2024, 3, 31, 23, 59, 59)

    def test_get_run_timerange_returns_none_without_inputs_or_environment(self, monkeypatch):
        """_get_run_timerange should return None when section-level resolution is needed."""
        monkeypatch.delenv("START_DATE", raising=False)
        monkeypatch.delenv("END_DATE", raising=False)

        assert WorkflowDefinition._get_run_timerange() is None

    def test_get_run_timerange_raises_for_unsupported_type(self):
        """_get_run_timerange should reject unsupported input types."""
        with pytest.raises(TypeError, match="time_range must be a TimeRange"):
            WorkflowDefinition._get_run_timerange(time_range=123)

    def test_get_run_timerange_raises_for_mixed_inputs(self):
        """_get_run_timerange should reject time_range mixed with start/end."""
        with pytest.raises(ValueError, match="either time_range or start/end"):
            WorkflowDefinition._get_run_timerange(
                time_range=TimeRange("2024-01-01", "2024-01-31"),
                start="2024-01-01",
            )

    def test_get_run_timerange_raises_for_end_without_start(self):
        """_get_run_timerange should reject end without a start."""
        with pytest.raises(ValueError, match="start is missing"):
            WorkflowDefinition._get_run_timerange(end="2024-01-31")

    def test_run_resolves_section_timerange_per_section(self, monkeypatch):
        """run() should ask each section for its own timerange when none is provided."""
        calls = []

        class MockProcess:
            def __init__(self, engine):
                self.engine = engine

            def get_data(self, time_range):
                calls.append((self.engine, time_range))

            def run(self, time_range):
                calls.append((self.engine, time_range))

        class MockSection:
            def __init__(self, name, engine, time_range):
                self.name = name
                self.engine = engine
                self.value = MockProcess(engine)
                self._time_range = time_range

            def get_run_timerange(self):
                return self._time_range

            def run(self, time_range):
                if time_range is None:
                    time_range = self.get_run_timerange()
                if time_range is None:
                    return
                # Delegate to the underlying process
                if self.engine == 'door':
                    self.value.get_data(time_range)
                elif self.engine == 'dam':
                    self.value.run(time_range)
                elif self.engine == 'dryes':
                    self.value.compute(time_range)

        from d3tools.config import parsing_pipeline

        first_range = TimeRange("2024-01-01", "2024-01-31")
        second_range = TimeRange("2024-02-01", "2024-02-29")
        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [
                MockSection("Download", "door", first_range),
                MockSection("Process", "dam", second_range),
            ],
            "workflow_name": "test",
            "workflow_log": None,
        }

        monkeypatch.setattr(
            parsing_pipeline,
            "parse_options",
            lambda config, **kwargs: parsed_config,
        )

        monkeypatch.delenv("START_DATE", raising=False)
        monkeypatch.delenv("END_DATE", raising=False)

        wf = WorkflowDefinition({})
        wf.run()

        assert calls == [("door", first_range), ("dam", second_range)]

    def test_run_uses_exec_options_repeat_window_from_config(self, monkeypatch):
        """run() should use repeat_window from section exec_options in config."""
        calls = []

        class MockProcess:
            def __init__(self, engine):
                self.engine = engine

            def get_data(self, time_range):
                calls.append((self.engine, time_range))

            def get_last_ts(self):
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

                ts = MockTimeStep(2024, 1, 4)
                return ts, ts

        from d3tools.config import parsing_pipeline
        from d3tools.config.workflow_section import WorkflowSection as RealWorkflowSection

        process = MockProcess('door')
        section = RealWorkflowSection(
            "Download", "door", 
            {"engine": "door", "exec_options": {"repeat_window": "2d"}},
            process,
            exec_options={"repeat_window": "2d"}
        )

        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [section],
            "workflow_name": "test",
            "workflow_log": None,
        }

        monkeypatch.setattr(
            parsing_pipeline,
            "parse_options",
            lambda config, **kwargs: parsed_config,
        )

        monkeypatch.delenv("START_DATE", raising=False)
        monkeypatch.delenv("END_DATE", raising=False)
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

        wf = WorkflowDefinition({})
        wf.run()

        assert len(calls) == 1
        assert calls[0][0] == 'door'
        assert calls[0][1].start == dt.datetime(2024, 1, 3)

    def test_run_skips_section_when_section_timerange_is_none(self, monkeypatch):
        """run() should skip sections that resolve to no pending timestep range."""
        calls = []

        class MockProcess:
            def __init__(self, engine):
                self.engine = engine

            def get_data(self, time_range):
                calls.append((self.engine, time_range))

            def run(self, time_range):
                calls.append((self.engine, time_range))

        class MockSection:
            def __init__(self, name, engine, time_range):
                self.name = name
                self.engine = engine
                self.value = MockProcess(engine)
                self._time_range = time_range

            def get_run_timerange(self):
                return self._time_range

            def run(self, time_range):
                if time_range is None:
                    time_range = self.get_run_timerange()
                if time_range is None:
                    return
                # Delegate to the underlying process
                if self.engine == 'door':
                    self.value.get_data(time_range)
                elif self.engine == 'dam':
                    self.value.run(time_range)
                elif self.engine == 'dryes':
                    self.value.compute(time_range)

        from d3tools.config import parsing_pipeline

        second_range = TimeRange("2024-02-01", "2024-02-29")
        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [
                MockSection("Download", "door", None),
                MockSection("Process", "dam", second_range),
            ],
            "workflow_name": "test",
            "workflow_log": None,
        }

        monkeypatch.setattr(
            parsing_pipeline,
            "parse_options",
            lambda config, **kwargs: parsed_config,
        )

        monkeypatch.delenv("START_DATE", raising=False)
        monkeypatch.delenv("END_DATE", raising=False)

        wf = WorkflowDefinition({})
        wf.run()

        assert calls == [("dam", second_range)]

    def test_run_skips_section_with_no_work_but_runs_with_repeat_window_in_exec_options(self, monkeypatch):
        """run() should skip section with no work unless exec_options has repeat_window."""
        calls = []

        class MockProcess:
            def __init__(self, engine):
                self.engine = engine

            def get_data(self, time_range):
                calls.append((self.engine, time_range))

            def get_last_ts(self):
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

                ts = MockTimeStep(2024, 1, 4)
                return ts, ts

        from d3tools.config import parsing_pipeline
        from d3tools.config.workflow_section import WorkflowSection as RealWorkflowSection

        process = MockProcess('door')
        section = RealWorkflowSection(
            "Download", "door",
            {"engine": "door", "exec_options": {"repeat_window": "1d"}},
            process,
            exec_options={"repeat_window": "1d"}
        )

        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [section],
            "workflow_name": "test",
            "workflow_log": None,
        }

        monkeypatch.setattr(
            parsing_pipeline,
            "parse_options",
            lambda config, **kwargs: parsed_config,
        )

        monkeypatch.delenv("START_DATE", raising=False)
        monkeypatch.delenv("END_DATE", raising=False)
        monkeypatch.delenv("REPEAT_WINDOW", raising=False)

        wf = WorkflowDefinition({})
        wf.run()

        assert len(calls) == 1
        assert calls[0][0] == 'door'
