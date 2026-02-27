"""
Tests for WorkflowDefinition behavior.
"""
import json
import datetime as dt

from d3tools import Options, WorkflowDefinition
from d3tools.config.workflow_section import WorkflowSection


import warnings

class TestWorkflowDefinitionInitialization:
    """Test WorkflowDefinition initialization and parsing."""

    def test_workflow_definition_is_not_options_subclass(self):
        """WorkflowDefinition should be a standalone class, not a subclass of Options."""
        assert not issubclass(WorkflowDefinition, Options)

    def test_workflow_definition_has_options_attribute(self):
        """WorkflowDefinition should have an options attribute containing full config."""
        wf = WorkflowDefinition({"TAGS": {"source": "ERA5"}, "DATASETS": {}})
        
        assert hasattr(wf, "options")
        assert isinstance(wf.options, Options)
        assert wf.options["TAGS"]["source"] == "ERA5"

    def test_options_attribute_access_and_recursive_wrapping(self):
        """Options should support attribute access and recursive wrapping."""
        opts = Options({"foo": {"bar": 42}, "baz": [{"qux": 99}]})
        # Attribute access
        assert opts.foo.bar == 42
        # Recursive wrapping in lists
        assert isinstance(opts.baz[0], Options)
        assert opts.baz[0].qux == 99

    def test_options_load_deprecation_warning(self, tmp_path):
        """Options.load should emit a deprecation warning."""
        cfg = {"TAGS": {"source": "ERA5"}, "DATASETS": {}}
        cfg_path = tmp_path / "workflow.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Options.load(str(cfg_path))
            assert any(issubclass(warn.category, DeprecationWarning) for warn in w)
    
    def test_init_automatically_parses(self):
        """__init__ should automatically parse configuration."""
        wf = WorkflowDefinition({"TAGS": {"x": 1}, "DATASETS": {}})
        
        # Should have extracted workflow components
        assert hasattr(wf, "workflow_sections")
        assert hasattr(wf, "workflow_name")
        assert hasattr(wf, "tags")
        assert hasattr(wf, "datasets")
        assert hasattr(wf, "logger")
        
        # Should have empty workflow sections for minimal config
        assert wf.workflow_sections == []

    def test_parse_method_does_not_exist(self):
        """WorkflowDefinition should not have a parse() method - parsing is automatic."""
        wf = WorkflowDefinition({"TAGS": {"a": "x"}, "DATASETS": {}})
        assert not hasattr(wf, "parse")

    def test_load_returns_workflow_definition(self, tmp_path):
        """load() should return a parsed WorkflowDefinition instance."""
        cfg = {
            "TAGS": {"source": "ERA5"},
            "DATASETS": {},
        }
        cfg_path = tmp_path / "workflow.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        loaded_wf = WorkflowDefinition.load(str(cfg_path))
        
        assert type(loaded_wf) is WorkflowDefinition
        assert loaded_wf.tags["source"] == "ERA5"

    def test_init_forwards_build_flags(self, monkeypatch):
        """WorkflowDefinition.__init__ should forward workflow build flags."""
        from d3tools.config import parsing_pipeline as parsing_module
        from d3tools.config import workflow_definition
        from d3tools.config import parsers

        seen = {}
        original_parse_options = parsing_module.parse_options

        def _parse_proxy(workflow, build_workflow_objects=False, strict_workflow_imports=False):
            seen["build"] = build_workflow_objects
            seen["strict"] = strict_workflow_imports
            return original_parse_options(
                workflow,
                build_workflow_objects=build_workflow_objects,
                strict_workflow_imports=strict_workflow_imports,
            )

        # Patch in the workflow_definition module namespace
        monkeypatch.setattr(workflow_definition, "parse_options", _parse_proxy)
        monkeypatch.setitem(parsers._WORKFLOW_ENGINE_BUILDERS, "door", lambda section: {"built": True, **section})

        wf = WorkflowDefinition(
            {"TAGS": {}, "DATASETS": {}, "Download": {"source": "ERA5"}},
            build_workflow_objects=True,
            strict_workflow_imports=False
        )
        assert seen == {"build": True, "strict": False}

    def test_init_extracts_workflow_sections(self):
        """__init__ should extract workflow sections as a list attribute."""
        wf = WorkflowDefinition({"TAGS": {"x": 1}, "DATASETS": {}})
        
        assert hasattr(wf, "workflow_sections")
        assert isinstance(wf.workflow_sections, list)
        assert wf.workflow_sections == []

    def test_init_collects_workflow_sections(self):
        """__init__ should collect workflow sections from top-level keys."""
        wf = WorkflowDefinition(
            {
                "TAGS": {},
                "DATASETS": {},
                "Download": {"source": "ERA5"},
            }
        )

        assert len(wf.workflow_sections) == 1
        assert wf.workflow_sections[0].name == "Download"
        assert wf.workflow_sections[0].engine == "door"
        # Download key should NOT be in options at top level
        assert "Download" not in wf.options

    def test_init_preserves_workflow_section_order(self):
        """__init__ should preserve top-level section order in workflow_sections."""
        wf = WorkflowDefinition(
            {
                "TAGS": {},
                "DATASETS": {},
                "Download": {"source": "A"},
                "Process": {"input": "x"},
                "Calculate": {"io_options": {"data": "y"}},
            }
        )

        names = [section.name for section in wf.workflow_sections]
        engines = [section.engine for section in wf.workflow_sections]

        assert names == ["Download", "Process", "Calculate"]
        assert engines == ["door", "dam", "dryes"]


class TestWorkflowDefinitionRun:
    """Test WorkflowDefinition.run ordered execution behavior and edge cases."""

    def test_run_executes_sections_in_order(self, monkeypatch):
        """run() should execute section workflow objects in list order."""
        calls = []

        class _DoorProcess:
            def get_data(self, time_range):
                calls.append(("door", time_range))

        class _DamProcess:
            def run(self, time_range):
                calls.append(("dam", time_range))

        class _DryesProcess:
            def compute(self, time_range):
                calls.append(("dryes", time_range))

        # Create workflow with pre-built sections (simulating parsed result)
        from d3tools.config import workflow_definition
        
        # Mock parsed config with workflow sections
        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [
                WorkflowSection("Download", "door", {}, _DoorProcess()),
                WorkflowSection("Process", "dam", {}, _DamProcess()),
                WorkflowSection("Calculate", "dryes", {}, _DryesProcess()),
            ],
            "workflow_name": "test",
            "workflow_log": None
        }
        
        # Patch parse_options to return our mocked config
        monkeypatch.setattr(
            workflow_definition,
            "parse_options",
            lambda config, **kwargs: parsed_config
        )
        
        wf = WorkflowDefinition({})

        wf.run("2024-01-01", "2024-01-31")

        assert [item[0] for item in calls] == ["door", "dam", "dryes"]

    def test_options_attribute_access_multiple_values(self):
        """Options attribute access should raise ValueError if multiple values found."""
        opts = Options({"foo": {"bar": 1}, "baz": {"bar": 2}})
        try:
            _ = opts.bar
        except ValueError as exc:
            assert "Multiple values found" in str(exc)
        else:
            raise AssertionError("Expected ValueError for multiple values")

    def test_run_raises_for_non_runnable_section_value(self, monkeypatch):
        """run() should fail clearly when a section has an unrecognized engine."""
        from d3tools.config import workflow_definition
        
        # Mock parsed config with invalid engine
        parsed_config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_sections": [
                WorkflowSection("Not a real workflow section", "not_a_real_engine", {}, {}),
            ],
            "workflow_name": "test",
            "workflow_log": None
        }
        
        # Patch parse_options to return our mocked config
        monkeypatch.setattr(
            workflow_definition,
            "parse_options",
            lambda config, **kwargs: parsed_config
        )
        
        wf = WorkflowDefinition({})

        try:
            wf.run(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2))
        except TypeError as exc:
            assert "unrecognized engine" in str(exc)
        else:
            raise AssertionError("Expected TypeError for unrecognized engine")
