"""
Tests for WorkflowDefinition/Options compatibility behavior.
"""
import json

from d3tools import Options, WorkflowDefinition


class TestWorkflowDefinitionCompatibility:
    """Ensure new canonical name keeps backward-compatible behavior."""

    def test_options_is_workflow_definition_subclass(self):
        """Options should remain usable as compatibility subclass."""
        assert issubclass(Options, WorkflowDefinition)

    def test_parse_preserves_concrete_class(self):
        """parse() should return the same concrete class as the receiver."""
        wf = WorkflowDefinition({"TAGS": {"a": "x"}, "DATASETS": {}})
        opts = Options({"TAGS": {"a": "x"}, "DATASETS": {}})

        parsed_wf = wf.parse()
        parsed_opts = opts.parse()

        assert isinstance(parsed_wf, WorkflowDefinition)
        assert type(parsed_wf) is WorkflowDefinition
        assert isinstance(parsed_opts, Options)
        assert type(parsed_opts) is Options

    def test_load_preserves_concrete_class(self, tmp_path):
        """load() should instantiate parsed result with the calling class."""
        cfg = {
            "TAGS": {"source": "ERA5"},
            "DATASETS": {},
        }
        cfg_path = tmp_path / "workflow.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        loaded_wf = WorkflowDefinition.load(str(cfg_path))
        loaded_opts = Options.load(str(cfg_path))

        assert type(loaded_wf) is WorkflowDefinition
        assert type(loaded_opts) is Options

    def test_parse_forwards_build_flags(self, monkeypatch):
        """WorkflowDefinition.parse should forward workflow build flags."""
        from d3tools.config import options as options_module
        from d3tools.config import parsers

        seen = {}
        original_parse_options = options_module.parse_options

        def _parse_proxy(workflow, build_workflow_objects=False, strict_workflow_imports=False):
            seen["build"] = build_workflow_objects
            seen["strict"] = strict_workflow_imports
            return original_parse_options(
                workflow,
                build_workflow_objects=build_workflow_objects,
                strict_workflow_imports=strict_workflow_imports,
            )

        monkeypatch.setattr(options_module, "parse_options", _parse_proxy)
        monkeypatch.setitem(parsers._WORKFLOW_ENGINE_BUILDERS, "door", lambda section: {"built": True, **section})

        wf = WorkflowDefinition({"TAGS": {}, "DATASETS": {}, "Download": {"source": "ERA5"}})
        wf.parse(build_workflow_objects=True, strict_workflow_imports=False)
        assert seen == {"build": True, "strict": False}
