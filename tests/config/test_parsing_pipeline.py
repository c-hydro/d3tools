"""
Tests for config/parsing_pipeline.py stage-by-stage behavior.

These tests lock down the explicit parsing stages introduced in the pipeline:
- resolve_env
- resolve_tags
- build_datasets
- resolve_dataset_refs
- parse_options
"""
import os
import datetime as dt

import pytest

from d3tools.config.options import Options
from d3tools.config.parsing_pipeline import (
    build_datasets,
    collect_workflow_sections,
    parse_options,
    resolve_dataset_refs,
    resolve_env,
    resolve_tags,
)
from d3tools.data import Dataset
from d3tools.config.workflow_section import WorkflowSection


class TestResolveEnv:
    """Test resolve_env stage."""

    def test_resolve_env_replaces_env_placeholders(self, monkeypatch):
        """Test ENV placeholders are resolved recursively."""
        monkeypatch.setenv("CIMA_TEST_PIPELINE_VAR", "from_env")
        options = Options(
            {
                "TAGS": {
                    "source": "{ENV.CIMA_TEST_PIPELINE_VAR}",
                    "fallback": "{ENV.CIMA_TEST_PIPELINE_MISSING, default = 'default_value'}",
                },
                "DATASETS": {
                    "destination": {
                        "type": "local",
                        "path": "output/{ENV.CIMA_TEST_PIPELINE_VAR}",
                        "filename": "x.tif",
                    }
                },
            }
        )

        parsed = resolve_env(options)

        assert parsed["TAGS"]["source"] == "from_env"
        assert parsed["TAGS"]["fallback"] == "default_value"
        assert parsed["DATASETS"]["destination"]["path"] == "output/from_env"

    def test_resolve_env_raises_without_default(self):
        """Test missing ENV variables without defaults raise ValueError."""
        options = Options({"TAGS": {"source": "{ENV.CIMA_TEST_PIPELINE_NEVER_SET}"}})

        with pytest.raises(ValueError):
            resolve_env(options)


class TestResolveTags:
    """Test resolve_tags stage."""

    def test_resolve_tags_substitutes_tag_values_across_structure(self):
        """Test TAG placeholders are rendered in both tags and config fields."""
        options = Options(
            {
                "TAGS": {
                    "source": "ERA5",
                    "product": "reanalysis",
                    "prod_name": "{source}-{product}",
                    "run_day": dt.datetime(2024, 2, 20),
                },
                "DATASETS": {
                    "destination": {
                        "type": "local",
                        "path": "output/{prod_name}",
                        "filename": "file_{run_day:%Y%m%d}.tif",
                    }
                },
            }
        )

        parsed = resolve_tags(options)

        assert parsed["TAGS"]["prod_name"] == "ERA5-reanalysis"
        assert parsed["DATASETS"]["destination"]["path"] == "output/ERA5-reanalysis"
        assert parsed["DATASETS"]["destination"]["filename"] == "file_20240220.tif"


class TestBuildDatasets:
    """Test build_datasets stage."""

    def test_build_datasets_instantiates_dataset_objects(self):
        """Test datasets section entries are converted to Dataset instances."""
        options = Options(
            {
                "DATASETS": {
                    "__defaults__": {"type": "local"},
                    "in_data": {"path": "/data/in", "filename": "in_%Y%m%d.tif"},
                    "out_data": {"path": "/data/out", "filename": "out_%Y%m%d.tif"},
                }
            }
        )

        parsed = build_datasets(options)
        datasets = parsed["DATASETS"]

        assert isinstance(datasets["in_data"], Dataset)
        assert isinstance(datasets["out_data"], Dataset)
        assert "__defaults__" not in datasets


class TestResolveDatasetRefs:
    """Test resolve_dataset_refs stage."""

    def test_resolve_dataset_refs_replaces_dataset_placeholders(self):
        """Test placeholders like {DATASETS.foo} are replaced with Dataset objects."""
        options = Options(
            {
                "DATASETS": {
                    "destination": Dataset.from_options(
                        {"type": "local", "path": "/data/out", "filename": "out_%Y%m%d.tif"}
                    )
                },
                "DOOR_DOWNLOADER": {"destination": "{DATASETS.destination}"},
            }
        )

        parsed = resolve_dataset_refs(options)

        assert isinstance(parsed["DOOR_DOWNLOADER"]["destination"], Dataset)
        assert parsed["DOOR_DOWNLOADER"]["destination"] is parsed["DATASETS"]["destination"]

    def test_resolve_dataset_refs_supports_tag_overrides(self):
        """Test dataset placeholders with tag assignments call Dataset.update()."""
        options = Options(
            {
                "DATASETS": {
                    "parameter_ds": Dataset.from_options(
                        {"type": "local", "path": "/data/{par_name}", "filename": "x.tif"}
                    )
                },
                "DRYES_INDEX": {
                    "io_options": {"gamma": "{DATASETS.parameter_ds, par_name = 'gamma.a'}"}
                },
            }
        )

        parsed = resolve_dataset_refs(options)
        gamma_ds = parsed["DRYES_INDEX"]["io_options"]["gamma"]

        assert isinstance(gamma_ds, Dataset)
        assert gamma_ds.tags.get("par_name") == "gamma.a"


class TestCollectWorkflowSections:
    """Test collect_workflow_sections stage."""

    def test_collect_workflow_sections_preserves_order_and_keywords(self):
        """Collect sections in top-level insertion order with engine keywords."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Download": {"source": "ERA5"},
                "Process": {"process_list": [{"function": "a"}]},
                "Calculate": {"index_options": {"index": "SPI"}},
                "Publish": {"process_list": [{"function": "b"}]},
            }
        )

        collected = collect_workflow_sections(options,  False, False)
        sections = collected["workflow_sections"]

        assert all(isinstance(s, WorkflowSection) for s in sections)
        assert [s.name for s in sections] == ["Download", "Process", "Calculate", "Publish"]
        assert [s.engine for s in sections] == ["door", "dam", "dryes", "dam"]

    def test_collect_workflow_sections_flattens_list_values(self):
        """List-valued workflow sections should be expanded into multiple entries."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Publish": [
                    {"process_list": [{"function": "a"}]},
                    {"process_list": [{"function": "b"}]},
                ],
            }
        )

        collected = collect_workflow_sections(options, False, False)
        sections = collected["workflow_sections"]

        assert len(sections) == 2
        assert sections[0].name == "Publish_01"
        assert sections[1].name == "Publish_02"
        assert sections[0].definition["process_list"][0]["function"] == "a"
        assert sections[1].definition["process_list"][0]["function"] == "b"

    def test_collect_workflow_sections_forwards_build_flags(self, monkeypatch):
        """Collector should forward build/strict flags to WorkflowSection factory."""
        calls = []
        from d3tools.config import parsing_pipeline as pipeline

        def _fake_from_config(name, definition, build_object=False, strict_imports=False):
            calls.append((name, build_object, strict_imports))
            return WorkflowSection(name=name, engine="door", definition=definition, value=definition)

        monkeypatch.setattr(pipeline.WorkflowSection, "from_config", staticmethod(_fake_from_config))

        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Download": {"source": "ERA5"},
            }
        )
        collected = collect_workflow_sections(
            options,
            build_workflow_objects=True,
            strict_workflow_imports=True,
        )

        assert len(collected["workflow_sections"]) == 1
        assert calls == [("Download", True, True)]

    def test_collect_workflow_sections_applies_workflow_level_defaults(self):
        """Workflow-level engine and exec_options should be applied to sections."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "engine": "door",
                "exec_options": {"timeout": 300},
                "Download": {"source": "ERA5"},
                "Process": {"engine": "dam", "process_list": [{"function": "a"}]},
            }
        )

        collected = collect_workflow_sections(options, build_workflow_objects=False)
        sections = collected["workflow_sections"]

        # Download should inherit workflow-level defaults
        assert sections[0].name == "Download"
        assert sections[0].engine == "door"
        assert sections[0].exec_options == {"timeout": 300}

        # Process should override the workflow-level engine but keep exec_options
        assert sections[1].name == "Process"
        assert sections[1].engine == "dam"
        assert sections[1].exec_options == {"timeout": 300}

    def test_collect_workflow_sections_numbers_list_items(self):
        """List-valued sections should be numbered with 2-digit padding (section_01, section_02, etc.)."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Process": [
                    {"engine": "dam", "process_list": [{"function": "a"}]},
                    {"engine": "dam", "process_list": [{"function": "b"}]},
                    {"engine": "dam", "process_list": [{"function": "c"}]},
                ],
            }
        )

        collected = collect_workflow_sections(options, build_workflow_objects=False)
        sections = collected["workflow_sections"]

        assert len(sections) == 3
        assert sections[0].name == "Process_01"
        assert sections[1].name == "Process_02"
        assert sections[2].name == "Process_03"

    def test_collect_workflow_sections_removes_collected_keys_from_options(self):
        """Collected workflow section keys should be removed from top-level options."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Download": {"source": "ERA5"},
                "Process": {"process_list": [{"function": "a"}]},
            }
        )

        collected = collect_workflow_sections(options, build_workflow_objects=False)

        # Keys should be removed from options
        assert "Download" not in collected
        assert "Process" not in collected
        # Preserved keys should still be there
        assert "TAGS" in collected
        assert "DATASETS" in collected

    def test_collect_workflow_sections_resolves_alias_in_section_name_with_suffix(self):
        """Alias resolution should match when alias is contained in section name (e.g., Process_01)."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Process_01": {"engine": "dam", "process_list": [{"function": "a"}]},
                "Download_02": {"engine": "door", "source": "ERA5"},
                "Calculate_03": {"engine": "dryes", "index_options": {"index": "SPI"}},
            }
        )

        collected = collect_workflow_sections(options, build_workflow_objects=False)
        sections = collected["workflow_sections"]

        assert len(sections) == 3
        # Verify correct engines resolved from aliases contained in names
        assert sections[0].name == "Process_01"
        assert sections[0].engine == "dam"
        assert sections[1].name == "Download_02"
        assert sections[1].engine == "door"
        assert sections[2].name == "Calculate_03"
        assert sections[2].engine == "dryes"

    def test_collect_workflow_sections_resolves_alias_with_custom_prefix(self):
        """Alias resolution should work with custom prefixes (e.g., MyProcess, DataDownload)."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "MyProcess": {"engine": "dam", "process_list": [{"function": "a"}]},
                "DataDownload": {"engine": "door", "source": "ERA5"},
                "RiskCalculate": {"engine": "dryes", "index_options": {"index": "SPI"}},
            }
        )

        collected = collect_workflow_sections(options, build_workflow_objects=False)
        sections = collected["workflow_sections"]

        assert len(sections) == 3
        assert sections[0].engine == "dam"
        assert sections[1].engine == "door"
        assert sections[2].engine == "dryes"

    def test_collect_workflow_sections_default_flags_are_true(self, monkeypatch):
        """Default values for build_workflow_objects and strict_workflow_imports should be True."""
        calls = []
        from d3tools.config import parsing_pipeline as pipeline

        def _fake_from_config(name, definition, build_object=False, strict_imports=False):
            calls.append((name, build_object, strict_imports))
            return WorkflowSection(name=name, engine="dam", definition=definition, value=definition)

        monkeypatch.setattr(pipeline.WorkflowSection, "from_config", staticmethod(_fake_from_config))

        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "Process": {"process_list": [{"function": "a"}]},
            }
        )

        # Call without explicit flags - should use defaults (True, True)
        collected = collect_workflow_sections(options)

        assert len(collected["workflow_sections"]) == 1
        assert calls == [("Process", True, True)]

    def test_collect_workflow_sections_exact_alias_match_still_works(self):
        """Exact alias matches (without suffix) should still work."""
        options = Options(
            {
                "TAGS": {},
                "DATASETS": {},
                "download": {"engine": "door", "source": "ERA5"},
                "process": {"engine": "dam", "process_list": [{"function": "a"}]},
                "calculate": {"engine": "dryes", "index_options": {"index": "SPI"}},
            }
        )

        collected = collect_workflow_sections(options, build_workflow_objects=False)
        sections = collected["workflow_sections"]

        assert len(sections) == 3
        assert sections[0].engine == "door"
        assert sections[1].engine == "dam"
        assert sections[2].engine == "dryes"


class TestParseOptionsIntegration:
    """Integration tests for full parse_options stage."""

    def test_parse_options_sets_env_before_placeholder_resolution(self, monkeypatch):
        """Full pipeline should allow ENV values defined in config to be reused immediately."""
        monkeypatch.delenv("CIMA_PIPELINE_INLINE_ENV", raising=False)

        options = Options(
            {
                "ENV": {
                    "PIPELINE_INLINE_ENV": "configured_value",
                    "PIPELINE_YEAR": 2024,
                },
                "TAGS": {
                    "source": "{ENV.PIPELINE_INLINE_ENV}",
                    "year": "{ENV.PIPELINE_YEAR}",
                },
                "DATASETS": {},
            }
        )

        parsed = parse_options(options)

        assert os.getenv("PIPELINE_INLINE_ENV") == "configured_value"
        assert os.getenv("PIPELINE_YEAR") == "2024"
        assert parsed["TAGS"]["source"] == "configured_value"
        assert parsed["TAGS"]["year"] == "2024"

    def test_parse_options_treats_env_as_reserved_top_level_key(self):
        """ENV should not be collected as a workflow section."""
        options = Options(
            {
                "ENV": {"CIMA_PIPELINE_SECTION_ENV": "configured_value"},
                "TAGS": {},
                "DATASETS": {},
                "Download": {"source": "ERA5"},
            }
        )

        parsed = parse_options(options)

        assert "ENV" in parsed
        assert [section.name for section in parsed["workflow_sections"]] == ["Download"]

    def test_parse_options_raises_for_invalid_env_section_type(self):
        """env should be a mapping if provided."""
        options = Options(
            {
                "ENV": ["NOT", "A", "DICT"],
                "TAGS": {},
                "DATASETS": {},
            }
        )

        with pytest.raises(TypeError, match="env must be a dict or None"):
            parse_options(options)

    def test_parse_options_resolves_tags_datasets_and_collects_workflow_sections(self):
        """Full pipeline should resolve placeholders and collect sections generically."""
        options = Options(
            {
                "TAGS": {
                    "root": "/tmp",
                    "source": "ERA5",
                },
                "DATASETS": {
                    "__defaults__": {"type": "local"},
                    "in_data": {
                        "path": "{root}/in",
                        "filename": "in_%Y%m%d.tif",
                    },
                    "out_data": {
                        "path": "{root}/out",
                        "filename": "out_%Y%m%d.tif",
                    },
                },
                "Download": {
                    "source": "{source}",
                    "destination": "{DATASETS.out_data}",
                },
                "Process": {
                    "input": "{DATASETS.in_data}",
                    "output": "{DATASETS.out_data}",
                },
                "Calculate": {
                    "io_options": {"data": "{DATASETS.in_data}"},
                },
            }
        )

        parsed = parse_options(options)
        sections = parsed["workflow_sections"]

        assert isinstance(parsed["DATASETS"]["in_data"], Dataset)
        assert isinstance(parsed["DATASETS"]["out_data"], Dataset)
        assert [section.name for section in sections] == ["Download", "Process", "Calculate"]
        assert [section.engine for section in sections] == ["door", "dam", "dryes"]
        assert sections[0].definition["source"] == "ERA5"
        assert isinstance(sections[0].definition["destination"], Dataset)
        assert isinstance(sections[1].definition["input"], Dataset)
        assert isinstance(sections[1].definition["output"], Dataset)
        assert isinstance(sections[2].definition["io_options"]["data"], Dataset)

    def test_parse_options_forwards_build_flags_to_collector(self, monkeypatch):
        """parse_options should forward workflow build flags to collector stage."""
        from d3tools.config import parsing_pipeline as pipeline
        from d3tools.config import parsers

        seen = {}
        original_collector = pipeline.collect_workflow_sections

        def _collector_proxy(options, build_workflow_objects=False, strict_workflow_imports=False):
            seen["build"] = build_workflow_objects
            seen["strict"] = strict_workflow_imports
            return original_collector(
                options,
                build_workflow_objects=build_workflow_objects,
                strict_workflow_imports=strict_workflow_imports,
            )

        monkeypatch.setattr(pipeline, "collect_workflow_sections", _collector_proxy)
        monkeypatch.setitem(parsers._WORKFLOW_ENGINE_BUILDERS, "door", lambda section: {"built": True, **section})

        options = Options(
            {
                "TAGS": {},
                "DATASETS": {"destination": {"type": "local", "path": "/tmp", "filename": "x.tif"}},
                "Download": {"source": "ERA5"},
            }
        )

        parse_options(options, build_workflow_objects=True, strict_workflow_imports=False)
        assert seen == {"build": True, "strict": False}
