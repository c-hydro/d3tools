# Documentation topic inventory

This inventory assigns an owner to the main public or tested d3tools surfaces.
It is a coverage map, not a claim that every internal helper needs a guide. Phase 1
audits existing pages; a missing page produces a finding, not a speculative guide.
Each topic branch starts from the same `docs` commit for a phase-1 batch.

| Topic / branch | Source and public surface | Tests | Phase-1 pages | Intended tier |
|---|---|---|---|---|
| `configuration` / `docs_configuration` | `config/options.py`, `config/parsing_pipeline.py`, `config/parsers.py` for JSON parsing, environment, tags and dataset references | `tests/config/test_options.py`, `test_parsing_pipeline.py`, `test_parsing_pipeline_workflow_log.py`, `test_parsers.py`; `tests/parse/test_config_resolution.py` | `docs/configuration/overview.md`, `tags.md`, `environment.md` | User configuration; developer parser API reference |
| `workflow-sections` / `docs_workflow-sections` | `config/workflow_definition.py`, `config/workflow_section.py`, `config/utils.py`, `scripts/run_workflow.py`, `errors/workflow_import.py` for execution, ranges and state | `tests/config/test_workflow_definition*.py`, `test_workflow_section.py`, `test_workflow_state.py` | `docs/configuration/workflow-sections.md` | User workflows; developer orchestration APIs |
| `datasets` / `docs_datasets` | `data/datasets/`, `data/data_catalogue.py`, `data/format_mixins/`, `data/io_utils*.py`, `config/parsers.py` dataset factory, `errors/template_errors.py` | `tests/data/`, `tests/data/format_mixins/`, `tests/config/test_manager_integration.py` | `docs/configuration/datasets.md`, `docs/datasets/*.md` | User definitions and storage; developer Dataset, catalogue, mixin and raster template integration |
| `timestepping` / `docs_timestepping` | `timestepping/` time parsing, periods, units, windows and estimation | `tests/timestepping/` | `docs/timestepping.md` | User time configuration; developer time API |
| `logging` / `docs_logging` | `logging/` including dataset and workflow managers, output and run state | `tests/logging/`, `tests/config/test_workflow_definition_logging.py` | `docs/logging/*.md` | User logging configuration; developer managers |
| `thumbnails` / `docs_thumbnails` | `thumbnails/`, dataset thumbnail manager integration | `tests/thumbnails/` | `docs/thumbnails/overview.md` | User thumbnail options; developer rendering API |
| `notification` / `docs_notification` | `notification/email_notification.py` | No dedicated tests found; treat as experimental | No existing page | Developer experimental status only until behaviour is verified |
| `spatial` / `docs_spatial` | `spatial/bounding_box.py`, `spatial/space_utils.py`, standalone `TemplateManager` API | `tests/data/test_template_manager*.py`; no dedicated bounding-box tests found | No existing page; do not edit dataset pages | Developer spatial API; dataset template usage stays with datasets |
| `cases` / `docs_cases` | `cases/case.py`, `case_manager.py`, `utils.py` including `CaseManager` and `withcases` | `tests/cases/` | No existing page; do not edit workflow-sections | Developer case API; engine-specific use stays with workflow-sections |
| `parsing` / `docs_parsing` | `parse/key_parser.py`, `string_rendering.py`, `parse_utils.py` and legacy exports | `tests/parse/test_key_parser.py`, `test_string_rendering.py`, `test_legacy_parse_utils.py`, `tests/test_legacy_module_aliases.py` | No existing page; do not edit configuration or dataset pages | Developer parsing API; JSON pipeline stays with configuration |
| `utilities` / `docs_utilities` | `exit/exit_handler.py`, `exit/__init__.py`, small public cleanup helpers and compatibility entry points | No dedicated exit tests found; legacy alias tests belong to parsing | No existing page | Short developer API reference, not a full User Guide |
| `getting-started` / `docs_getting-started` | Root exports in `src/d3tools/__init__.py`, installation metadata in `pyproject.toml`, end-to-end examples | Cross-topic smoke tests and documentation checker | `docs/getting-started.md`, `docs/index.md`, `docs/examples/*.md` | User onboarding and navigation, after source topics |

## Cross-topic ownership rules

- `dataset_from_config` is implemented in `config/parsers.py` but its dataset
  options are owned by datasets; configuration owns pipeline order and reference
  substitution. Neither topic edits the other's phase-1 pages.
- `TemplateManager` as a standalone API belongs to spatial; its raster Dataset
  integration belongs to datasets. Thumbnails own rendering, not raster Dataset
  templates.
- Dataset `log` configuration and `DatasetLogManager` belong to logging for
  behavioural detail; datasets owns the brief `log` option summary.
- `KeyParser` as a Python API belongs to parsing; dataset key-pattern usage
  belongs to datasets. `withcases` belongs to cases; engine payloads belong to
  their respective engine packages, not this repository.
- `getting-started` owns navigation and examples and must be audited after the
  other phase-1 branches are reviewed and merged, to avoid cross-topic edits.

Phase-1 audits can run independently for all source topics. Pages in other
branches must not be edited to fix cross-topic links; record a handoff finding
for the owning topic instead. Merge conflicts or duplicated claims are resolved
on `docs` after human review. After phase 1, reassess whether any genuinely
public surface remains unmapped before generating new guides or API stubs.