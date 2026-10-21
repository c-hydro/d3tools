# Phase 1 Findings: getting-started

Scope: `docs/getting-started.md`, `docs/index.md`, and `docs/examples/*.md`.
Ground truth was checked against `src/d3tools/` and the workflow configuration
tests. The documentation checker was run with:

`/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py docs/getting-started.md docs/index.md docs/examples`

| Page claim or gap | Evidence | Verdict | Action |
|---|---|---|---|
| The minimal example uses `name` and `time` as the workflow identity and execution model. | `src/d3tools/config/workflow_definition.py:39`, `src/d3tools/config/workflow_definition.py:78-81`; `tests/config/test_workflow_definition.py:182-195` | wrong | Replace with `workflow_name`, `tags`, `datasets`, and an optional section; explain that the run range is supplied to `run()`. |
| The minimal example is complete and runs silently with no sections. | `src/d3tools/config/workflow_definition.py:125-174`; `tests/config/test_workflow_definition.py:350-355` | wrong | Replace the legacy example with a valid no-section workflow and a checked Python invocation. |
| Tags are exposed as arbitrary workflow attributes such as `workflow.project` and `workflow.output_dir`. | `src/d3tools/config/workflow_definition.py:77-84`; `src/d3tools/config/options.py:7-18` | wrong | Use the supported `workflow.tags` / `workflow.options` access instead. |
| Dataset access in the minimal example uses `workflow.datasets['test_data']` and then `.type`/`.path` as a guaranteed public dataset object API. | `src/d3tools/config/workflow_definition.py:77-81`; dataset behavior is owned by the datasets topic and is not established by the getting-started source/tests cited here. | unverified | Keep onboarding at the definition/reference level and link to the datasets guide for dataset object details. |
| All three dataset references `{DATASETS.x}`, `{datasets.x}`, and `{x}` are interchangeable. | `src/d3tools/config/workflow_definition.py:39`; configuration behavior inventory records exact-prefix matching for prefixed references. | wrong | State the verified prefix and case rules; avoid claiming interchangeable forms. |
| Omitted execution dates resolve from prior run state as well as environment variables and data availability. | `src/d3tools/config/workflow_definition.py:141-144`, `src/d3tools/config/workflow_definition.py:190-214` | wrong | Remove the unsupported prior-run-state claim. |
| The getting-started and example pages contain runnable JSON snippets. | Checker: `docs/getting-started.md:186`, `docs/getting-started.md:238`, `docs/examples/multi-dataset.md:835`, `docs/examples/multi-dataset.md:850`, `docs/examples/multi-dataset.md:864`, `docs/examples/multi-dataset.md:874`, `docs/examples/with-logging.md:453` | wrong | Use valid JSON or mark intentionally partial snippets with the checker skip marker. |
| The example pages document real workflow engine payloads and generated log output as d3tools behavior. | `src/d3tools/config/workflow_definition.py:146-150`; `tests/config/test_workflow_definition.py:307-348` only establishes the d3tools engine dispatch contract, not DOOR/DAM/DRYES payload schemas or log text. | unverified | Reduce examples to d3tools-owned configuration and link engine-specific syntax to the owning DOOR, DAM, and DRYES documentation. |
| The minimal example links to a full-pipeline page. | `docs/examples/minimal-workflow.md:188`; checker reports missing target `full-pipeline.md`. | wrong | Replace with an existing in-scope example or configuration guide link. |
| The logging example's production configuration is usable as written. | `docs/examples/with-logging.md:409`; checker warning reports `PermissionError` for `/var/log/workflows`. | unverified | Use a user-writable path in the example and document deployment-specific permissions separately. |
| The index lists the configuration, dataset, timestepping, logging, and parsing surfaces as d3tools features without consistently linking their owning topic pages. | `src/d3tools/__init__.py:1-3`; topic ownership map in `_working_summaries/docs_topic_inventory.md:8-24` | wrong-owner | Keep the index as navigation and point detailed behavior to the owning topic pages; do not duplicate their API claims here. |
| Installation documents only the base install and omits the declared optional extras. | `pyproject.toml:25-49` | missing | Add concise installation commands for `more-data`, `thumbnails`, and `remote-data`, with links to the relevant guides. |
| The top-level public imports are not explicitly identified for developers who need the Python entry point. | `src/d3tools/__init__.py:1-3`; `tests/config/test_workflow_definition.py:95-103` | missing | Add a short API entry-point note without turning getting-started into a developer reference. |

## Checker status before edits

- Errors: 8
- Warnings: 1
- Errors: malformed JSON fences in `getting-started.md`, `multi-dataset.md`, and `with-logging.md`, plus the missing `full-pipeline.md` link.
- Warning: environment-dependent `/var/log/workflows` permission check in `with-logging.md`.