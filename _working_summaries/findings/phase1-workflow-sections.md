# Phase 1 findings: workflow-sections

## Initial checker output

Command: `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py docs/configuration/workflow-sections.md`

Initial result: 13 errors, 0 warnings.

- Broken links at `docs/configuration/workflow-sections.md:159`, `:205`, `:262`, and `:638-641`: the DOOR, DAM, and DRYES integration pages and full-pipeline example do not exist at the linked paths.
- Invalid JSON at `docs/configuration/workflow-sections.md:124` (unquoted bounds placeholders), `:332` and `:356` (ellipsis placeholders), `:547` (JavaScript-style comment), `:559` and `:585` (ellipsis placeholders).

## Findings

| # | Doc location | Claim or omission | Evidence | Verdict | Action |
|---|--------------|------------------|----------|---------|--------|
| 1 | `docs/configuration/workflow-sections.md:5-17` | The alias table presents `dryes_calculator` as a supported alias, omits supported aliases (`downloader`, `ingest`, `dryes_index`, `index`), and describes aliases as fixed generic operation types. The actual aliases map to only the `door`, `dam`, and `dryes` engines; matching is case-insensitive and permits the alias within a longer section key. | `src/d3tools/config/workflow_section.py:13-24`, `:46-50`; `tests/config/test_workflow_section.py:20-60` | wrong | Replace the alias table with the actual mappings and explain alias resolution without implying custom engine types. |
| 2 | `docs/configuration/workflow-sections.md:68-110`, `:141-159`, `:184-205`, `:233-263`, `:539-550`, `:621-633` | Arrays are described as parallel execution. The parser instead flattens each list item into a separately numbered section, preserving list order; `WorkflowDefinition` runs collected sections sequentially. | `src/d3tools/config/parsing_pipeline.py:169-190`; `src/d3tools/config/workflow_definition.py:176-204`; `tests/config/test_parsing_pipeline.py:184-205`; `tests/config/test_workflow_definition.py:307-354` | wrong | State that list items are separate sequential sections; remove parallel execution claims and examples. |
| 3 | `docs/configuration/workflow-sections.md:21-67`, `:397-529` | Examples use top-level `name` and `time` fields, and the complete example repeats them. Those keys are not reserved workflow configuration keys, so the collector attempts to interpret them as sections; `workflow_name` is the supported name key. | `src/d3tools/config/workflow_definition.py:39`, `:77-82`; `src/d3tools/config/parsing_pipeline.py:161-190` | wrong | Use `workflow_name` where a name is needed and remove `time` examples from this page. |
| 4 | `docs/configuration/workflow-sections.md:112-318`, `:397-529` | Detailed DOOR/DAM/DRYES and generic custom-section schemas, operations, parameters, and publication behavior are asserted without support in d3tools. d3tools maps section payloads to external engine builders and dispatches their runtime methods; those engine-specific schemas belong to their respective engine documentation. | `src/d3tools/config/parsers.py:152-181`; `src/d3tools/config/workflow_section.py:246-252` | wrong-owner | Remove detailed engine schemas and replace with a concise pointer to the corresponding DOOR, DAM, or DRYES documentation when available. |
| 5 | `docs/configuration/workflow-sections.md:319-346` | The page claims the shown explicit `workflow_sections` JSON list is a supported input format. The collector skips that key, then replaces it with the collected list. A runtime probe with the documented mapping shape produced an empty `workflow_sections` list. The maintainer decision is that users must not define `workflow_sections` directly in JSON; it is an internal parsed-workflow field. | `src/d3tools/config/parsing_pipeline.py:161-165`, `:193-195`; `src/d3tools/config/workflow_definition.py:78`, `:189-204` | wrong | Replace the unsupported input example with a clear statement that users define sections with ordinary top-level section keys and that `workflow_sections` is internal. |
| 6 | `docs/configuration/workflow-sections.md:348-382` | Sections are said to iterate over the configured time range one timestep at a time. `WorkflowDefinition` passes a single resolved range to each section; only the optional `split` execution option subdivides a range into monthly ranges. | `src/d3tools/config/workflow_definition.py:176-204`; `src/d3tools/config/workflow_section.py:193-252`; `tests/config/test_workflow_definition.py:307-354` | wrong | Replace the timestep walkthrough with the actual range propagation and monthly `split` behavior. |
| 7 | `docs/configuration/workflow-sections.md` | The page omits section execution options and their range-resolution behavior: `all_available` (DAM only), `times_from_run`, `repeat_window`, and `split`; it also omits workflow-level `engine` and `exec_options` defaults, per-section overrides, environment-variable overrides, and automatic range resolution/skipping. | `src/d3tools/config/workflow_section.py:26-29`, `:89-110`, `:114-190`, `:193-252`; `src/d3tools/config/parsing_pipeline.py:154-190`; `tests/config/test_workflow_section.py:110-195`, `:279-370` | missing | Add a compact, verified explanation of workflow-level defaults, section options, environment overrides, and automatic range resolution, including the DAM-only constraint and skip/error cases. |
| 8 | `docs/configuration/workflow-sections.md:384-395`, `:592-594` | The logger-format walkthrough and blanket recommendation to always enable workflow logging are detailed logging-topic material. WorkflowDefinition does provide workflow and section logging when configured, but manager configuration and output details are owned by logging. | `src/d3tools/config/workflow_definition.py:87-88`, `:151-170`, `:186-204`; `tests/config/test_workflow_definition_logging.py:76-101` | wrong-owner | Keep a short pointer to the logging guide; remove the fabricated sample log format and unconditional recommendation. |
| 9 | `docs/configuration/workflow-sections.md:596-634` | The troubleshooting section gives an unsupported alias error message and repeats the incorrect parallel-array advice. Recognized keys and engines are validated by the implementation with different behavior and messages. | `src/d3tools/config/workflow_section.py:73-97`; `src/d3tools/config/parsing_pipeline.py:161-190` | wrong | Remove unsupported error text and parallel advice; document only verified errors if retaining troubleshooting. |
| 10 | `docs/configuration/workflow-sections.md:5-17`, `:68-110`, `:348-382` | The page does not distinguish engine dispatch from workflow-section orchestration or explain that ordinary section keys are scanned in JSON insertion order and unknown non-reserved keys fail section recognition. | `src/d3tools/config/parsing_pipeline.py:161-190`; `src/d3tools/config/workflow_section.py:73-97`; `tests/config/test_parsing_pipeline.py:164-182` | missing | Add the ordering, recognized-alias/engine, and unknown-key behavior to the corrected orchestration overview. |

## Verdict counts

- correct: 0
- wrong: 6
- missing: 2
- unverified: 0
- code-bug?: 0
- wrong-owner: 2

## Maintainer decision

- Row 5: users should not author `workflow_sections` directly in JSON. The page now documents it as an internal field populated by the parser; ordinary top-level section keys are the supported configuration form.

## Remaining checker errors

- Final run: `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py docs/configuration/workflow-sections.md` passed with 0 errors and 0 warnings.