# Config package doc audit

| ID | Description | File & line no. | Type | Severity | Confidence | Proposed action |
|---|---|---|---|---|---|---|
| 1 | The package root `d3tools.config` exposes a curated export list in `src/d3tools/config/__init__.py`, but the reference page renders module-level documentation for additional public helpers such as `resolve_now`, `prepare_workflow_log`, `resolve_workflow_section_alias`, and `WorkflowSectionRunResult`. This is not a code bug, but it is a documentation gap: readers can infer a broader package API from the reference page than the root package actually exports via `from d3tools.config import ...`. | `src/d3tools/config/__init__.py:1-29`; `src/d3tools/config/parsing_pipeline.py:14-28, 54-70`; `src/d3tools/config/workflow_section.py:1-41, 82-111`; `docs/developer/reference/config.md:1-10` | Documentation gap | low | confirmed | Clarify in the package docstring or the reference page that `d3tools.config` exports a curated root API while the module pages document additional helper-level public names. |

No findings in the requested categories beyond the package-root versus module-docs visibility gap: no documentation mismatches in parameter/return semantics were confirmed against the implementation, no missing public API entries were identified in the module reference coverage, and no untested or likely defects were supported by the config tests.

Audit scope and inventory:
- Package audited: `src/d3tools/config`
- Source modules inspected: `src/d3tools/config/__init__.py`, `src/d3tools/config/options.py`, `src/d3tools/config/parsers.py`, `src/d3tools/config/parsing_pipeline.py`, `src/d3tools/config/utils.py`, `src/d3tools/config/workflow_definition.py`, `src/d3tools/config/workflow_section.py`
- Reference page inspected: `docs/developer/reference/config.md`
- Behavioral tests reviewed: `tests/config/test_options.py`, `tests/config/test_parsers.py`, `tests/config/test_parsing_pipeline.py`, `tests/config/test_parsing_pipeline_workflow_log.py`, `tests/config/test_workflow_definition.py`, `tests/config/test_workflow_section.py`, `tests/config/test_workflow_state.py`
- Recursive inventory status: complete for the package directory and its public API surface.

Relevant verification performed:
- `python scripts/check_docs.py docs/developer/reference/config.md` -> 0 errors, 0 warnings
- `python -m compileall src/d3tools/config` -> exit 0
- `pytest tests/config -q` -> 214 passed in 1.60s

Areas that could not be verified:
- No additional public API outside the audited config package was inspected, by design.
- The observed gap is limited to API-visibility clarity between the package-root export list and the module reference pages; no confirmed code defect was identified.
