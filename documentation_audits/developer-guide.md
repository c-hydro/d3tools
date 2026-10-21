Gate: PASS

| ID | Area/journey | Finding | Type | Severity | Evidence | Required action |
|---|---|---|---|---|---|---|
| D-A1 | Section construction and engine selection | The stated precedence is incomplete for a section-level `engine` value that is `null` or unsupported. Section options overwrite the workflow default during collection, then `WorkflowSection.from_config()` falls directly to the section-name alias. For example, workflow default `dam` with a `download` section whose `engine` is `null` selects DOOR, not DAM. | Incorrect | Low | `docs/developer/workflow-lifecycle.md:39-45`; `src/d3tools/config/parsing_pipeline.py:235,247-265`; `src/d3tools/config/workflow_section.py:125-129`; `tests/config/test_workflow_section.py:74-90` (covers alias fallback with no section engine, not this override case). Independently probed `engine="dam"` with `download: {"engine": null}`; selected engine was `door`. | Clarify that a workflow default applies when a section omits `engine`, while a null/unsupported section value falls back to alias matching; add a focused regression test if this behavior is intended. |

## Scope coverage

Audited all nine narrative Markdown files under `docs/developer/`, including the
two legacy link-only pages; generated pages under `docs/developer/reference/`
were evidence and checker inputs, not narrative audit targets. The seven
outline-defined pages and all 25 required section headings are present.
Independently counted and checked all 89 mapping rows destined for the
Developer Guide: each maps to an existing target page and heading, and the
destination pages provide substantive detail and actionable coverage. Read
`mkdocs.yml`: all seven narrative Developer Guide nav entries and all eleven
generated-reference targets exist. Internal links are checker-clean.

The requested prior-finding ledger D1–D10 is not present in the worktree or
available Git history, so those identifiers cannot be reconciled one-by-one.
The accessible predecessor report is `DEV-01`–`DEV-04` in commit `5c0d32a`:
the first three narrative findings no longer apply (current-time resolution is
distinguished, extension seams have detailed bounded recipes, and off-site
open-question links are absent); the generated-reference/strict-build finding
is out of scope under the current instructions. No known prior narrative
finding remains.

## Checks run

- `PYTHONPATH=src /home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py docs/developer` — **20 files, 0 errors, 0 warnings**.
- The checker found 17 fenced Python examples; all compiled and their checked imports succeeded.
- Focused tests: **583 passed** across configuration, workflow sections/state, dataset construction/update/fallback/versioning/catalogue/templates, cases, timestepping, thumbnails, and logging. Additional timestamp/logger/dataset checks: **199 passed** (overlapping coverage, not an additional total).
- Independently executed safe in-memory construction/key-rendering, parent-composition, update, and Dataset write-order probes; all passed. The dispatch-precedence probe confirmed the low-severity finding above.
- `git diff --check` passed. Navigation targets were checked by reading `mkdocs.yml`; **MkDocs was not run**.

## Unresolved code decisions

The maintained open-question register remains explicit about unresolved exit
callback ordering/timing/error behavior and the `ExitHandler` export boundary;
curated spatial/timestepping exports; and intended package-level exception
exports plus untested exception details. `EmailNotification` remains
experimental and unsupported for current use. These are documented
conservatively and do not block the gate.

## Criteria preventing a pass

None. The single current finding is low severity and does not meet the
high/medium finding threshold; the approved outline is implemented and the
documentation checker is clean.
