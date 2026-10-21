Gate: FAIL

| ID | Area/journey | Finding | Type | Severity | Evidence | Required action |
|---|---|---|---|---|---|---|
| UG-04 | Operator-facing API and internals boundary | The nine new `docs/user-guide/` pages keep Python examples to `WorkflowDefinition.load()` / `run()`, but other pages in the audited non-developer documentation still expose direct d3tools APIs and object internals: `Dataset.from_options()`, `TimeRange`/`TimeStep` constructors, and inspection of `WorkflowDefinition` attributes. The configuration overview and environment page also expose parser implementation names. These pages remain part of the User Guide audit scope and some are linked from operator-facing pages. | Wrong owner | Medium | `.github/instructions/docs.instructions.md:34-35`; `docs/user-guide/configuration.md:83-84`; `docs/datasets/concepts.md:17,24,37-38`; `docs/datasets/time-and-availability.md:12`; `docs/timestepping.md:25-28`; `docs/examples/minimal-workflow.md:71-75`; `docs/examples/multi-dataset.md:49,58-59`; `docs/configuration/overview.md:3-5,17-18`; `docs/configuration/environment.md:3-5` | Move the direct Python API examples and parser/helper implementation detail to the Developer Guide, or replace these non-developer remnants with concise operator guidance and links to the Developer Guide. Keep Python examples outside `docs/developer/` within the documented `WorkflowDefinition.load()` / `run()` boundary. |

## Scope coverage

Audited all nine narrative pages under `docs/user-guide/` and the complete
non-developer Markdown scope, including mapped configuration, dataset,
logging, example, and time-reference pages. Every required User Guide page and
heading in the approved outline is present. The new User Guide pages maintain
the required engine-ownership boundary; the finding above concerns the
remaining non-developer pages in the same audited documentation system.

UG-01 through UG-03 were independently rechecked and are resolved:

- Workflow-log `{now:...}` paths are described as resolving when the workflow
  and logging manager are loaded/constructed in both User Guide configuration
  and logging material and in the mapped logging/configuration pages.
- Omitted section engines use the workflow default before alias matching;
  explicit null or unsupported values fall back to alias matching. Source
  inspection and a read-only construction probe with workflow default `dam`
  selected `door` for a `download` section in both cases.
- The environment page now limits the `.env` note to d3tools not loading such
  files and sends Python-specific setup to the Developer Guide. No
  `python-dotenv`/`load_dotenv()` recipe or stale workflow-log timing or engine
  fallback claim remains in the searched pages.

The four case-study JSON files have matching snippet references in
`docs/user-guide/case-study.md`; each include is marked to skip standalone
snippet checking, and `pymdownx.snippets` is enabled in `mkdocs.yml`. All four
files loaded through `WorkflowDefinition.load(...,
build_workflow_objects=False)`. Engine section object construction also
passed for all four using an in-memory validation copy with the workflow-log
file destination omitted; this validated configuration construction only,
not engine execution. The case-study payloads were compared with the approved
DOOR, DAM, and DRYES source examples and keep engine-owned options out of the
narrative contract.

No engine workflow was re-run during this audit. The existing output tree
contains 45 CHIRPS input files, 43 one-month aggregate files, and 37
three-month aggregate files. Step 1 and Step 2 were therefore verified only
against those existing artifacts, not by re-execution. No SPI output files
were found. Step 3 and the combined workflow were not run: the parameter
bundle is absent, and DRYES parameter-window binding remains an open question.
The configuration loads/builds do not verify the DRYES runtime behavior or
parameter availability.

## Checks run

- `scripts/check_docs.py` over all non-developer documentation pages:
  32 Markdown files, 0 errors, and 1 DNS-dependent warning at
  `docs/configuration/datasets.md:146` for its remote-data example.
- `pytest -q tests/config/test_workflow_section.py tests/config/test_parsing_pipeline_workflow_log.py tests/logging/test_workflow_log_manager.py`:
  117 passed.
- Direct case-study JSON load and engine-object construction checks, as
  described above.
- Read `mkdocs.yml`; independently checked all 27 Markdown navigation targets,
  and all exist.
- Checked required User Guide headings, case-study snippet references,
  operator-facing stale-claim searches, and `git diff --check`.
- Did not run MkDocs or execute the case-study engines.

## Unresolved code and maintainer decisions

The open-question file correctly leaves the parameter bundle's location and
provenance, DRYES parameter layout/window binding and `timesteps_per_year`
value, engine-owned option semantics, and CHIRPS product/redistribution
decisions unresolved. It also records candidate additional examples. The
User Guide identifies Step 3 as not engine-verified and the missing bundle as
a prerequisite; these decisions do not independently block the gate.

Maintainer note: the approved outline's case-study index says “shipped SPI
parameters” at `documentation_audits/guide-outline.md:141`, while its
distribution decision says to use a placeholder until that decision is
resolved. The User Guide itself states that the bundle is unavailable and
uses a placeholder; reconcile the outline wording.

## Criteria preventing a pass

UG-04 is a current medium-severity ownership finding. The gate remains FAIL
until the non-developer documentation no longer exposes Python APIs and
parser internals outside the permitted `WorkflowDefinition.load()` / `run()`
surface. The DNS warning is not a checker error; the open engine decisions
are explicitly qualified and are not gate blockers.
