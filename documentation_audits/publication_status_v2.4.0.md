# d3tools 2.4.0 documentation publication status

**Audit date:** 2026-11-25  
**Branch:** `docs`  
**Overall status:** documentation content is complete for the agreed 2.4.0 scope, but publication still requires the maintainer-side rendered-site check and a decision or correction for two failing tests.

## Completed scope

| Work | Commit |
| --- | --- |
| Added the architecture and engine-integration page and completed the navigation target. | `cffe0d1` |
| Corrected the DRYES aggregation-window binding and defined the accepted SPI release scope. | `8f040a1` |
| Added the runnable multi-dataset workflow example. | `66fb40b` |
| Documented CLI arguments, preflight behavior, engine imports, and exit codes. | `0693aad` |
| Expanded troubleshooting for optional imports, native dependencies, and template failures. | `482537f` |
| Replaced obsolete two-guide governance and removed its unused agents and guard hook. | `3723379` |
| Added root-scoped ignore rules for `.pytest_cache/`, `.ruff_cache/`, and `tmp/`. | `3975109` |

As a separate ignored/untracked filesystem cleanup with no commit, `_old_docs/`,
`build/`, `.pytest_cache/`, `.ruff_cache/`, `src/d3tools.egg-info/`, repository
`__pycache__/` directories, `.coverage`, and the empty `tmp/` directory were removed.
This reclaimed about 11 MiB and changed no tracked files.

No commit was pushed, and `main` was not changed.

## Verification results

- `scripts/check_docs.py`: **29 files checked, 0 errors, 0 warnings**.
- `mkdocs.yml` navigation target audit: **29 targets, 0 missing**.
- `git diff --check`: clean before this report was created.
- Worktree: clean on `docs` before this report was created.
- Full test suite: **1360 passed, 2 failed**.
- MkDocs was not run, as explicitly requested.

The test failures are:

1. `tests/config/test_workflow_definition.py::TestWorkflowDefinitionRunExecution::test_run_executes_sections_in_order`
2. `tests/config/test_workflow_section.py::TestWorkflowSection::test_run_calls_dryes_compute`

Both failures use DRYES test doubles whose `compute()` methods accept only `time_range`.
Current dispatch calls `compute(time_range, make_parameters=SETUP)`, so the mocks reject
the required `make_parameters` keyword. This appears to be test-fixture drift rather than
a documentation failure, but it should be corrected or explicitly accepted before the
release is finalized.

## Items remaining before publication

| Severity | Item | Required action |
| --- | --- | --- |
| **Publication blocker** | The rendered MkDocs site has not been checked. Mechanical checks do not validate rendering, generated API pages, displayed navigation, theme behavior, or MkDocs warnings. | The maintainer must run the final rendered-site/API/navigation/warning check before publication. |
| **Release decision required** | Two full-suite tests fail because their DRYES mocks do not accept the current `make_parameters` keyword. | Update the mocks or record an explicit release acceptance after confirming the production dispatch contract. |
| **Accepted non-blocking limitation for 2.4.0** | No matching, publishable CHIRPS/dekadal SPI parameter bundle is available. Step 3 output and the combined end-to-end workflow therefore remain unexecuted. | Keep the case study's limitation visible. Supply and validate a matching bundle in follow-up work if Step 3 is to become a fully runnable example. |

During validation, Fontconfig reported that its default cache directories were not
writable. This did not change the documentation-checker result, but a writable cache may
reduce noise during the maintainer's rendered-site check.

