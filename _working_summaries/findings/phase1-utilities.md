# Phase 1 Findings: Utilities

Scope: no existing utilities documentation pages. Per the topic inventory, phase 1 is a gap inventory only. `scripts/check_docs.py` was not run because there are no pages in scope.

| Verdict | Documentation claim or gap | Evidence | Action |
|---|---|---|---|
| missing | The cleanup API has no owned developer documentation page. The `d3tools.exit` package exports `run_at_exit`, `run_at_exit_first`, and `rm_at_exit`. | `src/d3tools/exit/__init__.py:1` | Create a short developer API reference in a later documentation phase; this is not User Guide behavior. |
| missing | Later developer documentation must explain callback registration and execution: callbacks receive the supplied positional and keyword arguments; ordinary registrations append to the list, `run_at_exit_first` inserts at the front, and one global handler is registered with `atexit` to execute the list in order. | `src/d3tools/exit/exit_handler.py:3-13,15-28` | Document ordering, process-exit timing, and the fact that an exception from a callback interrupts the loop and prevents later callbacks from running. |
| missing | Later developer documentation must describe `rm_at_exit(path, force=True)`: it defers a `subprocess.run` invocation until exit, selecting `rm -rf` when forced and `rm -r` otherwise. | `src/d3tools/exit/exit_handler.py:31-36` | State the exact deferred behavior and platform dependency; do not imply immediate removal. |
| missing | Utility documentation should identify the shared cleanup API's current uses without taking over their feature guidance: workflow-log closing registers `self.close`, and remote datasets and thumbnail generation schedule temporary-path removal. | `src/d3tools/logging/workflow_log_manager.py:113-114`; `src/d3tools/data/datasets/remote_dataset.py:27,32-41`; `src/d3tools/thumbnails/thumbnail_collection.py:90-97` | Keep logging, dataset, and thumbnail behavior detailed on their owning topic pages; link to those topics if useful. |
| unverified | The cleanup helpers and callback ordering have no dedicated tests in the repository's scoped test surface, so error handling and edge-case guarantees are not test-verified. | `src/d3tools/exit/exit_handler.py:3-36`; `tests/test_legacy_module_aliases.py:8-35,47-74` tests data and timestepping aliases, not the exit helpers. | Keep future documentation limited to behavior explicit in the implementation or add focused tests before promising additional guarantees. |

## Verdict counts

- correct: 0
- wrong: 0
- missing: 4
- unverified: 1
- wrong-owner: 0
- code-bug?: 0

## Remaining checker errors

None. No utilities pages exist to check.