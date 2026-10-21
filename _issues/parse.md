# Parse package doc audit

| ID | Description | File & line no. | Type | Severity | Confidence | Proposed action |
|---|---|---|---|---|---|---|
| 1 | `set_dataset` documents that a tagged placeholder resolves to the mapped object after it is mutated via `update(**tags)`, and that “the object itself, not the return value of `update()`, is the replacement value.” The implementation in `src/d3tools/parse/config_resolution.py:95-102` assigns `structure = structure.update(**tags)`, which returns the value from `update()`. For ordinary mutating dataset objects, `update()` commonly returns `None`, so the function can replace the object with `None` instead of preserving the mutated object. This is a caller-facing contract break because tagged placeholders can silently disappear or return `None` when a dataset-like object follows the standard in-place mutation pattern. | `src/d3tools/parse/config_resolution.py:57-103`; `tests/parse/test_config_resolution.py:8-15, 52-65` | Probable bug | high | confirmed | Fix `set_dataset` so it preserves the original mapped object after mutation, or narrow the public contract to dataset-like objects whose `update()` returns the mutated object; add a regression test covering in-place mutators that return `None`. |
| 2 | The regression surface for tagged dataset placeholders is under-tested: the existing tests use a custom object whose `update()` returns `self`, which masks the contract break above. This means the public behavior is only validated for a non-standard `update()` return contract, leaving a real in-place mutator path untested. | `tests/parse/test_config_resolution.py:8-15, 52-65`; `src/d3tools/parse/config_resolution.py:95-102` | Untested behavior | medium | confirmed | Add a focused regression test using a dataset-like object whose `update()` mutates in place but returns `None`, and assert that `set_dataset` still returns the original object with the tag applied. |

No findings in the category of missing public API docs for the `parse` package: the recursive inventory shows the public names exposed by `src/d3tools/parse/__init__.py`, the module entry points, and the reference page all cover the package surface without an obvious omission. No confirmed uncertain behavior was found outside the `set_dataset` contract issue above.

Audit scope and inventory:
- Package audited: `src/d3tools/parse`
- Source modules inspected: `src/d3tools/parse/__init__.py`, `src/d3tools/parse/config_resolution.py`, `src/d3tools/parse/string_rendering.py`, `src/d3tools/parse/parse_utils.py`, `src/d3tools/parse/key_parser.py`
- Reference page inspected: `docs/developer/reference/parse.md`
- Relevant checks run: `pytest tests/parse -q` -> `100 passed in 1.02s`; direct reproduction with a dataset-like object whose `update()` mutates in place and returns `None` -> result was `None` while the object itself was mutated, confirming the contract bug.
- Recursive inventory status: complete for the package directory and its documented public API surface.

Areas that could not be verified: no additional public API omissions were evident beyond the `set_dataset` contract issue; no alternative documented intent for `update()` semantics was found elsewhere in the package.
