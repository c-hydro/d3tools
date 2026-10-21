# Data package doc audit

| ID | Description | File & line no. | Type | Severity | Confidence | Proposed action |
|---|---|---|---|---|---|---|
| 1 | The compatibility aliases exported by `d3tools.data` (`dataset`, `local_dataset`, `memory_dataset`, `remote_dataset`) are part of the supported public API: `src/d3tools/data/__init__.py` installs them in `sys.modules` and exposes them as package attributes, and `tests/test_legacy_module_aliases.py` explicitly asserts that these alias modules are imported and preserve identity with the canonical modules. The reference page at `docs/developer/reference/data.md` documents only the canonical submodules (`d3tools.data.datasets.dataset`, etc.) and omits the legacy alias paths. That is a real documentation gap for callers who import through the supported package-level aliases. | `src/d3tools/data/__init__.py:15-29`; `tests/test_legacy_module_aliases.py:1-87`; `docs/developer/reference/data.md:1-27` | Missing public API docs | medium | confirmed | Add the `d3tools.data.dataset`, `d3tools.data.local_dataset`, `d3tools.data.memory_dataset`, and `d3tools.data.remote_dataset` alias modules to the reference page or explicitly mark the package-level compatibility aliases as supported legacy entry points. |

No findings in the requested categories beyond the package-level compatibility-alias visibility gap: no confirmed docstring parameter/return mismatches against the implementation were identified, no untested public API omissions were found in the canonical dataset classes, and no confirmed behavioral defects were supported by the data tests.

Audit scope and inventory:
- Package audited: `src/d3tools/data`
- Source modules inspected: `src/d3tools/data/__init__.py`, `src/d3tools/data/data_catalogue.py`, `src/d3tools/data/io_utils.py`, `src/d3tools/data/io_utils_raster.py`, `src/d3tools/data/datasets/__init__.py`, `src/d3tools/data/datasets/dataset.py`, `src/d3tools/data/datasets/local_dataset.py`, `src/d3tools/data/datasets/memory_dataset.py`, `src/d3tools/data/datasets/remote_dataset.py`, and the format-mixin modules under `src/d3tools/data/format_mixins/`
- Reference page inspected: `docs/developer/reference/data.md`
- Behavioral tests reviewed: `tests/test_legacy_module_aliases.py`, `tests/data/test_data_catalogue.py`, `tests/data/test_dataset_initialization.py`, `tests/data/test_dataset_creation_kwargs.py`, `tests/data/test_dataset_update_behavior.py`, `tests/data/test_dataset_key_parsing.py`, and `tests/data/test_dataset_fallback_behavior.py`
- Recursive inventory status: complete for the package directory and its public API surface within the selected module scope.

Relevant verification performed:
- `python scripts/check_docs.py docs/developer/reference/data.md` -> `checked 1 file(s): 0 error(s), 0 warning(s)`
- `pytest tests/test_legacy_module_aliases.py -q` -> `20 passed in 0.80s`

Areas that could not be verified:
- No broader package-level API outside `src/d3tools/data` was inspected, by design.
- The identified gap is limited to API visibility/documentation clarity for the compatibility alias modules; no code defect in the canonical dataset implementations was confirmed.
