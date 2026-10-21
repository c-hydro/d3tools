# Errors package doc audit

| ID | Description | File & line no. | Type | Severity | Confidence | Proposed action |
|---|---|---|---|---|---|---|
| 1 | The package-level public API is exported in `src/d3tools/errors/__init__.py:1-16`, but the reference page `docs/developer/reference/errors.md:1-12` documents only the module groupings (`d3tools.errors`, `d3tools.errors.gdal_import`, and `d3tools.errors.workflow_import`, `d3tools.errors.template_errors`) instead of explicitly surfacing the package-level exception names that callers import from `d3tools.errors`. This is a discoverability gap for the public API: the re-exported exceptions are present in the package but not clearly exposed as the primary reference entry for user callers. | `src/d3tools/errors/__init__.py:1-16`; `docs/developer/reference/errors.md:1-12` | Missing public API docs | medium | confirmed | Add explicit entries for `GDAL_ImportError`, `WorkflowEngineImportError`, `TemplateError`, `TemplateValidationError`, `TemplateNotFoundError`, `TemplateMemoryError`, and `TemplateCompatibilityError` to the reference page or document the package-level re-export in the page intro so the public contract is discoverable at the package entry point. |
| 2 | The exported exceptions are not directly covered by dedicated public-API tests: `WorkflowEngineImportError` is asserted in `tests/config/test_parsers.py:484-503`, and `TemplateValidationError` is exercised in `tests/data/test_template_manager_advanced.py:41-72`, but `GDAL_ImportError`, `TemplateNotFoundError`, `TemplateMemoryError`, and `TemplateCompatibilityError` have no matching direct coverage. The docstrings describe these classes as part of the public contract, but the exact runtime behavior of their message formatting and public attributes is unverified in the suite. | `src/d3tools/errors/gdal_import.py:8-37`; `src/d3tools/errors/template_errors.py:66-161`; `tests/config/test_parsers.py:484-503`; `tests/data/test_template_manager_advanced.py:41-72` | Untested behavior | low | confirmed | Add focused tests for each exported exception class to lock in the documented message format, public attributes, and the expected failure modes. |

No findings in the requested categories beyond the package discoverability and test-coverage gaps above: no confirmed docstring/implementation mismatches were found in the public exception signatures or message-generation logic, and no documented behavioral defects were supported by the current validation checks.

Audit scope and inventory:
- Package audited: `src/d3tools/errors`
- Source modules inspected: `src/d3tools/errors/__init__.py`, `src/d3tools/errors/gdal_import.py`, `src/d3tools/errors/workflow_import.py`, and `src/d3tools/errors/template_errors.py`
- Reference page inspected: `docs/developer/reference/errors.md`
- Behavioral tests reviewed: `tests/config/test_parsers.py` and `tests/data/test_template_manager_advanced.py`
- Recursive inventory status: complete for the selected package scope.

Relevant verification performed:
- `python -m compileall src/d3tools/errors` -> `0 error(s)`
- `python scripts/check_docs.py docs/developer/reference/errors.md` -> `0 error(s), 0 warning(s)`
- `pytest tests/config/test_parsers.py tests/data/test_template_manager_advanced.py -q` -> `53 passed in 1.58s`

Areas that could not be verified:
- There is no dedicated direct test file for the `errors` package itself; the coverage here is indirect via parser and template-manager tests.
- The audit deliberately excluded modules outside `src/d3tools/errors`, by design.
