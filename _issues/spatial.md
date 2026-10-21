# Spatial package doc audit

| ID | Description | File & line no. | Type | Severity | Confidence | Proposed action |
|---|---|---|---|---|---|---|
| 1 | `d3tools.spatial` declares a package-level re-export with `from .space_utils import *` in `src/d3tools/spatial/__init__.py:10`, but `space_utils.py` imports module-level names like `CRS`, `xr`, and `Sequence` at import time. That means the runtime package namespace exposes undocumented symbols beyond the intended API (`BoundingBox`, `TemplateManager`, `get_crs`, `crop_to_bb`, `clip_xarray`, `buffer_bbox`). The reference page at `docs/developer/reference/spatial.md` presents a curated module list, but the actual public namespace is broader than the documented contract, which is a real API/doc mismatch for callers that inspect `dir(d3tools.spatial)` or rely on a clean public surface. | `src/d3tools/spatial/__init__.py:1-10`; `src/d3tools/spatial/space_utils.py:1-10`; `docs/developer/reference/spatial.md:1-9`; runtime introspection of `dir(d3tools.spatial)` during audit | Documentation mismatch | medium | confirmed | Define an explicit `__all__` in `src/d3tools/spatial/space_utils.py` and/or replace the wildcard import in `src/d3tools/spatial/__init__.py` with explicit re-exports so the package docs match the supported public API. |

No findings in the requested categories beyond the package-export mismatch: no confirmed docstring parameter or return mismatches were found in the audited public classes/functions, and no missing public docstrings were identified in `BoundingBox`, `TemplateManager`, or the space-utils helpers.

Audit scope and inventory:
- Package audited: `src/d3tools/spatial`
- Source modules inspected: `src/d3tools/spatial/__init__.py`, `src/d3tools/spatial/bounding_box.py`, `src/d3tools/spatial/space_utils.py`, and `src/d3tools/spatial/template_manager.py`
- Reference page inspected: `docs/developer/reference/spatial.md`
- Behavioral tests reviewed: `tests/data/test_template_manager.py`, `tests/data/test_template_manager_advanced.py`, `tests/data/test_template_manager_time.py`, `tests/data/test_dataset_templates.py`, and `tests/data/format_mixins/test_raster_mixin.py` (TemplateManager behavior is covered; no direct tests validate the package-level `d3tools.spatial` export namespace)
- Recursive inventory status: complete for the package directory and its public API surface within the selected module scope.

Relevant checks considered:
- Writer summary: “docs checker 0 errors, 0 warnings; compileall spatial -> success.”
- Runtime namespace check during this audit: `d3tools.spatial` exposes undocumented names including `CRS`, `Sequence`, and `xr` because of the wildcard re-export.
- No broader package tree was inspected beyond the selected module.

Areas that could not be verified:
- The exact rendered HTML for the spatial reference page was not generated during this read-only audit; the issue is therefore limited to the source page and the runtime namespace, not a built docs artifact.
- No direct tests validate the `d3tools.spatial` namespace export list or the generated reference page’s included members.
