# Phase 1 Findings: Spatial

Scope: no existing spatial documentation pages. Per the topic inventory, phase 1 is a gap inventory only. `scripts/check_docs.py` was not run because there are no pages in scope.

| Verdict | Documentation claim or gap | Evidence | Action |
|---|---|---|---|
| missing | The standalone spatial API has no owned documentation page. | `src/d3tools/spatial/__init__.py:1-3` exports `BoundingBox`, the space helpers, and `TemplateManager`. | Create a developer-only spatial page in a later documentation phase. |
| missing | Later developer documentation must cover `BoundingBox` construction, CRS properties, factory methods, coordinate buffering, and CRS transformation, including the `inplace` distinction. | `src/d3tools/spatial/bounding_box.py:12-29,31-49,51-82,84-117` implements these behaviors. | Document the public `BoundingBox` API with exact input and return behavior. |
| missing | Later developer documentation must cover CRS parsing and raster cropping helpers: `get_crs`, `crop_to_bb`, `clip_xarray`, and `buffer_bbox`. | `src/d3tools/spatial/space_utils.py:5-19,21-60,62-70` defines the helpers, including longitude normalization, CRS assignment when absent, and the small negative clip buffer. | Document helper signatures, accepted inputs, projection handling, and edge behavior. Dedicated tests for these helpers were not found. |
| missing | Later developer documentation must cover `TemplateManager` lifecycle operations, template schema, dataset/DataArray extraction, validation, and copy/iteration behavior. | `src/d3tools/spatial/template_manager.py:21-30,40-177,179-239`; `tests/data/test_template_manager.py:56-278`; `tests/data/test_template_manager_advanced.py:42-147` exercise these surfaces. | Add a developer API section with the required template keys and validation controls. |
| missing | Later developer documentation must cover disk caching and reconstruction/application operations, including cache loading controls, corrupted-cache handling, time-dimension reconstruction, `build_array`, and `apply_to_data`. | `src/d3tools/spatial/template_manager.py:241-319,321-424`; `tests/data/test_template_manager_advanced.py:153-277`; `tests/data/test_template_manager_time.py:1-268` verify these behaviors. | Document cache file behavior, supported input types, and reconstruction/application semantics. |
| unverified | `BoundingBox` and the standalone spatial helpers have no dedicated tests in the scoped test surface, so their edge cases and error behavior are not verified by tests. | `src/d3tools/spatial/bounding_box.py:12-117`; `src/d3tools/spatial/space_utils.py:5-70`; the scoped spatial tests are TemplateManager tests at `tests/data/test_template_manager.py:1-307`, `tests/data/test_template_manager_advanced.py:1-293`, and `tests/data/test_template_manager_time.py:1-268`. | Keep later documentation limited to behavior confirmed by implementation or add focused tests before documenting edge cases. |
| wrong-owner | Raster Dataset integration of `TemplateManager` is not owned by the standalone spatial page. | `src/d3tools/data/format_mixins/raster_mixin.py:16-33,283-300` integrates the manager with raster datasets. | Keep detailed integration guidance on the datasets topic; the later spatial page may link to it. |

## Verdict counts

- correct: 0
- wrong: 0
- missing: 5
- unverified: 1
- wrong-owner: 1
- code-bug?: 0

## Remaining checker errors

None. No spatial pages exist to check.