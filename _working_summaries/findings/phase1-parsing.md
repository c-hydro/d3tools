# Phase 1 Findings: Parsing

Scope: no existing parsing documentation pages. Per the topic inventory, phase 1 is a gap inventory only. `scripts/check_docs.py` was not run because there are no pages in scope.

| Verdict | Documentation claim or gap | Evidence | Action |
|---|---|---|---|
| missing | The parsing topic has no owned developer documentation page. The public parsing surface is the `d3tools.parse` package and `KeyParser`/`ParsedKey` exports, not the workflow JSON pipeline. | `src/d3tools/parse/__init__.py:1-12`; `src/d3tools/parse/key_parser.py:37-46,48-54`; `tests/parse/test_key_parser.py:1-260` | Create a developer-only parsing page in a later documentation phase, and keep JSON workflow parsing under configuration. |
| missing | Later developer documentation must cover the canonical `KeyParser` API: rendering from datetime/timestep inputs, `time_signature` handling, `normalize_time`, `expand_overlap_range`, `match`, `get_matching_keys`, and `prefix` semantics. | `src/d3tools/parse/key_parser.py:49-95,116-171,182-245,260-297,302-373`; `tests/parse/test_key_parser.py:18-200` | Document accepted input types, supported signatures, error cases, and resolution rules for file-key discovery. |
| missing | Later developer documentation must cover the string/template rendering helpers: `substitute_string`, `substitute_values`, `normalise_string`, and `increment_version`, including list expansion, datetime formatting, and unresolved placeholder behavior. | `src/d3tools/parse/string_rendering.py:11-96`; `tests/parse/test_string_rendering.py:1-100`; `tests/parse/test_legacy_parse_utils.py:1-39` | Document the placeholder grammar and the exact output behavior for list and datetime values. |
| missing | Later developer documentation must cover environment and dataset placeholder resolution in `set_env` and `set_dataset`, including nested structure traversal, default values, dataset-tag updates, and unresolved-string passthrough. | `src/d3tools/parse/config_resolution.py:9-71`; `tests/parse/test_config_resolution.py:1-71` | Record the accepted placeholder syntax separately from the workflow configuration pipeline, which belongs to configuration. |
| missing | Later developer documentation must cover the compatibility layer in `parse_utils.py` and the explicit legacy exports exposed through `d3tools.parse`, including `extract_date_and_tags`, `flatten_dict`, `make_hashable`, and `format_dict`. | `src/d3tools/parse/parse_utils.py:1-201`; `tests/parse/test_legacy_parse_utils.py:1-150`; `tests/test_legacy_module_aliases.py:1-87` | Document which helpers are compatibility-only and which ones remain part of the public parsing surface. |
| unverified | The parsing surface has no dedicated documentation page, and some legacy compatibility behaviors are only exercised in broad or mixed tests rather than parsing-specific API documentation; the exact edge-case contract should stay conservative until a later phase adds a page. | `src/d3tools/parse/parse_utils.py:66-201`; `tests/parse/test_legacy_parse_utils.py:1-150`; `tests/test_legacy_module_aliases.py:1-87` | Keep the later parsing page scoped to behaviors verified in source and tests, including where compatibility wrappers are intentionally thin. |

## Verdict counts

- correct: 0
- wrong: 0
- missing: 5
- unverified: 1
- wrong-owner: 0
- code-bug?: 0

## Remaining checker errors

None. No parsing pages exist to check.
