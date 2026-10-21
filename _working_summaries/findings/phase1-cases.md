# Phase 1 Findings: Cases

Scope: no existing cases documentation pages. Per the topic inventory, phase 1 is a gap inventory only. `scripts/check_docs.py` was not run because there are no pages in scope.

| Verdict | Documentation claim or gap | Evidence | Action |
|---|---|---|---|
| missing | The cases API has no owned developer documentation page. | `src/d3tools/cases/__init__.py:1` exports `CaseManager`; `src/d3tools/cases/case.py:3-38` defines `Case` and `get_cases` in the case module; `src/d3tools/cases/utils.py:4-180` defines the decorator and case/ID helpers. | Create a developer-only cases page in a later documentation phase. |
| missing | Later developer documentation must cover `Case` construction, copied `options` and tags, filtered runtime tags, generated names, string representations, equality, and addition of cases without mutating the operands. | `src/d3tools/cases/case.py:5-38`; `tests/cases/test_case_manager.py:18-42` verifies these behaviors. | Document the `Case` object and the distinction between `tags` and `alltags`. |
| missing | Later developer documentation must cover `permutate_options` and `get_cases`: dictionary-valued options produce the Cartesian product, dictionary keys become case tags, and non-dictionary values remain fixed; no mapping still produces one case. | `src/d3tools/cases/utils.py:100-120`; `src/d3tools/cases/case.py:34-38`; `tests/cases/test_case_manager.py:45-74` verifies these behaviors. | Document accepted option shapes, returned values, and the no-mapping edge case. |
| missing | Later developer documentation must cover `CaseManager` initialization, named layers, integer and string layer lookup, generated case IDs, and `add_layer` behavior for expanded and fixed options. | `src/d3tools/cases/case_manager.py:7-35,38-104`; `tests/cases/test_case_manager.py:77-130` exercises root layers, named layers, Cartesian children, fixed values, and already-expanded mappings. | Document the layer model, layer naming, IDs, and option-merging rules. |
| missing | Later developer documentation must cover layer merging and converging parent paths, including the removal of the merged dimension from the resulting case and parent ID representation. | `src/d3tools/cases/case_manager.py:54-82`; `tests/cases/test_case_manager.py:133-143,172-180` verifies merge and converging-tree traversal. | Document `add_layer(..., merge=...)` with exact ID and case-content behavior. |
| missing | Later developer documentation must cover lookup and tree traversal: `find_case`, `iterate_tree`, `get_subtree`, and `iterate_subtree`, including layer-return controls, depth limits, parent-before-child order, diverging-tree traversal, and the unknown-ID `ValueError`. | `src/d3tools/cases/case_manager.py:106-164`; `tests/cases/test_case_manager.py:146-180,183-200` verifies lookup, traversal, depth, and the error. | Add API-level examples and state the return shapes and traversal limitations. |
| missing | Later developer documentation must cover `withcases`: absent or `None` `cases` performs a normal call; a list runs once per case and returns a result list; each case must provide `tags`; case tags override existing keyword values; the decorator consumes `cases`; empty input returns an empty list; and wrapped exceptions propagate. | `src/d3tools/cases/utils.py:4-98`; `tests/cases/test_withcases.py:11-180` verifies these behaviors. | Document the decorator contract and its required case structure. |
| missing | Later developer documentation should cover `split_id` and `get_parents`, which preserve separators inside bracketed merged IDs and return deduplicated parent paths in length order. | `src/d3tools/cases/utils.py:132-180`; `tests/cases/test_case_manager.py:202-220` verifies merged-parent parsing. | Document these helpers as ID-format support APIs, or explicitly mark them internal if they are not intended for direct use. |
| unverified | `rand_str` and some invalid-input behavior have implementation but no dedicated scoped tests, so their edge cases should not be documented beyond behavior directly evident from the implementation. | `src/d3tools/cases/utils.py:122-130`; the scoped tests cover manager-generated IDs but do not directly test `rand_str` input validation or collision behavior. | Keep later documentation conservative or add focused tests before specifying edge-case guarantees. |

## Verdict counts

- correct: 0
- wrong: 0
- missing: 8
- unverified: 1
- wrong-owner: 0
- code-bug?: 0

## Remaining checker errors

None. No cases pages exist to check.