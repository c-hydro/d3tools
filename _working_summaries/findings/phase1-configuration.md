# Phase 1 findings: configuration

| # | Doc location | Claim / gap | Evidence | Verdict | Action |
|---|--------------|-------------|----------|---------|--------|
| 1 | `docs/configuration/overview.md:40-49` | The abbreviated JSON is treated as a complete workflow by the docs checker; its dataset placeholder has no usable local dataset fields and parsing fails. | `src/d3tools/config/parsing_pipeline.py:46-59`; `src/d3tools/data/datasets/local_dataset.py:13-28` | wrong | Mark this intentionally partial example with the checker skip marker. |
| 2 | `docs/configuration/tags.md:26-39` | The example creates a `local` dataset with `path` but no `filename`/`file`; later construction expects the path components. | `src/d3tools/data/datasets/local_dataset.py:13-28`; `src/d3tools/data/datasets/dataset.py:102-119` | wrong | Add a filename so the example represents a valid dataset. |
| 3 | `docs/configuration/tags.md:119-170` | The dataset-update examples declare lowercase `datasets` but use `{DATASETS...}`. Dataset prefixes are literal flattened keys; the examples therefore leave a string and update-tag resolution calls `.update()` on that string. | `src/d3tools/config/parsing_pipeline.py:61-68`; `src/d3tools/parse/config_resolution.py:77-91`; `tests/config/test_parsing_pipeline.py:127-151` | wrong | Replace the out-of-topic detail with a concise pointer to the datasets guide. |
| 4 | `docs/configuration/environment.md:20-25` | The page does not describe the error for a non-dictionary top-level `env` value (`TypeError`); it currently states only that the key is an object. | `src/d3tools/config/parsing_pipeline.py:216-223`; `tests/config/test_parsing_pipeline.py:445` | missing | State that `env` may be `null` or a mapping, and other values raise `TypeError`. |
| 5 | `docs/configuration/tags.md:96-103` | Top-level tag list expansion is not documented: a list-valued tag expands the containing string into a list of rendered strings. | `src/d3tools/parse/string_rendering.py:35-37`; `tests/parse/test_string_rendering.py:25-28` | missing | Add a concise note on list-valued tag expansion. |
| 6 | `docs/configuration/tags.md:121-170` | Detailed dataset update syntax and effects belong to the datasets topic, not the top-level configuration-tags page. | `src/d3tools/data/datasets/dataset.py:243`; `tests/config/test_parsing_pipeline.py:139-151` | wrong-owner | Keep a short link to the datasets guide; do not duplicate update semantics. |
| 7 | `docs/configuration/tags.md:172-198` | Detailed engine-specific case behavior belongs to the workflow-sections topic, not configuration tags; d3tools passes parsed section payloads to the selected engine. | `src/d3tools/config/workflow_section.py:88-103`; `_working_summaries/docs_topic_inventory.md:18` | wrong-owner | Replace detailed examples with a short pointer to Workflow Sections. |
| 8 | `docs/configuration/overview.md:101-102` | The page says dataset placeholders must occupy the whole string, but `set_dataset()` uses an unanchored `re.match`; a runtime check confirmed `{x}/suffix` resolves to the dataset object. | `src/d3tools/parse/config_resolution.py:77-82`; runtime check with the shared venv | code-bug? | Leave code and documentation unchanged for maintainer decision. No code change planned; trailing-text acceptance is not supported usage and will not be documented as a feature.|
| 9 | `docs/configuration/overview.md:82-125` | The described top-level keys and resolution order match the implementation. | `src/d3tools/config/workflow_definition.py:39`; `src/d3tools/config/parsing_pipeline.py:216-238` | correct | Keep. |
| 10 | `docs/configuration/tags.md:21-24,44-79` | Recursive tag substitution and environment/time-before-tags-before-datasets ordering match the pipeline. | `src/d3tools/config/parsing_pipeline.py:228-233`; `tests/config/test_parsing_pipeline.py:58-97` | correct | Keep. |
| 11 | `docs/configuration/environment.md:1-19,45-82` | Environment placeholders support the documented default forms, resolve recursively, and raise `ValueError` when a variable has no value or default. | `src/d3tools/parse/config_resolution.py:16-50`; `tests/config/test_parsing_pipeline.py:30-55` | correct | Keep. |

## Verdict counts
- correct: 3
- wrong: 3
- missing: 2
- wrong-owner: 2
- unverified: 0
- code-bug?: 1

## code-bug? rows
- Row 8: dataset-reference matching accepts a valid reference prefix followed by extra text.

## Remaining checker errors
- Before corrections: 4 errors across `overview.md` and `tags.md` (the phase-1 checker run).
- After corrections: none; the same three pages pass with 0 errors and 0 warnings.