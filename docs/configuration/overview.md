# Configuration Overview

d3tools workflows are configured with JSON (or a plain Python `dict`) and turned into a
`WorkflowDefinition` by running the config through a fixed parsing pipeline
(defined in `d3tools.config.parsing_pipeline.parse_options`). JSON configuration files are parsed
with the `WorkflowDefinition.load()` method.

```python
from d3tools import WorkflowDefinition

wf = WorkflowDefinition.load("workflow.json")
wf.run(start="2024-01-01", end="2024-12-31")
```

## Top-Level Keys

`WorkflowDefinition.RESERVED_TOP_LEVEL_KEYS` is the exact set of top-level keys the
parser treats specially. **Any other top-level key is assumed to be a workflow
section** (see below) and must resolve to a recognised engine, or parsing raises
`ValueError`.

| Key | Type | Meaning |
|-----|------|---------|
| `workflow_name` | `string` | Name used in logging and run-state files. Defaults to `"workflow"` if omitted. |
| `tags` | `object` | Reusable placeholder values, see [Tags](tags.md). |
| `datasets` | `object` | Dataset definitions, see [Datasets](datasets.md). |
| `env` | `object` | Key/value pairs exported to `os.environ` *before* any placeholder resolution. |
| `workflow_log` | `string \| object \| null` | Logging configuration, see [Logging](../logging/overview.md). |
| `workflow_sections` | `array` | Escape hatch to pass already-built section entries directly (rarely used by hand). |
| `exec_options` | `object` | Default execution options applied to every section that doesn't define its own (see [Workflow Sections](workflow-sections.md)). |
| `engine` | `string` | Default engine (`door`, `dam`, `dryes`) applied to sections that don't specify one. |
| `other_options` | `object` | Free-form bag stored on `WorkflowDefinition.other_options`, not otherwise interpreted. |

There is no top level key to define timestepping: each
workflow section works out its own execution range at run time (from `run()`
arguments, `START_DATE`/`END_DATE` environment variables, prior run state, or the
section's own data availability). See [Workflow Sections](workflow-sections.md) for
how time resolution works.

<!-- check-docs: skip -->
```json
{
    "workflow_name": "era5_download_workflow",
    "tags": { "...": "..." },
    "datasets": { "...": "..." },
    "workflow_log": { "file": "logs/workflow.log" },

    "door_downloader": { "...": "..." }
}
```

## Workflow Sections and Aliases

Every non-reserved top-level key becomes a `WorkflowSection`. Its engine is resolved
using these rules:

1. If the section omits `engine`, use the workflow-level `engine` default when
   configured; otherwise match the section name (case/dash/space-insensitively)
   against the alias table.
2. If the section explicitly sets a supported `engine`, use it.
3. If the section explicitly sets `engine` to null or an unsupported value, match
   the section name against the alias table instead; do not use the workflow default.

If none of these provides a recognised engine, parsing raises `ValueError`.
The recognised engines are `door`, `dam`, and `dryes`.

The recognised aliases are:

| Alias | Engine |
|-------|--------|
| `door_downloader`, `downloader`, `download`, `ingest` | `door` |
| `dam_workflow`, `process`, `publish` | `dam` |
| `dryes_index`, `index`, `calculate` | `dryes` |

A section key can also hold a **list** of definitions; each item becomes its own
section named `{key}_01`, `{key}_02`, etc. This list form defines multiple section
entries; it is separate from execution configuration. Each section definition may
set its own `exec_options`, which override the workflow-level `exec_options` default.

See [Workflow Sections](workflow-sections.md) for the full engine-resolution rules,
list definitions, `exec_options`, and execution-range resolution options
(`times_from_run`, `all_available`, `repeat_window`, `split`).

## Placeholder Syntax

Four kinds of placeholders are resolved while parsing, each covered in its own guide:

| Placeholder | Resolved by | Guide |
|-------------|-------------|-------|
| `{ENV.VAR_NAME}`, `{ENV.VAR_NAME, default='value'}` | `resolve_env` | [Environment Variables](environment.md) |
| `{now}`, `{now:%Y%m%d}` | `resolve_now` | this page |
| `{tag}`, `{tag:format}` | `resolve_tags` | [Tags](tags.md) |
| `{dataset_key}`, `{dataset_key, tag='value'}` | `resolve_dataset_refs` | [Datasets](datasets.md) |

`{now:...}` renders the current `datetime.datetime.now()` at parse time, with
`...` used as an `strftime` format. For `workflow_log.file`, its `{now:...}`
placeholder is resolved when the workflow is loaded and its logging manager is
constructed, not when the workflow runs.

Dataset placeholders can be written as `{my_data}` or with the datasets key as a
prefix, e.g. `{datasets.my_data}`. The prefix must match the top-level key exactly
as written in the config: `{DATASETS.my_data}` only resolves if the key is literally
`"DATASETS"`. An unresolved dataset placeholder is silently left as a string (or
raises `AttributeError` if it carries update tags like `{x, variable='t'}`). The
placeholder should be the entire length of the string value: do not include
additional text before or after it.

## Parsing Pipeline

See the [Developer Guide — Configuration contract and parser stages](../developer/configuration.md#configuration-contract-and-parser-stages).

## Next Steps

- [Tags Guide](tags.md)
- [Datasets Guide](datasets.md)
- [Environment Variables Guide](environment.md)
- [Workflow Sections Guide](workflow-sections.md)
- [Logging Guide](../logging/overview.md)
