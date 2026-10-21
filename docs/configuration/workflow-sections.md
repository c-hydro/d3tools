# Workflow Sections

Workflow sections are collected from non-reserved top-level workflow keys and run sequentially. d3tools resolves each section to the `door`, `dam`, or `dryes` engine, then passes the section payload to that engine. Engine-specific payload options belong to the corresponding engine documentation.

## Section Aliases

Section names are matched case-insensitively. An alias can occur inside a longer key, such as `Download_01` or `MyProcess`.

| Alias | Engine |
|-------|--------|
| `door_downloader`, `downloader`, `download`, `ingest` | `door` |
| `dam_workflow`, `process`, `publish` | `dam` |
| `dryes_index`, `index`, `calculate` | `dryes` |

A section can specify an `engine` in its definition. Otherwise it uses the workflow-level `engine` default, if provided, then resolves the section key as an alias. The supported engine values are `door`, `dam`, and `dryes`. An unrecognized non-reserved top-level key raises `ValueError` during parsing.

## Section Order and Lists

The parser scans top-level keys in insertion order. A section value can be a single definition or a list of definitions. Each list item becomes a separate section with a two-digit suffix, such as `process_01` and `process_02`; sections and list items are still executed sequentially. Lists do not enable parallel execution.

The example shows the section-collection shape only. Each payload must contain options accepted by its selected engine.

<!-- check-docs: skip -->
```json
{
    "workflow_name": "example",
    "process": [
        {"engine": "dam", "...": "engine-specific options"},
        {"engine": "dam", "...": "engine-specific options"}
    ]
}
```

## Execution Options

`engine` and `exec_options` may be set at the workflow level as defaults. A section-level `engine` or `exec_options` takes precedence for that section. For execution options, an environment variable named by the upper-case option key takes precedence over the configured value.

| Option | Type | Behavior |
|--------|------|----------|
| `times_from_run` | string or object | Use a time range saved by a prior workflow run. A string can be a run-state file or `section_name@file`; an object uses `file` and optional `section` keys. If the referenced state has no range, the section is skipped. |
| `all_available` | boolean | For `dam` sections only, use the range from the first to the last available timestep. Other engines raise `ValueError`; no available data also raises `ValueError`. |
| `repeat_window` | time-window string | Extend the selected range backwards, including ranges loaded by `times_from_run`. Can also cause already-completed available data to be reprocessed. |
| `split` | boolean | When the selected range is longer than 31 days, run the section over monthly sub-ranges. The first and last sub-ranges are clipped to the requested start and end. |

For example, `exec_options: {"repeat_window": "7d"}` can be set at the workflow level or inside a section definition. Setting `REPEAT_WINDOW=3d` in the environment overrides that configured value. Boolean environment values are true for `true`, `1`, `yes`, or `on` (case-insensitive).

## Execution Time Range

The command-line runner accepts `--start` and `--end` dates. In Python, `WorkflowDefinition.run()` accepts either a `TimeRange` or `start` and `end` values. A supplied `TimeRange` cannot be combined with `start` or `end`. If a start is supplied without an end, the end defaults to the current time; an end without a start raises `ValueError`.

When run arguments are omitted, `START_DATE` and `END_DATE` environment variables are used. If neither an argument nor an environment variable supplies a range, each section resolves its own range from its data availability:

- By default, the range begins at the first timestep after the last completed timestep and ends at the last available timestep.
- If no work remains, the section is skipped. Missing availability needed to determine the range raises `ValueError`.
- `all_available`, `times_from_run`, and `repeat_window` modify this resolution as described above.

When a workflow-level range is supplied, that same range is passed to each section. Sections execute in configuration order; a failure stops the sequential run.

## Explicit workflow_sections

`workflow_sections` is an internal parsed-workflow field, not a user-authored JSON format. Do not define it directly in workflow configuration; the parser collects sections from ordinary top-level section keys such as `download`, `process`, and `calculate`. See [Section Aliases](#section-aliases) for supported names and [Section Order and Lists](#section-order-and-lists) for list behavior.

## Logging

When `workflow_log` is configured, workflow and section execution events are logged. See the [Logging Guide](../logging/overview.md) for logging configuration and output details.

## See Also

- [Configuration Overview](overview.md)
- [Logging Guide](../logging/overview.md)
