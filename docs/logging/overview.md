# Logging Overview

The `workflow_log` workflow option configures logging for `WorkflowDefinition` execution. `Dataset` also supports per-write dataset logging; see [Dataset Logging](dataset-logging.md).

## Configuration

A string value is treated as the log destination:

```json
{
  "workflow_log": "logs/workflow.log"
}
```

A mapping exposes the logging options:

```json
{
  "workflow_log": {
    "file": "logs/workflow.log",
    "level": "INFO",
    "console": true,
    "format_file": "detailed",
    "format_console": "simple"
  }
}
```

| Option | Type | Default for a mapping | Description |
|---|---|---|---|
| `file` | string or Dataset configuration | None | Optional log destination. Local destinations are written by a file handler; non-local Dataset destinations use a temporary local file which is uploaded when the manager closes. |
| `run_state_file` | string or Dataset configuration | None | Optional Dataset destination for structured workflow and section run state. |
| `level` | string or integer | `INFO` | Python logging threshold. |
| `console` | boolean | `true` | Add a console handler. |
| `format` | string | `simple` | Set both output formats unless overridden by `format_file` or `format_console`. |
| `format_file` | string | `format`, otherwise `simple` | File output format: `detailed`, `simple`, or `minimal`. |
| `format_console` | string | `format`, otherwise `simple` | Console output format: `detailed`, `simple`, or `minimal`. |
| `logger_name` | string | `d3tools` | Name of the primary configured logger. |

A mapping without `file` creates no file handler. An omitted or `null` `workflow_log` value does **not** disable logging: it creates the default console-only manager. To suppress both configured outputs, set `console` to `false` and omit `file`.

For a mapping, `format` defaults to `simple` for both handlers. The direct `WorkflowLogManager` constructor instead defaults to `detailed` for files and `simple` for console. A string destination also uses the constructor defaults.

`{now:...}` in destination strings is formatted with Python datetime directives when the logging configuration is resolved. Local file parent directories are created when the file handler is configured. Rotation is not configured by this manager.

## Logged Events

`WorkflowDefinition.run()` logs a workflow start and either successful completion or failure. Each workflow section logs its start and completion or failure, including elapsed time; a supplied engine name is included in the section start message. Exceptions are logged and re-raised. If section run results are available, the workflow can also write them to `run_state_file`.

For manager ownership, logger attachment, and Python execution contexts, see [Developer Guide — Logging, exceptions, and cleanup lifecycle](../developer/workflow-lifecycle.md#logging-exceptions-and-cleanup-lifecycle).

## Formats

The predefined format strings are:

| Format | Output fields |
|---|---|
| `detailed` | Timestamp, logger name, level, source filename and line, message |
| `simple` | Timestamp, level, message |
| `minimal` | Message |

The timestamp format is `%Y-%m-%d %H:%M:%S`. Unknown format names fall back to `detailed` for file handlers and `simple` for console handlers; use one of the listed names for predictable output.

## Related Topics

- [Workflow configuration](../configuration/overview.md) describes the workflow JSON structure and configuration parsing.
- [Workflow sections](../configuration/workflow-sections.md) describes engine selection and section execution.
- [WorkflowLogManager](workflow-log-manager.md) documents its Python API and run-state output.
- [Dataset configuration](../configuration/datasets.md) owns dataset schemas and storage options.
