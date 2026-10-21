# Logging

## Configure workflow logs

`workflow_log` records workflow and section start, completion, and failure
events. A string is a destination; a mapping provides options:

<!-- check-docs: skip -->
```json
{
  "workflow_log": {
    "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log",
    "run_state_file": "logs/workflow_state.json",
    "level": "INFO",
    "console": true,
    "format_file": "detailed",
    "format_console": "simple"
  }
}
```

For a mapping, `level` defaults to `INFO`, `console` defaults to `true`, and
formats default to `simple`. Available formats are `detailed`, `simple`, and
`minimal`. `format` sets both formats unless `format_file` or
`format_console` overrides it. A mapping without `file` has no file handler.
To suppress both configured outputs, omit `file` and set `console` to
`false`. An omitted or null `workflow_log` does not disable the default
console-only logging.

For local files, parent directories are created when the file handler is
configured. `{now:...}` in the workflow log path is resolved when the workflow
is loaded and its logging manager is constructed, not when `run()` starts.
`run_state_file` is a separate structured output for workflow and section
outcomes; it is not the human-readable text log. See the
[logging configuration reference](../logging/overview.md#configuration) for
all options and output formats.

## Configure dataset logs

Dataset `log` records information associated with writes to that dataset. It
is separate from `workflow_log`:

<!-- check-docs: skip -->
```json
{
  "datasets": {
    "output": {
      "type": "local",
      "key_pattern": "/tmp/workflow-data/output_%Y%m%d.tif",
      "log": {
        "file": "/tmp/workflow-data/logs/output.json"
      }
    }
  }
}
```

The value may be a path string or a mapping with a `file` destination.
Entries are produced as data is written; the details recorded depend on the
write and destination format. JSON and text destinations have defined output
handling; other formats must support the mapping they receive. See
[Dataset Logging](../logging/dataset-logging.md#configuration) for the
destination-specific qualifications.

## Logging failures and lifecycle

The account running the workflow must be able to create or write configured
destinations. An unwritable log path can prevent setup or output and should be
fixed at the destination rather than treated as a successful run. Workflow
exceptions are logged and re-raised; sequential execution stops at the
failing section. Dataset logs are written as part of dataset writes and do
not replace workflow logs.

Inspect the human-readable file or console output for workflow and section
events. If configured, inspect `run_state_file` for structured workflow and
section state. Do not rely on recovery or cleanup guarantees after a failed
run. Python manager lifecycle and implementation details are in the
[Developer Guide — workflow lifecycle](../developer/workflow-lifecycle.md#logging-exceptions-and-cleanup-lifecycle).
