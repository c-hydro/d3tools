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

| Option | Type | Default | Purpose |
| --- | --- | --- | --- |
| `file` | string or dataset configuration | none | Human-readable log destination. Remote datasets use a temporary local file that is uploaded when the manager closes. |
| `run_state_file` | string or dataset configuration | none | Structured workflow and section state, separate from the text log. |
| `level` | string or integer | `INFO` | Python logging threshold for managed loggers and handlers. |
| `console` | boolean | `true` | Enable console output. |
| `format` | string | `simple` | Set both handler formats unless a handler-specific option overrides it. |
| `format_file` | string | value of `format` | Select the file format. |
| `format_console` | string | value of `format` | Select the console format. |
| `logger_name` | string | `d3tools` | Name of the primary configured logger. |

The predefined formats contain these fields:

| Format | Fields |
| --- | --- |
| `detailed` | Timestamp, logger name, level, source filename and line, message. |
| `simple` | Timestamp, level, message. |
| `minimal` | Message only. |

Timestamps use `%Y-%m-%d %H:%M:%S`. An unknown file format falls back to
`detailed`; an unknown console format falls back to `simple`. Workflow logs
append to existing local files. The manager does not configure log rotation.

For local files, parent directories are created when the file handler is
configured. `{now:...}` in the workflow log path is resolved when the workflow
is loaded and its logging manager is constructed, not when `run()` starts.
`run_state_file` is a separate structured output for workflow and section
outcomes; it is not the human-readable text log.

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

The value may be a path string or a mapping with a required `file`
destination. An empty mapping disables dataset logging. Additional mapping
keys are retained as manager options but currently have no defined effect.
`{now:...}` in the destination is resolved when the manager is created.

A successful `Dataset.write_data()` writes the output first, then creates one
log entry. If thumbnail generation is configured, its output path is available
to the entry. The standard fields are:

| Field | Meaning |
| --- | --- |
| `dataset` | Resolved dataset name. |
| `source_key` | Key or path written by the dataset. |
| `thumbnail` | Generated thumbnail path, when present. |
| `time` | Requested dataset time, when present. |
| `time_produced` | Production timestamp; defaults to the time the entry is built. |
| `data_checks` | Summary quality metrics for the written xarray object. |

Data attributes and other write context can add fields; data attributes take
precedence when names overlap. Empty strings, internal strings beginning with
`__`, and unsupported value types are omitted. Datetimes are formatted as
`%Y-%m-%d %H:%M:%S`, numeric metadata is converted to strings, and floats are
rounded to four decimal places.

For an `xarray.DataArray`, `data_checks` contains maximum, minimum, NaN and
zero counts and percentages, sum, and absolute sum. For an `xarray.Dataset`,
the same checks are prefixed with each variable name. The checks ignore NaNs
for extrema and sums.

JSON destinations receive the entry mapping. Text destinations receive a
`---`-delimited block of `key: value` lines. Other dataset formats receive the
mapping unchanged and must support it. Every log write requests append mode;
the chosen destination format and backend must implement compatible append
behaviour.

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
[logging, exceptions, and cleanup](under-the-hood/lifecycle.md#logging-exceptions-and-cleanup).
