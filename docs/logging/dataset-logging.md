# Dataset Logging

Dataset logging is still supported and is separate from workflow logging.

- `workflow_log` records workflow and section execution;
- the dataset `log` option records information about data written by one
  Dataset.

Dataset logging is handled by `DatasetLogManager`.

## Configuration

Add `log` to a dataset definition. A string is treated as the output path for
the log dataset:

```json
{
    "datasets": {
        "output_data": {
            "type": "local",
            "key_pattern": "/data/output_%Y%m%d.tif",
            "log": "/logs/output_data.json"
        }
    }
}
```

A mapping must contain `file`; additional keys are retained as manager options:

```json
{
    "datasets": {
        "output_data": {
            "type": "local",
            "key_pattern": "/data/output_%Y%m%d.tif",
            "log": {
                "file": "/logs/output_data.json"
            }
        }
    }
}
```

The log path is parsed as a Dataset configuration, so its format is inferred
from the path. JSON output is useful for structured log entries; a `.txt` path
produces a delimited text entry. An already-created Dataset or log manager may
also be supplied through the Python API.

## When Logging Happens

`Dataset.write_data()` performs the normal validation, formatting, metadata, and
storage write first. If a dataset log manager is configured, it then creates a
log entry and writes it to the configured output Dataset with `append=True`.
Reading data does not create a dataset log entry.

Dataset copies preserve the configured log manager. Dataset updates preserve the
manager while updating the Dataset's runtime tags and key pattern.

## Log Contents

A log entry can include:

- the dataset name;
- the source key, when available;
- the output time;
- the time at which the data was produced;
- metadata supplied during the write;
- the thumbnail path, if a thumbnail was generated;
- quality checks for the written values.

Quality checks include maximum and minimum values, NaN counts and percentages,
zero counts and percentages, the sum, and the sum of absolute values. For an
`xarray.Dataset`, checks are recorded for each data variable.

## Output Formats

The manager writes structured entries when the output Dataset format is JSON.
For text output it writes a block containing `key: value` lines delimited by
`---`. Other formats receive the log dictionary and must be supported by the
output Dataset format layer.

## Dataset Logging versus Workflow Logging

Use dataset logging when the record belongs to a particular data product and
should travel with its write operation. Use workflow logging when the record is
about workflow execution, section timing, or orchestration.

See [Logging Overview](overview.md) for workflow logging and
[Dataset Configuration](../configuration/datasets.md) for the complete dataset
configuration syntax.
