# Dataset Logging

Dataset logging is still supported and is separate from workflow logging.

- `workflow_log` records workflow and section execution;
- the dataset `log` option records information about data written by one
  Dataset.

Dataset logging is handled by `DatasetLogManager`.

## Configuration

Set the Dataset `log` option to a path string or a mapping with a `file` destination. For example, a Dataset definition can include:

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

A mapping must contain `file`; other keys are retained in the manager's `options` dictionary, but their behavior is not specified here. An already-created log manager can also be supplied through the Python API. See [Dataset Configuration](../configuration/datasets.md) for the owning Dataset schema, storage types, and complete configuration syntax.

The path is parsed as a Dataset destination. For output format `json`, the manager passes a dictionary entry to the output Dataset. For `txt`, it writes `key: value` lines between `---` delimiters. Other Dataset formats receive the dictionary unchanged and must support it. Entries are written with `append=True`.

## When Entries Are Written

See [Developer Guide — Dataset logging and thumbnails](../developer/datasets.md#dataset-logging-and-thumbnails).

## Entry Fields

See [Developer Guide — Dataset logging and thumbnails](../developer/datasets.md#dataset-logging-and-thumbnails).

## Ownership

See [Developer Guide — Dataset logging and thumbnails](../developer/datasets.md#dataset-logging-and-thumbnails) for write lifecycle and entry behavior.
