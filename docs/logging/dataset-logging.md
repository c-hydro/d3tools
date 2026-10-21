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

`Dataset.write_data()` first validates, formats, annotates, and writes the output data. If logging is configured, it then creates and writes the log entry. Reading data does not produce an entry. If the configured log output cannot be written, the data write has already occurred.

Dataset copies preserve the configured log manager. Updating a Dataset preserves the manager while updating the Dataset's runtime tags and key pattern.

## Entry Fields

Entries can contain:

| Field | Value |
|---|---|
| `dataset` | Dataset name. |
| `source_key` | Output key passed by the Dataset write path, unless replaced by data metadata. |
| `thumbnail` | Thumbnail path when supplied by the write path. |
| `time` | Output time when represented by a supported value. |
| `time_produced` | Existing data metadata value or the current local timestamp. |
| Additional fields | Supported scalar values from write context and data metadata. |
| `data_checks` | Quality-check results described below. |

Values are retained when they are supported time objects, datetimes, non-empty strings not beginning with `__`, integers, or floats. Integers and floats are serialized as strings; floats are rounded to four decimal places. Unsupported values and `None` are omitted, so optional fields such as `time` and `thumbnail` are not guaranteed to appear.

For an `xarray.DataArray`, `data_checks` includes `max`, `min`, `nans`, `nans_pc`, `zeros`, `zeros_pc`, `sum`, and `sum_abs`. NaNs are ignored for extrema and sums. `nans_pc` uses the total number of array elements as its denominator; `zeros_pc` uses the number of non-NaN elements. If all elements are NaN, `zeros_pc` is set to zero. Metrics other than integer counts are rounded to four decimal places and serialized as strings in the entry.

For an `xarray.Dataset`, checks are calculated for each data variable and flattened into keys such as `temperature_max` and `temperature_nans`.

## Ownership

Dataset logging details belong here; Dataset schema, storage options, and complete examples belong to [Dataset Configuration](../configuration/datasets.md).
