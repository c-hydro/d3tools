# Dataset Concepts

A `Dataset` is d3tools' common description of data storage, naming, time
semantics, and format. It is not limited to a local file: the same public
interface is used for local, in-memory, S3, and SFTP data.

## Key Patterns

`key_pattern` is the central dataset property. It describes the path or object
key for one item and may contain:

- `strftime` directives such as `%Y`, `%m`, and `%d` for time;
- named tags such as `{variable}` or `{tile}`;
- `{file_version}` for versioned outputs.

```python
from d3tools.data import Dataset

dataset = Dataset.from_options({
    "type": "local",
    "key_pattern": "/data/{variable}/%Y/data_%Y%m%d.tif",
})

key = dataset.get_key("2024-01-15", variable="temperature")
```

For local datasets, `path` plus `filename` is an alternative way to construct
the same key pattern. Remote datasets use `key_pattern` directly.

## Static and Time-Dependent Datasets

A dataset is static when its key pattern contains neither time directives nor
named placeholders. A pattern containing `%Y` or `{variable}` is resolved at
runtime using a time and/or tag values.

```python
static = Dataset.from_options({"type": "local", "key_pattern": "/data/mask.tif"})
daily = Dataset.from_options({
    "type": "local",
    "key_pattern": "/data/%Y/mask_%Y%m%d.tif",
    "timestep": "daily",
})
```

## Runtime Tags

Tags passed to methods such as `get_key()`, `get_data()`, and `write_data()`
are runtime values used to render the key pattern. They are distinct from the
top-level configuration `tags` mapping and from dataset-reference update tags.

```python
key = daily.get_key("2024-01-15", variable="temperature")
```

See [Configuration Tags](../configuration/tags.md) for the three tag mechanisms
used in workflow configuration.

## Time Properties

- `time_signature` controls whether a stored timestamp represents the `start` or `end` boundary of a logical timestep;
- `timestep` describes the dataset's time unit;
- `aggregation` attaches an aggregation window to the timestep;
- `has_time` reports whether the key pattern contains time directives.

The default time signature is `end`. See [Timestepping](../timestepping.md) for
the time model and [Dataset Time and Availability](time-and-availability.md) for
how these properties affect dataset queries.

## Tiles, Versions, and Names

`{tile}` identifies tiled data. Configure explicit tile names as described in
[Dataset Configuration](../configuration/datasets.md#additional-options).
At runtime, `tile_names` reports the configured names or discovers them from
available keys.

`{file_version}` identifies versioned files. When a version is omitted, reads
and availability checks discover the available version tags and select the
lexicographically greatest value. When writing without an explicit version,
d3tools writes `01` if no version exists; otherwise it increments the last
numeric part of the greatest existing version, preserving its zero-padding when
possible. For example, `v08` becomes `v09`, while `v99` becomes `v100`.

Pass `file_version` explicitly to read or write a particular version.

A dataset `name` may be supplied explicitly. Otherwise it is derived from the
basename of `key_pattern` after removing its extension and common date fields.

## Format and Storage

The storage backend determines how keys are reached; the format determines how
bytes become application data. Format-specific behaviour is added through mixins
selected from the dataset format. See [Storage Backends](storage-backends.md)
and [Formats and Mixins](formats-and-mixins.md).
