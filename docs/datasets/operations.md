# Dataset Operations

`Dataset` exposes one interface for reading and writing data regardless of the
storage backend. The dataset resolves a concrete key from its `key_pattern`,
retrieves or stores the data, and applies format-specific processing unless
`as_is=True` is requested.

## Reading

```python
value = dataset.get_data(time="2024-01-15", variable="temperature")
```

`get_data()` checks the dataset, parent datasets, and fallback dataset before
reading. See [Dataset Composition](composition.md) for how parent and fallback
datasets are configured and resolved. It raises `FileNotFoundError` when no
source contains the requested data. With `as_is=True`, format post-processing
is skipped; memory datasets already contain data in their usable form.

## Writing

```python
dataset.write_data(
    data,
    time="2024-01-15",
    variable="temperature",
    metadata={"source": "ERA5"},
)
```

`write_data()` validates the data against the dataset format, renders the output
key, applies format-specific preparation, attaches metadata, writes the data,
and then invokes configured thumbnail and dataset-log managers. `as_is=True`
skips format preparation where the backend supports it.

For versioned patterns containing `{file_version}`, writes choose the next
available version automatically unless `file_version` is supplied.

## Key Rendering

Use `get_key()` when a concrete path or object key is needed without reading or
writing data:

```python
key = dataset.get_key("2024-01-15", variable="temperature")
```

`get_key()` applies the dataset's `time_signature` and runtime tag values. It is
also the basis for storage operations and catalogue matching.

## File Operations

- `copy_data(new_key_pattern, time, **tags)` reads one item and writes it to a
  new key pattern;
- `move_data(new_key_pattern, time, **tags)` copies the item and then removes the
  original;
- `rm_data(time, **tags)` removes the rendered item.

These operations use the backend implementation, so the same calls work for
local and remote datasets subject to backend permissions and capabilities.

## Format Processing

The normal read path is:

```text
storage read -> format post-processing -> application data
```

The normal write path is:

```text
application data -> validation -> format preparation -> storage write
```

Use `as_is=True` only when the data is already in the representation expected by
the backend and format mixin. See [Formats and Mixins](formats-and-mixins.md).

## Metadata, Thumbnails, and Logs

`write_data()` combines supplied metadata with format-specific metadata. If the
dataset has a `thumbnail` manager or `log` manager, those are invoked after the
write. Thumbnail configuration is documented in the [Thumbnail Generation](../thumbnails/overview.md)
guide; dataset logging is documented in the [Dataset Logging](../logging/dataset-logging.md)
guide and configured through the `log` dataset option.
