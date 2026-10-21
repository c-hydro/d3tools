# Dataset Composition and Updates

Datasets can be copied, updated, connected to parent datasets, or given a
fallback. These features let a workflow describe derived data without
duplicating storage configuration.

## Updates

`update()` renders placeholders in the dataset `name` and `key_pattern` and
records the supplied values in the returned dataset's `tags` mapping:

```python
updated = dataset.update(variable="temperature", region="europe")
```

The default is non-mutating. Use `in_place=True` to update the existing object;
that form returns `None`. `preserve_temporal` and `preserve_spatial` control
whether temporal and format-specific spatial state is carried to the copy.

Dataset-reference assignments use the same mechanism:

```json
{
    "destination": "{DATASETS.base_data, variable='temperature'}"
}
```

See [Configuration Tags](../configuration/tags.md#dataset-update-tags) for the
configuration syntax.

## Copying

`copy()` creates a new Dataset instance, preserving temporal and spatial state
by default. It does not duplicate files or objects in local or remote storage,
and it shares configured log and thumbnail managers with the source.

```python
copy = dataset.copy()
```

## Parents

A dataset can be composed from parent datasets with `set_parents(parents, fn)`.
When the requested data is not present in the main dataset, `get_data()` can
create it from the parents using the supplied function. Parent relationships
are propagated when a dataset is updated.

The exact parent function depends on the data product and should be documented
by the workflow or application that creates it.

## Fallbacks

A fallback is another dataset consulted when the main dataset and its parents do
not provide the requested data. It is configured with the `fallback` dataset
option and is also updated when the containing dataset is updated.

```json
{
    "datasets": {
        "primary": {
            "type": "local",
            "key_pattern": "/data/primary_%Y%m%d.tif",
            "fallback": "/data/backup_%Y%m%d.tif"
        }
    }
}
```

String fallback definitions are interpreted as `key_pattern` values and inherit
relevant settings from the parent dataset.

## Versions

A `{file_version}` placeholder marks a versioned dataset. When reading without
a version, d3tools selects the latest available version. When writing without a
version, it creates `01` for the first item and increments the latest version
thereafter.

```json
{
    "type": "local",
    "key_pattern": "/data/result_%Y%m%d_{file_version}.tif"
}
```

Explicitly pass `file_version` to read or write a particular version.

## Tiles

A `{tile}` placeholder marks tiled data. Supply `tile_names` as a list or as a
file containing one tile name per line. The dataset exposes `tile_names`,
`ntiles`, `has_tiles`, and `find_tiles()` for tile-aware workflows.

## Relationships and Updates

Updates propagate through parents and fallbacks so that a runtime tag such as
`region="europe"` resolves consistently across a composed dataset tree. Spatial
state such as raster templates and tile names can be preserved with the default
`preserve_spatial=True` behaviour.
