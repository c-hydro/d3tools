# Dataset Composition and Updates

Datasets can be copied, updated, connected to parent datasets, or given a
fallback. These features let a workflow describe derived data without
duplicating storage configuration.
For Python object behavior, see [Developer Guide — Construction, composition, and key rendering](../developer/datasets.md#construction-composition-and-key-rendering).

## Updates

See [Developer Guide — Construction, composition, and key rendering](../developer/datasets.md#construction-composition-and-key-rendering).

## Copying

See [Developer Guide — Construction, composition, and key rendering](../developer/datasets.md#construction-composition-and-key-rendering).

## Parents

See [Developer Guide — Construction, composition, and key rendering](../developer/datasets.md#construction-composition-and-key-rendering).

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

See [Developer Guide — Construction, composition, and key rendering](../developer/datasets.md#construction-composition-and-key-rendering) for Python fallback construction behavior.

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

A `{tile}` placeholder marks tiled data. Configure explicit tile names in
[Dataset Configuration](../configuration/datasets.md#additional-options). The
dataset exposes `tile_names`, `ntiles`, `has_tiles`, and `find_tiles()` for
tile-aware workflows.

## Relationships and Updates

See [Developer Guide — Construction, composition, and key rendering](../developer/datasets.md#construction-composition-and-key-rendering) for propagation and preservation behavior.
