# Dataset Formats and Mixins

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle).

## Format Detection

If `format` is omitted, d3tools detects it from `key_pattern`. You can set it
explicitly when the filename is ambiguous or when a backend needs a particular
format.

```json
{
    "type": "local",
    "format": "geotiff",
    "key_pattern": "/data/result_%Y%m%d.tif"
}
```

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle) for the format-mixin lifecycle.

## Read and Write Hooks

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle).

## Format Mixins

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle) for mixin responsibilities; operator format selection belongs to [Dataset Configuration](../configuration/datasets.md).

## Raster Templates

See [Developer Guide — Spatial templates and dataset integration](../developer/datasets.md#spatial-templates-and-dataset-integration).

## Metadata and Validation

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle).

## `as_is`

See [Developer Guide — Storage backends and format lifecycle](../developer/datasets.md#storage-backends-and-format-lifecycle).
