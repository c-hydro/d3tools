# Dataset Formats and Mixins

A Dataset has both a storage type and a data format. The storage type describes
where data lives; the format describes how it is read, validated, transformed,
and written.

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

The Dataset class dynamically combines the storage class with the appropriate
format mixin.

## Read and Write Hooks

Format mixins participate in the Dataset lifecycle:

```text
read storage -> _format_after_read -> application data
application data -> _format_before_write -> write storage
```

They also provide format-aware validation, metadata handling, and file I/O.
These hooks are implementation details; application code should use
`get_data()` and `write_data()`.

## Format Keywords and Mixins

These are the `format` keywords accepted by the Dataset factory and the mixin
selected for each one:

| `format` keyword | Mixin | Data family |
|---|---|---|
| `netcdf`, `geotiff` | `RasterMixin` | Raster and gridded data |
| `csv`, `parquet` | `TableMixin` | Tables and data frames |
| `shp`, `geojson` | `VectorMixin` | Vector and geometry data |
| `json` | `StructuredTextMixin` | JSON data |
| `txt`, `log` | `PlainTextMixin` | Plain text and log files |
| `file` | `FileMixin` | Generic files without format processing |

The `format` value can be supplied explicitly in a Dataset definition:

```json
{
    "type": "local",
    "format": "parquet",
    "key_pattern": "/data/results.parquet"
}
```

When `format` is omitted, d3tools detects it from the key pattern. `.tif` and
`.tiff` map to `geotiff`, `.nc` maps to `netcdf`, and common image/document
extensions such as `.png`, `.pdf`, `.jpg`, and `.jpeg` map to `file`. Other
extensions must match one of the keywords in the table; otherwise format
detection raises `ValueError`.

## Raster Templates

Raster datasets initialise a `TemplateManager`. Templates preserve or apply
spatial properties such as dimensions, coordinates, transform, and CRS during
format processing. Raster update operations preserve the template manager and
tile names when `preserve_spatial=True`.

The template-related methods are available on raster datasets and are used by
the raster mixin during reads and writes. See
[Catalogues and Templates](catalogues-and-templates.md) for the template role in
key and spatial handling.

## Metadata and Validation

Format mixins can implement:

- `validate_data()` before writing;
- `get_metadata()` and `set_metadata()`;
- format-specific data conversion on read and write.

`Dataset.write_data()` combines supplied metadata with metadata produced by the
format layer before storing the result.

## `as_is`

Passing `as_is=True` to `get_data()` or `write_data()` skips normal format
conversion where supported. This is an advanced option: use it only when the
input or output already matches the backend's expected representation.
