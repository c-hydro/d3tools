# Datasets

## Define a dataset

Datasets name the locations and keys that workflow sections read or write.
Local storage is the simplest option:

```json
{
  "datasets": {
    "__defaults__": {
      "type": "local",
      "path": "/tmp/workflow-data"
    },
    "input": {
      "filename": "input_%Y%m%d.tif",
      "timestep": "daily"
    },
    "output": {
      "filename": "result_%Y%m%d.tif",
      "timestep": "daily"
    }
  }
}
```

`__defaults__` supplies shared settings; an individual dataset can override
them. Without `type`, a dataset defaults to `local`. For local datasets, use
`path` with `filename`, or provide a complete `key_pattern`.

Other configured locations include:

| Type | Operator-facing settings | Notes |
|---|---|---|
| `memory` | `key_pattern`; optionally `keep_after_reading` | Keeps values in the current process. Read-retention behavior is not fully covered by tests. |
| `s3` | `bucket_name`, `key_pattern`; optional `region_name`, `profile_name`, `tmp_dir` | Uses standard AWS credential discovery. |
| `s3-ovh` | S3 settings plus `endpoint_url` | For an S3-compatible endpoint. |
| `sftp` | `host`, `key_pattern`, `username`; optional password or `private_key`, `port`, `tmp_dir` | Use environment placeholders for credentials. Username is required. |

An SFTP example with sanitized settings (the endpoint is illustrative and is
not contacted by the documentation checker):

<!-- check-docs: skip -->
```json
{
  "datasets": {
    "remote_input": {
      "type": "sftp",
      "host": "sftp.example.invalid",
      "key_pattern": "/shared/input_%Y%m%d.tif",
      "username": "example-user",
      "password": "placeholder-only"
    }
  }
}
```

The example values are placeholders, not usable credentials. For an actual
workflow, supply credentials outside committed JSON, for example with
`{ENV.SFTP_USER}` and `{ENV.SFTP_PASS}`. SFTP configuration does not use SSH
configuration or an SSH agent; when using key authentication, provide an
explicit private-key path. Password and automatic key discovery paths are not
covered by the same test evidence as explicit key loading; see
[configuration failures](troubleshooting.md#configuration-and-dataset-failures).
For remote dependencies and backend details, see the
[Developer Guide — Datasets](../developer/datasets.md#storage-backends-and-format-lifecycle).

## Choose format and describe keys

Use `key_pattern` for the full storage key. Local `path` and `filename` are a
convenient alternative. Patterns can combine time directives (`%Y`, `%m`,
`%d`) and named placeholders such as `{tile}` or `{variable}`:

```json
{
  "datasets": {
    "daily_raster": {
      "type": "local",
      "key_pattern": "/tmp/workflow-data/{tile}/%Y/data_%Y%m%d.tif",
      "format": "geotiff",
      "timestep": "daily",
      "time_signature": "end",
      "tile_names": ["north", "south"],
      "nan_value": -9999
    }
  }
}
```

If `format` is omitted, it is inferred from the key pattern's extension.
Common recognized extensions include `.tif`/`.tiff` for GeoTIFF and `.nc`
for NetCDF. Set `format` explicitly when inference is not suitable; supported
format names include `netcdf`, `geotiff`, `csv`, `parquet`, `shp`, `geojson`,
`json`, `txt`, `log`, and `file`. Unknown extensions or format names can fail
during dataset setup.

`timestep` identifies the expected time frequency. `time_signature` can be
`start`, `end`, or `end+1`; it defaults to `end`. `aggregation` can attach a
window to the timestep. A pattern without time directives or named
placeholders describes a static key; a time directive makes it time-varying.
See [time and availability](#understand-time-and-availability) when a dataset
is used to derive a section's run range.

For patterns containing `{tile}`, `tile_names` may be a list or a text-file
path (one tile per line). If omitted, tile names may be discovered from
available keys. File versions use `{file_version}` in the pattern; see
[composition and versions](#configure-composition-and-versions).

## Configure composition and versions

Dataset update references let a workflow use one definition with different
placeholder values. See [Reference and update datasets](configuration.md#reference-and-update-datasets)
for the JSON syntax. A `fallback` can point at another location to consult
when the primary dataset and its parents do not provide requested data:

```json
{
  "datasets": {
    "primary": {
      "type": "local",
      "key_pattern": "/tmp/workflow-data/primary_%Y%m%d.tif",
      "fallback": "/tmp/workflow-data/backup_%Y%m%d.tif"
    }
  }
}
```

A file-version pattern uses `{file_version}`. On reads without an explicit
version, the latest available version is selected. On writes without an
explicit version, d3tools creates `01` if none exists; later writes increment
the last numeric part of the greatest existing version. For example, `v08`
becomes `v09`. These naming rules are distinct from engine-specific update
policies.

Tile names are configured with `tile_names` as described in
[Choose format and describe keys](#choose-format-and-describe-keys). Parents,
fallbacks, versions, and tiles affect storage lookup; confirm that the
selected engine uses the dataset as intended. Detailed Dataset composition
behavior belongs to the [Developer Guide](../developer/datasets.md#construction-composition-and-key-rendering).

## Configure thumbnails

To create thumbnails from raster writes, add a `thumbnail` mapping to the
dataset. It needs color definitions and a destination; an overlay, annotation,
and legend are optional:

<!-- check-docs: skip -->
```json
{
  "datasets": {
    "raster_output": {
      "type": "local",
      "key_pattern": "/tmp/workflow-data/output_%Y%m%d.tif",
      "thumbnail": {
        "colors": "/tmp/workflow-data/colors.txt",
        "destination": "/tmp/workflow-data/thumbnails/output_%Y%m%d.png",
        "overlay": "/tmp/workflow-data/boundaries.geojson",
        "annotation": {"text": "Output"},
        "legend": true
      }
    }
  }
}
```

The color file must use the supported QGIS-style color-definition format.
Thumbnail generation also needs the thumbnail extra and its rendering
dependencies. See [Thumbnail Generation](../thumbnails/overview.md#dataset-json-configuration)
for accepted options, input requirements, and defaults; direct rendering APIs
are in the [Developer Guide](../developer/python-apis.md#thumbnail-and-collection-apis).

## Understand time and availability

`time_signature` tells d3tools whether a stored timestamp represents the start
or end boundary of a logical timestep (or `end+1`). It defaults to `end`.
Set `timestep` explicitly when the frequency is known. If omitted, it is not
inferred when the dataset is created; a later availability query may infer it
from stored keys. Inference can be slow for large or remote datasets and may
fail or be unavailable when too few matching dates exist.

Workflow sections use available and completed data to choose work when no
workflow-wide date range is supplied. A missing availability result needed
to calculate a range may cause an error; an already-complete range may be
skipped. The exact effects depend on execution options and the selected
engine; see [Workflows](workflows.md#set-execution-ranges-and-options).
Tile patterns and configured tile names influence which stored keys are
available. For the detailed availability contract, see
[Developer Guide — Datasets](../developer/datasets.md#availability-and-catalogue-contract).
