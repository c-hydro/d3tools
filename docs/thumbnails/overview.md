# Thumbnail Generation

`d3tools.thumbnails` creates PNG thumbnails from raster and vector data. In normal workflows thumbnails are configured on a `Dataset` and generated through `DatasetThumbnailManager`; the lower-level `Thumbnail` class can also be used directly when needed.

Supported thumbnail sources are:

- a raster file path as `str`
- an `xarray.DataArray`
- a `geopandas.GeoDataFrame`

The thumbnail source is classified with a QGIS-style colour definition, rendered with Matplotlib, and optionally decorated with an overlay, annotation, and legend.

## Colour Definitions

Colour definitions are text files with one colour entry per line:

```text
position,r,g,b,a,label
-1,255,0,0,255,low
0,255,255,0,255,normal
1,0,255,0,255,high
inf,0,0,255,255,extreme
```

Each valid line has at least six comma-separated fields:

| Field | Meaning |
|-------|---------|
| `position` | Numeric break value or `inf` |
| `r` | Red channel, integer from 0 to 255 |
| `g` | Green channel, integer from 0 to 255 |
| `b` | Blue channel, integer from 0 to 255 |
| `a` | Alpha channel, integer from 0 to 255 |
| `label` | Legend label. Additional comma-separated fields are joined into the label. |

Breaks are sorted numerically. A terminal `inf` entry is kept last and represents values above the last finite break.

Classification is right-closed:

- first colour: values less than or equal to the first break
- following colours: values greater than the previous break and less than or equal to the current break
- terminal `inf`: values above the last finite break
- missing values: separate missing-data class

For example, with breaks `-1`, `0`, `1`, `inf`:

| Value | Class |
|------:|------:|
| `-2` | `0` |
| `-1` | `0` |
| `-0.5` | `1` |
| `0` | `1` |
| `0.5` | `2` |
| `1` | `2` |
| `1.5` | `3` |
| `NaN` | `4` |

Malformed lines are ignored when at least one valid colour entry remains. Empty or fully malformed colour files raise `ValueError`.

## Dataset JSON Configuration

Add a `thumbnail` object to a dataset definition:

```json
{
    "type": "local",
    "key_pattern": "/data/fapar_%Y%m%d.tif",
    "thumbnail": {
        "colors": {
            "type": "local",
            "key_pattern": "/config/colors/fapar_anomaly.txt"
        },
        "destination": {
            "type": "local",
            "key_pattern": "/output/thumbnails/fapar_%Y%m%d.png"
        },
        "overlay": {
            "type": "local",
            "key_pattern": "/data/boundaries.geojson"
        },
        "annotation": {
            "text": "FAPAR anomaly"
        },
        "legend": {
            "loc": "lower left"
        }
    }
}
```

String shorthand is also accepted for `colors`, `destination`, and `overlay`:

```json
{
    "thumbnail": {
        "colors": "/config/colors/fapar_anomaly.txt",
        "destination": "/output/thumbnails/fapar_%Y%m%d.png",
        "overlay": "/data/boundaries.geojson"
    }
}
```

The parser converts these into `Dataset` objects using the parent dataset defaults. If defaults could infer the wrong type, specify nested objects with explicit `type` values.

### JSON Keys

| Key | Required | Type | Description |
|-----|----------|------|-------------|
| `colors` | yes | `string` or dataset object | Colour definition text file. |
| `destination` | yes | `string` or dataset object | Thumbnail output path. Usually a PNG path. |
| `overlay` | no | `string`, dataset object, `false`, or `null` | Optional vector overlay. |
| `field` | no | `string` | Vector data column to classify. Defaults to `value`. Used by `DatasetThumbnailManager` before calling `Thumbnail`. |
| `annotation` | no | `string`, object, `false`, or `null` | Optional annotation. Omitted means infer source filename when available. |
| `legend` | no | `true`, `false`, `null`, `"none"`, or object | Legend behaviour. Omitted means enabled. |
| `shape` | no | two-item array | Explicit render shape as `[height, width]`. Strongest size override. |
| `vector_short_side` | no | positive integer | For vector thumbnails, target pixel size for the shorter side. Defaults to `900`. |
| `vector_max_long_side` | no | positive integer | For vector thumbnails, cap for the longer side. Defaults to `2400`. |
| `size` | no | number | Existing Matplotlib size scaling option. Advanced use. |
| `dpi` | no | number | Figure DPI. Defaults are chosen automatically. |

Recognized thumbnail options are passed to `Thumbnail.save()`. Unsupported option types for annotation, legend, or overlay raise clear errors; unrecognized keys are ignored by `Thumbnail.save()`.

[Thumbnail Python APIs, source constraints, and collection behavior have moved to the Developer Guide — Python APIs](../developer/python-apis.md#thumbnail-and-collection-apis).
