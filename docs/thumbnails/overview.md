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
            "key_pattern": "/config/colors/fapar_anomaly.txt",
        },
        "destination": {
            "type": "local",
            "key_pattern": "/output/thumbnails/fapar_%Y%m%d.png",
        },
        "overlay": {
            "type": "local",
            "key_pattern": "/data/boundaries.geojson",
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

Additional keys are passed to `Thumbnail.save()` as options. Unsupported option types for annotation, legend, or overlay raise clear errors.

## API Usage

### Through `Dataset.from_options()`

This is the closest API form to JSON workflow usage:

```python
from d3tools.data import Dataset

dataset = Dataset.from_options({
    "type": "local",
    "key_pattern": "/data/fapar_20260831.tif",
    "thumbnail": {
        "colors": {
            "type": "local",
            "key_pattern": "/config/colors/fapar_anomaly.txt",
        },
        "destination": {
            "type": "local",
            "key_pattern": "/output/thumbnails/fapar_20260831.png",
        },
        "overlay": {
            "type": "local",
            "key_pattern": "/data/boundaries.geojson",
        },
        "annotation": {"text": "FAPAR anomaly"},
        "legend": {"loc": "lower left"},
    },
})

data = dataset.get_data()
thumbnail = dataset.thumbnail.make_thumbnail(data=data)
print(thumbnail.thumbnail_file)
```

### Through `DatasetThumbnailManager`

```python
from d3tools.data import LocalDataset
from d3tools.thumbnails import DatasetThumbnailManager

manager = DatasetThumbnailManager(
    colors=LocalDataset(key_pattern="/config/colors/fapar_anomaly.txt"),
    destination=LocalDataset(key_pattern="/output/thumbnails/fapar.png"),
    overlay=LocalDataset(key_pattern="/data/boundaries.geojson"),
    annotation={"text": "FAPAR anomaly"},
    legend={"loc": "lower left"},
)

thumbnail = manager.make_thumbnail(data_array)
```

For vector data, use `field` when the source column is not already named `value`:

```python
manager = DatasetThumbnailManager(
    colors=colors_dataset,
    destination=destination_dataset,
    field="Shape_Area",
    vector_short_side=700,
    vector_max_long_side=1200,
)
```

### Direct `Thumbnail` Usage

```python
from d3tools.thumbnails import Thumbnail

thumbnail = Thumbnail(data_array, "/config/colors/fapar_anomaly.txt")
thumbnail.save(
    "/output/thumbnails/fapar.png",
    overlay="/data/boundaries.geojson",
    annotation={"text": "FAPAR anomaly"},
    legend={"loc": "lower left"},
)
```

When passing a `GeoDataFrame` directly to `Thumbnail`, it must contain a numeric `value` column:

```python
thumbnail = Thumbnail(gdf[["geometry", "risk"]].rename(columns={"risk": "value"}), colors_file)
```

`DatasetThumbnailManager` does this rename automatically when `field` is configured.

## Source Data Rules

### Raster Sources

Raster input can be a file path or `xarray.DataArray`.

Raster behaviour:

- singleton dimensions are squeezed
- the final raster must be two-dimensional
- true multiband rasters raise `ValueError`; select one band before creating the thumbnail
- finite rasters do not require explicit nodata metadata
- `NaN` values are missing
- `rio.nodata` values are missing when nodata is defined
- missing CRS is allowed for plain rendering
- source CRS is required when adding an overlay

### Vector Sources

Vector input must be a non-empty `geopandas.GeoDataFrame`.

Direct `Thumbnail` vector input must include:

- `geometry`
- numeric `value`

Missing values in `value` are allowed and rendered as missing data.

Supported geometry types include polygons, lines, and points. Pixel dimensions are derived from bounds only as an aspect ratio, not from CRS units. This avoids very large projected-CRS allocations.

## Save Options

`Thumbnail.save(destination, **options)` writes the thumbnail and returns `destination`.

The destination is a string path. Parent directories are created when needed; basename-only paths such as `"thumbnail.png"` are allowed.

Every save creates a fresh Matplotlib figure and closes it in a `finally` block. Reusing the same `Thumbnail` object for multiple saves does not accumulate previous annotations, legends, overlays, or closed figure state.

### Size Options

| Option | Applies to | Description |
|--------|------------|-------------|
| `shape` | raster and vector | Explicit render shape as `(height, width)` or `[height, width]`. Overrides vector sizing options. |
| `vector_short_side` | vector only | Target pixel size for the shorter vector side. Must be positive. |
| `vector_max_long_side` | vector only | Maximum pixel size for the longer vector side. Must be positive. |
| `size` | raster and vector | Advanced scaling multiplier used by the Matplotlib figure-size calculation. |
| `dpi` | raster and vector | Figure DPI. |

For vector thumbnails, `vector_short_side` and `vector_max_long_side` preserve aspect ratio:

```json
{
    "thumbnail": {
        "colors": "/config/colors/vector.txt",
        "destination": "/output/thumb.png",
        "field": "Shape_Area",
        "vector_short_side": 700,
        "vector_max_long_side": 1200
    }
}
```

The saved PNG can be slightly smaller than the internal render shape because thumbnails are saved with tight bounding boxes that crop unused margins.

### Annotation Options

Omitted `annotation` means infer an annotation from `source_key` or raster filename when available.

Accepted forms:

| Value | Behaviour |
|-------|----------|
| omitted | Infer source filename when available. |
| `false` or `null` | Disable annotation. |
| `"none"` | Disable annotation, case-insensitive. |
| `""` or whitespace | Disable annotation. |
| non-empty string | Render literal annotation text. |
| object | Copy options and use `text` when provided; otherwise infer text. |

JSON examples:

```json
{ "annotation": "none" }
```

```json
{ "annotation": { "text": "FAPAR anomaly", "xy": [0.02, 0.02] } }
```

Annotation objects are passed to Matplotlib `Axes.annotate()` after removing the optional `text` key. Common options include `xy`, `xycoords`, `fontsize`, `color`, `backgroundcolor`, `ha`, and `va`.

If `fontsize` is omitted, d3tools chooses a size based on figure dimensions and text length. Annotation dictionaries are copied before use, so caller-provided options are not mutated.

### Legend Options

Omitted `legend` means enabled.

Accepted forms:

| Value | Behaviour |
|-------|----------|
| omitted | Add default legend. |
| `true` | Add default legend. |
| `false` or `null` | Disable legend. |
| `"none"` | Disable legend, case-insensitive. |
| object | Copy options and pass them to Matplotlib `Axes.legend()`. |

JSON examples:

```json
{ "legend": "none" }
```

```json
{ "legend": { "loc": "lower left" } }
```

Legends are rendered on the map axes, so `loc` behaves like normal Matplotlib axes legend placement and does not expand the saved PNG canvas.

For backward compatibility, `bbox_to_anchor` is accepted in legend dictionaries but ignored. Older JSON configs that include it should continue to run, but placement is now controlled by `loc` on the axes.

Unsupported legend strings or types raise clear errors.

### Overlay Options

Accepted forms:

| Value | Behaviour |
|-------|----------|
| omitted | No overlay. |
| `false` or `null` | No overlay. |
| string | Read overlay with `geopandas.read_file()`. |
| dataset object | Read overlay with `Dataset.get_data()`. |
| `GeoDataFrame` | Use a copy of the provided frame. |
| object | Copy options, remove `shp_file`, and pass the remaining options to overlay plotting. |

JSON examples:

```json
{ "overlay": "/data/boundaries.geojson" }
```

```json
{
    "overlay": {
        "type": "local",
        "key_pattern": "/data/boundaries.geojson",
        "format": "geojson"
    }
}
```

Direct `Thumbnail.save()` also accepts an overlay options object:

```python
thumbnail.save(
    "thumb.png",
    overlay={
        "shp_file": "/data/boundaries.geojson",
        "edgecolor": "red",
        "linewidth": 0.8,
    },
)
```

Overlay option objects must include `shp_file`. Defaults are:

- `facecolor="none"`
- `edgecolor="black"`
- `linewidth=0.5`

The thumbnail source and overlay must both have a CRS. The overlay is reprojected to the thumbnail source CRS, and the original thumbnail extent is restored afterward.

## All-Missing Data

All-missing raster or vector inputs construct successfully and save a real neutral thumbnail containing `No data`.

This is intentional: downstream logs and consumers receive an existing thumbnail file instead of a path to a missing output.

For all-missing thumbnails:

- `thumbnail.allnan` is `True`
- normal data colormap rendering is skipped
- the output PNG is still written
- `save()` returns the destination path