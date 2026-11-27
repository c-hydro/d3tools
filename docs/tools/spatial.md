# Spatial tools

D3tools provides a small set of CRS-aware helpers for raster bounds and
cropping. Use `BoundingBox` to carry bounds with their coordinate reference
system (CRS), and use `crop_to_bb()` when a raster may need CRS transformation
before clipping.

Spatial templates are a Dataset concern rather than a general geometry helper.
See [Datasets — Spatial templates](datasets.md#spatial-templates) for
`TemplateManager`, raster alignment, validation, and caching.

## Coordinate and CRS conventions

Bounds are always ordered as:

```text
(left, bottom, right, top)
```

The coordinate values use the units of their CRS. A buffer of `0.1` therefore
means 0.1 degrees in EPSG:4326, but 0.1 metres in a projected CRS whose units
are metres.

Spatial transformations use x/y order consistently: x is longitude or
easting, and y is latitude or northing. `BoundingBox.transform()` constructs
its `pyproj.Transformer` with `always_xy=True`, avoiding authority-specific
latitude/longitude axis ordering. This does not change the dimension order of
an xarray object; its raster x and y dimensions must already be identified by
the `rio` accessor.

`get_crs()` normalizes an EPSG integer, CRS string, or existing `pyproj.CRS`:

```python
from d3tools.spatial import get_crs

wgs84 = get_crs(4326)
web_mercator = get_crs("EPSG:3857")

assert wgs84.to_epsg() == 4326
assert web_mercator.is_projected
```

CRS strings are interpreted by pyproj and may be EPSG identifiers, WKT, or
another syntax it supports. Prefer an explicit projected CRS before applying
distance-like buffers; degree buffers are angular, not fixed ground distances.

## Bounding boxes

Construct `BoundingBox` from four coordinates and a CRS. The default CRS is
EPSG:4326:

```python
from d3tools.spatial import BoundingBox

area = BoundingBox(
    left=8,
    bottom=38.5,
    right=10,
    top=41.5,
    datum=4326,
    buffer=0.5,
)

assert area.bbox == (7.5, 38.0, 10.5, 42.0)
assert area.epsg_code == "EPSG:4326"
```

Use `bbox` for the coordinate tuple, `epsg_code` for a compact CRS identifier
when one is available, and `wkt_datum` for the complete CRS definition.

The constructor applies `buffer` immediately. `add_buffer()` expands the
current bounds again and updates both `bbox` and `buffer`; a negative value
contracts the bounds. These operations mutate the object.

### Construct from raster data

`BoundingBox.from_xarray()` reads bounds and CRS from a raster-aware
`DataArray`. `from_file()` opens a raster through rioxarray, while
`from_dataset()` calls `dataset.get_data()` and derives bounds from the result.
All three accept an optional buffer in the source CRS units.

```python
import numpy as np
import xarray as xr

from d3tools.spatial import BoundingBox

grid = xr.DataArray(
    np.arange(25).reshape(5, 5),
    dims=("y", "x"),
    coords={
        "x": [0.0, 1.0, 2.0, 3.0, 4.0],
        "y": [4.0, 3.0, 2.0, 1.0, 0.0],
    },
)
grid = grid.rio.set_spatial_dims(x_dim="x", y_dim="y").rio.write_crs(
    "EPSG:4326"
)

extent = BoundingBox.from_xarray(grid)
assert extent.bbox == (-0.5, -0.5, 4.5, 4.5)
```

Raster bounds describe pixel edges, which is why this example extends half a
cell beyond its first and last coordinate centres.

### Transform to another CRS

`transform()` transforms all four corners, then takes their minimum and maximum
coordinates in the target CRS. By default it returns a new object and leaves
the source unchanged:

```python
from d3tools.spatial import BoundingBox

wgs84 = BoundingBox(8, 38.5, 10, 41.5, datum="EPSG:4326")
projected = wgs84.transform("EPSG:3857")

assert projected.epsg_code == "EPSG:3857"
assert wgs84.epsg_code == "EPSG:4326"
```

Pass `inplace=True` to replace the original bounds and CRS; the method then
returns `None`. Transforming only four corners gives an enclosing box for
those transformed points. For strongly curved boundaries or large extents,
it is not a densified transformation of every edge point.

## Buffer raw bounds

Use `buffer_bbox()` when no CRS-bearing object is needed. It returns a new
tuple and does not modify the input sequence:

```python
from d3tools.spatial import buffer_bbox

bounds = (0, 1, 2, 3)

assert buffer_bbox(bounds, 0.5) == (-0.5, 0.5, 2.5, 3.5)
assert buffer_bbox(bounds, -0.5) == (0.5, 1.5, 1.5, 2.5)
assert bounds == (0, 1, 2, 3)
```

As with `BoundingBox.add_buffer()`, the distance is expressed in the unknown
or caller-supplied CRS units.

## Crop raster data

`crop_to_bb()` is the high-level helper. It accepts a raster path, raster-aware
`DataArray`, or raster-aware `Dataset`, plus a `BoundingBox`:

1. File paths are read through D3tools' format-aware file reader.
2. Longitudes greater than 180 are shifted by -360 and the x axis is sorted.
3. If the source has a CRS, the bounding box is transformed to it.
4. If the source has no CRS, the bounding-box CRS is assigned to the source.
5. The source is clipped to the resulting bounds.

```python
import numpy as np
import xarray as xr

from d3tools.spatial import BoundingBox, crop_to_bb

grid = xr.DataArray(
    np.arange(25).reshape(5, 5),
    dims=("y", "x"),
    coords={
        "x": [0.0, 1.0, 2.0, 3.0, 4.0],
        "y": [4.0, 3.0, 2.0, 1.0, 0.0],
    },
)
grid = grid.rio.set_spatial_dims(x_dim="x", y_dim="y").rio.write_crs(
    "EPSG:4326"
)

cropped = crop_to_bb(grid, BoundingBox(1, 1, 3, 3))

assert cropped.shape == (3, 3)
assert cropped.x.values.tolist() == [1.0, 2.0, 3.0]
assert cropped.y.values.tolist() == [3.0, 2.0, 1.0]
```

Assigning the bounding-box CRS to a CRS-less source is an assumption, not a
reprojection. Only use that path when the source coordinates are already
expressed in the bounding-box CRS.

The longitude normalization is intended for rasters using 0–360 coordinates.
It shifts every x coordinate greater than 180, so use care with projected
coordinate systems whose eastings can also exceed 180.

## Clip an already aligned xarray object

Use `clip_xarray()` when the bounds and raster are already in the same CRS. It
calls `rio.clip_box()` after contracting every bound by `1e-6` coordinate
units. This small inset avoids edge ambiguity but can matter for extremely
small cells or coordinate units:

```python
import numpy as np
import xarray as xr

from d3tools.spatial import clip_xarray

grid = xr.DataArray(
    np.arange(25).reshape(5, 5),
    dims=("y", "x"),
    coords={"x": range(5), "y": range(4, -1, -1)},
)
grid = grid.rio.set_spatial_dims(x_dim="x", y_dim="y").rio.write_crs(4326)
cropped = clip_xarray(grid, (1, 1, 3, 3))

assert cropped.shape == (3, 3)
```

`clip_xarray()` does not transform a CRS or normalize longitudes. Prefer
`crop_to_bb()` when either operation may be needed.

For complete member signatures, see the
[Spatial API reference](../reference/spatial.md).
