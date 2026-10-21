# Python APIs

## Timestepping value objects

`TimeStep`, `TimeWindow`, `TimeRange`, and fixed timestep families are
reusable calendar primitives. Dataset timing and workflow range execution
consume them, but their Python model is distinct from workflow JSON controls.
For execution options, see [Workflow Sections](../configuration/workflow-sections.md);
dataset time signatures and availability belong to
[Datasets](datasets.md#availability-and-catalogue-contract).

`TimeStep.from_unit()` normalizes supported unit names and aliases to a step
definition. `find_unit_of_time()` exposes the underlying unit lookup. Common
aliases include `h` / `hourly`, `d` / `daily`, `w` / `weekly`,
`t` / `dekads`, `m` / `monthly`, `y` / `annual`, and `v` / `viirs`.
Composite forms such as `8d` are available for fixed period families. The generated [timestepping
reference](reference/timestepping.md) is authoritative for supported names.

`TimeWindow.from_str()` parses relative durations. A window is a duration, not
a calendar timestep. For example, it can represent a three-month lookback or
an eight-day interval. Some strings that can mean either days or a special
period are ambiguous; use a separator to distinguish those forms. Applying a
window to a date creates a `TimeRange` before or after the reference time
depending on the selected direction.

`TimeRange` expands a calendar interval into timestep objects through its
`get_timesteps*()` methods. The method family includes ordinary calendar
expansion, timestep-number expansion, and day-of-year expansion. Fixed step
families have distinct period semantics: `FixedLenTimeStep` for fixed-length
periods, `FixedNTimeStep` for repeated calendar-number families, and
`FixedDOYTimeStep` for day-of-year anchored periods. Use `FixedNTimeStep`
(not the historical misspelling `FixedNumTimeStep`).

```python
from d3tools.timestepping import TimeRange, TimeStep, TimeWindow

period = TimeRange("2024-01-01", "2024-01-31")
step = TimeStep.from_unit("daily")
window = TimeWindow.from_str("3 months")

steps = period.get_timesteps(step.unit)
lookback = window.apply(period.end)
```

The timestepping package is a set of value objects and normalization helpers,
not a plugin registry. Prefer its existing classes and keep higher-level
custom semantics in the owning Dataset or workflow application. See the
complete [timestepping API reference](reference/timestepping.md).

## Cases and layered case management

`Case` represents a concrete option mapping plus the tag values that selected
it. Each case keeps its resolved options, the family tag mapping, and the
selected tag values for that branch. `get_cases()` expands dictionary-valued
dimensions into one case per Cartesian combination while keeping
non-dictionary values fixed. Combining cases merges their option and tag
mappings; later values override earlier ones for duplicate keys.

`CaseManager` stores case mappings in named layers. Mapping-valued options
create dimensions; fixed options apply across generated cases. Adding a layer
combines the new cases with the previous final layer. An already-parsed
dimension is ignored if any of its keys appeared in an earlier layer; a
`merge` dimension combines cases across that dimension before generating the
next layer.

The manager's layer names preserve insertion order. It supports access by
layer index or name, `find_case()` lookup, `iterate_tree()` traversal from a
root through a selected layer, and `get_subtree()` /
`iterate_subtree()` descendant traversal. The tree iterator yields parent
cases before their descendants; subtree traversal excludes its starting case.
`get_ids()` assembles IDs from tokens for tag/value pairs and records those
assignments in `id_map`; within a manager, an existing pair reuses its token.
The exact random-token edge behavior is not relied on as a stable contract.

The `withcases` decorator can expand a call over a `cases` keyword containing
case dictionaries: each case's `tags` are merged into the call arguments and
the wrapped function is invoked once per case. Without such a list, the
wrapped function is called normally. The `split_id` and `get_parents` helpers
support hierarchical ID traversal. `rand_str` and other member details are
described in the generated [cases reference](reference/cases.md).

```python
from d3tools.cases.case import get_cases
from d3tools.cases.utils import withcases

cases = get_cases({"variable": {"rain": "rainfall", "temp": "temperature"}})

@withcases
def output_name(year, variable):
    return f"{variable}_{year}.nc"

names = output_name(year=2024, cases=[{"tags": case.tags} for case in cases])
```

This utility concerns option combinations only; engine-specific interpretation
of a case payload belongs to the engine that consumes it. The generated
[cases reference](reference/cases.md) links to `CaseManager`, `Case`, and
supporting helpers.

## Spatial helpers and templates

`BoundingBox` carries `(left, bottom, right, top)` bounds with a CRS. It can be
constructed from coordinates or raster-aware data and can expand or contract
its bounds in CRS coordinate units. Transforming to another CRS returns a
transformed copy by default; callers can request in-place transformation.
It can also be constructed from raster-aware xarray data, a Dataset, or a
raster file. Transformation computes new bounds from the transformed corners. The
[spatial reference](reference/spatial.md) documents CRS and helper APIs.

`get_crs()` normalizes a CRS string, EPSG integer, or `pyproj.CRS` to a CRS
object. `crop_to_bb()` accepts a raster path or raster-aware xarray value and a
bounding box, transforms the bounds when the input has a CRS, and clips the
source. If the source lacks a CRS, the helper assigns the bounding-box CRS to
the source before cropping. `clip_xarray()` clips a raster-aware array to
bounds; `buffer_bbox()` expands or contracts four ordered bounds.

These helper docstrings describe the supported inputs and transformations,
but bounding-box and crop edge behavior has limited focused testing. Avoid
promising outcomes for unusual CRS, coordinate, or raster cases without
additional evidence.

`TemplateManager` is the Dataset/raster integration that stores spatial
metadata, validates it when configured to do so, and optionally caches it as
JSON. Templates can be loaded from memory or cache, copied, enumerated, and
cleared. Clearing in-memory templates does not delete cache files. The manager
can reconstruct a raster-aware `DataArray` from stored metadata or apply a
template's coordinates and CRS to supported data. Cache loading does not
validate the template schema; a successful load is not a guarantee that the
content can be reconstructed or applied. The exact caught failures and
counterexamples are owned by
[Datasets — Spatial templates and dataset integration](datasets.md#spatial-templates-and-dataset-integration).
The reconstruction metadata includes CRS, fill value, dimension names,
spatial dimensions, and dimension lengths and coordinate extents/steps.
Numeric coordinates are reconstructed with linear spacing; datetime-like
coordinates use their stored start and step. When the template represents an
xarray `Dataset`, its stored variable names determine which variables are
handled during reconstruction/application.

Raster Dataset behavior and template preservation on Dataset updates belong
to [Datasets — Spatial templates and dataset integration](datasets.md#spatial-templates-and-dataset-integration).
General template operations are in the generated
[spatial reference](reference/spatial.md).

## Thumbnail and collection APIs

Thumbnail APIs provide two levels of use: Dataset-managed generation and
direct rendering. `DatasetThumbnailManager` resolves its color-definition and
destination Dataset references and can also use an optional overlay. Dataset
configuration is explained in [Dataset Configuration](../configuration/datasets.md).
The manager itself accepts Dataset-like references and forwards its rendering
options to `Thumbnail`.

Direct `Thumbnail` inputs are raster data/file paths or a non-empty
`GeoDataFrame`. Raster input is squeezed and must resolve to one band.
Vector input needs a numeric `value` column. A color-definition file provides
classification bins. The object can classify values, make or save an image,
and add an overlay, annotation, or legend. An overlay needs a CRS on both the
source and overlay; it is reprojected to the source CRS.

`save()` creates parent directories as needed, writes a no-data image for
all-missing input, and closes the temporary figure. For ordinary data, the
renderer recognizes size, DPI, shape, vector dimensions, overlay, annotation,
and legend options. Use the generated [thumbnail
reference](reference/thumbnails.md) rather than relying on incidental
Matplotlib defaults or undocumented rendering details.

For a direct render, supply the source and a color-definition file:

```python
from d3tools.thumbnails import Thumbnail

renderer = Thumbnail("data/source.tif", "config/colors.txt")
renderer.save("output/preview.png")
```

Dataset-managed rendering uses `DatasetThumbnailManager` and Dataset objects
for the color and output locations:

```python
from d3tools.data import LocalDataset
from d3tools.thumbnails import DatasetThumbnailManager

manager = DatasetThumbnailManager(
    colors=LocalDataset(key_pattern="config/colors.txt"),
    destination=LocalDataset(key_pattern="output/preview.png"),
)
source = LocalDataset(key_pattern="data/source.tif")
thumbnail = manager.make_thumbnail(source.get_data())
```

These examples compile as Python; they require the named input files and color
definition to exist when run.

For configuration-backed Dataset construction, the same manager is attached
by `Dataset.from_options()`:

```python
from d3tools.data import Dataset

dataset = Dataset.from_options({
    "type": "local",
    "key_pattern": "data/source.tif",
    "thumbnail": {
        "colors": "config/colors.txt",
        "destination": "output/preview.png",
    },
})
data = dataset.get_data()
thumbnail = dataset.thumbnail.make_thumbnail(data=data)
print(thumbnail.thumbnail_file)
```

For a vector source whose numeric column is not named `value`, the manager can
select it with `field`; direct `Thumbnail` callers must provide the expected
column themselves:

```python
import geopandas as gpd

from d3tools.thumbnails import Thumbnail

features = gpd.read_file("data/regions.geojson")
features = features[["geometry", "risk"]].rename(columns={"risk": "value"})
renderer = Thumbnail(features, "config/colors.txt")
renderer.save("output/regions.png")
```

`ThumbnailCollection` creates one thumbnail for each raster file and combines
the saved outputs as a PNG grid or multipage PDF. One color-definition file
can be reused for all rasters, or a matching list can provide one per raster.
PNG layout is given as `(columns, rows)`. PNG combination with inconsistent
input dimensions is redirected to PDF with the same basename; PDF assembly
skips missing inputs and existing non-PNG files. Unsupported output suffixes
do not produce a combined result. Combining raises `ValueError` if no eligible
images are available. The output path must contain a parent directory
component.

```python
from d3tools.thumbnails import ThumbnailCollection

collection = ThumbnailCollection(
    ["data/first.tif", "data/second.tif"],
    "config/colors.txt",
)
collection.save("output/combined.png", grid=(2, 1))
```

The previous thumbnail workflow examples and source constraints are
consolidated here. For complete method-level behavior, refer to the generated
[thumbnail API reference](reference/thumbnails.md); for option tables and
dataset JSON, use [Dataset Configuration](../configuration/datasets.md).

## Errors, exits, and notification boundary

The `d3tools.errors` package exports `GDAL_ImportError`,
`WorkflowEngineImportError`, `TemplateError`, `TemplateValidationError`,
`TemplateNotFoundError`, `TemplateMemoryError`, and
`TemplateCompatibilityError`. Use the
[errors reference](reference/errors.md) for generated member-level
documentation. Avoid depending on untested exception attributes or
message-formatting details.

The exit package exposes `run_at_exit`, `run_at_exit_first`, and `rm_at_exit`
helpers for registering callbacks and scheduling path removal. Current uses
include logging, remote data, and thumbnail temporary resources. Direct tests have not established callback
ordering, process-exit timing, or behavior when a callback raises. The
`ExitHandler` package-level versus submodule API decision is also unresolved.
Consult the [exit reference](reference/exit.md) and the maintained
developer open-question register; do not make cleanup-order assumptions part
of an integration contract.

`EmailNotification` is experimental and unsupported for current use. Its API
reference is available for investigation, but no operational or reliability
guarantee is implied and it is not recommended for production workflows. See
[Architecture — Experimental and unsupported features](architecture.md#experimental-and-unsupported-features).
