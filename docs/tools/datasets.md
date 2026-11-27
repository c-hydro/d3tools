# Datasets

A `Dataset` combines storage, naming, time semantics, and format handling
behind one Python interface. The same methods address local files, in-process
values, S3 objects, and SFTP paths. Workflow authors configure these objects
through JSON; this page documents direct Python use.

This page is organized as follows:

1. [Choosing and creating a dataset](#choosing-and-creating-a-dataset)
2. [Key patterns and `KeyParser`](#key-patterns-and-keyparser)
3. [Reading and writing](#reading-and-writing)
4. [Formats and mixins](#formats-and-mixins)
5. [Storage backends](#storage-backends)
6. [Time and availability](#time-and-availability)
7. [Composition, versions, and tiles](#composition-versions-and-tiles)
8. [Spatial templates](#spatial-templates)
9. [Dataset logging](#dataset-logging)
10. [Dataset-driven thumbnails](#dataset-driven-thumbnails)
11. [Extending datasets](#extending-datasets)

## Choosing and creating a dataset

The storage classes form this hierarchy:

```text
Dataset
├── LocalDataset
├── MemoryDataset
└── RemoteDataset
    ├── S3Dataset
    ├── OVHS3Dataset
    └── SFTPDataset
```

`Dataset.from_options()` is the normal factory. It chooses a storage subclass
from `type` and combines it with a format mixin selected from `format` or the
key's extension. Omitting `type` selects local storage.

| Storage type | Use it for |
| --- | --- |
| `local` | Files visible on the local filesystem. |
| `memory` | Temporary Python values kept in the current process. |
| `s3` | Objects in an AWS S3 bucket. |
| `s3-ovh` | Objects behind the supported S3-compatible endpoint implementation. |
| `sftp` | Files on a server reachable through SFTP. |

Create a local dataset from a complete `key_pattern`:

```python
from datetime import datetime

from d3tools.data import Dataset

daily = Dataset.from_options(
    {
        "type": "local",
        "key_pattern": "/data/{variable}/%Y/data_%Y%m%d.tif",
        "timestep": "daily",
    }
)

key = daily.get_key(datetime(2024, 1, 15), variable="temperature")
assert key == "/data/temperature/2024/data_20240115.tif"
```

For `LocalDataset`, `path` plus `filename` is equivalent to one complete key;
`dir` and `file` are constructor aliases. A name can be supplied explicitly.
Otherwise D3tools derives it from the key-pattern basename after removing the
extension and common date directives.

The concrete classes can also be constructed directly when the storage type
is already known:

```python
from d3tools.data import MemoryDataset

messages = MemoryDataset(
    "message-%Y%m%d.txt",
    timestep="daily",
    keep_after_reading=True,
)
```

`from_options()` and direct constructors create the same public Dataset
interface. The factory is preferable when options originate in configuration
or when storage and format are selected dynamically.

## Key patterns and `KeyParser`

`key_pattern` describes one concrete storage key. It can contain:

- `strftime` directives such as `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, and `%j`;
- named runtime tags such as `{variable}`; and
- the conventional tags `{now}`, `{tile}`, and `{file_version}`, which have
  additional Dataset behaviour.

Runtime tags passed to methods such as `get_key()`, `get_data()`, and
`write_data()` fill named placeholders. They are distinct from top-level
workflow tags and from tags applied through workflow dataset references.

A static dataset has no time directives. A time-dependent dataset uses at
least one date directive; `dataset.has_time` reports whether the pattern is
time-dependent. A pattern can also be tag-dependent without containing time.
Unresolved named tags remain visible in the rendered string, so callers should
supply every tag needed for a concrete storage operation.

Three conventional tags are treated specially:

- `{now}` is resolved once, when the Dataset is constructed, using the current
  local datetime. It is therefore fixed for that Dataset instance rather than
  supplied to each read or write.
- `{tile}` marks a tiled dataset. `has_tiles`, `tile_names`, `ntiles`, and
  `find_tiles()` use it for discovery and availability; checks without an
  explicit tile can require all configured tiles.
- `{file_version}` marks a versioned dataset. Reads and availability checks
  without an explicit version select the greatest available version; writes
  without one create `01` or increment the last numeric part of the greatest
  available version.

See [Composition, versions, and tiles](#composition-versions-and-tiles) for the
complete update and lookup behaviour.

`Dataset.get_key()` uses `KeyParser` internally. Use `KeyParser` directly when
an application needs to render and parse keys without constructing a Dataset:

```python
from datetime import datetime

from d3tools.parse import KeyParser

parser = KeyParser("/data/{variable}/%Y/data_%Y%m%d.tif")
key = parser.render(
    datetime(2024, 1, 15),
    {"variable": "temperature"},
)
parsed = parser.match(key)

assert key == "/data/temperature/2024/data_20240115.tif"
assert parsed.time == datetime(2024, 1, 15)
assert parsed.tags == {"variable": "temperature"}
```

`match()` returns an immutable `ParsedKey` containing the original key, its
parsed datetime, and extracted tags. Missing date components take their
standard defaults: year 1900, month and day 1, and time zero. Duplicate uses
of the same tag must resolve to the same value or matching raises `ValueError`.

`prefix()` produces the stable directory or object prefix that a backend can
walk. It substitutes supplied tags and any date components that are fixed by
the query, then removes unresolved pattern components. `get_matching_keys()`
filters candidate keys with the same matching rules.

When key rendering receives a `TimeStep`, the Dataset's `time_signature`
chooses its `start` or `end`. Passing a `datetime` uses that datetime directly,
with precision normalized to the directives present in the pattern.

## Reading and writing

The main storage-independent methods are:

| Method | Behaviour |
| --- | --- |
| `get_key(time, **tags)` | Render the concrete key without reading it. |
| `check_data(time, **tags)` | Report whether the requested item is available. |
| `get_data(time, **tags)` | Read from the dataset, its parents, or its fallback. |
| `write_data(data, time, **tags)` | Validate, prepare, annotate, and store a value. |
| `copy_data(new_key_pattern, time, **tags)` | Read an item and write it under another rendered key. |
| `move_data(new_key_pattern, time, **tags)` | Write the destination, then remove the original. |
| `rm_data(time, **tags)` | Remove the rendered item. |

This in-memory example performs no filesystem or network access:

```python
from datetime import datetime

from d3tools.data import MemoryDataset

messages = MemoryDataset(
    "message-%Y%m%d.txt",
    timestep="daily",
    keep_after_reading=True,
)
when = datetime(2024, 1, 15)

messages.write_data("ready", time=when)

assert messages.check_data(when)
assert messages.get_data(when) == "ready"
```

Memory reads remove the stored value by default. Set `keep_after_reading=True`
when later reads should see it again.

`get_data()` first locates the value in the dataset, configured parents, or a
fallback. It raises `FileNotFoundError` when none provides the requested item.
Normal reads pass stored values through format post-processing. `as_is=True`
returns the storage representation instead; memory reads already behave as
`as_is` because values are Python objects.

`write_data()` follows this order:

1. Select a version when `{file_version}` is present and no version was given.
2. Check that the Python value is compatible with the selected format.
3. Render the output key and apply format validation/preparation unless
   `as_is=True`.
4. Generate a configured thumbnail.
5. Attach combined format and caller metadata.
6. Write through the storage backend.
7. Write a configured dataset-log entry.

These side effects are not transactional. For example, a log failure happens
after the stored output has been written. See [Dataset logging](#dataset-logging)
and [Dataset-driven thumbnails](#dataset-driven-thumbnails) for the two
manager integrations.

## Formats and mixins

Storage selection and format selection are independent. The storage subclass
finds and moves bytes; its format mixin validates Python values, reads and
writes local files, handles metadata, and converts between storage and
application objects.

If `format` is omitted, D3tools infers it from the final extension. Files with
no extension and `.png`, `.pdf`, `.jpg`, or `.jpeg` use the generic `file`
format. Set `format` explicitly when the extension is ambiguous.

| Format | Extensions or selection | Python write value | Normal read result |
| --- | --- | --- | --- |
| `geotiff` | `.tif`, `.tiff` | `numpy.ndarray` or `xarray.DataArray` | `xarray.DataArray` |
| `netcdf` | `.nc` | `numpy.ndarray`, `xarray.DataArray`, or `xarray.Dataset` | `DataArray` for one variable; otherwise `Dataset` |
| `csv` | `.csv` | `pandas.DataFrame` | `pandas.DataFrame` |
| `parquet` | `.parquet` | `pandas.DataFrame` | `pandas.DataFrame` |
| `shp` | `.shp` | `geopandas.GeoDataFrame` | `geopandas.GeoDataFrame` |
| `geojson` | `.geojson` | `geopandas.GeoDataFrame` | `geopandas.GeoDataFrame` |
| `json` | `.json` | dictionary or `GeoDataFrame` | decoded structured value |
| `txt`, `log` | `.txt`, `.log` | string | list of text lines |
| `file` | no extension, image/PDF extension, or explicit selection | source file path | stored file path |

Writing a NumPy array to a raster requires an existing spatial template;
writing an xarray object can establish that template from its coordinates and
CRS. See [Spatial templates](#spatial-templates).

CSV and Parquet support require the `more-data` installation extra; remote
backends require `remote-data`. Thumbnail rendering uses the separate
`thumbnails` extra.

The generic file format moves an existing local file into the rendered
destination rather than decoding it. Plain text accepts a string on write but
returns a list of lines on read:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from d3tools.data import Dataset

with TemporaryDirectory() as tmp:
    notes = Dataset.from_options(
        {"type": "local", "key_pattern": str(Path(tmp) / "notes.txt")}
    )
    notes.write_data("first line\nsecond line\n")
    assert notes.get_data() == ["first line\n", "second line\n"]
```

Applications should use Dataset methods rather than private mixin hooks.
Standalone `read_from_file()`, `write_to_file()`, and `rm_file()` are available
from `d3tools.data.io_utils` for code that needs format-aware local file I/O
without a Dataset.

## Storage backends

### Local storage

`LocalDataset` renders filesystem paths. It creates parent directories as
needed through the selected format writer, checks availability with the local
filesystem, and walks directories for catalogue discovery. Normal filesystem
permissions and overwrite behaviour apply.

### In-memory storage

`MemoryDataset` stores values in a process-local dictionary keyed by rendered
keys. It is useful for intermediate values, tests, and pipelines that do not
need persistence. Data disappears with the process, and it is not shared
across workers. Format compatibility is checked, but file-format conversion is
skipped because the stored value is already a Python object.

### Remote storage

`S3Dataset`, `OVHS3Dataset`, and `SFTPDataset` stage remote files under a local
temporary directory so format mixins can process them. If `tmp_dir` is omitted,
D3tools creates one and registers it for removal at interpreter exit. A read
downloads a missing staged file; a write serializes locally and uploads it.
Remote shapefile operations also transfer the `.dbf`, `.shx`, and `.prj`
sidecar files when present.

| Type | Required backend options | Optional backend options |
| --- | --- | --- |
| `s3` | `bucket_name`, `key_pattern` | `region_name`, `profile_name`, `tmp_dir` |
| `s3-ovh` | S3 options plus `endpoint_url` | The optional S3 settings above |
| `sftp` | `host`, `username`, `key_pattern` | `password`, `private_key`, `port` (default 22), `tmp_dir` |

S3 uses Boto3 session credential discovery and can select a named profile.
The S3-compatible backend signs requests for its configured endpoint. SFTP
accepts a literal password, a `$NAME` environment reference, or a private-key
path; when neither credential is supplied, the implementation tries supported
private keys under `~/.ssh`. Avoid embedding credentials in source or workflow
JSON.

Remote datasets cache discovered available keys after that property is first
used. Updates and writes keep that cached list aligned with the new pattern or
key. S3 clients and SFTP connections are also shared by their backend classes;
application code should use Dataset methods instead of managing those cached
objects directly.

## Time and availability

Time configuration has three separate parts:

- `timestep` describes the logical frequency, such as `daily`, `dekadly`, or
  `monthly`; see the [unit aliases and their timestep/window support](timestepping.md#units-and-aliases).
- `time_signature` tells a key whether its timestamp represents the logical
  `start` or `end`; the default is `end`.
- `aggregation` attaches a
  [`TimeWindow`](timestepping.md#windows-and-aggregation) to the timestep.

Set `timestep` explicitly when it is known. Otherwise creation leaves it
unset, and a catalogue query that needs timestep objects may estimate it from
available dates. The estimate is stored back on the Dataset. Estimation can be
slow for remote catalogues and can return no result when too few usable dates
exist. See [timestep estimation](timestepping.md#estimation-and-utility-helpers)
for the inference rules and limitations.

Every Dataset owns a `DataCatalogue`. Dataset convenience methods delegate to
it, while `dataset.catalogue` exposes the query object directly. Discovery
works by calculating a stable prefix, walking the backend, matching candidate
keys with `KeyParser`, and converting storage timestamps back to logical time.

| Query | Result |
| --- | --- |
| `get_available_keys(time, **tags)` | Matching concrete keys. |
| `get_available_tags(time, **tags)` | Unique parsed tag values and times. |
| `get_times(time_range, **tags)` | Sorted logical datetimes in the range. |
| `get_timesteps(time_range, **tags)` | Logical `TimeStep` objects overlapping the range. |
| `get_first_date()` / `get_last_date()` | Boundary dates, or `None`. |
| `get_first_ts()` / `get_last_ts()` | Boundary timesteps, or `None`. |
| `get_any_date()` | One available date without choosing a boundary. |
| `check_data(time, **tags)` | Whether one requested item is available. |
| `find_times(times, **tags)` | Available members of a requested time list. |
| `find_tiles(time, **tags)` | Available tiles for a requested time. |

This example enumerates an in-memory catalogue:

```python
from datetime import datetime

from d3tools.data import MemoryDataset
from d3tools.timestepping import TimeRange

rain = MemoryDataset(
    "rain-%Y%m%d.txt",
    timestep="daily",
    keep_after_reading=True,
)
for day in (1, 3, 5):
    rain.write_data(str(day), time=datetime(2024, 1, day))

period = TimeRange("2024-01-01", "2024-01-05")

assert rain.get_available_keys(period) == [
    "rain-20240101.txt",
    "rain-20240103.txt",
    "rain-20240105.txt",
]
assert rain.get_times(period) == [
    datetime(2024, 1, 1),
    datetime(2024, 1, 3),
    datetime(2024, 1, 5),
]
```

Datasets with parents combine parent availability according to the composition
contract; fallbacks can contribute additional times. Tile-aware point checks
without an explicit tile require all configured tiles. See
[Composition, versions, and tiles](#composition-versions-and-tiles) for those
relationships and their update behaviour.

## Composition, versions, and tiles

### Updates and copies

`update(**tags)` substitutes values into a Dataset's key pattern and name and
returns a new Dataset by default. It also records those values in the new
object's `tags` mapping. The source object is unchanged. With `in_place=True`,
the source is changed and the method returns `None`.

```python
from d3tools.data import MemoryDataset

base = MemoryDataset("{variable}.txt", keep_after_reading=True)
rain = base.update(variable="rain")

assert base.get_key() == "{variable}.txt"
assert rain.get_key() == "rain.txt"
assert rain.tags == {"variable": "rain"}
```

By default, updates preserve configured timestep and aggregation state. Raster
updates also preserve their template manager and tile names. Set
`preserve_temporal=False` or `preserve_spatial=False` when a derived Dataset
must not inherit those properties.

`copy()` is an update without substitutions. It returns a distinct Dataset
object and does not copy stored files. A `MemoryDataset` copy carries matching
entries into a separate dictionary; the stored Python values themselves are
not deep-copied. Dataset log and thumbnail managers are shared with the copy.
Remote subclasses preserve their connection or relevant cached discovery
state through their update hooks.

### Parents and derived data

`set_parents()` connects named input Datasets to a Python function. When the
child has no stored value, `get_data()` reads every parent and calls the
function with keyword arguments matching the parent mapping. Non-memory
children write the calculated result back; memory children return it without
persisting it.

```python
from d3tools.data import MemoryDataset

left = MemoryDataset("left.txt", keep_after_reading=True)
right = MemoryDataset("right.txt", keep_after_reading=True)
combined = MemoryDataset("combined.txt")

left.write_data("rain")
right.write_data("fall")

def combine_text(left_text, right_text):
    return left_text + right_text

combined.set_parents(
    {"left_text": left, "right_text": right},
    combine_text,
)

assert combined.get_data() == "rainfall"
```

Parent availability is the intersection of times available from every parent:
the derived item is available only when all function inputs exist. Updates
propagate through parent Datasets and preserve the derivation function.

### Fallbacks

A fallback is consulted after neither the main Dataset nor its parents provide
the requested item. `Dataset.from_options()` accepts another Dataset, a nested
configuration mapping, or a key-pattern string through `fallback`. Strings
inherit relevant construction defaults from the containing Dataset. Fallback
reads use the receiving Dataset's format-processing path.

Fallback availability contributes additional times to the containing Dataset.
Updates also update the fallback, so placeholders shared by both patterns can
be resolved together.

### Versions

`{file_version}` activates version-aware lookup. When a read, availability
check, or point check omits `file_version`, D3tools extracts available version
tags, sorts them, and uses the greatest value. Pass `file_version` explicitly
to select a particular item.

On a write without an explicit version, D3tools uses `01` if none exists.
Otherwise it increments the last numeric part of the greatest version while
preserving its width where possible: `v08` becomes `v09`, and `v99` becomes
`v100`. Version ordering before that increment is lexicographic, so choose a
consistent zero-padded naming scheme.

### Tiles

`{tile}` activates tile-aware discovery for GeoTIFF and NetCDF Datasets.
Set `tile_names` to a list/tuple or a
text-file path containing one tile name per line. If it is omitted, accessing
`tile_names` discovers values from available keys. `ntiles` reports the count,
and `find_tiles()` returns available tiles for a time and other tags.

For point availability without an explicit tile, `check_data()` tests every
configured tile and succeeds only when all are available. Raster templates are
stored per tile. Pass `tile=...` explicitly when reading, writing, or querying
one tile.

## Spatial templates

GeoTIFF and NetCDF Datasets attach a `TemplateManager`. A template records the
grid information needed to rebuild or align raster values: CRS, fill value,
dimension names, spatial dimensions, coordinate starts/ends/steps and lengths,
and Dataset variable names where applicable.

On a normal raster read, the Dataset creates a template from the first suitable
value if one is not already available; later values are aligned to it. On
write, an xarray object can establish the template. A bare NumPy array needs an
existing template because it carries no coordinates or CRS.

`TemplateManager` can also be used directly:

```python
from tempfile import TemporaryDirectory

import numpy as np
import xarray as xr

from d3tools.data import TemplateManager

sample = xr.DataArray(
    np.arange(6).reshape(2, 3),
    dims=("y", "x"),
    coords={"y": [1.0, 0.0], "x": [10.0, 11.0, 12.0]},
    attrs={"_FillValue": -9999},
).rio.write_crs("EPSG:4326")

with TemporaryDirectory() as cache:
    templates = TemplateManager(cache_dir=cache)
    templates.set(sample)
    template = templates.get()

    rebuilt = TemplateManager.build_array(template, np.ones((2, 3)))
    assert rebuilt.shape == (2, 3)

    templates.clear()
    assert templates.load_from_cache("__tile__")
```

The default spatial key is `__tile__`; tiled Datasets use each tile name.
`get()` checks memory first and can load a JSON cache file. `exists()`,
`get_all()`, and `iter_spatial_keys()` inspect in-memory state. `clear()`
removes memory entries but not cache files; `clear_cache()` removes matching
cache files. `copy(deep=False)` copies only the outer mapping, while a deep
copy duplicates nested template values.

With validation enabled, malformed extracted templates raise
`TemplateValidationError`. Treat cached template JSON as internal data:
loading a file does not establish that it is suitable for a later
reconstruction.

Dataset helpers `get_template_dict()`, `set_template()`,
`build_templatearray()`, and `set_data_to_template()` delegate to the manager.
Raster updates share the existing manager by default; use
`preserve_spatial=False` when the new Dataset represents another grid.

## Dataset logging

Dataset logging is separate from workflow logging. Attach a
`DatasetLogManager` to collect metadata and numeric quality checks after each
successful data write. Configuration created through `Dataset.from_options()`
accepts `log` as a path or a mapping with a required `file` destination.

Python code can construct the manager directly:

```python
import numpy as np
import xarray as xr

from d3tools.data import MemoryDataset
from d3tools.logging import DatasetLogManager

log_output = MemoryDataset("dataset-log.json", keep_after_reading=True)
manager = DatasetLogManager(log_output)
data = xr.DataArray(np.array([0.0, 2.0, np.nan]))

record = manager.get_log(
    "rain",
    data,
    lambda value: value,
    source_key="rain.tif",
)
manager.write_log(record)

assert log_output.get_data()["dataset"] == "rain"
```

In normal Dataset use, `_make_log()` supplies the resolved dataset name,
written key, optional thumbnail path, requested time, write context, and data
attributes. Attributes take precedence when names overlap. Supported scalar
metadata is normalized; unsupported values, empty strings, and internal values
beginning with `__` are omitted.

For a `DataArray`, `data_checks` includes maximum, minimum, NaN and zero counts
and percentages, sum, and absolute sum. For an `xarray.Dataset`, the checks are
flattened with each variable name as a prefix. Text destinations receive a
`---`-delimited block; JSON destinations receive the mapping. Log writes use
append mode, so the destination format must support compatible append
behaviour.

The log occurs after storage. A logging failure can therefore leave valid data
already written. Full field and formatting details are also summarized in
[Workflow orchestration: Logging](../workflow_orchestration/logging.md#configure-dataset-logs).

## Dataset-driven thumbnails

Attach a `DatasetThumbnailManager` when a raster or vector write should also
produce a preview. The manager requires:

- `colors`: a Dataset resolving a QGIS-style colour-definition file;
- `destination`: a Dataset whose key resolves the output image; and
- optionally `overlay`: a Dataset used as a boundary or feature overlay.

Other options are passed to thumbnail saving. `field` chooses the value column
for a `GeoDataFrame` and defaults to `value`; common renderer options include
annotation, legend, dimensions, and DPI.

The manager can be constructed and attached from Python:

```python
from d3tools.data import Dataset
from d3tools.thumbnails import DatasetThumbnailManager

colors = Dataset.from_options(
    {"type": "local", "key_pattern": "colors.txt"}
)
destination = Dataset.from_options(
    {"type": "local", "key_pattern": "thumbnail.png"}
)
manager = DatasetThumbnailManager(colors, destination, legend=True)

output = Dataset.from_options(
    {"type": "local", "key_pattern": "output.tif"}
)
output.thumbnail = manager
```

When built from dataset configuration, `thumbnail` is a mapping containing
`colors`, `destination`, optional `overlay`, and renderer options. String or
nested mapping values inherit appropriate settings from the containing
Dataset. If either required key is absent, no manager is created.

During `write_data()`, the manager receives the format-prepared output before
metadata is attached or storage is written. It resolves contextual tags in the
colour and destination Datasets, creates a `Thumbnail`, saves it, and returns
the renderer object. A thumbnail failure prevents the main data write; a later
storage failure can leave the thumbnail behind.

See [Thumbnails](thumbnails.md) for colour-file syntax, standalone raster and
vector rendering, overlays, legends, and `ThumbnailCollection`.

## Extending datasets

D3tools has no runtime plugin registry for arbitrary backends or formats.
Extensions are coordinated source changes using the existing class and mixin
registration paths.

To add a storage backend:

1. Subclass `Dataset` and define a unique lower-case class attribute `type`.
2. Implement the `key_pattern` property and `_read_data()`, `_write_data()`,
   `_rm_data()`, `_check_data()`, and `_walk()`.
3. Import the implementation before factory construction so `DatasetMeta`
   registers the new type in `Dataset.subclasses`.
4. Keep key matching and logical-time conversion in `KeyParser` and
   `DataCatalogue`; backend methods should operate on concrete keys.
5. Test factory construction, round-trip I/O, removal, prefix discovery,
   availability, and update/copy reconstruction.

To add a format:

1. Implement a `FormatMixin` with initialization, local file read/write, and
   before-write/after-read conversion hooks.
2. Override validation and metadata hooks when the format needs them, retaining
   cooperative update behaviour for format state.
3. Export the mixin, add its canonical name to `FORMAT_MIXIN_MAP`, update
   extension detection, and extend `check_data_format()` for accepted Python
   types.
4. Test explicit and inferred selection, local round trips, metadata,
   `as_is`, memory behaviour, and remote staging where applicable.

New backend discovery must keep `_walk()` and `_check_data()` in the same key
namespace used by `key_pattern`. New write-side integrations should preserve
the existing ordering: thumbnail before storage, dataset log after storage.
Templates describe raster geometry; catalogues describe keys and availability;
keep those responsibilities separate.

For member signatures, see the [Data API reference](../reference/data.md) and
[Parsing API reference](../reference/parse.md). Workflow JSON options are
documented separately in [Workflow orchestration: Datasets](../workflow_orchestration/datasets.md).
