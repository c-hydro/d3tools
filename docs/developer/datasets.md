# Datasets

## Construction, composition, and key rendering

A `Dataset` combines storage identity, a key pattern, time interpretation, and
format behavior behind a common data-access interface. The factory selects a
storage subclass from the dataset type and a format mixin from the configured
or detected format. Use `Dataset.from_options()` or the shared configuration
parser to construct instances; the generated
[data reference](reference/data.md) owns member-level API details.

Key patterns can contain calendar directives such as `%Y%m%d` and named
placeholders such as `{variable}`, `{tile}`, or `{file_version}`. Runtime tags
passed to Dataset methods are values for these placeholders. They are distinct
from workflow-level tags and workflow dataset-reference updates.

```python
from datetime import datetime

from d3tools.data import Dataset

daily = Dataset.from_options({
    "type": "local",
    "key_pattern": "/data/{variable}/%Y/data_%Y%m%d.tif",
    "timestep": "daily",
})

static_mask = Dataset.from_options({
    "type": "local",
    "key_pattern": "/data/mask.tif",
})

date = datetime(2024, 1, 15)
key = daily.get_key(date, variable="temperature")
assert key == "/data/temperature/2024/data_20240115.tif"
assert static_mask.get_key() == "/data/mask.tif"
```

This construction/rendering example does not read or write files. Dataset
point-time methods require a `datetime` or timestep object, not a date string
(unlike `TimeRange` construction and workflow run boundaries).

`get_key()` resolves supplied time according to the dataset's
`time_signature`, normalizes it to directives in the pattern, and renders tag
values. Static patterns need no time input; patterns with time directives or
placeholders are resolved when a method receives the matching time or tags.
Dataset time conventions and operator-configurable key syntax are documented
in [Dataset Configuration](../configuration/datasets.md).

`get_data()` consults the Dataset and its composition sources; it raises
`FileNotFoundError` when the requested item is unavailable from the Dataset,
parents, and fallback. Stored data is passed through format post-processing
unless `as_is=True`; a direct read from memory bypasses that processing.
The fallback-side call uses `as_is=True` to skip the fallback's post-processing,
but the receiving Dataset still applies its own `_format_after_read()` unless
the caller requested `as_is=True`.
Parent-derived data is computed using the configured function and written back
for non-memory datasets.

```python
value = daily.get_data(time=date, variable="temperature")
```

`write_data()` validates and prepares the value for its format, resolves the
output key, generates any configured thumbnail, attaches metadata, writes to
storage, then invokes the dataset-log manager. Supplied metadata is combined with
format-specific metadata. For versioned patterns, a missing explicit version
causes a new version to be assigned.

```python
daily.write_data(
    value,
    time=date,
    variable="temperature",
    metadata={"source": "example"},
)
```

`copy_data()` reads one item and writes it under a separately rendered key.
`move_data()` writes the destination before removing the source, and
`rm_data()` removes the rendered item. They use backend operations and
therefore remain subject to backend capabilities and permissions.

`update()` substitutes supplied values into the dataset's name and key pattern
and records the values as runtime tags. By default it returns an updated copy;
`in_place=True` updates the original object and returns `None`. Temporal and
spatial state can be preserved or omitted with the corresponding preservation
controls. `copy()` creates a distinct Dataset object but does not copy stored
files; configured logging and thumbnail managers are shared. Subclasses and
mixins that customize update behavior participate through the post-update
hook, which should preserve cooperative method resolution.

```python
updated = daily.update(variable="temperature", region="europe")
copied = daily.copy()
```

`set_parents()` connects parent datasets so a Dataset can derive missing data
through an application-provided function. Parent-derived data is written back for non-memory
datasets. A fallback is consulted after the main dataset and its parents do
not provide the requested data. Updates propagate through parents and
fallbacks; string fallback definitions are interpreted as `key_pattern`
values and inherit relevant settings from the parent Dataset. Parent functions
remain application-specific. This file-independent example supplies a parent
mapping whose keys match the function's keyword arguments:

```python
from d3tools.data import Dataset

left = Dataset.from_options({"type": "memory", "key_pattern": "left.txt"})
right = Dataset.from_options({"type": "memory", "key_pattern": "right.txt"})
combined = Dataset.from_options({"type": "memory", "key_pattern": "combined.txt"})

def combine_text(left_text, right_text):
    return left_text + right_text

left.write_data("rain")
right.write_data("fall")
combined.set_parents({"left_text": left, "right_text": right}, combine_text)
assert combined.get_data() == "rainfall"
```

The child has no stored value, so it reads the parents and calls `combine_text`.
As a memory Dataset it returns the derived value without writing it back.
Use compatible parent outputs and a result accepted by the child's format;
the function's domain behavior is owned by the application. See the generated
[data reference](reference/data.md) for the API.

Versioned key patterns use `{file_version}`. Reads and availability checks
without an explicit version select the lexicographically greatest available
version. A write without an explicit version creates `01` when no version
exists; otherwise it increments the last numeric part of the greatest
existing version, preserving zero padding where possible (for example,
`v08` becomes `v09`, while `v99` becomes `v100`). Pass `file_version`
explicitly to select a version. Tiles use `{tile}` and can be enumerated
through `tile_names`, `ntiles`, `has_tiles`, and `find_tiles()`. A Dataset
name may be supplied explicitly; otherwise it is derived from the key-pattern
basename after removing its extension and common date fields. Operator
configuration and versioning examples belong to
[Dataset Configuration](../configuration/datasets.md).

## Availability and catalogue contract

Each Dataset has a `DataCatalogue`. Dataset convenience methods delegate
discovery and availability queries to this catalogue, while `dataset.catalogue`
exposes the underlying query object for direct inspection or extension.

The catalogue combines a Dataset key pattern with `KeyParser` and storage
operations:

1. It resolves a prefix, optionally specialized by time and runtime tags.
2. It asks the backend to walk the relevant path or object-key prefix.
3. It matches candidate keys against the pattern and extracts timestamps and
   tag values.
4. It provides keys, tags, dates, timesteps, boundary results, or existence
   checks through catalogue methods and Dataset convenience methods.

`get_available_keys()` and `get_available_tags()` enumerate the dataset's
storage using supplied time and tag filters. `get_times()` and
`get_timesteps()` expose logical time values; composition can affect
availability results, so do not assume all convenience methods mean "keys
found only in this Dataset's backend." `check_data()`, `find_times()`, and
`find_tiles()` answer point, batch, and tile-availability questions.
For `check_data()` without an explicit tile, the catalogue checks every
configured tile and reports availability only when all are available.

```python
from d3tools.timestepping import TimeRange

period = TimeRange("2024-01-01", "2024-01-31")
keys = daily.get_available_keys(time=period, variable="temperature")
tags = daily.get_available_tags(time=period, variable="temperature")
times = daily.get_times(period)
timesteps = daily.get_timesteps(period)
```

Other availability helpers report first/last dates or timesteps and the
aggregation-aware available start. `get_first_ts()` and `get_last_ts()` may
return `None` when no date or usable timestep can be found. `get_any_date()`
provides an available date without requiring callers to select a boundary.

Logical query time and storage time are converted through the key parser and
the dataset's `time_signature`. For `end+1`, storage dates are shifted
relative to logical time. Catalogue range discovery expands candidate ranges
for anchor semantics before matching keys. Callers should use logical Dataset
times rather than manually shifting timestamps.

If a timestep is not configured, the catalogue may infer it lazily from
available dates and cache the estimate on the Dataset. An estimate can be
unavailable when there are too few usable dates; discovery on remote or large
catalogues may require backend traversal. Define a known timestep explicitly
when the application needs predictable temporal boundaries.

With tile-aware datasets, availability checks without an explicit tile can
check every configured tile. Query methods, time-signature behavior, and
inference are described by the generated [data reference](reference/data.md)
and [Dataset Time and Availability](../datasets/time-and-availability.md).
Operator time and tile options belong to [Dataset Configuration](../configuration/datasets.md).

## Storage backends and format lifecycle

The backend owns storage access: reading and writing concrete keys, removing
keys, checking whether a key or prefix exists, and walking a prefix for
catalogue discovery. Dataset methods are the application-facing interface;
normal client code should not couple itself to backend methods.

Local and memory datasets use their storage implementations directly.
Remote S3-compatible and SFTP datasets stage data through a temporary local
area for format processing; remote connections and temporary files are
managed by the backend layer. Remote writes are uploaded to the remote
backend, and remote datasets can share cached connections that persist across
updates or copies. The memory backend supports in-process data
access, but its read-retention option is not a guaranteed behavior because
the existing documentation identifies it as lacking focused test coverage.
Backend-specific settings belong to [Dataset Configuration](../configuration/datasets.md).

Format selection is separate from storage selection. The dataset factory
combines the chosen storage class with a format mixin responsible for
format-aware reads, validation, metadata, and writes. The normal data flow is:

```text
read:  storage read -> format post-processing -> application data
write: application data -> validation -> format preparation -> thumbnail
       -> metadata attachment -> storage write -> dataset log
```

The format lifecycle hooks are internal implementation seams. Applications
should use `get_data()` and `write_data()` rather than invoking private hooks.
The full write-side ordering and failure boundary are owned by
[Dataset logging and thumbnails](#dataset-logging-and-thumbnails).
`as_is=True` skips format conversion where supported;
use it only when data is already in the representation expected by the
backend and format.

Mixins currently cover raster, table, vector, structured text, plain text,
and generic file handling. Raster datasets initialize a `TemplateManager` to
preserve or apply spatial properties while processing. Format and backend
extension contracts are detailed in
[Extend supported dataset seams](extensions.md#extend-supported-dataset-seams)
and the generated [data reference](reference/data.md).

## Spatial templates and dataset integration

Raster `Dataset` instances use a `TemplateManager` to preserve spatial
metadata independently of the `DataCatalogue`, which handles key discovery.
The template may capture CRS, dimension names, coordinates, transform, fill
value, and variable names. Dataset update/copy operations preserve raster
spatial state by default when applicable; disable spatial preservation when a
derived Dataset must not inherit the source grid.

`TemplateManager` can retain templates in memory and optionally cache them as
JSON. A cache lookup checks memory first and can load from disk; a successful
load is placed in memory. `exists()` and `get_all()` inspect in-memory state,
while `clear()` removes in-memory entries and does not remove cache files.
`get_all()` returns a shallow mapping copy whose nested template dictionaries
are shared. `copy()` can copy nested values shallowly or deeply. `clear_cache()`
removes matching template JSON files from the configured cache directory.
The loader returns `False` for a missing cache file or unconfigured cache
directory and catches JSON decoding, `KeyError`, and I/O failures. It does not
validate the loaded schema: `{}` loads successfully, even though reconstruction
needs additional fields, while a JSON scalar such as `17` raises an uncaught
`TypeError`. A successful load therefore establishes neither schema validity
nor suitability for reconstruction/application.

Setting a template extracts spatial metadata from a `DataArray` or
`Dataset`. Optional validation can raise `TemplateValidationError`; a
missing CRS is represented as EPSG:4326. A stored template can reconstruct a
raster-aware array or apply its coordinates and CRS to supported input data.
These utilities sit at the Dataset/format boundary; general bounding-box and
crop helpers belong to [Python APIs — Spatial helpers and
templates](python-apis.md#spatial-helpers-and-templates). See the generated
[spatial reference](reference/spatial.md) and [data reference](reference/data.md).

## Dataset logging and thumbnails

Dataset logging and thumbnail generation are attached through manager
objects. On a `write_data()` call, Dataset resolves the version/key, validates
and prepares the data, then generates the configured thumbnail from that
prepared output. It next combines and attaches metadata, writes the data to
storage, and finally invokes the configured dataset-log manager with the
output key and thumbnail result.

These side effects are not transactional. A thumbnail failure prevents the
subsequent metadata attachment and data write; a metadata or storage failure
may leave an already-created thumbnail. A dataset-log failure occurs after
the data write. Callers must not assume that failures roll back earlier
outputs.

Dataset log entries combine dataset context, write arguments, data metadata,
and quality checks. Supported values include time objects, datetimes,
non-empty strings that do not begin with `__`, integers, and floats.
Unsupported values and `None` are omitted. Numeric values are serialized as
strings; floats are rounded to four decimal places.

Recognized entry keys include `dataset`, `source_key`, `thumbnail`, `time`,
`time_produced`, additional supported scalar values, and `data_checks`.
Optional values such as `time` and `thumbnail` may be omitted when no
supported value is supplied.

For `xarray.DataArray` values, quality fields include extrema, NaN and zero
counts/percentages, sum, and absolute sum. NaNs are excluded from extrema and
sums. NaN percentage uses all array elements as its denominator; zero
percentage uses non-NaN elements and is zero if there are none. Non-integer
metrics are rounded to four decimal places. For `xarray.Dataset`, metrics are
flattened by variable name (for example, `temperature_max`).

Dataset copies share their configured log and thumbnail managers. A thumbnail
manager resolves color, destination, and optional overlay datasets before
delegating rendering to the thumbnail APIs. The manager and renderer contract
is described under [Python APIs — Thumbnail and collection
APIs](python-apis.md#thumbnail-and-collection-apis).
Workflow/dataset log configuration belongs to
[Dataset Logging](../logging/dataset-logging.md); thumbnail configuration
belongs to [Thumbnail Generation](../thumbnails/overview.md).
