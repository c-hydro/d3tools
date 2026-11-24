# Datasets

A `Dataset` combines storage, naming, time semantics, and format handling
behind one Python interface. The same methods address local files, in-process
values, S3 objects, and SFTP paths. Workflow authors configure these objects
through JSON; this page documents direct Python use.

This first part covers the dataset foundations:

1. [Choosing and creating a dataset](#choosing-and-creating-a-dataset)
2. [Key patterns and `KeyParser`](#key-patterns-and-keyparser)
3. [Reading and writing](#reading-and-writing)
4. [Formats and mixins](#formats-and-mixins)
5. [Storage backends](#storage-backends)
6. [Time and availability](#time-and-availability)

Composition, spatial templates, dataset logging, thumbnails, and extension
contracts will be added in the second part.

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
Otherwise d3tools derives it from the key-pattern basename after removing the
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
- named runtime tags such as `{variable}` and `{tile}`; and
- the conventional `{file_version}` tag for versioned datasets.

Runtime tags passed to methods such as `get_key()`, `get_data()`, and
`write_data()` fill named placeholders. They are distinct from top-level
workflow tags and from tags applied through workflow dataset references.

A static dataset has no time directives. A time-dependent dataset uses at
least one date directive; `dataset.has_time` reports whether the pattern is
time-dependent. A pattern can also be tag-dependent without containing time.
Unresolved named tags remain visible in the rendered string, so callers should
supply every tag needed for a concrete storage operation.

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

Key rendering distinguishes logical time from storage time. When passed a
`TimeStep`, the Dataset's `time_signature` chooses its `start`, `end`, or the
start of the next step (`end+1`). Passing a `datetime` uses that datetime
directly, with precision normalized to the directives present in the pattern.

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
after the stored output has been written. The second part of this page will
document composition, thumbnails, and logging in detail.

## Formats and mixins

Storage selection and format selection are independent. The storage subclass
finds and moves bytes; its format mixin validates Python values, reads and
writes local files, handles metadata, and converts between storage and
application objects.

If `format` is omitted, d3tools infers it from the final extension. Files with
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
CRS. Raster templates will be covered in the second part.

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
d3tools creates one and registers it for removal at interpreter exit. A read
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
  `monthly`.
- `time_signature` tells a key whether its timestamp represents the logical
  `start`, `end`, or `end+1`; the default is `end`.
- `aggregation` attaches a `TimeWindow` to the timestep.

Set `timestep` explicitly when it is known. Otherwise creation leaves it
unset, and a catalogue query that needs timestep objects may estimate it from
available dates. The estimate is stored back on the Dataset. Estimation can be
slow for remote catalogues and can return no result when too few usable dates
exist.

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
without an explicit tile require all configured tiles. Those relationships,
along with versions and update/copy behaviour, belong to the second part.

For member signatures, see the [Data API reference](../reference/data.md) and
[Parsing API reference](../reference/parse.md). Workflow JSON options are
documented separately in [Workflow orchestration: Datasets](../workflow_orchestration/datasets.md).
