# Dataset Time and Availability

Dataset time methods answer questions about what exists in storage and how
stored keys map to logical workflow time. They delegate discovery to the
Dataset's `DataCatalogue`.

## Time Signatures

`time_signature` may be `start`, `end`, or `end+1`. It controls how a timestamp
encoded in `key_pattern` is interpreted when a `TimeStep` or datetime is
rendered or queried. The default is `end`.

```python
dataset = Dataset.from_options({
    "type": "local",
    "key_pattern": "/data/%Y/data_%Y%m%d.tif",
    "time_signature": "end",
    "timestep": "daily",
})
```

The key parser converts between logical query time and storage time, so callers
should normally use the dataset's logical time convention rather than manually
shifting dates.

If `timestep` is not defined, d3tools does not infer it when the Dataset is
created. It infers it lazily when a catalogue operation needs a `TimeStep`, such
as `get_timesteps()` or a query that requires timestep boundaries. The catalogue
may inspect recent available keys to estimate the spacing between dates, then
caches the result on the Dataset. This can be slow for remote or large
catalogues, and inference can fail or return no usable timestep when too few
matching dates are available. Define `timestep` explicitly when the frequency is
known, especially for remote datasets or empty/partial catalogues.

See [Timestepping](../timestepping.md) for the available timestep classes and
the accepted aliases such as `hourly`, `daily`, `weekly`, `dekadly`, `monthly`,
`yearly`, and `viirs`.

## Boundary and Availability Queries

Common queries include:

```python
dataset.get_first_date()
dataset.get_first_ts()
dataset.get_last_date()
dataset.get_last_ts()
dataset.get_start()
dataset.check_data(time="2024-01-15")
```

`get_first_ts()` and `get_last_ts()` return `TimeStep` objects. The date methods
return datetimes or lists of datetimes. `get_start()` returns the available start
according to the dataset aggregation setting.

`get_any_date()` finds an available date quickly. `find_times(times)` filters a
requested collection to the times that exist, and `find_tiles()` discovers tile
values.

## Enumerating Storage

```python
from d3tools.timestepping import TimeRange

period = TimeRange("2024-01-01", "2024-01-31")
keys = dataset.get_available_keys(time=period)
tags = dataset.get_available_tags(time=period)
times = dataset.get_times(period)
timesteps = dataset.get_timesteps(period)
```

`get_available_keys()` walks the backend, uses the key pattern to match files or
object keys, and filters by time and tags. `get_available_tags()` returns the
unique tag values extracted from matching keys. `get_times()` and
`get_timesteps()` convert matching keys into logical dates and timestep objects.

`get_prefix()` can be used to inspect the directory or object prefix that will be
searched. `estimate_timestep()` infers a timestep from a sample of available
keys or dates.

## Query Tags

Availability methods accept the same runtime tag values as `get_key()`:

```python
keys = dataset.get_available_keys(
    time=period,
    variable="temperature",
)
```

For datasets with parents or fallbacks, availability and data retrieval may
include those related datasets according to the catalogue and dataset
composition rules.

## Workflow Use

Workflow sections use these methods to decide what work remains. In particular,
`get_last_ts()` and `get_first_ts()` are used by section execution-range logic.
See [Workflow Sections](../configuration/workflow-sections.md) for the
relationship between dataset availability and workflow execution.

## Catalogue Object

The underlying `DataCatalogue` is available as `dataset.catalogue`. Its methods
are also exposed as convenience methods on `Dataset`; application code should
usually call the Dataset methods unless direct catalogue access is useful for
inspection or extension.
