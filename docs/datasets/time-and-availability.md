# Dataset Time and Availability

See [Developer Guide — Availability and catalogue contract](../developer/datasets.md#availability-and-catalogue-contract) for the catalogue and discovery lifecycle.

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

See [Developer Guide — Availability and catalogue contract](../developer/datasets.md#availability-and-catalogue-contract).

## Enumerating Storage

See [Developer Guide — Availability and catalogue contract](../developer/datasets.md#availability-and-catalogue-contract).

## Query Tags

See [Developer Guide — Availability and catalogue contract](../developer/datasets.md#availability-and-catalogue-contract).

## Workflow Use

Workflow sections use these methods to decide what work remains. In particular,
`get_last_ts()` and `get_first_ts()` are used by section execution-range logic.
See [Workflow Sections](../configuration/workflow-sections.md) for the
relationship between dataset availability and workflow execution.

## Catalogue Object

See [Developer Guide — Availability and catalogue contract](../developer/datasets.md#availability-and-catalogue-contract).
