# Dataset Catalogues and Templates

Every Dataset owns a `DataCatalogue` used to discover and validate stored keys.
The Dataset convenience methods delegate to this catalogue.

## Key Matching

The catalogue uses `key_pattern` and `KeyParser` to:

1. resolve a searchable prefix;
2. walk the storage backend;
3. match keys against the pattern;
4. extract time and tag values;
5. return logical dates, timesteps, keys, or tags.

```python
keys = dataset.get_available_keys(time=period, variable="temperature")
tags = dataset.get_available_tags(time=period, variable="temperature")
```

`dataset.catalogue` is available for direct inspection, but most application
code should use the Dataset methods.

## Discovery Methods

- `get_prefix()` resolves the directory or object prefix to search;
- `get_available_keys()` returns matching paths or object keys;
- `get_available_tags()` extracts unique tag values;
- `get_times()` and `get_timesteps()` enumerate available temporal items;
- `estimate_timestep()` infers a timestep from available data;
- `check_data()` tests for a specific item;
- `find_times()` filters requested times by availability;
- `find_tiles()` discovers tile values.

Discovery accepts runtime tag values and can operate on a datetime or
`TimeRange`.

## Time Conversion

The catalogue converts between logical workflow time and storage time using the
Dataset `time_signature` and `KeyParser`. This is why callers can use the same
logical time when the underlying pattern stores an end date or an offset date.

## Spatial Templates

Raster datasets also own a `TemplateManager`. It stores spatial template
information used by raster format processing, such as grid shape, coordinates,
transform, and CRS. This is separate from `DataCatalogue`, which is concerned
with key discovery and availability.

Templates are preserved across Dataset updates by default. Use
`preserve_spatial=False` when an updated dataset should not reuse that spatial
state.

## Extending Discovery

Storage backends provide the walking and existence operations used by the
catalogue. If a new backend is added, it must support the backend contract
required by `DataCatalogue`; the catalogue itself should remain the common query
interface.
