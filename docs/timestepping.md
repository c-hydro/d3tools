# Timestepping

The `d3tools.timestepping` module provides the time objects used by datasets and
workflows. It separates a logical time range from the individual timesteps used
to locate or process data.

## Main Objects

- `TimeRange` represents a continuous interval with a start and end;
- `TimeStep` represents one interval and can be created from a unit such as
  `daily`, `monthly`, or `yearly`;
- `TimeWindow` represents a relative duration used for aggregation or lookback;
- `FixedLenTimeStep` creates steps of a fixed length;
- `FixedNTimeStep` divides a range into a fixed number of steps;
- `FixedDOYTimeStep` represents recurring day-of-year periods.

`TimeStep.from_unit()` accepts canonical codes and aliases. Common aliases are
`h`/`hourly`, `d`/`daily`, `w`/`weekly`, `t`/`dekadly` or `dekads`,
`m`/`monthly`, `y`/`yearly` or `annual`, and `v`/`viirs` for 8-day
VIIRS/MODIS periods. Composite forms such as `8d` are also supported for the
VIIRS/MODIS period. See `d3tools.timestepping.time_utils.find_unit_of_time()`
for the complete normalisation rules.

```python
from d3tools.timestepping import TimeRange, TimeStep, TimeWindow

period = TimeRange("2024-01-01", "2024-01-31")
step = TimeStep.from_unit("daily")
window = TimeWindow.from_str("3 months")

timesteps = period.get_timesteps(step.unit)
```

The public API is defined by these classes and their helpers:

- `TimeRange.get_timesteps()`, `get_timesteps_from_tsnumber()`, and
  `get_timesteps_from_DOY()` generate step lists for a period;
- `TimeWindow.from_str()` and `TimeWindow.apply()` parse and apply relative
  windows; `find_unit_of_time()` normalises aliases such as `annual`, `viirs`,
  `weekly`, and `8d`;
- `FixedNTimeStep` is the exported base class for `Month`, `Year`, and `Dekad`.

The exact constructors and iteration methods are defined by the classes in
`d3tools.timestepping.timeperiods`.

## Related Guides

Dataset-facing time handling belongs in [Dataset Time and Availability](datasets/time-and-availability.md), including `time_signature`, dataset availability checks, and key discovery.

Workflow execution-range resolution belongs in [Workflow Sections](configuration/workflow-sections.md), including `times_from_run`, `all_available`, `repeat_window`, and `split`.
