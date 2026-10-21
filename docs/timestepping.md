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
- `FixedNumTimeStep` divides a range into a fixed number of steps;
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

timesteps = period.get_timesteps(step)
```

The exact constructor and iteration methods are defined by the classes in
`d3tools.timestepping.timeperiods`.

## Dataset Timestepping

A Dataset may define:

- `timestep`, such as `daily` or `monthly`;
- `aggregation`, such as a duration used to extend or aggregate a timestep;
- `time_signature`, which may be `start`, `end`, or `end+1`.

```json
{
    "type": "local",
    "key_pattern": "/data/%Y/data_%Y%m%d.tif",
    "timestep": "daily",
    "time_signature": "end"
}
```

The dataset uses these properties when converting a logical datetime or
`TimeStep` into the timestamp represented by its `key_pattern`. The same
conversion is used when catalogues discover stored keys, so callers normally
work in logical workflow time rather than manually shifting dates.

See [Dataset Time and Availability](datasets/time-and-availability.md) for the
Dataset-facing API and [Workflow Sections](configuration/workflow-sections.md)
for execution-range resolution.

## Workflow Ranges

`WorkflowDefinition.run()` accepts an explicit `TimeRange`, or `start` and `end`
values. When no range is supplied, workflow execution can use environment
variables, prior run state, or section and dataset availability. The workflow
section decides how its execution range is resolved.

The section execution options `times_from_run`, `all_available`,
`repeat_window`, and `split` are described in
[Workflow Sections](configuration/workflow-sections.md).
