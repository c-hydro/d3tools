# Timestepping

The timestepping layer is the bridge between calendar-aware logic and the concrete data slices that a dataset or workflow needs to process. In practice, a caller usually normalizes a time unit, creates a range, and then asks that range for a list of interval objects or applies a relative window to another range.

## What the component is responsible for

The module defines a small set of public types:

- `TimeRange`: a continuous interval with a start and end.
- `TimeStep`: a single interval type, created from a unit code or alias.
- `TimeWindow`: a relative duration used for aggregation or lookback windows.
- `FixedLenTimeStep`, `FixedNTimeStep`, `FixedDOYTimeStep`: concrete step families with different period semantics.

These objects are not workflow configuration by themselves. They are the Python primitives used by dataset and workflow logic when it needs to resolve which periods are available, how to iterate over calendars, or how to calculate a relative lookback.

For user-facing configuration, see the time configuration page in [docs/timestepping.md](../timestepping.md). For dataset time availability and key discovery, see the dataset guides. For workflow execution ranges, see the workflow-section docs.

## Core public API

The most useful developer entry points are the operations around range generation and normalization.

### Unit normalization

`TimeStep.from_unit()` takes a unit string and resolves it to the canonical timestep definition. The documented public aliases include common forms such as:

- `h` / `hourly`
- `d` / `daily`
- `w` / `weekly`
- `t` / `dekads`
- `m` / `monthly`
- `y` / `annual`
- `v` / `viirs` (i.e. 8-day intervals starting on Jan-1)

Composite forms such as `8d` are also supported for VIIRS/MODIS-style periods. The underlying normalization rules are exposed via `find_unit_of_time()`.

### Relative windows

`TimeWindow.from_str()` parses relative durations such as:

- `3 months`
- `1-year`
- `8d` / `10d`

Input parsing is tolerant of separator variations, but the important point for developers is that a window is a relative duration object rather than a fixed calendar period.

### Range expansion

`TimeRange` exposes generation helpers that turn a range into a sequence of concrete steps:

- `get_timesteps()`
- `get_timesteps_from_tsnumber()`
- `get_timesteps_from_DOY()`

These methods are the primary bridge from a date range to the step objects used elsewhere in the library.

### Examples

```python
from d3tools.timestepping import TimeRange, TimeStep, TimeWindow

period = TimeRange("2024-01-01", "2024-01-31")
step = TimeStep.from_unit("daily")
window = TimeWindow.from_str("3 months")

steps = period.get_timesteps(step.unit)
lookback = window.apply(period)
```

A small variation for a fixed period family:

```python
from d3tools.timestepping import TimeRange, TimeStep

range_ = TimeRange("2024-01-01", "2024-12-31")
month_step = TimeStep.from_unit("monthly")
months = range_.get_timesteps(month_step.unit)
```

## Extension points and Python-only features

The timestepping module is primarily a set of composable value objects rather than a plugin framework. The main extension pattern is to build or subclass the step types rather than rely on a registry.

The public API is centred on:

- unit normalization via `TimeStep.from_unit()`;
- relative duration parsing via `TimeWindow.from_str()`;
- range expansion via `TimeRange.get_timesteps*()`;
- concrete step families such as `FixedLenTimeStep` and `FixedNTimeStep`.

For downstream code, this means the expected extension pattern is to use the existing classes when possible and move any custom semantic behaviour into a higher-level dataset or workflow object rather than into configuration JSON.

## Relationship with dataset and workflow concerns

This page intentionally avoids repeating dataset and workflow configuration material:

- dataset time availability, key discovery, and `time_signature` belong with dataset docs;
- workflow execution ranges such as `times_from_run`, `all_available`, `repeat_window`, and `split` belong with workflow-section docs.

`timestepping` should be treated as the reusable Python model for calendar periods, while the dataset and workflow layers decide how those periods are expressed in configuration and applied to concrete data.

## API reference

The generated API reference for the timestepping package is here:

- [Timestepping reference](reference/timestepping.md)

For the user-facing overview of time configuration and aliases, see:

- [Time configuration overview](../timestepping.md)
