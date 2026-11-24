# Timestepping

The `d3tools.timestepping` package provides the calendar objects used by
Datasets and workflow execution. It distinguishes a continuous period, an
aligned timestep, and a relative window:

| Object | Meaning |
| --- | --- |
| `TimePeriod` | Inclusive start and end datetimes, with containment, length, extension, and comparison. |
| `TimeRange` | A `TimePeriod` that can generate aligned timestep sequences. |
| `TimeStep` | One navigable, aligned interval such as an hour, day, dekad, month, year, or VIIRS/MODIS period. |
| `TimeWindow` | A relative duration used for lookback, aggregation, or extension. |

## Units and aliases

`find_unit_of_time()` removes non-alphanumeric characters, ignores case, and
normalizes names to a one-character code.

Support differs between aligned `TimeStep` objects (including Dataset
`timestep` values) and relative `TimeWindow` durations:

| Code | Accepted aliases | `TimeStep` | `TimeWindow` | Alignment or duration |
| --- | --- | --- | --- | --- |
| `h` | `h`, `hour`, `hours`, `hourly` | Yes (`Hour`) | Yes | Clock hours numbered within a year. |
| `d` | `d`, `day`, `days`, `daily` | Yes (`Day`) | Yes | Calendar days. |
| `w` | `w`, `week`, `weeks`, `weekly` | No | Yes | Seven-day relative durations. |
| `t` | `t`, `dekad`, `dekads`, `dekadly`, `10d`, `10day`, `10days` | Yes (`Dekad`) | Yes | Days 1–10, 11–20, and 21–month end. |
| `m` | `m`, `month`, `months`, `monthly` | Yes (`Month`) | Yes | Calendar months. |
| `y` | `y`, `a`, `year`, `years`, `yearly`, `annual`, `annually` | Yes (`Year`) | Yes | Calendar years. |
| `v` | `v`, `viirs`, `modis`, `8d`, `8day`, `8days` | Yes (`ViirsModisTimeStep`) | Yes | Periods beginning every eight days from day of year 1; windows are relative eight-day durations. |

There is no registered weekly `TimeStep`, so `TimeStep.from_unit("weekly")`,
Dataset `timestep="weekly"`, and `TimeRange.get_timesteps("weekly")` are not
supported.

```python
from d3tools.timestepping import TimeStep, find_unit_of_time

assert find_unit_of_time("DAY") == "d"
assert find_unit_of_time("month_ly") == "m"
assert find_unit_of_time("10-days") == "t"
assert find_unit_of_time("8d") == "v"

assert TimeStep.from_unit("daily").__name__ == "Day"
assert TimeStep.from_unit("monthly").__name__ == "Month"
```

When a unit string is unavailable, `find_unit_of_time()` can infer `d`, `t`,
`m`, or `y` from 365, 36, 12, or 1 timesteps per year respectively.

## Periods and ranges

`TimePeriod(start, end)` accepts datetimes or supported date strings. A date
string used as the start becomes midnight; a date-only end becomes 23:59:59.
Endpoints are inclusive for `contains()` and `get_length()`.

```python
from d3tools.timestepping import TimeRange
from d3tools.timestepping.timeperiods.timeperiod import TimePeriod

period = TimePeriod("2024-02-01", "2024-02-29")

assert period.start.isoformat() == "2024-02-01T00:00:00"
assert period.end.isoformat() == "2024-02-29T23:59:59"
assert period.contains("2024-02-29")
assert period.get_length("days") == 29

same = TimePeriod.from_any(["2024-02-01", "2024-02-29"])
assert same == period
assert isinstance(TimeRange.from_any(period), TimeRange)
```

`get_length()` returns zero when start is after end. Period ordering is based
on separation: `left < right` only when `left.end < right.start`; overlapping
periods are neither less than nor greater than one another.

`extend(window, before=False)` returns a new period extended after its end;
`before=True` extends before its start. `TimeRange.extend()` preserves the
`TimeRange` type.

## Timestep families

All concrete timesteps inherit `TimeRange`, so they expose `start`, `end`,
containment, length, comparison, and range extension. The families differ in
how periods align and cross year boundaries.

| Family | Concrete classes | Rule | Leap/year-boundary behaviour |
| --- | --- | --- | --- |
| `FixedLenTimeStep` | `Hour`, `Day` | Constant duration, numbered within the year. | Day/hour numbering naturally includes leap day; arithmetic reconstructs the result from its new datetime. |
| `FixedNTimeStep` | `Dekad`, `Month`, `Year` | Fixed number of calendar periods per year, with variable duration. | Month and third-dekad lengths follow the calendar; step arithmetic wraps the annual step number. |
| `FixedDOYTimeStep` | `ViirsModisTimeStep`, custom DOY sets | Periods start on configured days of year. | Starts remain fixed by DOY; the final period ends on December 31 and is longer in leap years when applicable. |

Calendar timestep endpoints are inclusive, but their time precision reflects
the class implementation. `Day` and `Hour` end at 23:59:59 and `:59:59`;
`Dekad`, `Month`, `Year`, and fixed-DOY periods end at midnight on their final
calendar date.

### Construct from a step or date

Concrete classes accept a year and one-based step number. `from_date()` finds
the aligned timestep containing a datetime or supported date string.

```python
from d3tools.timestepping import Day, Dekad, Month, ViirsModisTimeStep, Year

leap_day = Day.from_date("2024-02-29")
third_february_dekad = Dekad.from_date("2024-02-29")
february = Month.from_date("2024-02-29")
satellite_period = ViirsModisTimeStep.from_date("2024-01-15")

assert leap_day.day_of_year == 60
assert third_february_dekad.dekad_of_month == 3
assert third_february_dekad.end.day == 29
assert february.get_length() == 29
assert satellite_period.step_of_year == 2
assert Year(2024).is_leap()
```

The base factories select a concrete class from their family parameter:
`FixedLenTimeStep.from_date(value, length=1)` selects `Day`,
`FixedNTimeStep.from_date(value, n_steps=12)` selects `Month`, and
`FixedDOYTimeStep.from_date(value, start_doys=...)` constructs a custom
day-of-year schedule.

### Arithmetic, history, and comparison

Adding or subtracting an integer moves by whole timesteps and returns another
aligned timestep. Year wrapping is automatic:

```python
from d3tools.timestepping import Dekad, Month

december = Month(2024, 12)
january = december + 1

assert (january.year, january.month) == (2025, 1)
assert january - 2 == Month(2024, 11)

last_dekad = Dekad(2024, 36)
assert (last_dekad + 1).start.isoformat() == "2025-01-01T00:00:00"
```

`set_year()` keeps the annual step number and returns the corresponding step in
another year. `get_history_timesteps(history)` returns occurrences with the
same annual step whose entire interval fits inside the history period; this is
useful for climatological collections such as every January or every fifth
dekad.

TimeStep comparison inherits the period rules. Equal endpoints are equal;
one timestep is less than another only when it finishes before the other
starts.

## Generate timesteps from a range

`TimeRange.get_timesteps(freq, agg=None)` returns whole aligned timesteps whose
starts fall through the range end. The first or last timestep is not clipped
to the range boundary.

```python
from d3tools.timestepping import TimeRange

period = TimeRange("2023-12-15", "2024-02-10")
months = period.get_timesteps("monthly")

assert [(month.year, month.month) for month in months] == [
    (2023, 12),
    (2024, 1),
    (2024, 2),
]
assert months[0].start.isoformat() == "2023-12-01T00:00:00"
assert months[-1].end.isoformat() == "2024-02-29T00:00:00"
```

Convenience properties provide `hours`, `days`, `dekads`, `months`, `years`,
and `viirstimes`. Integer frequencies support 8760 (`365 * 24`), 365, 36, 12,
and 1 timesteps per year. The corresponding `gen_...` methods yield lazily;
the `get_...` methods return lists.

`get_timesteps_like(timestep)` reuses another timestep's unit and aggregation
window. `get_timesteps_from_DOY()` accepts custom annual start days, and
`get_timesteps_from_issue_hour()` produces hourly steps for selected issue
hours.

```python
from d3tools.timestepping import TimeRange

period = TimeRange("2024-01-01", "2024-01-31")
custom = period.get_timesteps_from_DOY([1, 11, 21])
issues = TimeRange("2024-01-01", "2024-01-02").get_timesteps_from_issue_hour(
    [0, 12]
)

assert [step.start.day for step in custom] == [1, 11, 21]
assert [step.start.hour for step in issues] == [0, 12, 0, 12]
```

## Windows and aggregation

`TimeWindow(size, unit)` represents a duration, not an aligned timestep.
`from_str()` accepts joined forms such as `3days` and separated forms such as
`3 days`, `3-d`, or `3.days`.

The joined strings `10d` and `8d` are ambiguous because the unit normalizer
also uses them for dekad and VIIRS timestep aliases. `TimeWindow.from_str()`
therefore rejects them. Use a separator for literal days (`10 days`, `8 days`)
or name the intended special unit (`1 dekad`, `1 viirs`).

```python
from datetime import datetime

from d3tools.timestepping import TimeWindow

lookback = TimeWindow.from_str("3 months")
period = lookback.apply(datetime(2024, 3, 31))

assert str(lookback) == "3m"
assert period.start.isoformat() == "2024-01-01T00:00:00"
assert period.end.isoformat() == "2024-03-31T00:00:00"

forward = TimeWindow(7, "days").apply(datetime(2024, 1, 1), start=True)
assert forward.start.isoformat() == "2024-01-01T00:00:00"
assert forward.end.isoformat() == "2024-01-07T00:00:00"
```

Day, week, month, and year windows use calendar-aware inclusive date
arithmetic. Dekad windows snap to dekad boundaries and warn if the reference
date is not the expected start/end boundary. VIIRS windows are treated as
fixed eight-day durations. Hour windows use exact hour offsets.

Windows with compatible units support addition, subtraction, conservative
size comparison, `is_multiple()`, and `to_hours()`. Month, year, and dekad
conversion requires `limit="min"` or `"max"` because their duration varies.

### Attach an aggregation window

An aggregation window describes the lookback associated with each output
timestep. Assign `agg_window`, call `TimeStep.with_agg()`, or pass `agg` to
`TimeRange.get_timesteps()`:

```python
from d3tools.timestepping import Dekad, TimeRange

period = TimeRange("2024-04-01", "2024-04-30")
spi1_steps = period.get_timesteps("dekadly", agg="1 month")
Spi3Dekad = Dekad.with_agg("3 months")
spi3_step = Spi3Dekad.from_date("2024-04-20")

assert str(spi1_steps[0].agg_window) == "1m"
assert str(spi3_step.agg_window) == "3m"
assert spi3_step.agg_range.end == spi3_step.end
```

This is the pattern used when monthly or three-month inputs are produced on a
dekadal output cadence. The aggregation window changes `agg_range`; it does
not change the underlying step's start/end or arithmetic cadence.

## Date parsing and formatting

`get_date_from_str()` accepts these built-in forms:

| Form | Example |
| --- | --- |
| ISO date or compact date | `2024-02-20`, `20240220` |
| European numeric date | `20/02/2024`, `20-02-2024`, `20.02.2024` |
| Named month | `20 Feb 2024`, `20 February 2024`, `2024 Feb 20` |
| Date and time | `2024-02-20 13`, `2024-02-20 13:45`, `2024-02-20 13:45:30` |
| ISO-style timestamp | `2024-02-20T13:45:30` |

Pass an explicit `strptime` format for another layout. With `end=True`, missing
hour, minute, or second fields are filled with 23, 59, and 59 respectively.
Parsing returns naive `datetime` objects; the package does not attach a time
zone or convert values to UTC.

```python
from d3tools.timestepping import get_date_from_str

assert get_date_from_str("20 February 2024").isoformat() == (
    "2024-02-20T00:00:00"
)
assert get_date_from_str("2024-02-20", end=True).isoformat() == (
    "2024-02-20T23:59:59"
)
assert get_date_from_str("2024-Feb-20", format="%Y-%b-%d").day == 20
```

Dataset key formatting uses Python `strftime` directives through `KeyParser`.
The Dataset's `time_signature` selects the timestep start or end before the
pattern is rendered. See [Datasets — Key patterns](datasets.md#key-patterns-and-keyparser).

## Estimation and utility helpers

`estimate_timestep(sample)` sorts a mutable datetime sample in place and uses
the modal consecutive difference. It recognizes hourly, daily, VIIRS-like
eight-day, dekadal (roughly 9–11 day), monthly (28–31 day), and yearly
(365–366 day) spacing. It returns a timestep class or `None`; irregular data
can be misclassified when one difference dominates, so explicit Dataset
`timestep` configuration is preferable.

```python
from datetime import datetime

from d3tools.timestepping import Month, estimate_timestep

sample = [
    datetime(2024, 3, 1),
    datetime(2024, 1, 1),
    datetime(2024, 2, 1),
]

assert estimate_timestep(sample) is Month
assert sample[0] == datetime(2024, 1, 1)  # the input list was sorted in place
```

Other focused helpers include:

- `get_window(time, size, unit, start=False)`: compatibility function for
  applying supported day/week/month/year/dekad windows;
- `get_md_dates(years, month, day)`: the same month/day across years, mapping
  requests for February 28 or 29 to each year's final February day;
- `unit_is_multiple(unit1, unit2)`: whether the first unit is treated as an
  integer multiple of the second under the package's aggregation rules; and
- `UNIT_CONVERSIONS`: the conversion factors used by window arithmetic.

```python
from datetime import datetime

from d3tools.timestepping import get_md_dates, get_window, unit_is_multiple

assert get_md_dates([2023, 2024], 2, 29) == [
    datetime(2023, 2, 28),
    datetime(2024, 2, 29),
]
assert get_window(datetime(2024, 3, 31), 3, "months").start == datetime(
    2024, 1, 1
)
assert unit_is_multiple("month", "day")
assert not unit_is_multiple("day", "month")
```

## Errors and edge cases

- Unknown unit strings raise `ValueError`; unsupported timesteps such as weeks
  normalize successfully but then fail class lookup or range generation.
- `TimeWindow.from_str()` raises for an unparseable or ambiguous string.
- Fractional sizes are not a stable public duration model. `TimeWindow`
  converts its size with `int()`, while `get_window()` rejects floats.
- `TimeRange` generates whole aligned timesteps, so returned boundaries may
  extend beyond the requested range.
- Leap years change February, annual day/hour counts, third-February-dekad
  length, and the final fixed-DOY period.
- Fixed-DOY schedules are annual definitions; invalid or unsorted start days
  are not normalized into a safe schedule for callers.
- Parsed dates are timezone-naive. Avoid mixing naive and timezone-aware
  datetimes in comparisons or range construction.

For complete signatures and lower-level classes, see the
[Timestepping API reference](../reference/timestepping.md).
