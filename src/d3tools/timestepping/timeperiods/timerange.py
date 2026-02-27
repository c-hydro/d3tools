"""Time range representation with timestep generation capabilities.

This module provides the TimeRange class, which extends TimePeriod with the ability
to divide itself into regular timesteps (daily, monthly, dekadly, etc.) and provides
utilities for working with time series data.
"""

import datetime
from typing import Generator, Iterable, TYPE_CHECKING

from .timeperiod import TimePeriod
from ..time_utils import find_unit_of_time
from ..timewindow import TimeWindow

if TYPE_CHECKING:
    from .fixed_num_timestep import Year, Month, Dekad, FixedNTimeStep
    from .fixed_len_timestep import Day, Hour, FixedLenTimeStep
    from .fixed_doy_timestep import FixedDOYTimeStep
    from .timestep import TimeStep

class TimeRange(TimePeriod):
    """A time period that can be divided into regular timesteps.

    TimeRange extends TimePeriod with methods to generate sequences of timesteps
    at various frequencies (daily, monthly, yearly, dekadly, etc.). This is essential
    for processing time series data and iterating over time periods.

    The class provides convenient properties for common timestep frequencies and
    flexible methods for custom timestep generation.

    Examples:
        >>> range = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))\n        >>> months = range.months  # Get all months in 2024\n        >>> days = range.days  # Get all days in 2024\n        >>> dekads = range.dekads  # Get all dekads in 2024
        >>> timesteps = range.get_timesteps('m')  # Alternative way to get months
    """

    @property
    def months(self) -> list['Month']:
        """Get all monthly timesteps within this time range.

        Returns:
            list[Month]: A list of Month objects covering the entire range.
        """
        return self.get_timesteps_from_tsnumber(12)

    @property
    def years(self) -> list['Year']:
        """Get all yearly timesteps within this time range.

        Returns:
            list[Year]: A list of Year objects covering the entire range.
        """
        return self.get_timesteps_from_tsnumber(1)

    @property
    def dekads(self) -> list['Dekad']:
        """Get all dekadly (10-day period) timesteps within this time range.

        Returns:
            list[Dekad]: A list of Dekad objects covering the entire range.
        """
        return self.get_timesteps_from_tsnumber(36)

    @property
    def days(self) -> list['Day']:
        """Get all daily timesteps within this time range.

        Returns:
            list[Day]: A list of Day objects covering the entire range.
        """
        return self.get_timesteps_from_tsnumber(365)
    
    @property
    def hours(self) -> list['Hour']:
        """Get all hourly timesteps within this time range.

        Returns:
            list[Hour]: A list of Hour objects covering the entire range.
        """
        return self.get_timesteps_from_tsnumber(365*24)

    @property
    def viirstimes(self) -> list:
        """Get all VIIRS/MODIS 8-day period timesteps within this time range.

        Returns:
            list[ViirsModisTimeStep]: Timesteps aligned with VIIRS/MODIS data availability.
        """
        return self.get_timesteps_from_DOY(range(1, 366, 8))

    def extend(self, window: TimeWindow, before = False):
        extended_period = super().extend(window, before)
        return TimeRange(extended_period.start, extended_period.end)
    
    def get_timesteps(self, freq: str|int, agg: str|tuple|None = None) -> list:
        """Get timesteps at a specified frequency.

        This is the primary method for obtaining timesteps. It accepts either
        a unit string or an integer representing timesteps per year.

        Args:
            freq (str|int): Frequency specification. Can be:
                - String: 'd'/'daily', 'm'/'monthly', 'y'/'yearly', 't'/'dekadly',
                         'h'/'hourly', 'v'/'viirs'
                - Integer: timesteps per year (1, 12, 36, 365, 365*24)
            agg (str|tuple|None, optional): Aggregation window to attach to each
                timestep. Can be a string like '3d' or tuple like (3, 'd').
                Defaults to None.

        Returns:
            list: List of timestep objects appropriate for the frequency.

        Raises:
            ValueError: If frequency is not supported.
            TypeError: If freq is not int or str.

        Examples:
            >>> range.get_timesteps('m')  # Monthly timesteps
            >>> range.get_timesteps(12)   # Same as above
            >>> range.get_timesteps('d', agg='7d')  # Daily with 7-day aggregation
        """

        if isinstance(freq, int):
            tss = self.get_timesteps_from_tsnumber(freq)
        elif isinstance(freq, str):
            freq = find_unit_of_time(freq.lower())
            if freq == 'd': tss = self.days
            elif freq == 't': tss = self.dekads
            elif freq == 'm': tss = self.months
            elif freq == 'y': tss = self.years
            elif freq == 'v': tss = self.viirstimes
            elif freq == 'h': tss = self.hours
            else:
                raise ValueError(f'Frequency {freq} not supported')
        else:
            raise TypeError(f'Frequency must be an int or str, not {type(freq)}')
        
        if agg is not None:
            for ts in tss:
                ts.agg_window = agg
        
        return tss

    def get_timesteps_like(self, timestep: 'TimeStep') -> list['TimeStep']:
        return self.get_timesteps(timestep.unit, timestep.agg_window)

    def gen_timesteps_from_tsnumber(self,
                                    timesteps_per_year: int,
                                    agg: str = None) -> Generator['FixedNTimeStep|FixedLenTimeStep', None, None]:
        """Generate timesteps based on the number of timesteps per year.

        This generator yields timesteps at regular intervals defined by how many
        occur in a year (e.g., 365 for daily, 12 for monthly, 36 for dekadly).

        Args:
            timesteps_per_year (int): Number of timesteps in a year. Supported values:
                - 1: yearly
                - 12: monthly
                - 36: dekadly (10-day periods)
                - 365: daily
                - 365*24: hourly
            agg (str, optional): Aggregation window string to attach. Defaults to None.

        Yields:
            FixedNTimeStep|FixedLenTimeStep: Timestep objects covering the range.

        Examples:
            >>> for month in range.gen_timesteps_from_tsnumber(12):
            ...     print(month)
        """
        from .fixed_len_timestep import FixedLenTimeStep
        from .fixed_num_timestep import FixedNTimeStep

        # get the first timestep
        if timesteps_per_year == 365:
            ts:FixedLenTimeStep = FixedLenTimeStep.from_date(self.start, length = 1)
        elif timesteps_per_year == 365*24:
            ts:FixedLenTimeStep = FixedLenTimeStep.from_date(self.start, length = 1/24)
        else:
            ts:FixedNTimeStep = FixedNTimeStep.from_date(self.start, timesteps_per_year)

        if agg is not None: ts.agg_window = agg
        while ts.start <= self.end:
            yield ts
            ts = ts + 1

    def get_timesteps_from_tsnumber(self, timesteps_per_year: int, agg: str = None) -> list['FixedNTimeStep']:
        return list(self.gen_timesteps_from_tsnumber(timesteps_per_year, agg))
    
    def gen_timesteps_from_DOY(self, doy_list: Iterable[int], agg: str = None) -> Generator['FixedDOYTimeStep', None, None]:
        """Generate timesteps based on fixed day-of-year values.

        This generator is useful for satellite data products (like MODIS/VIIRS) that
        are available at preset days of the year.

        Args:
            doy_list (Iterable[int]): Days of year (1-366) when timesteps begin.
            agg (str, optional): Aggregation window string. Defaults to None.

        Yields:
            FixedDOYTimeStep: Timesteps aligned with the specified DOYs.

        Examples:
            >>> # VIIRS 8-day periods starting at DOYs 1, 9, 17, ...
            >>> for ts in range.gen_timesteps_from_DOY(range(1, 366, 8)):
            ...     print(ts)
        """
        from .fixed_doy_timestep import FixedDOYTimeStep

        ts:FixedDOYTimeStep = FixedDOYTimeStep.from_date(self.start, doy_list)
        if agg is not None: ts.agg_window = agg
        while ts.start <= self.end:
            yield ts
            ts = ts + 1

    def get_timesteps_from_DOY(self, doy_list: list[int], agg: str = None) -> list['FixedLenTimeStep']:
        return list(self.gen_timesteps_from_DOY(doy_list, agg))

    def gen_timesteps_from_issue_hour(self, issue_hours: list[int]) -> Generator['FixedLenTimeStep', None, None]:
        """
        This will yield the timesteps to of product issued daily at given hours
        """
        from .fixed_len_timestep import FixedLenTimeStep
        now = self.start
        while now <= self.end:
            for issue_hour in issue_hours:
                issue_time = datetime.datetime(now.year, now.month, now.day, issue_hour)
                if now <= issue_time <= self.end + datetime.timedelta(hours=23, minutes=59):
                    now = issue_time
                    yield FixedLenTimeStep.from_date(now, 1/24)
            day_after = now + datetime.timedelta(days=1)
            now = datetime.datetime(day_after.year, day_after.month, day_after.day)

    def get_timesteps_from_issue_hour(self, issue_hours: list[int]) -> list['FixedLenTimeStep']:
        return list(self.gen_timesteps_from_issue_hour(issue_hours))
    
