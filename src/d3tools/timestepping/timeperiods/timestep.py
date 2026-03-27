"""Timestep base classes and estimation utilities.

This module provides the abstract TimeStep class, which represents time periods
that support sequential navigation (previous/next timesteps). It includes a
metaclass system for registering different timestep types and a utility function
for estimating timestep types from datetime samples.
"""

from abc import ABC, ABCMeta, abstractmethod

from .timerange import TimeRange, TimePeriod
from ..time_utils import find_unit_of_time
from ..timewindow import TimeWindow

class TimeStepMeta(ABCMeta):
    """Metaclass for TimeStep to register subclasses by their time unit."""
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'unit' in attrs:
            cls.subclasses[attrs['unit']] = cls

class TimeStep(TimeRange, ABC, metaclass=TimeStepMeta):
    """
    Abstract base class for time periods with sequential navigation.

    A TimeStep is a TimeRange that supports arithmetic operations for navigating
    to previous/next timesteps. Different subclasses implement fixed-number-per-year
    (months, dekads, years), fixed-length (days, hours), and fixed-DOY (VIIRS/MODIS)
    timesteps.

    TimeSteps can also carry aggregation windows, specifying a lookback period for
    data aggregation (e.g., \"calculate using the past 7 days\").

    Examples:
        >>> from d3tools.timestepping import Month
        >>> jan_2024 = Month(2024, 1)
        >>> feb_2024 = jan_2024 + 1  # Next month
        >>> dec_2023 = jan_2024 - 1  # Previous month\n
    """

    @classmethod
    def from_unit(cls, unit: str):
        """Get the appropriate TimeStep subclass for a given time unit.
        
        Args:
            unit (str): Time unit code ('d', 'h', 'm', 'y', 't', 'v') or name 
                ('daily', 'monthly', etc.).
        
        Returns:
            Type[TimeStep]: The TimeStep subclass for the unit.
        
        Examples:
            >>> TimeStep.from_unit('m')
            <class 'Month'>
            >>> TimeStep.from_unit('daily')
            <class 'Day'>
        """
        return cls.get_subclass(unit)

    @classmethod
    def get_subclass(cls, unit: str):
        """Get the TimeStep subclass registered for a specific unit.
        
        Args:
            unit (str): Time unit identifier.
        
        Returns:
            Type[TimeStep]: The registered TimeStep subclass.
        
        Raises:
            ValueError: If no subclass is registered for the unit.
        """
        unit = find_unit_of_time(unit)
        Subclass: 'TimeStep' = cls.subclasses.get(unit)
        if Subclass is None:
            raise ValueError(f"Invalid unit of time: {unit}")
        return Subclass

    @classmethod
    def with_agg(cls, agg_window: str|tuple|None|TimeWindow):
        """Create a TimeStep subclass with a default aggregation window.
        
        This returns a modified version of the timestep class where each instance
        has an associated aggregation window for computing rolling statistics.
        
        Args:
            agg_window (str|tuple|TimeWindow): Aggregation window specification.
                Can be a string like '7days', a tuple like (7, 'd'), or a TimeWindow.
        
        Returns:
            Type[TimeStep]: Modified TimeStep class with aggregation window.
        
        Examples:
            >>> MonthWith7Days = Month.with_agg('7days')
            >>> jan = MonthWith7Days(2024, 1)
            >>> jan.agg_window
            TimeWindow(7, d)
        """
        
        class AggTimeStep(cls):

            super_name = cls.__name__

            def __repr__(self):
                return f"{self.super_name} ({self.start:%Y%m%d} - {self.end:%Y%m%d}) agg = {self.agg_window}"

            def __add__(self, other):
                super_add = super().__add__(other)
                super_add.agg_window = self.agg_window
                return super_add

            pass

        if isinstance(agg_window, tuple):
            AggTimeStep._agg_window = TimeWindow(agg_window)
        elif isinstance(agg_window, str):
            AggTimeStep._agg_window = TimeWindow.from_str(agg_window)
        elif isinstance(agg_window, TimeWindow):
            AggTimeStep._agg_window = agg_window
        else:
            raise ValueError(f"Invalid value for agg_window: {agg_window}")

        AggTimeStep.agg_window = AggTimeStep._agg_window

        return AggTimeStep

    @property
    def agg_window(self):
        if hasattr(self, '_agg_window'):
            return self._agg_window
        return None

    @agg_window.setter
    def agg_window(self, value: str|tuple|None|TimeWindow):
        if value is None:
            return

        if isinstance(value, tuple):
            self._agg_window = TimeWindow(value)
        elif isinstance(value, str):
            self._agg_window = TimeWindow.from_str(value)
        elif isinstance(value, TimeWindow):
            self._agg_window = value
        else:
            raise ValueError(f"Invalid value for agg_window: {value}")

    @property
    def agg_range(self):
        if not hasattr(self, 'start') or not hasattr(self, 'end'):
            return None
        
        if not hasattr(self, '_agg_window') or self._agg_window is None:
            return TimeRange(self.start, self.end)
        
        return self.agg_window.apply(self.end)
    
    @agg_range.setter
    def agg_range(self, value: str):
        self.agg_window = value

    @abstractmethod
    def __add__(self, n: int):
        raise NotImplementedError
        
    def __sub__(self, n: int):
        return self + (-n)

    def set_year(self, year: int):
        """Create a new timestep in a different year with the same step number.
        
        Args:
            year (int): Target year.
        
        Returns:
            TimeStep: New timestep instance with the specified year.
        
        Examples:
            >>> month = Month(2024, 3)  # March 2024
            >>> month.set_year(2025)    # March 2025
            Month (20250301 - 20250331)
        """
        new_timestep = self.__class__(year, self.step)
        return new_timestep

    def get_history_timesteps(self, history:TimePeriod):
        """Get all timesteps matching this step number within a historical period.
        
        This is useful for extracting climatological data - for example, getting
        all January months or all dekad 15s within a historical time range.
        
        Args:
            history (TimePeriod): Historical time period to search within.
        
        Returns:
            list[TimeStep]: All timesteps with the same step number that fall
                within the history period.
        
        Examples:
            >>> jan_2024 = Month(2024, 1)
            >>> history = TimePeriod(datetime(2020, 1, 1), datetime(2024, 12, 31))
            >>> jan_months = jan_2024.get_history_timesteps(history)
            # Returns [Jan 2020, Jan 2021, Jan 2022, Jan 2023, Jan 2024]
        """

        history_years = range(history.start.year, history.end.year + 1)
        all_timesteps = [self.set_year(year) for year in history_years]

        return [ts for ts in all_timesteps if ts.start >= history.start and ts.end <= history.end]
    
def estimate_timestep(sample) -> TimeStep:
        """Estimate the timestep type from a sample of datetime values.
        
        Analyzes the differences between consecutive dates to determine the most
        likely timestep frequency (hourly, daily, 8-day, dekadly, monthly, yearly).
        
        Args:
            sample (list[datetime.datetime]): List of at least 2 datetime objects.
        
        Returns:
            Type[TimeStep] | None: The estimated TimeStep class, or None if the
                pattern cannot be determined.
        
        Examples:
            >>> dates = [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)]
            >>> estimate_timestep(dates)
            <class 'Month'>
            
            >>> dates = [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)]
            >>> estimate_timestep(dates)
            <class 'Day'>
        
        Note:
            - Requires at least 2 samples
            - Uses mode of time differences to identify pattern
            - Returns None for irregular or unrecognized patterns
        """
        import numpy as np

        from .fixed_num_timestep import Year, Month, Dekad
        from .fixed_len_timestep import Day, Hour
        from .fixed_doy_timestep import ViirsModisTimeStep

        def mode(arr: list): return max(set(arr), key = arr.count)

        if len(sample) < 2:
            return None

        sample.sort()
        all_diff = [(sample[i+1] - sample[i]).days for i in range(len(sample)-1)]
        step_length = mode(all_diff)
        if np.isclose(step_length, 0):
            all_diff_seconds = [(sample[i+1] - sample[i]).seconds for i in range(len(sample)-1)]
            step_length = mode(all_diff_seconds)
            if np.isclose(step_length, 3600):
                return Hour
            else:
                return None
        if np.isclose(step_length, 1):
            return Day
        elif np.isclose(step_length, 8):
            if sample[-1].month == 2 and sample[-1].day == 28:
                return Dekad
            else:
                return ViirsModisTimeStep
        elif 9 <= step_length <= 11:
            return Dekad
        elif 28 <= step_length <= 31:
            return Month
        elif 365 <= step_length <= 366:
            return Year
        else:
            return None