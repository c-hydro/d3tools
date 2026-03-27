"""Time window representation and manipulation.

This module provides the TimeWindow class for representing fixed-duration time periods.
Time windows are defined by a size (integer) and a unit (e.g., days, months, years).
They can be applied to specific datetime points to create TimeRange objects and support
arithmetic operations and comparisons.
"""

import typing

from .time_utils import find_unit_of_time, UNIT_CONVERSIONS
from dateutil.relativedelta import relativedelta
import warnings

import datetime

if typing.TYPE_CHECKING:
    from .timeperiods.timerange import TimeRange

class TimeWindow():
    """A fixed-duration time period defined by size and unit.

    TimeWindow represents a duration of time (e.g., "3 days", "2 months", "1 year")
    that can be applied to specific datetime points to create time ranges. It supports
    arithmetic operations, comparisons, and flexible string parsing.

    Attributes:
        size (int): The numeric size of the window.
        unit (str): The canonical time unit code ('d', 'm', 'y', 't', 'v', 'h', 'w').

    Examples:
        >>> window = TimeWindow(3, 'd')  # 3-day window
        >>> window = TimeWindow(2, 'months')  # 2-month window
        >>> window = TimeWindow.from_str('3days')  # Parse from string
        >>> window = TimeWindow.from_str('2 months')  # With separator
    """

    def __init__(self, size: int, unit: str):
        """Initialize a TimeWindow with size and unit.

        Args:
            size (int): The numeric size of the window (will be cast to int).
            unit (str): The time unit, which will be normalized using find_unit_of_time().
                Can be 'd', 'days', 'm', 'months', 'y', 't', 'dekads', etc.
        """
        self.size = int(size)
        self.unit = find_unit_of_time(unit)

    def __repr__(self):
        return f'TimeWindow({self.size}, {self.unit})'
    
    def __str__(self):
        return f'{self.size}{self.unit}'

    @classmethod
    def from_str(cls, window: str) -> 'TimeWindow':
        """Parse a TimeWindow from a string representation.

        The string can use various formats with or without separators between
        the size and unit. Common separators include space, dash, and dot.

        Args:
            window (str): String representation like '3days', '3 days', '3-d',
                '2months', '1 year', etc.

        Returns:
            TimeWindow: A new TimeWindow instance.

        Raises:
            ValueError: If the window string cannot be parsed or if ambiguous
                (e.g., '10day' could be 10 days or 1 dekad without separator).

        Examples:
            >>> TimeWindow.from_str('3days')
            TimeWindow(3, d)
            >>> TimeWindow.from_str('2 months')
            TimeWindow(2, m)
            >>> TimeWindow.from_str('1-year')
            TimeWindow(1, y)
            >>> TimeWindow.from_str('10 days')  # OK, unambiguous with separator
            TimeWindow(10, d)

        Note:
            For composite numbers like '10' or '8' that could represent dekads or
            VIIRS periods, use a separator to disambiguate (e.g., '10 days' vs '1 dekad').
        """
        import re

        list_of_separators = [' ', '.', '-']
        if any([sep in window for sep in list_of_separators]):
            size_str, unit = re.split(f'[{"".join(list_of_separators)}]', window)
            unit = find_unit_of_time(unit)
        else:
            size_str = re.sub(r'[^0-9]', '', window)
            unit     = find_unit_of_time(re.sub(r'[0-9]' , '', window))
            if unit == 'd' and (size_str.endswith('10') or size_str.endswith('8')):
                raise ValueError('Cannot figure out window size, use a separator between size and unit')
        
        size = int(size_str)
        return TimeWindow(size, unit)
    
    def apply(self, time: datetime.datetime, start: bool = False) -> 'TimeRange':
        """Apply the time window to a specific datetime to create a TimeRange.

        This method creates a TimeRange by applying the window duration either
        forward (if start=True) or backward (if start=False, default) from the
        given time point.

        Args:
            time (datetime.datetime): The reference datetime to apply the window to.
            start (bool, optional): If False (default), the TimeRange ends at `time`.
                If True, the TimeRange starts at `time`. Defaults to False.

        Returns:
            TimeRange: A TimeRange object spanning the window duration.

        Raises:
            ValueError: If the window unit is not recognized.

        Examples:
            >>> window = TimeWindow(3, 'd')
            >>> window.apply(datetime.datetime(2024, 2, 20))  # 3-day window ending Feb 20
            TimeRange(...)
            >>> window.apply(datetime.datetime(2024, 2, 20), start=True)  # starting Feb 20
            TimeRange(...)

        Warnings:
            For dekad ('t') units, if the time doesn't align with natural dekad
            boundaries, a warning is issued.
        """
        
        from .timeperiods import Dekad, TimeRange

        unit = self.unit
        size = self.size

        if unit == 'v': unit = 'd'; size = size * 8
        
        if unit in ['m', 'y', 'd', 'w']:
            reldelta_unitmap = {'d': 'days', 'm': 'months', 'y': 'years', 'w': 'weeks'}
            reldelta_unit = reldelta_unitmap[unit]
            if start:
                time_start:datetime.datetime = time
                time_end:datetime.datetime = time + datetime.timedelta(days=1) + relativedelta(**{reldelta_unit: size}) - datetime.timedelta(days=2)
            else:
                if time.second == 0:
                    time_start:datetime.datetime = time + datetime.timedelta(days=1) - relativedelta(**{reldelta_unit: size})
                else:
                    time_start:datetime.datetime = time + datetime.timedelta(seconds=1) - relativedelta(**{reldelta_unit: size})
                time_end:datetime.datetime = time
        elif unit == 't':
            time_dekad:Dekad = Dekad.from_date(time) # dekad of the given time
            if start:
                start_dekad:Dekad = time_dekad
                end_dekad:Dekad = start_dekad + size - 1
                if start_dekad.start != time:
                    warnings.warn('The given time does not correspond to the start of a dekad. The window will start at the beginning of the dekad.')
            else:
                end_dekad:Dekad = time_dekad
                start_dekad:Dekad = end_dekad - size + 1
                if end_dekad.end != time:
                    warnings.warn('The given time does not correspond to the end of a dekad. The window will end at the end of the dekad.')
            time_start:datetime.datetime = start_dekad.start
            time_end:datetime.datetime = end_dekad.end
        elif unit == 'h':
            if start:
                time_start:datetime.datetime = time
                time_end:datetime.datetime = time + datetime.timedelta(hours=size)
            else:
                time_start:datetime.datetime = time - datetime.timedelta(hours=size)
                time_end:datetime.datetime = time
        else:
            raise ValueError('Unit for aggregator not recognized: must be one of dekads, months, years, days, weeks')
        return TimeRange(time_start, time_end)
    
    def to_hours(self, limit:str = None):
        """Convert the time window to hours.

        For units with variable lengths (months, years, dekads), a limit parameter
        specifies whether to use the maximum or minimum possible hour count.

        Args:
            limit (str|None, optional): For variable-length units, specify 'max' or 'min'.
                Required for 'm', 'y', 't' units. Not needed for 'd', 'h', 'v', 'w'. 
                Defaults to None.

        Returns:
            int: The number of hours in the time window.

        Raises:
            ValueError: If limit is not specified for variable-length units.

        Examples:
            >>> TimeWindow(3, 'd').to_hours()
            72
            >>> TimeWindow(1, 'w').to_hours()
            168
            >>> TimeWindow(1, 'm').to_hours(limit='min')
            672  # 28 days * 24 hours
            >>> TimeWindow(1, 'm').to_hours(limit='max')
            744  # 31 days * 24 hours
        """
        if self.unit == 'd':
            return self.size * 24
        elif self.unit == 'h':
            return self.size
        elif self.unit == 'v':
            return self.size * 8 * 24
        elif self.unit == 'w':
            return self.size * 7 * 24
        elif limit is None:
            raise ValueError('Cannot convert to hours, sepecify if max or min limit is desired')
        elif self.unit == 'm':
            if limit == 'max':
                return self.size * 31 * 24
            elif limit == 'min':
                return self.size * 28 * 24
        elif self.unit == 'y':
            if limit == 'max':
                return self.size * 366 * 24
            elif limit == 'min':
                return self.size * 365 * 24
        elif self.unit == 't':
            if limit == 'max':
                return self.size * 11 * 24
            elif limit == 'min':
                return self.size * 8 * 24

    def __eq__(self, other: 'TimeWindow'):
        """Check if two TimeWindows are equal.

        Args:
            other (TimeWindow): Another TimeWindow to compare.

        Returns:
            bool: True if both size and unit are equal.
        """
        return self.size == other.size and self.unit == other.unit

    def __lt__(self, other: 'TimeWindow'):
        """Check if this TimeWindow is definitely shorter than another.

        Uses conservative comparison: compares maximum hours for self against
        minimum hours for other.

        Args:
            other (TimeWindow): Another TimeWindow to compare.

        Returns:
            bool: True if this window is definitely shorter.
        """
        this_hours = self.to_hours(limit = 'max')
        other_hours = other.to_hours(limit = 'min')
        return this_hours < other_hours
        
    def __gt__(self, other: 'TimeWindow'):
        """Check if this TimeWindow is definitely longer than another.

        Uses conservative comparison: compares minimum hours for self against
        maximum hours for other.

        Args:
            other (TimeWindow): Another TimeWindow to compare.

        Returns:
            bool: True if this window is definitely longer.
        """
        this_hours = self.to_hours(limit = 'min')
        other_hours = other.to_hours(limit = 'max')
        return this_hours > other_hours

    def __le__(self, other: 'TimeWindow'):
        return self < other or self == other
    
    def __ge__(self, other: 'TimeWindow'):
        return self > other or self == other
    
    def __add__(self,  other: 'TimeWindow'):
        """Add two TimeWindows together.

        Windows can be added if they have the same unit or compatible units
        with integer conversion factors.

        Args:
            other (TimeWindow): Another TimeWindow to add.

        Returns:
            TimeWindow: A new TimeWindow representing the sum.

        Raises:
            ValueError: If the windows cannot be added due to incompatible units.

        Examples:
            >>> TimeWindow(2, 'd') + TimeWindow(3, 'd')
            TimeWindow(5, d)
            >>> TimeWindow(1, 'w') + TimeWindow(7, 'd')
            TimeWindow(14, d)
        """
        if self.unit == other.unit:
            return TimeWindow(self.size + other.size, self.unit)
        elif other.unit in UNIT_CONVERSIONS[self.unit]:
            # find the integer conversion factor
            if UNIT_CONVERSIONS[self.unit][other.unit] == int(UNIT_CONVERSIONS[self.unit][other.unit]):
                factor = UNIT_CONVERSIONS[self.unit][other.unit]
                return TimeWindow(self.size + (other.size * factor), self.unit)
            elif UNIT_CONVERSIONS[other.unit][self.unit] == int(UNIT_CONVERSIONS[other.unit][self.unit]):
                factor = UNIT_CONVERSIONS[other.unit][self.unit]
                return TimeWindow((self.size * factor) + other.size, other.unit)
            else:
                raise ValueError(f'Cannot add these two time windows together {self} and {other}')

        else:
            raise ValueError(f'Cannot add these two time windows together {self} and {other}')
    
    def __sub__(self,  other: 'TimeWindow'):
        """Subtract one TimeWindow from another.

        Args:
            other (TimeWindow): TimeWindow to subtract.

        Returns:
            TimeWindow: A new TimeWindow representing the difference.
        """
        return self + TimeWindow(-other.size, other.unit)
    
    def is_multiple(self, other: 'TimeWindow'):
        """Check if this TimeWindow is an exact multiple of another.

        Args:
            other (TimeWindow): The TimeWindow to check against.

        Returns:
            bool: True if self is an integer multiple of other.

        Examples:
            >>> TimeWindow(6, 'd').is_multiple(TimeWindow(2, 'd'))
            True
            >>> TimeWindow(3, 'd').is_multiple(TimeWindow(2, 'd'))
            False
            >>> TimeWindow(2, 'w').is_multiple(TimeWindow(7, 'd'))
            True
        """
        if self.unit == other.unit:
            return self.size % other.size == 0
        elif other.unit in UNIT_CONVERSIONS[self.unit]:
            # find the integer conversion factor
            if UNIT_CONVERSIONS[self.unit][other.unit] == int(UNIT_CONVERSIONS[self.unit][other.unit]):
                factor = UNIT_CONVERSIONS[self.unit][other.unit]
                return self.size % (other.size * factor) == 0
            elif UNIT_CONVERSIONS[other.unit][self.unit] == int(UNIT_CONVERSIONS[other.unit][self.unit]):
                factor = UNIT_CONVERSIONS[other.unit][self.unit]
                return (self.size * factor) % other.size == 0
            else:
                return False
        else:
            return False

