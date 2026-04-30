"""Base time period representation.

This module provides the TimePeriod abstract base class, which represents any time
interval with a start and end datetime. It serves as the foundation for more specialized
time period classes like TimeRange and TimeStep.
"""

from abc import ABC
import datetime
from typing import Sequence

from ..time_utils import get_date_from_str
from ..timewindow import TimeWindow

class TimePeriod(ABC):
    """Base class for time periods defined by start and end datetimes.

    A TimePeriod represents any interval of time with definite start and end points.
    It provides basic operations like length calculation, containment checks, and
    period extension.

    Attributes:
        start (datetime.datetime): The beginning of the time period (inclusive).
        end (datetime.datetime): The end of the time period (inclusive).

    Examples:
        >>> period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        >>> period.get_length()
        31
        >>> period.contains(datetime.datetime(2024, 1, 15))
        True
    """

    @classmethod
    def from_any(cls, value: 'TimePeriod'|Sequence[datetime.datetime|str]|None, name = "") -> 'TimePeriod':
        """Create a TimePeriod from various input formats.

        This flexible factory method accepts TimePeriod objects, sequences of datetimes,
        or None, making it useful for accepting user input in multiple formats.

        Args:
            value: Can be:
                   - A TimePeriod (or object with start/end attributes)
                   - A sequence of two datetime.datetime or date strings
                   - None (returns None)
            name (str, optional): Name to include in error message for clarity.
                Defaults to "".

        Returns:
            TimePeriod|None: A new TimePeriod instance, or None if value is None.

        Raises:
            ValueError: If value cannot be converted to a TimePeriod.

        Examples:
            >>> period = TimePeriod.from_any([datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31)])
            >>> period = TimePeriod.from_any(['2024-01-01', '2024-01-31'])
            >>> period = TimePeriod.from_any(existing_period)
        """

        if hasattr(value, 'start') and hasattr(value, 'end'):
            return cls(value.start, value.end)
        elif isinstance(value, Sequence):
            if len(value) == 2:
                return cls(value[0], value[1])
        elif value is None:
            return None

        if len(name) > 0:
            str = f'{name} must be a {cls.__name__} or a sequence of two datetimes.'
        else:
            str = f'Expecting a {cls.__name__} or a sequence of two datetimes.'
        raise ValueError(str)

    def __init__(self, start: datetime.datetime|str, end: datetime.datetime|str):
        """Initialize a TimePeriod with start and end times.

        Args:
            start (datetime.datetime|str): The start of the period. If a string,
                will be parsed using get_date_from_str().
            end (datetime.datetime|str): The end of the period. If a string,
                will be parsed using get_date_from_str() with end=True.
        """
        self.start = start if isinstance(start, datetime.datetime) else get_date_from_str(start)
        self.end = end if isinstance(end, datetime.datetime) else get_date_from_str(end, end = True)

    def get_length(self, unit = 'days'):
        """Calculate the length of the time period.

        Args:
            unit (str, optional): Unit for the length. Either 'days' or 'hours'.
                Defaults to 'days'.

        Returns:
            int|float: The length of the period in the specified unit.
                Returns 0 if start > end.

        Raises:
            ValueError: If unit is not 'days' or 'hours'.

        Examples:
            >>> period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
            >>> period.get_length()
            31
            >>> period.get_length('hours')
            744

        Note:
            - Days are counted inclusively (start and end both included)
            - For hours, if the period spans less than 24 hours, uses seconds
            - Otherwise converts days to hours (days * 24)
        """

        if self.start > self.end:
            return 0

        if unit == 'days':
            days = (self.end - self.start).days + 1
            return days
        elif unit == 'hours':
            # Calculate total duration in hours
            delta = self.end - self.start
            # For periods >= 1 day, use day-based calculation
            if delta.days >= 1:
                hours = self.get_length(unit='days') * 24
            else:
                # For sub-day periods, use seconds
                hours = ((self.end - self.start).seconds + 1) / 3600
            return hours
        else:
            raise ValueError(f'Unknown unit "{unit}", must be "days" or "hours"')

    def extend(self, window: TimeWindow, before = False):
        """Extend the time period by a time window.

        Args:
            window (TimeWindow): The window by which to extend the period.
            before (bool, optional): If True, extend before the start.
                If False, extend after the end. Defaults to False.

        Returns:
            TimePeriod: A new TimePeriod with the extended range.

        Examples:
            >>> period = TimePeriod(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 1, 20))
            >>> extended = period.extend(TimeWindow(5, 'd'))  # Extends 5 days after end
            >>> extended_before = period.extend(TimeWindow(5, 'd'), before=True)  # Extends 5 days before start
        """
        if before:
            if window.unit == 'h':
                new_start = self.start - datetime.timedelta(hours=window.size)
                new_end = self.end + datetime.timedelta(1) - datetime.timedelta(minutes = 1)
            else:
                new_start = window.apply(self.start - datetime.timedelta(1)).start
                new_end = self.end
        else:
            new_start = self.start
            if window.unit == 'h':
                new_end = self.end + datetime.timedelta(1) - datetime.timedelta(minutes = 1) + datetime.timedelta(hours=window.size)
            else:
                new_end = window.apply(self.end + datetime.timedelta(1), start = True).end

        return TimePeriod(new_start, new_end)

    #TODO remove this method eventually, it conflicts with the .length property of FixedLenTimeStep
    def length(self, **kwargs):
        return self.get_length(**kwargs)

    def contains(self, time: datetime.datetime|str):
        """Check if a datetime falls within this period.

        Args:
            time (datetime.datetime|str): The datetime to check. If a string,
                will be parsed using get_date_from_str().

        Returns:
            bool: True if time is between start and end (inclusive), False otherwise.

        Examples:
            >>> period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
            >>> period.contains(datetime.datetime(2024, 1, 15))
            True
            >>> period.contains('2024-02-01')
            False
        """
        time = time if isinstance(time, datetime.datetime) else get_date_from_str(time)
        return self.start <= time <= self.end

    def __repr__(self):
        if hasattr(self, 'agg_window') and self.agg_window is not None:
            return f'{self.__class__.__name__} ({self.start:%Y%m%d} - {self.end:%Y%m%d}) agg = {self.agg_window}'
        return f'{self.__class__.__name__} ({self.start:%Y%m%d} - {self.end:%Y%m%d})'
    
    def __str__(self):
        if hasattr(self, 'agg_window') and self.agg_window is not None:
            return f'{self.__class__.__name__.lower()} {self.start:%Y%m%d}-{self.end:%Y%m%d} (agg={self.agg_window.__str__()})'
        return f'{self.__class__.__name__.lower()} {self.start:%Y%m%d}-{self.end:%Y%m%d}'

    def __eq__(self, other: 'TimePeriod'):
        return self.start == other.start and self.end == other.end

    def __lt__(self, other: 'TimePeriod'):
        return self.end < other.start
        
    def __gt__(self, other: 'TimePeriod'):
        return self.start > other.end

    def __le__(self, other: 'TimePeriod'):
        return self < other or self == other
    
    def __ge__(self, other: 'TimePeriod'):
        return self > other or self == other
    
    def __hash__(self):
        return hash((self.start, self.end))
