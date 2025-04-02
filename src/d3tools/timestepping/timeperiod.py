from abc import ABC
import datetime
from typing import Sequence

from .time_utils import get_date_from_str
from .timewindow import TimeWindow

class TimePeriod(ABC):
    
    """
    A TimePeriod is just defined by its start and end dates.
    """

    @classmethod
    def from_any(cls, value: 'TimePeriod'|Sequence[datetime.datetime|str]|None, name = "") -> 'TimePeriod':

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
        self.start = start if isinstance(start, datetime.datetime) else get_date_from_str(start)
        self.end = end if isinstance(end, datetime.datetime) else get_date_from_str(end, end = True)

    def get_length(self, unit = 'days'):
        if unit == 'days':
            days = (self.end - self.start).days + 1
            return days
        elif unit == 'hours':
            if len(self.hours) >= 24:
                hours = self.get_length(unit='days') * 24
                # hours = ((self.end + datetime.timedelta(1)) - self.start).seconds / 3600
            else:
                hours = ((self.end - self.start).seconds + 1) / 3600
            return hours
        else:
            raise ValueError(f'Unknown unit "{unit}", must be "days" or "hours"')

    def extend(self, window: TimeWindow, before = False):
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

        return self.__class__(new_start, new_end)

    #TODO remove this method eventually, it conflicts with the .length property of FixedLenTimeStep
    def length(self, **kwargs):
        return self.get_length(**kwargs)

    def contains(self, time: datetime.datetime|str):
        time = time if isinstance(time, datetime.datetime) else get_date_from_str(time)
        return self.start <= time <= self.end

    def __repr__(self):
        if hasattr(self, 'agg_window'):
            return f'{self.__class__.__name__} ({self.start:%Y%m%d} - {self.end:%Y%m%d}) agg = {self.agg_window}'
        return f'{self.__class__.__name__} ({self.start:%Y%m%d} - {self.end:%Y%m%d})'
    
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
