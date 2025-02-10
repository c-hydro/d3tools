from .time_utils import find_unit_of_time
from dateutil.relativedelta import relativedelta
import warnings

import datetime

class TimeWindow():
    """
    A class to represent a time window.
    """

    conversions = {
        'h' : {'d': 24,   'w': 168, 'v' : 192},
        'd' : {'h': 1/24, 'w': 7, 'v' : 8},
        'w' : {'h': 1/168,'d': 1/7},
        'v' : {'h': 1/192,'d': 1/8},
        't' : {'m': 3, 'y' : 36},
        'm' : {'t': 1/3, 'y': 12},
        'y' : {'t': 1/36, 'm': 1/12}
    }

    def __init__(self, size: int, unit: str):
        self.size = int(size)
        self.unit = find_unit_of_time(unit)

    def __repr__(self):
        return f'TimeWindow({self.size}, {self.unit})'
    
    def __str__(self):
        return f'{self.size}{self.unit}'

    @classmethod
    def from_str(cls, window: str) -> 'TimeWindow':
        """
        Returns a tuple with the size and unit of the window.
        split the string into size and unit
        allow for any separator, including no separator at all
        (e.g. '3days', '3 days', '3 days', '3 d', '3 d', '3d', '3.d', '3-d')
        if you use 10day or 8day (or 18day for example), you need to use a separator because the function cannot distinguish between 10 days and 1 dekad
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
        
        from .fixed_num_timestep import Dekad
        from .timerange import TimeRange

        unit = self.unit
        size = self.size
        
        if unit in ['m', 'y', 'd', 'w']:
            reldelta_unitmap = {'d': 'days', 'm': 'months', 'y': 'years', 'w': 'weeks'}
            reldelta_unit = reldelta_unitmap[unit]
            if start:
                time_start:datetime.datetime = time
                time_end:datetime.datetime = time + datetime.timedelta(days=1) + relativedelta(**{reldelta_unit: size}) - datetime.timedelta(days=2)
            else:
                time_start:datetime.datetime = time + datetime.timedelta(days=1) - relativedelta(**{reldelta_unit: size})
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
        else:
            raise ValueError('Unit for aggregator not recognized: must be one of dekads, months, years, days, weeks')
        return TimeRange(time_start, time_end)
    
    def to_hours(self, limit:str = None):
        if self.unit == 'd':
            return self.size * 24
        elif self.unit == 'h':
            return self.size
        elif self.unit == 'v':
            return self.size * 8 * 24
        elif self.unit == 'w':
            return self.size * 7 * 24
        elif limit is None:
            raise ValueError('Cannot convert to hourse, sepecify if max or min limit is desired')
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
        return self.size == other.size and self.unit == other.unit

    def __lt__(self, other: 'TimeWindow'):
        this_hours = self.to_hours(limit = 'max')
        other_hours = other.to_hours(limit = 'min')
        return this_hours < other_hours
        
    def __gt__(self, other: 'TimeWindow'):
        this_hours = self.to_hours(limit = 'min')
        other_hours = other.to_hours(limit = 'max')
        return this_hours > other_hours

    def __le__(self, other: 'TimeWindow'):
        return self < other or self == other
    
    def __ge__(self, other: 'TimeWindow'):
        return self > other or self == other
    
    def __add__(self,  other: 'TimeWindow'):
        if self.unit == other.unit:
            return TimeWindow(self.size + other.size, self.unit)
        elif other.unit in self.conversions[self.unit]:
            # find the integer conversion factor
            if self.conversions[self.unit][other.unit] == int(self.conversions[self.unit][other.unit]):
                factor = self.conversions[self.unit][other.unit]
                return TimeWindow(self.size + (other.size * factor), self.unit)
            elif self.conversions[other.unit][self.unit] == int(self.conversions[other.unit][self.unit]):
                factor = self.conversions[other.unit][self.unit]
                return TimeWindow((self.size * factor) + other.size, other.unit)
            else:
                raise ValueError(f'Cannot add these two time windows together {self} and {other}')

        else:
            raise ValueError(f'Cannot add these two time windows together {self} and {other}')
    
    def __sub__(self,  other: 'TimeWindow'):
        return self + TimeWindow(-other.size, other.unit)