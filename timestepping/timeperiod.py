from abc import ABC
import datetime

from .time_utils import get_date_from_str

class TimePeriod(ABC):
    
    """
    A TimePeriod is just defined by its start and end dates.
    """

    def __init__(self, start: datetime.datetime|str, end: datetime.datetime|str):
        self.start = start if isinstance(start, datetime.datetime) else get_date_from_str(start)
        self.end = end if isinstance(end, datetime.datetime) else get_date_from_str(end)

    def length(self, unit = 'days'):
        days = (self.end - self.start).days + 1
        if unit == 'days':
            return days
        elif unit == 'hours':
            return days * 24
        else:
            raise ValueError(f'Unknown unit "{unit}", must be "days" or "hours"')

    def __repr__(self):
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
