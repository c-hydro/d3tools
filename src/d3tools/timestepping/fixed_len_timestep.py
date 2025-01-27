from abc import ABC, ABCMeta, abstractmethod
import datetime
from typing import Optional

from .timestep import TimeStep
from .time_utils import get_date_from_str

class FixedLenTimeStepMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'length' in attrs:
            cls.subclasses[attrs['length']] = cls

class FixedLenTimeStep(TimeStep, ABC, metaclass=FixedLenTimeStepMeta):
    """
    A FixedLenTimeStep is a timestep with fixed lenght (measured in days).
    It can be a day (1 day) or an hour (1/24 day).
    """

    def __init__(self, year: int, step: int, length: float):
        self.year: int = year
        self.step: int = step       # step is the number of day (DOY) or hour in the year
        self.length: float = length # length of the timestp in days

        start: datetime.datetime = self.get_start()
        end: datetime.datetime = self.get_end()
        super().__init__(start, end)

    @classmethod
    def get_subclass(cls, length: float):
        Subclass: 'FixedLenTimeStep'|None = cls.subclasses.get(length)
        if Subclass is None:
            raise ValueError(f"Invalid step length: {length}")
        return Subclass

    @classmethod
    def get_length(cls, length:Optional[int] = None):
        if length is not None:
            return length
        elif hasattr(cls, 'length'):
            return cls.length
        else:
            raise TypeError('Could not find "length"')

    @classmethod
    def from_step(cls, year:int, step:int, length:Optional[int] = None):
        length = cls.get_length(length)
        Subclass: 'FixedLenTimeStep' = cls.get_subclass(length)
        return Subclass(year, step)

    @classmethod
    def from_date(cls, date: datetime.datetime|str, length:Optional[int] = None):
        date = date if isinstance(date, datetime.datetime) else get_date_from_str(date)
        length = cls.get_length(length)
        Subclass: 'FixedLenTimeStep' = cls.get_subclass(length)
        return Subclass(date.year, Subclass.get_step_from_date(date))


    def __add__(self, n: int):
        delta_days = n*self.length
        new_start = self.start + datetime.timedelta(days=delta_days)
        return self.from_date(new_start, self.length)
    
    @staticmethod
    @abstractmethod
    def get_step_from_date(date: datetime.datetime):
        """
        Returns the step of the year for the given date.
        """
        raise NotImplementedError

    @abstractmethod
    def get_start(self):
        raise NotImplementedError
    
    @abstractmethod
    def get_end(self):
        raise NotImplementedError

class Day(FixedLenTimeStep):

    length: float = 1

    def __init__(self, year: int, step: int):
        super().__init__(year, step, Day.length)
    
    # @classmethod
    # def from_step(cls, year:int, step:int):
    #     return super().from_step(year, step, Day.length)

    # @classmethod
    # def from_date(cls, date: datetime.datetime|str, length: int):
    #     return super().from_date(date, Day.length)

    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        return date.timetuple().tm_yday

    def get_start(self):
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=self.step - 1)

    def get_end(self):
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=self.step, seconds=-1)
    
    @property
    def day_of_year(self):
        return self.step
    
    @property
    def month(self):
        return self.start.month
    
    @property
    def day_of_month(self):
        return self.start.day

    def __repr__(self):
        return f'{self.__class__.__name__} ({self.start:%Y%m%d})'
    
class Hour(FixedLenTimeStep):

    length: float = 1/24

    def __init__(self, year: int, step: int):
        super().__init__(year, step, Hour.length)
    
    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        return (date.timetuple().tm_yday - 1) * 24 + date.hour + 1

    def get_start(self):
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(hours=self.step - 1)

    def get_end(self):
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(hours=self.step, seconds=-1)
    
    @property
    def hour_of_year(self):
        return self.step
    
    @property
    def day_of_year(self):
        return round(self.step/24) + 1
    
    def __repr__(self):
        return f'{self.__class__.__name__} ({self.start:%Y%m%d %H%M}-{self.end:%H%M})'