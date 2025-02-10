import datetime
from abc import ABC
from typing import Iterable, Optional, Sequence

from .fixed_num_timestep import FixedNTimeStep, FixedNTimeStepMeta
from .time_utils import get_date_from_str

class FixedDOYTimeStepStepMeta(FixedNTimeStepMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'fixed_doy_subclasses'):
            cls.fixed_doy_subclasses = {}
        elif 'start_doys' in attrs:
            cls.fixed_doy_subclasses[attrs['start_doys']] = cls

#TODO: make this not an ABC (need to implement get_start and get_end)
class FixedDOYTimeStep(FixedNTimeStep, ABC, metaclass=FixedDOYTimeStepStepMeta):
    """
    A FixedDOYTimeStep is a timestep that starts at fixed DOYs each year.
    """
    def __init__(self, year: int, step: int, start_doys: Iterable[int]):
        self.start_doys = tuple(start_doys)
        super().__init__(year, step, len(start_doys))

    @classmethod
    def get_subclass(cls, start_doys: Sequence|None):
        if start_doys is None: return cls
        start_doys = tuple(start_doys)
        Subclass: 'FixedDOYTimeStep'|None = cls.fixed_doy_subclasses.get(start_doys)
        return Subclass

    @classmethod
    def get_start_doys(cls, start_doys: Optional[Iterable[int]] = None):
        if start_doys is not None:
            return start_doys
        elif hasattr(cls, 'start_doys'):
            return cls.start_doys
        else:
            raise TypeError('Could not find "start_doys"')

    @classmethod
    def from_date(cls, date: datetime.datetime|str, start_doys: Optional[Iterable[int]] = None):
        date = date if isinstance(date, datetime.datetime) else get_date_from_str(date)
        Subclass: 'FixedDOYTimeStep'|None = cls.get_subclass(start_doys)
        if Subclass:
            return Subclass(date.year, Subclass.get_step_from_date(date))
        else:
            return cls(date.year, cls.get_step_from_date(date, start_doys), start_doys)
    
    @classmethod
    def from_step(cls, year: int, step: int, start_doys: Optional[Iterable[int]] = None):
        Subclass: 'FixedDOYTimeStep'|None = cls.get_subclass(start_doys)
        if Subclass:
            return Subclass(year, step)
        else:
            return cls(year, step, start_doys)
        
    @staticmethod
    def get_step_from_date(date: datetime.datetime, start_doys: Iterable[int]):
        """
        Returns the step number for a given date.
        """
        start_doys = tuple(start_doys)
        doy = date.timetuple().tm_yday
        for i, start_doy in enumerate(start_doys):
            if doy < start_doy:
                return i
        else:
            return len(start_doys)

    def get_start(self):
        start_doy = self.start_doys[self.step_of_year - 1]
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=start_doy - 1)
    
    def get_end(self):
        if self.step_of_year < len(self.start_doys):
            end_doy = self.start_doys[self.step_of_year] - 1
            return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=end_doy - 1)
        else:
            return datetime.datetime(self.year, 12, 31)
    
    def __add__(self, n: int):
        step = self.step + n
        year = self.year
        while step > self.n_steps:
            step -= self.n_steps
            year += 1

        while step < 1:
            step += self.n_steps
            year -= 1
        
        other = self.from_step(year, step, self.start_doys)
        return other

    @property
    def step_of_year(self):
        return self.step

class ViirsModisTimeStep(FixedDOYTimeStep):
    """
    A class for timestep of VIIRS and MODIS data.
    VIIRS and MODIS come in 8-day periods (except for the last period of the year, which is shorter) starting at fixed DOYs.
    """

    start_doys = tuple(range(1, 366, 8))
    unit = 'v'

    def __init__(self, year: int, step_of_year: int):
        super().__init__(year, step_of_year, ViirsModisTimeStep.start_doys)

    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        return FixedDOYTimeStep.get_step_from_date(date, ViirsModisTimeStep.start_doys)