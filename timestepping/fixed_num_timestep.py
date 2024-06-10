from abc import ABC, ABCMeta, abstractmethod
import datetime
from typing import Optional

from .timestep import TimeStep
from .time_utils import get_date_from_str

class FixedNTimeStepMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'n_steps' in attrs:
            cls.subclasses[attrs['n_steps']] = cls

class FixedNTimeStep(TimeStep, ABC, metaclass=FixedNTimeStepMeta):
    """
    A FixedNTimeStep is a timestep with variable lenght, but a fixed number of timesteps occurring in a year.
    It can be a a dekad (36 timesteps per year), a month (12), or a year (1).
    """

    def __init__(self, year: int, step: int, n_steps: int):
        self.year: int = year
        self.step: int = step       # step is the number of the day, dekad, month, or year (always 1 for year)
        self.n_steps: int = n_steps # number of steps in a year (36 for dekads, 12 for months)

        start: datetime.datetime = self.get_start()
        end: datetime.datetime = self.get_end()
        super().__init__(start, end)

    @classmethod
    def get_subclass(cls, n_steps: int):
        Subclass: 'FixedNTimeStep'|None = cls.subclasses.get(n_steps)
        if Subclass is None:
            raise ValueError(f"Invalid number of steps: {n_steps}")
        return Subclass
    
    @classmethod
    def get_n_steps(cls, n_steps: Optional[int] = None):
        if n_steps is not None:
            return n_steps
        elif hasattr(cls, 'n_steps'):
            return cls.n_steps
        else:
            raise TypeError('Could not find "n_steps"')

    @classmethod
    def from_step(cls, year:int, step:int, n_steps: Optional[int] = None):
        n_steps = cls.get_n_steps(n_steps)
        Subclass: 'FixedNTimeStep' = cls.get_subclass(n_steps)
        return Subclass(year, step)

    @classmethod
    def from_date(cls, date: datetime.datetime|str, n_steps: Optional[int] = None):
        date = date if isinstance(date, datetime.datetime) else get_date_from_str(date)
        n_steps = cls.get_n_steps(n_steps)
        Subclass: 'FixedNTimeStep' = cls.get_subclass(n_steps)
        return Subclass(date.year, Subclass.get_step_from_date(date))

    def __add__(self, n: int):
        step = self.step + n
        year = self.year
        while step > self.n_steps:
            step -= self.n_steps
            year += 1

        while step < 1:
            step += self.n_steps
            year -= 1
        
        return self.from_step(year, step, self.n_steps)

    @staticmethod
    @abstractmethod
    def get_step_from_date(date: datetime.datetime):
        """
        Returns the step of the year for the given date.
        The year is divided into n_steps steps.
        """
        raise NotImplementedError

    @abstractmethod
    def get_start(self):
        raise NotImplementedError
    
    @abstractmethod
    def get_end(self):
        raise NotImplementedError
    
class Dekad(FixedNTimeStep):

    n_steps:int = 36

    def __init__(self, year: int, dekad_of_year: int):
        super().__init__(year, dekad_of_year, Dekad.n_steps)

    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        """
        Returns the dekad of the year for the given date.
        The year is divided into 36 dekads, each of ~10 days.
        """
        month = date.month
        day = date.day
        if 1 <= day <= 10:
            return (month - 1) * 3 + 1
        elif 11 <= day <= 20:
            return (month - 1) * 3 + 2
        else:
            return (month - 1) * 3 + 3
    
    def get_start(self):
        dekad = self.step
        month = self.month
        day   = ((dekad - 1) % 3) * 10 + 1
        return datetime.datetime(self.year, month, day)
    
    def get_end(self):
        start_date = self.get_start()
        if self.dekad_of_month == 3:
            next_dkd_month = start_date.month + 1 if start_date.month < 12 else 1
            next_dkd_year = start_date.year if start_date.month < 12 else start_date.year + 1
            end_date = datetime.datetime(next_dkd_year, next_dkd_month, 1) - datetime.timedelta(days=1)
        else:
            end_date = start_date + datetime.timedelta(days=9)
        return end_date

    @property
    def month(self):
        return (self.step - 1) // 3 + 1
    
    @property
    def dekad_of_month(self):
        return (self.step - 1) % 3 + 1
    
    @property
    def dekad(self):
        return self.dekad_of_year

    @property
    def dekad_of_year(self):
        return self.step
    
class Month(FixedNTimeStep):

    n_steps:int = 12

    def __init__(self, year: int, month_of_year: int):
        super().__init__(year, month_of_year, Month.n_steps)

    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        """
        Returns the month of the year for the given date.
        """
        return date.month

    def get_start(self):
        return datetime.datetime(self.year, self.month, 1)
    
    def get_end(self):
        next_month_month = self.month + 1 if self.month < 12 else 1
        next_month_year = self.year + 1 if self.month == 12 else self.year
        return datetime.datetime(next_month_year, next_month_month, 1) - datetime.timedelta(days=1)
    
    @property
    def month(self):
        return self.step
    
    @property
    def month_of_year(self):
        return self.step
    
class Year(FixedNTimeStep):
    
        n_steps:int = 1
    
        def __init__(self, year: int, dummy: int = 1):
            super().__init__(year, 1, Year.n_steps)
    
        @staticmethod
        def get_step_from_date(date: datetime.datetime):
            """
            Returns the year for the given date.
            """
            return 1
    
        def get_start(self):
            return datetime.datetime(self.year, 1, 1)
        
        def get_end(self):
            return datetime.datetime(self.year, 12, 31)

        def is_leap(self):
            return self.year % 4 == 0 and (self.year % 100 != 0 or self.year % 400 == 0)