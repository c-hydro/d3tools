"""Fixed-number-per-year timesteps (Dekad, Month, Year).

This module implements timesteps that occur a fixed number of times per year:
- Dekad: 36 per year (three 10-day periods per month)
- Month: 12 per year
- Year: 1 per year

These timesteps have variable length but consistent numbering within each year.
"""

from abc import ABC, abstractmethod
import datetime
from typing import Optional

from .timestep import TimeStep, TimeStepMeta
from ..time_utils import get_date_from_str

class FixedNTimeStepMeta(TimeStepMeta):
    """Metaclass for registering FixedNTimeStep subclasses by their n_steps value."""
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'fixed_n_subclasses'):
            cls.fixed_n_subclasses = {}
        elif 'n_steps' in attrs:
            cls.fixed_n_subclasses[attrs['n_steps']] = cls

class FixedNTimeStep(TimeStep, ABC, metaclass=FixedNTimeStepMeta):
    """Base class for timesteps with a fixed number per year.
    
    These timesteps have variable duration but occur a fixed number of times
    annually (e.g., 36 dekads, 12 months, 1 year). Each timestep is identified
    by a year and step number.
    
    Attributes:
        year (int): The year of the timestep.
        step (int): The step number within the year (1 to n_steps).
        n_steps (int): Total number of steps per year.
    
    Examples:
        >>> dekad = Dekad(2024, 15)  # 15th dekad of 2024
        >>> month = Month(2024, 3)   # March 2024
        >>> year = Year(2024)        # Year 2024
    """

    def __init__(self, year: int, step: int, n_steps: int):
        self.year: int = year
        self.step: int = step       # step is the number of the day, dekad, month, or year (always 1 for year)
        self.n_steps: int = n_steps # number of steps in a year (36 for dekads, 12 for months)

        start: datetime.datetime = self.get_start()
        end: datetime.datetime = self.get_end()
        super().__init__(start, end)

    @classmethod
    def get_subclass(cls, n_steps: int|None):
        """Get the FixedNTimeStep subclass for a specific number of steps per year.
        
        Args:
            n_steps (int|None): Number of steps per year (36, 12, or 1).
        
        Returns:
            Type[FixedNTimeStep]: The appropriate subclass.
        
        Raises:
            ValueError: If n_steps is not recognized.
        """
        if n_steps is None: return cls
        Subclass: 'FixedNTimeStep'|None = cls.fixed_n_subclasses.get(n_steps)
        if Subclass is None:
            raise ValueError(f"Invalid number of steps: {n_steps}")
        return Subclass
    
    @classmethod
    def get_n_steps(cls, n_steps: Optional[int] = None):
        """Get or validate the number of steps per year.
        
        Args:
            n_steps (int|None): Explicit n_steps value, or None to use class default.
        
        Returns:
            int: The n_steps value.
        
        Raises:
            TypeError: If n_steps cannot be determined.
        """
        if n_steps is not None:
            return n_steps
        elif hasattr(cls, 'n_steps'):
            return cls.n_steps
        else:
            raise TypeError('Could not find "n_steps"')

    @classmethod
    def from_step(cls, year:int, step:int, n_steps: Optional[int] = None):
        """Create a timestep from year and step number.
        
        Args:
            year (int): The year.
            step (int): Step number within the year.
            n_steps (int|None): Number of steps per year to determine subclass.
        
        Returns:
            FixedNTimeStep: The appropriate timestep instance.
        
        Examples:
            >>> FixedNTimeStep.from_step(2024, 15, 36)  # 15th dekad
            Dekad (20240521 - 20240531)
        """
        Subclass: 'FixedNTimeStep' = cls.get_subclass(n_steps)
        return Subclass(year, step)

    @classmethod
    def from_date(cls, date: datetime.datetime|str, n_steps: Optional[int] = None):
        """Create a timestep containing the given date.
        
        Args:
            date (datetime|str): Date to locate within a timestep.
            n_steps (int|None): Number of steps per year to determine subclass.
        
        Returns:
            FixedNTimeStep: The timestep containing the date.
        
        Examples:
            >>> FixedNTimeStep.from_date('2024-05-25', 36)  # 25 May is in dekad 15
            Dekad (20240521 - 20240531)
        """
        date = date if isinstance(date, datetime.datetime) else get_date_from_str(date)
        Subclass: 'FixedNTimeStep' = cls.get_subclass(n_steps)
        return Subclass(date.year, Subclass.get_step_from_date(date))

    def __add__(self, n: int):
        """Add n timesteps to this timestep.
        
        Handles year wrapping automatically when stepping across year boundaries.
        
        Args:
            n (int): Number of timesteps to add (negative to go backward).
        
        Returns:
            FixedNTimeStep: The resulting timestep.
        
        Examples:
            >>> Month(2024, 11) + 3  # November + 3 = February next year
            Month (20250201 - 20250228)
        """
        step = self.step + n
        year = self.year
        while step > self.n_steps:
            step -= self.n_steps
            year += 1

        while step < 1:
            step += self.n_steps
            year -= 1
        
        other = self.from_step(year, step, self.n_steps)
        return other

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
    """A 10-day period timestep (36 per year).
    
    Dekads divide each month into three periods:
    - Dekad 1: days 1-10
    - Dekad 2: days 11-20
    - Dekad 3: days 21 to end of month (variable length)
    
    Attributes:
        year (int): The year.
        dekad_of_year (int): Dekad number (1-36).
        month (int): Month number (1-12).
        dekad_of_month (int): Dekad within the month (1-3).
    
    Examples:
        >>> dek = Dekad(2024, 15)  # 15th dekad = May 11-20
        >>> dek.month
        5
        >>> dek.dekad_of_month
        2
    """

    n_steps:int = 36
    unit = 't'

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
        """Month number (1-12) containing this dekad."""
        return (self.step - 1) // 3 + 1
    
    @property
    def dekad_of_month(self):
        """Dekad number within the month (1-3)."""
        return (self.step - 1) % 3 + 1
    
    @property
    def dekad(self):
        """Alias for dekad_of_year."""
        return self.dekad_of_year

    @property
    def dekad_of_year(self):
        """Dekad number within the year (1-36)."""
        return self.step
    
class Month(FixedNTimeStep):
    """A calendar month timestep (12 per year).
    
    Attributes:
        year (int): The year.
        month (int): Month number (1-12).
        month_of_year (int): Alias for month.
    
    Examples:
        >>> march = Month(2024, 3)
        >>> march.start
        datetime.datetime(2024, 3, 1, 0, 0)
        >>> march.end
        datetime.datetime(2024, 3, 31, 00, 00, 00)
    """

    n_steps:int = 12
    unit = 'm'

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
        """Month number (1-12)."""
        return self.step
    
    @property
    def month_of_year(self):
        """Alias for month."""
        return self.step
    
class Year(FixedNTimeStep):
    """A calendar year timestep (1 per year).
    
    Attributes:
        year (int): The year.
    
    Examples:
        >>> year = Year(2024)
        >>> year.start
        datetime.datetime(2024, 1, 1, 0, 0)
        >>> year.end
        datetime.datetime(2024, 12, 31, 00, 00, 00)
        >>> year.is_leap()
        True
    """
    
    n_steps:int = 1
    unit = 'y'

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
        """Check if this is a leap year.
        
        Returns:
            bool: True if leap year, False otherwise.
        
        Examples:
            >>> Year(2024).is_leap()
            True
            >>> Year(2023).is_leap()
            False
        """
        return self.year % 4 == 0 and (self.year % 100 != 0 or self.year % 400 == 0)