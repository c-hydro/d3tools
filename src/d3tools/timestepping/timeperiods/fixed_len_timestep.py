"""Fixed-length timesteps (Day, Hour).

This module implements timesteps with constant duration:
- Day: 1 day (24 hours)
- Hour: 1/24 day (60 minutes)

These timesteps have fixed length and are identified by sequential numbering
within each year (day of year, hour of year).
"""

from abc import ABC, abstractmethod
import datetime
from typing import Optional

from .timestep import TimeStep, TimeStepMeta
from ..time_utils import get_date_from_str

class FixedLenTimeStepMeta(TimeStepMeta):
    """Metaclass for registering FixedLenTimeStep subclasses by their length."""
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'fixed_l_subclasses'):
            cls.fixed_l_subclasses = {}
        elif 'length' in attrs:
            cls.fixed_l_subclasses[attrs['length']] = cls

class FixedLenTimeStep(TimeStep, ABC, metaclass=FixedLenTimeStepMeta):
    """Base class for timesteps with fixed duration.
    
    These timesteps have constant length in days and are numbered sequentially
    within each year.
    
    Attributes:
        year (int): The year of the timestep.
        step (int): The sequential step number within the year (e.g., day of year).
        length (float): Duration of the timestep in days.
    
    Examples:
        >>> day = Day(2024, 60)  # 60th day of 2024 (Feb 29)
        >>> hour = Hour(2024, 100)  # 100th hour of 2024
    """

    def __init__(self, year: int, step: int, length: float):
        self.year: int = year
        self.step: int = step       # step is the number of day (DOY) or hour in the year
        self.length: float = length # length of the timestp in days

        start: datetime.datetime = self.get_start()
        end: datetime.datetime = self.get_end()
        super().__init__(start, end)

    @classmethod
    def get_subclass(cls, length: float|None):
        """Get the FixedLenTimeStep subclass for a specific length.
        
        Args:
            length (float|None): Length in days (1 for Day, 1/24 for Hour).
        
        Returns:
            Type[FixedLenTimeStep]: The appropriate subclass.
        
        Raises:
            ValueError: If length is not recognized.
        """
        if length is None: return cls
        Subclass: 'FixedLenTimeStep'|None = cls.fixed_l_subclasses.get(length)
        if Subclass is None:
            raise ValueError(f"Invalid step length: {length}")
        return Subclass

    @classmethod
    def get_length(cls, length:Optional[int] = None):
        """Get or validate the timestep length.
        
        Args:
            length (int|None): Explicit length value, or None to use class default.
        
        Returns:
            float: The length in days.
        
        Raises:
            TypeError: If length cannot be determined.
        """
        if length is not None:
            return length
        elif hasattr(cls, 'length'):
            return cls.length
        else:
            raise TypeError('Could not find "length"')

    @classmethod
    def from_step(cls, year:int, step:int, length:Optional[int] = None):
        """Create a timestep from year and step number.
        
        Args:
            year (int): The year.
            step (int): Sequential step number within the year.
            length (float|None): Length in days to determine subclass.
        
        Returns:
            FixedLenTimeStep: The appropriate timestep instance.
        
        Examples:
            >>> FixedLenTimeStep.from_step(2024, 60, 1)  # 60th day
            Day (20240229)
        """
        Subclass: 'FixedLenTimeStep' = cls.get_subclass(length)
        return Subclass(year, step)

    @classmethod
    def from_date(cls, date: datetime.datetime|str, length:Optional[int] = None):
        """Create a timestep containing the given date.
        
        Args:
            date (datetime|str): Date to locate within a timestep.
            length (float|None): Length in days to determine subclass.
        
        Returns:
            FixedLenTimeStep: The timestep containing the date.
        
        Examples:
            >>> FixedLenTimeStep.from_date('2024-02-29', 1)
            Day (20240229)
        """
        date = date if isinstance(date, datetime.datetime) else get_date_from_str(date)
        Subclass: 'FixedLenTimeStep' = cls.get_subclass(length)
        return Subclass(date.year, Subclass.get_step_from_date(date))


    def __add__(self, n: int):
        """Add n timesteps to this timestep.
        
        Automatically handles year boundaries for the result.
        
        Args:
            n (int): Number of timesteps to add (negative to go backward).
        
        Returns:
            FixedLenTimeStep: The resulting timestep.
        
        Examples:
            >>> Day(2024, 365) + 1  # Last day of 2024 + 1 = first day of 2025
            Day (20250101)
        """
        delta_days = n*self.length
        new_start = self.start + datetime.timedelta(days=delta_days)

        other = self.from_date(new_start, self.length)
        return other
    
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
    """A single calendar day timestep.
    
    Each day runs from 00:00:00 to 23:59:59 and is numbered by day of year.
    
    Attributes:
        year (int): The year.
        day_of_year (int): Day number within the year (1-365/366).
        month (int): Month number (1-12).
        day_of_month (int): Day within the month (1-31).
    
    Examples:
        >>> day = Day(2024, 60)  # 60th day of 2024
        >>> day.start
        datetime.datetime(2024, 2, 29, 0, 0)
        >>> day.month
        2
        >>> day.day_of_month
        29
    """

    length: float = 1
    unit = 'd'

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
        """Day number within the year (1-365/366)."""
        return self.step
    
    @property
    def month(self):
        """Month number (1-12) of this day."""
        return self.start.month
    
    @property
    def day_of_month(self):
        """Day within the month (1-31)."""
        return self.start.day

    def __repr__(self):
        return f'{self.__class__.__name__} ({self.start:%Y%m%d})'
    
class Hour(FixedLenTimeStep):
    """A single hour timestep.
    
    Each hour is numbered sequentially within the year (1-8760/8784 for leap years).
    
    Attributes:
        year (int): The year.
        hour_of_year (int): Hour number within the year.
        day_of_year (int): Day containing this hour.
    
    Examples:
        >>> hour = Hour(2024, 100)  # 100th hour of 2024
        >>> hour.start
        datetime.datetime(2024, 1, 5, 3, 0)
        >>> hour.day_of_year
        5
    """

    length: float = 1/24
    unit = 'h'

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
        """Hour number within the year."""
        return self.step
    
    @property
    def day_of_year(self):
        """Day of year containing this hour."""
        return round(self.step/24) + 1
    
    def __repr__(self):
        return f'{self.__class__.__name__} ({self.start:%Y%m%d %H%M}-{self.end:%H%M})'