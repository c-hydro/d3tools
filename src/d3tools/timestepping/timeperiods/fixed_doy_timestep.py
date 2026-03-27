"""Fixed day-of-year timesteps (VIIRS/MODIS).

This module implements timesteps that start on fixed days of the year (DOY).
The primary use case is VIIRS and MODIS satellite data, which uses 8-day
periods starting on specific DOYs.

Unlike fixed-length timesteps, these can have variable length (especially
the last period of the year) but always start on the same DOYs each year.
"""

import datetime
from abc import ABC
from typing import Iterable, Optional, Sequence

from .fixed_num_timestep import FixedNTimeStep, FixedNTimeStepMeta
from ..time_utils import get_date_from_str

class FixedDOYTimeStepStepMeta(FixedNTimeStepMeta):
    """Metaclass for registering FixedDOYTimeStep subclasses by their start_doys."""
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'fixed_doy_subclasses'):
            cls.fixed_doy_subclasses = {}
        elif 'start_doys' in attrs:
            cls.fixed_doy_subclasses[attrs['start_doys']] = cls

#TODO: make this not an ABC (need to implement get_start and get_end)
class FixedDOYTimeStep(FixedNTimeStep, ABC, metaclass=FixedDOYTimeStepStepMeta):
    """Base class for timesteps starting on fixed days of the year.
    
    These timesteps divide the year based on predetermined start DOYs rather than
    fixed intervals or calendar boundaries. The last period of each year extends
    to December 31.
    
    Attributes:
        year (int): The year.
        step (int): Step number within the year.
        start_doys (tuple[int]): Tuple of DOYs where each period starts.
        n_steps (int): Number of steps per year (derived from start_doys length).
    
    Examples:
        >>> # VIIRS/MODIS 8-day periods start on DOYs 1, 9, 17, 25, ...
        >>> viirs = ViirsModisTimeStep(2024, 1)
        >>> viirs.start
        datetime.datetime(2024, 1, 1, 0, 0)
        >>> viirs.end
        datetime.datetime(2024, 1, 8, 23, 59, 59)
    """
    def __init__(self, year: int, step: int, start_doys: Iterable[int]):
        self.start_doys = tuple(start_doys)
        super().__init__(year, step, len(start_doys))

    @classmethod
    def get_subclass(cls, start_doys: Sequence|None):
        """Get the FixedDOYTimeStep subclass for specific start DOYs.
        
        Args:
            start_doys (Sequence|None): Tuple of start DOYs.
        
        Returns:
            Type[FixedDOYTimeStep]|None: The registered subclass, or None.
        """
        if start_doys is None: return cls
        start_doys = tuple(start_doys)
        Subclass: 'FixedDOYTimeStep'|None = cls.fixed_doy_subclasses.get(start_doys)
        return Subclass

    @classmethod
    def get_start_doys(cls, start_doys: Optional[Iterable[int]] = None):
        """Get or validate the start DOYs.
        
        Args:
            start_doys (Iterable[int]|None): Explicit DOYs, or None to use class default.
        
        Returns:
            tuple[int]: The start DOYs.
        
        Raises:
            TypeError: If start_doys cannot be determined.
        """
        if start_doys is not None:
            return start_doys
        elif hasattr(cls, 'start_doys'):
            return cls.start_doys
        else:
            raise TypeError('Could not find "start_doys"')

    @classmethod
    def from_date(cls, date: datetime.datetime|str, start_doys: Optional[Iterable[int]] = None):
        """Create a timestep containing the given date.
        
        Args:
            date (datetime|str): Date to locate within a timestep.
            start_doys (Iterable[int]|None): Start DOYs to determine subclass.
        
        Returns:
            FixedDOYTimeStep: The timestep containing the date.
        
        Examples:
            >>> FixedDOYTimeStep.from_date('2024-01-15', range(1, 366, 8))
            # Returns the VIIRS period containing Jan 15
        """
        date = date if isinstance(date, datetime.datetime) else get_date_from_str(date)
        Subclass: 'FixedDOYTimeStep'|None = cls.get_subclass(start_doys)
        if Subclass:
            return Subclass(date.year, Subclass.get_step_from_date(date))
        else:
            return cls(date.year, cls.get_step_from_date(date, start_doys), start_doys)
    
    @classmethod
    def from_step(cls, year: int, step: int, start_doys: Optional[Iterable[int]] = None):
        """Create a timestep from year and step number.
        
        Args:
            year (int): The year.
            step (int): Step number within the year.
            start_doys (Iterable[int]|None): Start DOYs to determine subclass.
        
        Returns:
            FixedDOYTimeStep: The timestep instance.
        """
        Subclass: 'FixedDOYTimeStep'|None = cls.get_subclass(start_doys)
        if Subclass:
            return Subclass(year, step)
        else:
            return cls(year, step, start_doys)
        
    @staticmethod
    def get_step_from_date(date: datetime.datetime, start_doys: Iterable[int]):
        """Determine which step a date falls into.
        
        Args:
            date (datetime): The date to check.
            start_doys (Iterable[int]): Start DOYs defining the periods.
        
        Returns:
            int: The step number (1-indexed) containing the date.
        
        Examples:
            >>> from datetime import datetime
            >>> start_doys = range(1, 366, 8)  # 1, 9, 17, 25, ...
            >>> FixedDOYTimeStep.get_step_from_date(datetime(2024, 1, 15), start_doys)
            2  # Falls in second period (DOY 9-16)
        """
        start_doys = tuple(start_doys)
        doy = date.timetuple().tm_yday
        for i, start_doy in enumerate(start_doys):
            if doy < start_doy:
                return i
        else:
            return len(start_doys)

    def get_start(self):
        """Calculate the start datetime of this timestep.
        
        Returns:
            datetime: Start of the period at 00:00:00.
        """
        start_doy = self.start_doys[self.step_of_year - 1]
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=start_doy - 1)
    
    def get_end(self):
        """Calculate the end datetime of this timestep.
        
        For all periods except the last, ends one day before the next period starts.
        The last period extends to December 31.
        
        Returns:
            datetime: End of the period at 23:59:59.
        """
        if self.step_of_year < len(self.start_doys):
            end_doy = self.start_doys[self.step_of_year] - 1
            return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=end_doy - 1)
        else:
            return datetime.datetime(self.year, 12, 31)
    
    def __add__(self, n: int):
        """Add n timesteps to this timestep.
        
        Handles year wrapping when stepping across year boundaries.
        
        Args:
            n (int): Number of timesteps to add (negative to go backward).
        
        Returns:
            FixedDOYTimeStep: The resulting timestep.
        """
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
        """Step number within the year (1-indexed)."""
        return self.step

class ViirsModisTimeStep(FixedDOYTimeStep):
    """Timestep for VIIRS and MODIS satellite data products.
    
    VIIRS (Visible Infrared Imaging Radiometer Suite) and MODIS (Moderate Resolution
    Imaging Spectroradiometer) provide data in ~8-day composites. These periods start
    on DOYs 1, 9, 17, 25, etc., resulting in 46 periods per year. The last period
    is shorter, covering the remaining days to December 31.
    
    Attributes:
        year (int): The year.
        step_of_year (int): Period number (1-46).
    
    Examples:
        >>> viirs = ViirsModisTimeStep(2024, 1)  # First period of 2024
        >>> viirs.start
        datetime.datetime(2024, 1, 1, 0, 0)
        >>> viirs.end
        datetime.datetime(2024, 1, 8, 23, 59, 59)
        >>> next_period = viirs + 1
        >>> next_period.start
        datetime.datetime(2024, 1, 9, 0, 0)
    
    Note:
        - 46 periods per year (45 full 8-day periods + 1 shorter final period)
        - Start DOYs: 1, 9, 17, 25, 33, ..., 353, 361
        - Last period length varies: 5 days (normal years), 6 days (leap years)
    """

    start_doys = tuple(range(1, 366, 8))
    unit = 'v'

    def __init__(self, year: int, step_of_year: int):
        super().__init__(year, step_of_year, ViirsModisTimeStep.start_doys)

    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        return FixedDOYTimeStep.get_step_from_date(date, ViirsModisTimeStep.start_doys)