from abc import ABC, ABCMeta, abstractmethod

from .timerange import TimeRange, TimePeriod
from .time_utils import get_window, get_window_from_str, find_unit_of_time

class TimeStepMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'unit' in attrs:
            cls.subclasses[attrs['unit']] = cls

    @property
    def agg_window(cls):
        if hasattr(cls, '_agg_window'):
            return cls._agg_window
        return None

    @agg_window.setter
    def agg_window(cls, value: str|tuple|None):
        if value is None:
            return

        if isinstance(value, tuple):
            size, unit = value
            size = int(size)
            unit = find_unit_of_time(unit)
        elif isinstance(value, str):
            size, unit = get_window_from_str(value)
        else:
            raise ValueError(f"Invalid value for agg_window: {value}")

        cls._agg_window = (size, unit)

class TimeStep(TimeRange, ABC, metaclass=TimeStepMeta):
    """
    A timestep is a TimePeriod that supports addition and subtraction of integers.
    (i.e. there is previous and next timesteps)
    """

    @classmethod
    def from_unit(cls, unit: str):
        return cls.get_subclass(unit)

    @classmethod
    def get_subclass(cls, unit: str):
        unit = find_unit_of_time(unit)
        Subclass: 'TimeStep' = cls.subclasses.get(unit)
        if Subclass is None:
            raise ValueError(f"Invalid unit of time: {type}")
        return Subclass

    @property
    def agg_window(self):
        if hasattr(self, '_agg_window'):
            return self._agg_window
        return None

    @agg_window.setter
    def agg_window(self, value: str|tuple|None):
        if value is None:
            return

        if isinstance(value, tuple):
            size, unit = value
            size = int(size)
            unit = find_unit_of_time(unit)
        elif isinstance(value, str):
            size, unit = get_window_from_str(value)
        else:
            raise ValueError(f"Invalid value for agg_window: {value}")

        self._agg_window = (size, unit)

    @property
    def agg_range(self):
        if not hasattr(self, 'start') or not hasattr(self, 'end'):
            return None
        
        if not hasattr(self, '_agg_window') or self._agg_window is None:
            return TimeRange(self.start, self.end)
        
        return get_window(self.end, *self._agg_window)
    
    @agg_range.setter
    def agg_range(self, value: str):
        self.agg_window = value

    @abstractmethod
    def __add__(self, n: int):
        raise NotImplementedError
        
    def __sub__(self, n: int):
        return self + (-n)

    def set_year(self, year: int):
        """
        Change the year of the timestep. Returns a new timestep.
        """
        new_timestep = self.__class__(year, self.step)
        return new_timestep

    def get_history_timesteps(self, history:TimePeriod):
        """
        Returns a list of all the timesteps in the history period with the same .step as timestep
        """

        history_years = range(history.start.year, history.end.year + 1)
        all_timesteps = [self.set_year(year) for year in history_years]

        return [ts for ts in all_timesteps if ts.start >= history.start and ts.end <= history.end]
    
def estimate_timestep(sample) -> TimeStep:
        import numpy as np

        from .fixed_num_timestep import Year, Month, Dekad
        from .fixed_len_timestep import Day, Hour
        from .fixed_doy_timestep import ViirsModisTimeStep

        def mode(arr: list): return max(set(arr), key = arr.count)

        sample.sort()
        all_diff = [(sample[i+1] - sample[i]).days for i in range(len(sample)-1)]
        step_length = mode(all_diff)
        if np.isclose(step_length, 0):
            all_diff_seconds = [(sample[i+1] - sample[i]).seconds for i in range(len(sample)-1)]
            step_length = mode(all_diff_seconds)
            if np.isclose(step_length, 3600):
                return Hour
            else:
                return None
        if np.isclose(step_length, 1):
            return Day
        elif np.isclose(step_length, 8):
            return ViirsModisTimeStep
        elif np.isclose(step_length, 10):
            return Dekad
        elif 30 <= step_length <= 31:
            return Month
        elif 365 <= step_length <= 366:
            return Year
        else:
            return None