from abc import ABC, ABCMeta, abstractmethod

from .timerange import TimeRange, TimePeriod
from .time_utils import find_unit_of_time
from .timewindow import TimeWindow

class TimeStepMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'unit' in attrs:
            cls.subclasses[attrs['unit']] = cls

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

    @classmethod
    def with_agg(cls, agg_window: str|tuple|None|TimeWindow):
        
        class AggTimeStep(cls):

            super_name = cls.__name__

            def __repr__(self):
                return f"{self.super_name} ({self.start:%Y%m%d} - {self.end:%Y%m%d}) agg = {self.agg_window}"

            def __add__(self, other):
                super_add = super().__add__(other)
                super_add.agg_window = self.agg_window
                return super_add

            pass

        if isinstance(agg_window, tuple):
            AggTimeStep._agg_window = TimeWindow(agg_window)
        elif isinstance(agg_window, str):
            AggTimeStep._agg_window = TimeWindow.from_str(agg_window)
        elif isinstance(agg_window, TimeWindow):
            AggTimeStep._agg_window = agg_window
        else:
            raise ValueError(f"Invalid value for agg_window: {agg_window}")

        AggTimeStep.agg_window = AggTimeStep._agg_window

        return AggTimeStep

    @property
    def agg_window(self):
        if hasattr(self, '_agg_window'):
            return self._agg_window
        return None

    @agg_window.setter
    def agg_window(self, value: str|tuple|None|TimeWindow):
        if value is None:
            return

        if isinstance(value, tuple):
            self._agg_window = TimeWindow(value)
        elif isinstance(value, str):
            self._agg_window = TimeWindow.from_str(value)
        elif isinstance(value, TimeWindow):
            self._agg_window = value
        else:
            raise ValueError(f"Invalid value for agg_window: {value}")

    @property
    def agg_range(self):
        if not hasattr(self, 'start') or not hasattr(self, 'end'):
            return None
        
        if not hasattr(self, '_agg_window') or self._agg_window is None:
            return TimeRange(self.start, self.end)
        
        return self.agg_window.apply(self.end)
    
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

        if len(sample) < 2:
            return None

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
            if sample[-1].month == 2 and sample[-1].day == 28:
                return Dekad
            else:
                return ViirsModisTimeStep
        elif 9 <= step_length <= 11:
            return Dekad
        elif 30 <= step_length <= 31:
            return Month
        elif 365 <= step_length <= 366:
            return Year
        else:
            return None