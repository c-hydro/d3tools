from abc import ABC, abstractmethod

from .timerange import TimeRange, TimePeriod

class TimeStep(TimeRange, ABC):
    """
    A timestep is a TimePeriod that supports addition and subtraction of integers.
    (i.e. there is previous and next timesteps)
    """
    
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