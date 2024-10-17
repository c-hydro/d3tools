from abc import ABC, abstractmethod
import datetime as dt

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