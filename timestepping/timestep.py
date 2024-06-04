from abc import ABC, abstractmethod

from .timeperiod import TimePeriod

class TimeStep(TimePeriod, ABC):
    """
    A timestep is a TimePeriod that supports addition and subtraction of integers.
    (i.e. there is previous and next timesteps)
    """
    
    @abstractmethod
    def __add__(self, n: int):
        raise NotImplementedError
        
    def __sub__(self, n: int):
        return self + (-n)

