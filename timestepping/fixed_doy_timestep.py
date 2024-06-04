import datetime
from abc import ABC

from .fixed_num_timestep import FixedNTimeStep

class FixedDOYTimeStep(FixedNTimeStep, ABC):
    """
    A class for timestep of fixed DOYs. Currently only used for VIIRS-MODIS timesteping.
    """
    def __init__(self, year: int, step: int, start_doys: list[int]):
        self.start_doys = start_doys
        super().__init__(year, step, len(start_doys))

class ViirsModisTimeStep(FixedDOYTimeStep):
    """
    A class for timestep of VIIRS and MODIS data.
    VIIRS and MODIS come in 8-day periods (except for the last period of the year, which is shorter) starting at fixed DOYs.
    """

    start_doys = list(range(1, 366, 8))

    def __init__(self, year: int, step_of_year: int):
        super().__init__(year, step_of_year, ViirsModisTimeStep.start_doys)

    @staticmethod
    def get_step_from_date(date: datetime.datetime):
        """
        Returns the step number for a given date.
        """
        doy = date.timetuple().tm_yday
        for i, start_doy in enumerate(ViirsModisTimeStep.start_doys):
            if doy < start_doy:
                return i
        else:
            return len(ViirsModisTimeStep.start_doys)

    def get_start(self):
        start_doy = ViirsModisTimeStep.start_doys[self.step_of_year - 1]
        return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=start_doy - 1)
    
    def get_end(self):
        if self.step_of_year < len(ViirsModisTimeStep.start_doys):
            end_doy = ViirsModisTimeStep.start_doys[self.step_of_year] - 1
            return datetime.datetime(self.year, 1, 1) + datetime.timedelta(days=end_doy - 1)
        else:
            return datetime.datetime(self.year, 12, 31)
        
    @property
    def step_of_year(self):
        return self.step