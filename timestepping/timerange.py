import datetime
from typing import Generator, Iterable

from .timeperiod import TimePeriod
from .fixed_num_timestep import FixedNTimeStep, Year, Month, Dekad
from .fixed_doy_timestep import FixedDOYTimeStep
from .fixed_len_timestep import FixedLenTimeStep, Day, Hour

class TimeRange(TimePeriod):
    """
    A TimeRange is a TimePeriod that can be divided into timesteps.
    """

    @property
    def months(self) -> list[Month]:
        return self.get_timesteps_from_tsnumber(12)

    @property
    def years(self) -> list[Year]:
        return self.get_timesteps_from_tsnumber(1)

    @property
    def dekads(self) -> list[Dekad]:
        return self.get_timesteps_from_tsnumber(36)

    @property
    def days(self) -> list[Day]:
        return self.get_timesteps_from_tsnumber(365)
    
    @property
    def hours(self) -> list[Hour]:
        return self.get_timesteps_from_tsnumber(365*24)

    @property
    def viirstimes(self) -> list:
        return self.get_timesteps_from_DOY(range(1, 366, 8))
        
    def gen_timesteps_from_tsnumber(self, timesteps_per_year: int) -> Generator[FixedNTimeStep|FixedLenTimeStep, None, None]:
        """
        This will yield the timesteps on a regular frequency by the number of timesteps per year.
        timesteps_per_year is expressed as an integer indicating the number of times per year
        Allows daily (365), dekadly (36), monthly (12) and yearly data (1).
        """

        # get the first timestep
        if timesteps_per_year == 365:
            ts:FixedLenTimeStep = FixedLenTimeStep.from_date(self.start, length = 1)
        elif timesteps_per_year == 365*24:
            ts:FixedLenTimeStep = FixedLenTimeStep.from_date(self.start, length = 1/24)
        else:
            ts:FixedNTimeStep = FixedNTimeStep.from_date(self.start, timesteps_per_year)

        while ts.start <= self.end:
            yield ts
            ts = ts + 1

    def get_timesteps_from_tsnumber(self, timesteps_per_year: int) -> list[FixedNTimeStep]:
        return list(self.gen_timesteps_from_tsnumber(timesteps_per_year))
    
    def gen_timesteps_from_DOY(self, doy_list: Iterable[int]) -> Generator[FixedDOYTimeStep, None, None]:
        """
        This will yield the timesteps based on a given list of days of the year.
        This is useful for MODIS and VIIRS data that are available at preset DOYs.
        """

        ts:FixedDOYTimeStep = FixedDOYTimeStep.from_date(self.start, doy_list)
        while ts.start <= self.end:
            yield ts
            ts = ts + 1

    def get_timesteps_from_DOY(self, doy_list: list[int]) -> list[FixedLenTimeStep]:
        return list(self.gen_timesteps_from_DOY(doy_list))

    def gen_timesteps_from_issue_hour(self, issue_hours: list[int]) -> Generator[FixedLenTimeStep, None, None]:
        """
        This will yield the timesteps to of product issued daily at given hours
        """
        now = self.start
        while now <= self.end:
            for issue_hour in issue_hours:
                issue_time = datetime.datetime(now.year, now.month, now.day, issue_hour)
                if now <= issue_time <= self.end + datetime.timedelta(hours=23, minutes=59):
                    now = issue_time
                    yield FixedLenTimeStep.from_date(now, 1/24)
            day_after = now + datetime.timedelta(days=1)
            now = datetime.datetime(day_after.year, day_after.month, day_after.day)

    def get_timesteps_from_issue_hour(self, issue_hours: list[int]) -> list[FixedLenTimeStep]:
        return list(self.gen_timesteps_from_issue_hour(issue_hours))
    
