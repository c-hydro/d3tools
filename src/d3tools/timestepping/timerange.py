import datetime
from typing import Generator, Iterable, TYPE_CHECKING

from .timeperiod import TimePeriod
from .time_utils import find_unit_of_time
from .timewindow import TimeWindow

if TYPE_CHECKING:
    from .fixed_num_timestep import Year, Month, Dekad, FixedNTimeStep
    from .fixed_len_timestep import Day, Hour, FixedLenTimeStep
    from .fixed_doy_timestep import FixedDOYTimeStep

class TimeRange(TimePeriod):
    """
    A TimeRange is a TimePeriod that can be divided into timesteps.
    """

    @property
    def months(self) -> list['Month']:
        return self.get_timesteps_from_tsnumber(12)

    @property
    def years(self) -> list['Year']:
        return self.get_timesteps_from_tsnumber(1)

    @property
    def dekads(self) -> list['Dekad']:
        return self.get_timesteps_from_tsnumber(36)

    @property
    def days(self) -> list['Day']:
        return self.get_timesteps_from_tsnumber(365)
    
    @property
    def hours(self) -> list['Hour']:
        return self.get_timesteps_from_tsnumber(365*24)

    @property
    def viirstimes(self) -> list:
        return self.get_timesteps_from_DOY(range(1, 366, 8))

    def extend(self, window: TimeWindow, before = False):
        extended_period = super().extend(window, before)
        return TimeRange(extended_period.start, extended_period.end)
    
    def get_timesteps(self, freq: str|int, agg: str|tuple|None = None) -> list:

        if isinstance(freq, int):
            tss = self.get_timesteps_from_tsnumber(freq)
        elif isinstance(freq, str):
            freq = find_unit_of_time(freq.lower())
            if freq == 'd': tss = self.days
            elif freq == 't': tss = self.dekads
            elif freq == 'm': tss = self.months
            elif freq == 'y': tss = self.years
            elif freq == 'v': tss = self.viirstimes
            elif freq == 'h': tss = self.hours
            else:
                raise ValueError(f'Frequency {freq} not supported')
        else:
            raise TypeError(f'Frequency must be an int or str, not {type(freq)}')
        
        if agg is not None:
            for ts in tss:
                ts.agg_window = agg
        
        return tss

    def get_timesteps_like(self, timestep: 'TimeStep') -> list['TimeStep']:
        return self.get_timesteps(timestep.unit, timestep.agg_window)

    def gen_timesteps_from_tsnumber(self,
                                    timesteps_per_year: int,
                                    agg: str = None) -> Generator['FixedNTimeStep|FixedLenTimeStep', None, None]:
        """
        This will yield the timesteps on a regular frequency by the number of timesteps per year.
        timesteps_per_year is expressed as an integer indicating the number of times per year
        Allows daily (365), dekadly (36), monthly (12) and yearly data (1).
        """
        from .fixed_len_timestep import FixedLenTimeStep
        from .fixed_num_timestep import FixedNTimeStep

        # get the first timestep
        if timesteps_per_year == 365:
            ts:FixedLenTimeStep = FixedLenTimeStep.from_date(self.start, length = 1)
        elif timesteps_per_year == 365*24:
            ts:FixedLenTimeStep = FixedLenTimeStep.from_date(self.start, length = 1/24)
        else:
            ts:FixedNTimeStep = FixedNTimeStep.from_date(self.start, timesteps_per_year)

        if agg is not None: ts.agg_window = agg
        while ts.start <= self.end:
            yield ts
            ts = ts + 1

    def get_timesteps_from_tsnumber(self, timesteps_per_year: int, agg: str = None) -> list['FixedNTimeStep']:
        return list(self.gen_timesteps_from_tsnumber(timesteps_per_year, agg))
    
    def gen_timesteps_from_DOY(self, doy_list: Iterable[int], agg: str = None) -> Generator['FixedDOYTimeStep', None, None]:
        """
        This will yield the timesteps based on a given list of days of the year.
        This is useful for MODIS and VIIRS data that are available at preset DOYs.
        """
        from .fixed_doy_timestep import FixedDOYTimeStep

        ts:FixedDOYTimeStep = FixedDOYTimeStep.from_date(self.start, doy_list)
        if agg is not None: ts.agg_window = agg
        while ts.start <= self.end:
            yield ts
            ts = ts + 1

    def get_timesteps_from_DOY(self, doy_list: list[int], agg: str = None) -> list['FixedLenTimeStep']:
        return list(self.gen_timesteps_from_DOY(doy_list, agg))

    def gen_timesteps_from_issue_hour(self, issue_hours: list[int]) -> Generator['FixedLenTimeStep', None, None]:
        """
        This will yield the timesteps to of product issued daily at given hours
        """
        from .fixed_len_timestep import FixedLenTimeStep
        now = self.start
        while now <= self.end:
            for issue_hour in issue_hours:
                issue_time = datetime.datetime(now.year, now.month, now.day, issue_hour)
                if now <= issue_time <= self.end + datetime.timedelta(hours=23, minutes=59):
                    now = issue_time
                    yield FixedLenTimeStep.from_date(now, 1/24)
            day_after = now + datetime.timedelta(days=1)
            now = datetime.datetime(day_after.year, day_after.month, day_after.day)

    def get_timesteps_from_issue_hour(self, issue_hours: list[int]) -> list['FixedLenTimeStep']:
        return list(self.gen_timesteps_from_issue_hour(issue_hours))
    
