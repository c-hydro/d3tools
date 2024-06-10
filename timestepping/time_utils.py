import datetime
from dateutil.relativedelta import relativedelta
import warnings
from typing import Iterable

def get_date_from_str(str: str, format: None|str = None) -> datetime.datetime:
    """
    Returns a datetime object from a string.
    """
    _date_formats = ['%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%d %b %Y', '%d %B %Y', '%Y %b %d', '%Y %B %d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H']
    if format:
        return datetime.datetime.strptime(str, format)

    for date_format in _date_formats:
        try:
            return datetime.datetime.strptime(str, date_format)
        except ValueError:
            pass
    else:
        raise ValueError(f'Cannot parse date string "{str}"')

def get_window(time: datetime.datetime, size: int, unit: str, start = False) -> 'TimeRange':
        """
        Returns a TimeRange object that represents a window of time ending (start == False) or starting (start == True) at the given time.
        The size is given in the unit specified.
        Units can be 'months', 'years', 'days', 'weeks', 'dekads'.
        """
        from .fixed_num_timestep import Dekad
        from .timerange import TimeRange

        if unit[-1] != 's': unit += 's'
        if unit in ['months', 'years', 'days', 'weeks']:
            if start:
                time_start:datetime.datetime = time
                time_end:datetime.datetime = time + datetime.timedelta(days=1) + relativedelta(**{unit: size}) - datetime.timedelta(days=2)
            else:
                time_start:datetime.datetime = time + datetime.timedelta(days=1) - relativedelta(**{unit: size})
                time_end:datetime.datetime = time
        elif unit == 'dekads':
            time_dekad:Dekad = Dekad.from_date(time) # dekad of the given time
            if start:
                start_dekad:Dekad = time_dekad
                end_dekad:Dekad = start_dekad + size - 1
                if start_dekad.start != time:
                    warnings.warn('The given time does not correspond to the start of a dekad. The window will start at the beginning of the dekad.')
            else:
                end_dekad:Dekad = time_dekad
                start_dekad:Dekad = end_dekad - size + 1
                if end_dekad.end != time:
                    warnings.warn('The given time does not correspond to the end of a dekad. The window will end at the end of the dekad.')
            time_start:datetime.datetime = start_dekad.start
            time_end:datetime.datetime = end_dekad.end
        else:
            raise ValueError('Unit for aggregator not recognized: must be one of dekads, months, years, days, weeks')
        return TimeRange(time_start, time_end)

def get_md_dates(years: Iterable[int], month: int, day: int) -> list[datetime.datetime]:
    from .fixed_num_timestep import Year
    if month == 2 and day in [28, 29]:
        leaps = [year for year in years if Year(year).is_leap()]
        nonleaps = [year for year in years if not Year(year).is_leap()]
        return [datetime.datetime(year, 2, 29) for year in leaps] + [datetime.datetime(year, 2, 28) for year in nonleaps]
    else:
        return [datetime.datetime(year, month, day) for year in years]