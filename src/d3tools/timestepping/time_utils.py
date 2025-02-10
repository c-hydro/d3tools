import datetime
from dateutil.relativedelta import relativedelta
import warnings
from typing import Iterable

def get_date_from_str(str: str, format: None|str = None, end = False) -> datetime.datetime:
    """
    Returns a datetime object from a string.
    """
    _date_formats = ['%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%d %b %Y', '%d %B %Y', '%Y %b %d', '%Y %B %d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H']
    if format:
        date = datetime.datetime.strptime(str, format)

    for date_format in _date_formats:
        try:
            date = datetime.datetime.strptime(str, date_format)
            format = date_format
            break
        except ValueError:
            pass
    else:
        raise ValueError(f'Cannot parse date string "{str}"')
    
    if end:
        if '%S' not in format: date = date.replace(second = 59)
        if '%M' not in format: date = date.replace(minute = 59)
        if '%H' not in format: date = date.replace(hour = 23)

    return date

def get_window(time: datetime.datetime, size: int, unit: str, start = False) -> 'TimeRange':
        """
        Returns a TimeRange object that represents a window of time ending (start == False) or starting (start == True) at the given time.
        The size is given in the unit specified.
        Units can be 'months', 'years', 'days', 'weeks', 'dekads'.
        """
        from .fixed_num_timestep import Dekad
        from .timerange import TimeRange

        unit = find_unit_of_time(unit)
        
        if unit in ['m', 'y', 'd', 'w']:
            reldelta_unitmap = {'d': 'days', 'm': 'months', 'y': 'years', 'w': 'weeks'}
            reldelta_unit = reldelta_unitmap[unit]
            if start:
                time_start:datetime.datetime = time
                time_end:datetime.datetime = time + datetime.timedelta(days=1) + relativedelta(**{reldelta_unit: size}) - datetime.timedelta(days=2)
            else:
                time_start:datetime.datetime = time + datetime.timedelta(days=1) - relativedelta(**{reldelta_unit: size})
                time_end:datetime.datetime = time
        elif unit == 't':
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
    
def find_unit_of_time(unit: str) -> str:
    """
    Tries to interpret the string given as a unit of time.
    It will return one of the following:
    'd' for days,
    'm' for months,
    'y' for years,
    't' for dekads,
    'v' for 8-day periods like viirs,
    'h' for hours,
    'w' for weeks.
    """

    # remove all non-alphanumeric characters
    unit = ''.join([c for c in unit if c.isalnum()])

    if unit in ['d', 'days', 'day', 'daily']:
        return 'd'

    elif unit in ['t', 'dekads', 'dekad', 'dekadly'] or (unit.startswith('10') and find_unit_of_time(unit[2:]) == 'd'):
        return 't'

    elif unit in ['m', 'months', 'month', 'monthly']:
        return 'm'

    elif unit in ['y', 'years', 'year', 'yearly', 'a', 'annual', 'annually']:
        return 'y'

    elif unit in ['v', 'viirs', 'modis'] or (unit.startswith('8') and find_unit_of_time(unit[1:]) == 'd'):
        return 'v'

    elif unit in ['h', 'hours', 'hour', 'hourly']:
        return 'h'
    
    elif unit in ['w', 'weeks', 'week', 'weekly']:
        return 'w'
    
    else:
        raise ValueError(f'Unit {unit} not recognized')
