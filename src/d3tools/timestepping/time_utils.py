import datetime as dt
from dateutil.relativedelta import relativedelta
import warnings

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from .timeperiods import TimeRange

# backward compatibility, this function used to be in this module
from .time_parsing import get_date_from_str

def get_window(time: dt.datetime, size: int, unit: str, start = False) -> 'TimeRange':
    """
    Construct a TimeRange window ending or starting at a given time.

    Args:
        time (datetime.datetime): The reference time.
        size (int): The window size.
        unit (str): The unit ('d', 'm', 'y', 'w', 't', 'h').
        start (bool, optional): If True, window starts at time; else ends at time.

    Returns:
        TimeRange: The constructed time window.

    Raises:
        ValueError: If the unit is not recognized.

    Example:
        >>> get_window(datetime.datetime(2024,2,20), 3, 'm')
        TimeRange(...)
    """
    from .timeperiods import Dekad, TimeRange

    # if size is a float, raise an error, because right now I cannot handle fractional sizes
    if isinstance(size, float):
        raise ValueError('Size must be an integer, fractional sizes are not supported yet')

    unit = find_unit_of_time(unit)

    if unit in ['m', 'y', 'd', 'w']:
        reldelta_unitmap = {'d': 'days', 'm': 'months', 'y': 'years', 'w': 'weeks'}
        reldelta_unit = reldelta_unitmap[unit]
        if start:
            time_start:dt.datetime = time
            time_end:dt.datetime = time + dt.timedelta(days=1) + relativedelta(**{reldelta_unit: size}) - dt.timedelta(days=2)
        else:
            time_start:dt.datetime = time + dt.timedelta(days=1) - relativedelta(**{reldelta_unit: size})
            time_end:dt.datetime = time
    elif unit == 't':
        time_dekad:Dekad = Dekad.from_date(time) # dekad of the given time
        if start:
            start_dekad:Dekad = time_dekad
            end_dekad:Dekad = start_dekad + size - 1
            if start_dekad.start.date() != time.date():
                warnings.warn('The given time does not correspond to the start of a dekad. The window will start at the beginning of the dekad.')
        else:
            end_dekad:Dekad = time_dekad
            start_dekad:Dekad = end_dekad - size + 1
            if end_dekad.end.date() != time.date():
                warnings.warn('The given time does not correspond to the end of a dekad. The window will end at the end of the dekad.')
        time_start:dt.datetime = start_dekad.start
        time_end:dt.datetime = end_dekad.end
    else:
        raise ValueError('Unit for aggregator not recognized: must be one of dekads, months, years, days, weeks')
    


    return TimeRange(time_start, time_end)

def get_md_dates(years: Iterable[int], month: int, day: int) -> list[dt.datetime]:
    """
    Generate a list of datetime objects for a given month and day across multiple years.
    Useful for generating dates for climatological means or other parameter calculations.

    Handles leap years for February 28/29.

    Args:
        years (Iterable[int]): Years to generate dates for.
        month (int): Month (1-12).
        day (int): Day of month.

    Returns:
        list[datetime.datetime]: List of datetime objects.

    Example:
        >>> get_md_dates([2020, 2021], 2, 29)
        [datetime.datetime(2020, 2, 29, 0, 0), datetime.datetime(2021, 2, 28, 0, 0)]
        >>> get_md_dates(range(2020, 2023), 3, 15)
        [datetime.datetime(2020, 3, 15, 0, 0), datetime.datetime(2021, 3, 15, 0, 0), datetime.datetime(2022, 3, 15, 0, 0)]
    """
    from .timeperiods import Year
    if month == 2 and day in [28, 29]:
        leaps = [year for year in years if Year(year).is_leap()]
        nonleaps = [year for year in years if not Year(year).is_leap()]
        dates = [dt.datetime(year, 2, 29) for year in leaps] + [dt.datetime(year, 2, 28) for year in nonleaps]
    else:
        dates = [dt.datetime(year, month, day) for year in years]
    
    return sorted(dates)

UNIT_CONVERSIONS = {
    'h' : {'d': 24,   'w': 168, 'v' : 192},
    'd' : {'h': 1/24, 'w': 7, 'v' : 8},
    'w' : {'h': 1/168,'d': 1/7},
    'v' : {'h': 1/192,'d': 1/8},
    't' : {'m': 3, 'y' : 36},
    'm' : {'t': 1/3, 'y': 12},
    'y' : {'t': 1/36, 'm': 1/12}
}

def unit_is_multiple(unit1: str, unit2: str) -> bool:

    """
    Determine if unit1 is a multiple of unit2 (e.g., 'd' is a multiple of 'm').

    Args:
        unit1 (str): The unit to check (e.g., 'd', 'm', 'y').
        unit2 (str): The reference unit.

    Returns:
        bool: True if unit1 is a multiple of unit2, False otherwise.

    Example:
        >>> unit_is_multiple('d', 'm')
        True
        >>> unit_is_multiple('m', 'd')
        False
    """

    unit1 = find_unit_of_time(unit1)
    unit2 = find_unit_of_time(unit2)

    # if the units are the same, they are multiples of each other
    if unit1 == unit2:
        return True
    # if their conversion factor is an integer, they are multiples of each other
    elif unit1 in UNIT_CONVERSIONS[unit2]:
        return  UNIT_CONVERSIONS[unit2][unit1] == int(UNIT_CONVERSIONS[unit2][unit1])
    # months and years are multiples of days (and hours) even if the conversion factor is not an integer
    elif unit2 in ['d', 'h']:
        return unit1 in ['m', 'y']
    # in all other cases, they are not multiples of each other
    else:
        return False
    
def find_unit_of_time(unit: str|None = None, *, timesteps_per_year: int|None = None) -> str:
    """
    Parse a string or integer into a canonical time unit code.

    Args:
        unit (str, optional): The unit string (e.g., 'daily', 'm', 'dekads').
        timesteps_per_year (int, optional): If unit is None, infer from this value.

    Returns:
        str: One of 'd' (days), 'm' (months), 'y' (years), 't' (dekads),
             'v' (8-day/VIIRS), 'h' (hours), 'w' (weeks).

    Raises:
        ValueError: If the unit cannot be recognized.

    Example:
        >>> find_unit_of_time('daily')
        'd'
        >>> find_unit_of_time(timesteps_per_year=36)
        't'
    """

    ts_unit_map = {365:'d', 36: 't', 12: 'm', 1: 'y'}

    if unit is None:
        if timesteps_per_year is not None:
            return ts_unit_map[timesteps_per_year]
        else:
            raise ValueError('Either unit or timesteps_per_year must be given')
    
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
