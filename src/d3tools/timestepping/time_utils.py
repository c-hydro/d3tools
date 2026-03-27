"""Utility functions for time manipulation and unit conversions.

This module provides various utilities for working with time periods, including:
- Constructing time windows and ranges
- Generating date lists for climatological calculations
- Converting and comparing time units (days, months, years, dekads, etc.)
- Parsing time unit strings into canonical forms

The module supports various time units:
- 'd': days
- 'h': hours
- 'w': weeks
- 'm': months
- 'y': years
- 't': dekads (10-day periods, 36 per year)
- 'v': VIIRS/MODIS periods (8-day periods, 46 per year)
"""

import datetime as dt
from dateutil.relativedelta import relativedelta
import warnings

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from .timeperiods import TimeRange

# backward compatibility, this function used to be in this module
from .time_parsing import get_date_from_str

def get_window(time: dt.datetime, size: int, unit: str, start = False) -> 'TimeRange':
    """Construct a TimeRange window ending or starting at a given time.

    This function creates a time window (TimeRange) of a specified size and unit,
    either ending at or starting from a given reference time. This is useful for
    defining rolling windows, lookback periods, or forward-looking periods in
    time series analysis.

    Args:
        time (datetime.datetime): The reference time point. By default, the window
            ends at this time (inclusive). If start=True, the window begins at this time.
        size (int): The size of the window in the specified units. Must be an integer;
            fractional sizes are not currently supported.
        unit (str): The time unit for the window size. Can be:
            - 'd' or 'days': days
            - 'm' or 'months': months
            - 'y' or 'years': years
            - 'w' or 'weeks': weeks
            - 't' or 'dekads': dekads (10-day periods)
            - 'h' or 'hours': hours
        start (bool, optional): If False (default), the window ends at `time`.
            If True, the window starts at `time`. Defaults to False.

    Returns:
        TimeRange: A TimeRange object representing the constructed time window,
            with start and end datetime attributes.

    Raises:
        ValueError: If the unit is not recognized, or if size is a float (fractional
            sizes are not yet supported).

    Examples:
        >>> # A 3-month window ending on Feb 20, 2024
        >>> window = get_window(datetime.datetime(2024, 2, 20), 3, 'm')
        >>> # window spans from Nov 21, 2023 to Feb 20, 2024
        
        >>> # A 2-dekad window starting on Jan 1, 2024
        >>> window = get_window(datetime.datetime(2024, 1, 1), 2, 't', start=True)
        >>> # window spans from Jan 1 to Jan 20, 2024
        
        >>> # A 7-day window ending on a specific date
        >>> window = get_window(datetime.datetime(2024, 2, 20), 7, 'd')

    Warnings:
        For dekad units ('t'), if the given time doesn't align with dekad boundaries,
        a warning is issued and the window is adjusted to align with dekad periods.

    Note:
        - For dekad units, the function ensures the window aligns with natural
          dekad boundaries (1st-10th, 11th-20th, 21st-end of month).
        - Month and year calculations account for variable month lengths.
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
    """Generate datetime objects for a specific month-day combination across multiple years.

    This function is particularly useful for generating dates for climatological means,
    calculating multi-year statistics, or creating reference date lists. It intelligently
    handles leap years for February 28/29, substituting February 28 in non-leap years
    when February 29 is requested.

    Args:
        years (Iterable[int]): An iterable of year values (e.g., list, range) for which
            to generate dates.
        month (int): The month number (1-12).
        day (int): The day of the month (1-31).

    Returns:
        list[datetime.datetime]: A sorted list of datetime objects (at 00:00:00),
            one for each year in the input. For Feb 29 in non-leap years, Feb 28 is
            substituted.

    Examples:
        >>> # Get Feb 29 for leap and non-leap years
        >>> get_md_dates([2020, 2021, 2024], 2, 29)
        [datetime.datetime(2020, 2, 29, 0, 0),
         datetime.datetime(2021, 2, 28, 0, 0),
         datetime.datetime(2024, 2, 29, 0, 0)]
        
        >>> # Get March 15 for a range of years
        >>> get_md_dates(range(2020, 2023), 3, 15)
        [datetime.datetime(2020, 3, 15, 0, 0),
         datetime.datetime(2021, 3, 15, 0, 0),
         datetime.datetime(2022, 3, 15, 0, 0)]
        
        >>> # Works with any iterable
        >>> get_md_dates([2020, 2022, 2024], 1, 1)
        [datetime.datetime(2020, 1, 1, 0, 0),
         datetime.datetime(2022, 1, 1, 0, 0),
         datetime.datetime(2024, 1, 1, 0, 0)]

    Note:
        - All returned datetime objects have time set to midnight (00:00:00).
        - The returned list is always sorted chronologically.
        - Special handling only applies to February 28 and 29.
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
    """Determine if unit1 can be expressed as an integer multiple of unit2.

    This function checks whether one time unit is a clean multiple of another,
    which is useful for determining compatibility in aggregation operations and
    time period calculations. For example, days are multiples of months (even
    though the conversion factor varies), but weeks are not multiples of months.

    Args:
        unit1 (str): The unit to check (will be parsed through find_unit_of_time).
            Examples: 'd', 'days', 'daily', 'm', 'months', 'y', 'years', etc.
        unit2 (str): The reference unit (also parsed through find_unit_of_time).

    Returns:
        bool: True if unit1 is a multiple of unit2, False otherwise.

    Examples:
        >>> unit_is_multiple('m', 'd')  # months are multiples of days
        True
        >>> unit_is_multiple('d', 'm')  # days are not multiples of months
        False
        >>> unit_is_multiple('d', 'd')  # same units are multiples
        True
        >>> unit_is_multiple('w', 'd')  # weeks are multiples of days (7)
        True
        >>> unit_is_multiple('y', 'm')  # years are multiples of months (12)
        True
        >>> unit_is_multiple('v', 'w')  # VIIRS/8-day not multiple of weeks
        False

    Note:
        - Units are first normalized using find_unit_of_time().
        - Months and years are considered multiples of days and hours even though
          the conversion factor varies (e.g., months have 28-31 days).
        - The relationship is directional: unit_is_multiple('d', 'w') != unit_is_multiple('w', 'd').
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
    """Parse and normalize a time unit string into a canonical single-character code.

    This function provides flexible parsing of various time unit representations,
    allowing for different naming conventions and formats. It can parse explicit
    unit strings or infer the unit from the number of timesteps per year.

    Args:
        unit (str|None, optional): The unit string to parse. Can be:
            - A single character code: 'd', 'm', 'y', 't', 'v', 'h', 'w'
            - A full word: 'daily', 'days', 'monthly', 'months', etc.
            - Variants: 'dekads', 'dekadly', 'yearly', 'annual', etc.
            - Composite forms: '10d' for dekads, '8d' for VIIRS
            Non-alphanumeric characters are ignored during parsing.
        timesteps_per_year (int|None, optional): If unit is None, infer the unit
            from this value. Supported values:
            - 365: daily ('d')
            - 36: dekadly ('t')
            - 12: monthly ('m')
            - 1: yearly ('y')

    Returns:
        str: A single-character canonical unit code:
            - 'd': days
            - 'h': hours
            - 'w': weeks
            - 'm': months
            - 'y': years
            - 't': dekads (10-day periods, 36 per year)
            - 'v': VIIRS/MODIS periods (8-day periods)

    Raises:
        ValueError: If neither unit nor timesteps_per_year is provided, or if
            the unit cannot be recognized.

    Examples:
        >>> find_unit_of_time('daily')
        'd'
        >>> find_unit_of_time('months')
        'm'
        >>> find_unit_of_time(timesteps_per_year=36)
        't'
        >>> find_unit_of_time('10-days')  # Interpreted as 10 days, not dekad
        'd'
        >>> find_unit_of_time('dekads')
        't'
        >>> find_unit_of_time('8d')
        'v'
        >>> find_unit_of_time('annual')
        'y'

    Note:
        - The function is case-insensitive and ignores non-alphanumeric characters.
        - Both 'a' and 'y' maps to yearly ('y').
        - 'annual', 'annually', 'yearly', 'year', 'years' all map to 'y'.
        - Special composite forms like '10d' and '8d' are recognized.
    """

    ts_unit_map = {365:'d', 36: 't', 12: 'm', 1: 'y'}

    if unit is None:
        if timesteps_per_year is not None:
            return ts_unit_map[timesteps_per_year]
        else:
            raise ValueError('Either unit or timesteps_per_year must be given')
    
    # remove all non-alphanumeric characters and convert to lowercase
    unit = ''.join([c.lower() for c in unit if c.isalnum()])

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
