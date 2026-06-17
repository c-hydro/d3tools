"""Date parsing utilities for the timestepping module.

This module provides functionality to parse date strings into datetime objects,
supporting multiple common date formats and allowing flexible end-of-period handling.
"""

import datetime as dt

def get_date_from_str(str: str, format: None|str = None, end = False) -> dt.datetime:
    """Parse a string into a datetime object, trying multiple common formats.

    This function attempts to parse a date string using a variety of common formats.
    If no specific format is provided, it tries formats like ISO 8601, European
    date formats, and others. The function also supports end-of-period adjustment,
    which sets the time to the last second/minute/hour if those components are not
    specified in the input string.

    Args:
        str (str): The date string to parse.
        format (None|str, optional): Specific strptime format to use. If not provided,
            the function tries multiple common formats. Defaults to None.
        end (bool, optional): If True, sets time components to their maximum values
            (23:59:59) if not explicitly specified in the input string. This is useful
            for defining time period end points. Defaults to False.

    Returns:
        datetime.datetime: The parsed datetime object.

    Raises:
        ValueError: If the string cannot be parsed as a date using any of the
            attempted formats.

    Examples:
        >>> get_date_from_str('2024-02-20')
        datetime.datetime(2024, 2, 20, 0, 0)
        >>> get_date_from_str('20240220')
        datetime.datetime(2024, 2, 20, 0, 0)
        >>> get_date_from_str('20/02/2024', end=True)
        datetime.datetime(2024, 2, 20, 23, 59, 59)
        >>> get_date_from_str('2024-02-20 13:45')
        datetime.datetime(2024, 2, 20, 13, 45)
        >>> get_date_from_str('20/02/2024', format='%d/%m/%Y')
        datetime.datetime(2024, 2, 20, 0, 0)

    Note:
        Supported date formats include:
        - '%Y-%m-%d' (ISO 8601)
        - '%Y%m%d' (compact format)
        - '%d/%m/%Y' (European format)
        - '%d-%m-%Y', '%d.%m.%Y' (European variants)
        - '%d %b %Y', '%d %B %Y' (with month names)
        - '%Y %b %d', '%Y %B %d' (ISO-like with month names)
        - '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H' (with time)
    """
    _date_formats = ['%Y-%m-%d', '%Y%m%d',   '%d/%m/%Y', '%d-%m-%Y',
                     '%d.%m.%Y', '%d %b %Y', '%d %B %Y', '%Y %b %d', '%Y %B %d',
                     '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H']
    if format:
        date = dt.datetime.strptime(str, format)
    else:
        for date_format in _date_formats:
            try:
                date = dt.datetime.strptime(str, date_format)
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
