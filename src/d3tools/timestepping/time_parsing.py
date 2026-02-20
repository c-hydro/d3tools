import datetime as dt

def get_date_from_str(str: str, format: None|str = None, end = False) -> dt.datetime:
    """
    Parse a string into a datetime object, trying multiple common formats.

    Args:
        str (str): The date string to parse.
        format (str, optional): Specific format to use. If not provided, tries common formats.
        end (bool, optional): If True, set time to end of day/hour/minute if not specified.

    Returns:
        datetime.datetime: The parsed datetime object.

    Raises:
        ValueError: If the string cannot be parsed as a date.

    Example:
        >>> get_date_from_str('2024-02-20')
        datetime.datetime(2024, 2, 20, 0, 0)
        >>> get_date_from_str('20240220')
        datetime.datetime(2024, 2, 20, 0, 0)
        >>> get_date_from_str('20/02/2024', end = True)
        datetime.datetime(2024, 2, 20, 23, 59, 59)
    """
    _date_formats = ['%Y-%m-%d', '%Y%m%d',   '%d/%m/%Y', '%d-%m-%Y',
                     '%d.%m.%Y', '%d %b %Y', '%d %B %Y', '%Y %b %d', '%Y %B %d',
                     '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H']
    if format:
        date = dt.datetime.strptime(str, format)

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
