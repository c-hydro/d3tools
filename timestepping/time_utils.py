import datetime

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
