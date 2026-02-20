import datetime as dt
from typing import Any

def flatten_dict(nested_dict:dict, sep:str = '.', parent_key:str = '') -> dict:
    """
    Flatten a nested dictionary into a single-level dictionary.

    Each nested key is combined with its parent keys, separated by `sep`.
    All combinations of parent keys are included.

    Args:
        nested_dict (dict): The dictionary to flatten.
        sep (str): Separator for combined keys. Default is '.'.
        parent_key (str): Used internally for recursion.

    Returns:
        dict: Flattened dictionary with combined keys.

    Example:
        >>> flatten_dict({'a': {'b': 1, 'c': 2}})
        {'a.b': 1, 'b': 1, 'a.c': 2, 'c': 2}
    """
    items = []
    for k, v in nested_dict.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, sep, new_key).items())
            # Include the current key without parent prefix for combinations
            items.extend(flatten_dict(v, sep=sep).items())
        else:
            items.append((new_key, v))

    flat_dict = {}
    for key, value in items:
        if key in flat_dict:
            if flat_dict[key] != value:
                if not isinstance(flat_dict[key], list):
                    flat_dict[key] = [flat_dict[key]].append(value)
                else:
                    flat_dict[key].append(value)
        else:
            flat_dict[key] = value
        
    return flat_dict

def make_hashable(obj: dict|list|Any) -> tuple|Any:
    """
    Convert a nested dictionary or list to a hashable object (tuple).

    Useful for deduplication and storing complex structures in sets.

    Args:
        obj (dict, list, or other): The object to convert.

    Returns:
        tuple or original object: Hashable representation.

    Example:
        >>> make_hashable({'a': [1, 2]})
        ('dict', ('a', ('list', 1, 2)))
    """
    if isinstance(obj, dict):
        return ('dict',) + tuple((k, make_hashable(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return ('list',) +  tuple(make_hashable(v) for v in obj)
    else:
        return obj

def transform_back(obj: tuple) -> dict|list|Any:
    """
    Transform a hashable tuple (created by make_hashable) back to its original form.

    Args:
        obj (tuple): Hashable object.

    Returns:
        dict, list, or original value: Original structure.

    Example:
        >>> transform_back(('dict', ('a', ('list', 1, 2))))
        {'a': [1, 2]}
    """
    if obj[0] == 'dict':
        return {k: transform_back(v) if isinstance(v, tuple) else v for k, v in obj[1:]}
    elif obj[0] == 'list':
        return [transform_back(v) if isinstance(v, tuple) else v for v in obj[1:]]
    else:
        return obj

def get_unique_values(values: list) -> list:
    """
    Deduplicate a list of values, handling nested dicts/lists.

    Args:
        values (list): List of values (can be dicts/lists).

    Returns:
        list: List of unique values.

    Example:
        >>> get_unique_values([{'a': 1}, {'a': 1}, {'a': 2}])
        [{'a': 1}, {'a': 2}]
    """
    unique_values = set()
    for value in values:
        unique_values.add(make_hashable(value))
    return [transform_back(value) if isinstance(value, tuple) else value for value in unique_values]

def format_dict(dict: dict) -> str:
    """
    Format a dictionary as a readable string for printing.

    Floats are formatted to two decimals, datetimes to YYYY-MM-DD.

    Args:
        dict (dict): Dictionary to format.

    Returns:
        str: Formatted string.

    Example:
        >>> format_dict({'a': 1.234, 'b': datetime.datetime(2024,2,20)})
        'a=1.23, b=2024-02-20'
    """
    str_list = []
    for key, value in dict.items():
        if type(value) == float:
            str_list.append(f'{key}={value:.2f}')
        elif type(value) == dt.datetime:
            str_list.append(f'{key}={value:%Y-%m-%d}')
        else:
            str_list.append(f'{key}={value}')
    return ', '.join(str_list)