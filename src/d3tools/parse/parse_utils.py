"""Legacy compatibility parsing utilities.

This module keeps stable public function names that are still widely imported
across the codebase. New implementations live in focused modules
(``string_rendering``, ``config_resolution``, ``key_parser``), while these
helpers either delegate to those modules or provide generic utility routines.
"""

import datetime as dt
from typing import Any, Optional

from .config_resolution import set_env as _set_env_impl
from .config_resolution import set_dataset as _set_dataset_impl
from .string_rendering import substitute_values as _substitute_values_impl
from .string_rendering import substitute_string as _substitute_string_impl

def extract_date_and_tags(string: str, string_pattern: str):
    """Parse a key string into datetime and tags using ``KeyParser``.

    Args:
        string: Concrete key/path value to parse.
        string_pattern: Key pattern template used for matching.

    Returns:
        A tuple ``(parsed_datetime, parsed_tags_dict)``.
    """
    # Local import avoids circular dependency:
    # key_parser -> string_rendering -> parse_utils.
    from .key_parser import KeyParser

    parsed = KeyParser(string_pattern).match(string)
    return parsed.time, parsed.tags

def set_env(structure):
    """Compatibility wrapper for :func:`d3tools.parse.config_resolution.set_env`.

    Kept for backward compatibility with existing imports from ``parse_utils``.
    """
    return _set_env_impl(structure)

def set_dataset(structure, obj_dict):
    """Compatibility wrapper for :func:`d3tools.parse.config_resolution.set_dataset`.

    Kept for backward compatibility with existing imports from ``parse_utils``.
    """
    return _set_dataset_impl(structure, obj_dict)

def substitute_values(structure, tag_dict, **kwargs):
    """Compatibility wrapper for :func:`d3tools.parse.string_rendering.substitute_values`.

    Kept for backward compatibility with existing imports from ``parse_utils``.
    """
    return _substitute_values_impl(structure, tag_dict, **kwargs)

def substitute_string(string, tag_dict, rec=False):
    """Compatibility wrapper for :func:`d3tools.parse.string_rendering.substitute_string`.

    Kept for backward compatibility with existing imports from ``parse_utils``.
    """
    return _substitute_string_impl(string, tag_dict, rec=rec)

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
                    flat_dict[key] = [flat_dict[key], value]
                else:
                    flat_dict[key].append(value)
        else:
            flat_dict[key] = value
        
    return flat_dict

def make_hashable(obj: dict|list|Any) -> tuple|Any:
    """
    Convert a nested dictionary or list (including subclasses like Options) to a hashable object (tuple).

    Useful for deduplication and storing complex structures in sets.

    Args:
        obj (dict, list, or subclass, or other): The object to convert.

    Returns:
        tuple or original object: Hashable representation. If the input is a custom dict/list subclass (e.g., Options),
        the class type is preserved and will be restored by transform_back.

    Example:
        >>> make_hashable({'a': [1, 2]})
        (<class 'dict'>, ('a', (<class 'list'>, 1, 2)))
        >>> make_hashable(Options({'a': [1, 2]}))
        (<class 'Options'>, ('a', (<class 'list'>, 1, 2)))
    """
    if isinstance(obj, dict):
        return (obj.__class__,) + tuple((k, make_hashable(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return (obj.__class__,) +  tuple(make_hashable(v) for v in obj)
    else:
        return obj

def transform_back(obj: tuple) -> dict|list|Any:
    """
    Transform a hashable tuple (created by make_hashable) back to its original form.

    If the tuple was created from a custom dict/list subclass (e.g., Options),
    the returned object will be of the same subclass type.

    Args:
        obj (tuple): Hashable object.

    Returns:
        dict, list, custom subclass, or original value: Original structure, preserving custom types.

    Example:
        >>> transform_back((dict, ('a', (list, 1, 2))))
        {'a': [1, 2]}
        >>> transform_back((Options, ('a', (list, 1, 2))))
        Options({'a': [1, 2]})
    """
    if type(obj[0]) == type:
        if issubclass(obj[0], dict):
            return obj[0]({k: transform_back(v) if isinstance(v, tuple) else v for k, v in obj[1:]})
        elif issubclass(obj[0], list):
            return obj[0]([transform_back(v) if isinstance(v, tuple) else v for v in obj[1:]])
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
