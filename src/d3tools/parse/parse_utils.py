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
        string: Concrete key or path value to parse.
        string_pattern: Pattern template containing datetime directives and/or
            tag placeholders.

    Returns:
        A tuple containing the parsed ``datetime`` and a dictionary of tag
        values.

    Raises:
        ValueError: If the key does not match the pattern or repeated tag
            placeholders contain conflicting values.
    """
    # Local import avoids circular dependency:
    # key_parser -> string_rendering -> parse_utils.
    from .key_parser import KeyParser

    parsed = KeyParser(string_pattern).match(string)
    return parsed.time, parsed.tags

def set_env(structure):
    """Compatibility wrapper for :func:`d3tools.parse.config_resolution.set_env`.

    Recursively resolves environment placeholders in strings, dictionary keys
    and values, and lists. This wrapper is retained for backward compatibility.

    Args:
        structure: Value or nested structure to resolve.

    Returns:
        The resolved value or structure.

    Raises:
        ValueError: If a referenced environment variable is unset and has no
            non-empty default.
    """
    return _set_env_impl(structure)

def set_dataset(structure, obj_dict):
    """Compatibility wrapper for :func:`d3tools.parse.config_resolution.set_dataset`.

    Recursively resolves dataset placeholders. This wrapper is retained for
    backward compatibility.

    Args:
        structure: Value or nested structure to resolve.
        obj_dict: Mapping of dataset keys to dataset-like objects.

    Returns:
        The resolved value or structure. A placeholder with tag assignments
        calls ``update(**tags)`` on its resolved object.

    Note:
        Placeholder matching starts at the beginning of a string; any suffix
        after the matched placeholder is not retained when it resolves.
    """
    return _set_dataset_impl(structure, obj_dict)

def substitute_values(structure, tag_dict, **kwargs):
    """Compatibility wrapper for :func:`d3tools.parse.string_rendering.substitute_values`.

    Recursively substitutes placeholders in nested structures. This wrapper
    is retained for backward compatibility.

    Args:
        structure: Value or nested structure to render.
        tag_dict: Mapping of placeholder names to values.
        **kwargs: Forwarded to :func:`d3tools.parse.string_rendering.substitute_values`.

    Returns:
        A rebuilt structure with substituted string leaves and dictionary
        keys. List-valued placeholders can expand a string leaf into a nested
        list.
    """
    return _substitute_values_impl(structure, tag_dict, **kwargs)

def substitute_string(string, tag_dict, rec=False):
    """Compatibility wrapper for :func:`d3tools.parse.string_rendering.substitute_string`.

    Renders placeholders in a string. List-valued tags expand into lists of
    rendered strings; the ``rec`` argument is retained for compatibility and
    does not disable recursive expansion.

    Args:
        string: Template string or value to render.
        tag_dict: Mapping of placeholder names to values.
        rec: Compatibility flag; list expansion remains recursive regardless
            of this value.

    Returns:
        The rendered value, possibly a nested list when list-valued tags are
        expanded.
    """
    return _substitute_string_impl(string, tag_dict, rec=rec)

def flatten_dict(nested_dict:dict, sep:str = '.', parent_key:str = '') -> dict:
    """Flatten nested dictionaries into a single-level dictionary.

    For nested entries, the result includes keys both with and without their
    ancestor prefixes. If different values produce the same flattened key,
    those values are accumulated in a list.

    Args:
        nested_dict: Dictionary to flatten.
        sep: Separator between nested key components. Defaults to ``'.'``.
        parent_key: Prefix used during recursive flattening; normally left
            empty by callers.

    Returns:
        A new dictionary containing flattened keys and their values. The input
        dictionary is not modified.

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
    """Encode dictionaries and lists as tuples for use in hash-based collections.

    Dictionary and list subclasses are recorded in the encoding so
    :func:`transform_back` can reconstruct their types. Other values, including
    tuples, are returned unchanged.

    Args:
        obj: Value to encode. Dictionaries and lists are traversed recursively.

    Returns:
        A tuple encoding for a dictionary or list, or the original value for
        other input types. The result is hashable only when its unchanged
        values are themselves hashable.

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
    """Reconstruct a dictionary or list encoded by :func:`make_hashable`.

    The encoded container type is preserved, including custom dictionary and
    list subclasses. Values that are not recognized container encodings are
    returned unchanged.

    Args:
        obj: Tuple encoding to reconstruct.

    Returns:
        Reconstructed dictionary or list, or the input tuple unchanged when it
        does not encode one of those container types.

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
    """Return unique values, supporting nested dictionaries and lists.

    Values are deduplicated through :func:`make_hashable`. Result ordering is
    not guaranteed because the encoded values are collected in a set.

    Args:
        values: Values to deduplicate; nested dictionaries and lists are
            supported.

    Returns:
        A list of unique values, reconstructing dictionary and list encodings.

    Raises:
        TypeError: If an encoded value is still unhashable, such as a set.

    Example:
        >>> get_unique_values([{'a': 1}, {'a': 1}, {'a': 2}])
        [{'a': 1}, {'a': 2}]
    """
    unique_values = set()
    for value in values:
        unique_values.add(make_hashable(value))
    return [transform_back(value) if isinstance(value, tuple) else value for value in unique_values]

def format_dict(dict: dict) -> str:
    """Format dictionary entries as a comma-separated string.

    Values whose exact type is ``float`` are formatted to two decimal places;
    values whose exact type is ``datetime.datetime`` use ``YYYY-MM-DD``. Other
    values use their standard string representation. Entry order follows the
    dictionary's iteration order.

    Args:
        dict: Dictionary to format.

    Returns:
        A comma-separated string of ``key=value`` entries, or an empty string
        for an empty dictionary.

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
