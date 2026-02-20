import datetime as dt
import re
import os
from typing import Any, Optional

from ..timestepping.time_utils import get_date_from_str

def extract_date_and_tags(string: str, string_pattern: str):
    import copy

    pattern = string_pattern
    pattern = re.sub(r'\{([\w#-\.]+)\}', r'(?P<\1>[^/]+)', pattern)
    pattern = pattern.replace('%Y', '(?P<year>\\d{4})')
    pattern = pattern.replace('%m', '(?P<month>\\d{2})')
    pattern = pattern.replace('%d', '(?P<day>\\d{2})')
    pattern = pattern.replace('%H', '(?P<hour>\\d{2})')
    pattern = pattern.replace('%M', '(?P<minute>\\d{2})')
    pattern = pattern.replace('%S', '(?P<second>\\d{2})')
    pattern = pattern.replace('%j', '(?P<doy>\\d{3})')

    # get all the substituted names (i.e. the parts of the pattern that are between < and >)
    substituted_names = re.findall(r'(?<=<)[\w#-\.]+(?=>)', pattern)
    if "file_version" in substituted_names:
        pattern = pattern.replace('(?P<file_version>[^/]+', '(?P<file_version>.+')
    names_map = {}

    for name in set(substituted_names):
        if '{'+name+'}' in string:
            new_name = name.replace('.', '').replace('-', '').replace('#', '').replace('_', '')
            string = string.replace('{'+name+'}', new_name)

    # if there are duplicate names or there are symbols in the names, change them to avoid conflicts
    for name in set(substituted_names):
        if any(s in name for s in ['.', '-', '#']):
            new_name = copy.deepcopy(name).replace('.', '_p_').replace('-', '_d_').replace('#', '_h_')
            pattern = pattern.replace(f'(?P<{name}>', f'(?P<{new_name}>')
            names_map[new_name] = name
        else:
            names_map[name] = name
    
    substituted_names_2 = re.findall(r'(?<=<)[\w]+(?=>)', pattern)
    for name in set(substituted_names_2):
        count = substituted_names_2.count(name)
        
        if count > 1:
            for i in range(count - 1):
                pattern = pattern.replace(f'(?P<{name}>', f'(?P<{name}{i}>', 1)
                names_map[f'{name}{i}'] = name if name not in names_map else names_map[name]
    
    # Match the string with the pattern
    match = re.match(pattern, string)
    
    if not match:
        raise ValueError("The string does not match the pattern")

    all_tags = match.groupdict()
    # Extract the date components
    if 'doy' in all_tags:
        year = int(all_tags.pop('year', 1900))
        doy = int(all_tags.pop('doy'))
        date = dt.datetime(year, 1, 1) + dt.timedelta(days=doy - 1)
    else:
        year   = int(all_tags.pop('year',   1900))
        month  = int(all_tags.pop('month',  1))
        day    = int(all_tags.pop('day',    1))
        hour   = int(all_tags.pop('hour',   0))
        minute = int(all_tags.pop('minute', 0))
        second = int(all_tags.pop('second', 0))
        date   = dt.datetime(year, month, day, hour, minute, second)

    # Extract the other key-value pairs
    tags = {}
    for key, value in names_map.items():
        if value in ['year', 'month', 'day', 'hour', 'minute', 'second', 'doy']:
            continue
        if value not in tags:
            tags[value] = all_tags[key]
        elif tags[value] != all_tags[key]:
            breakpoint()
            raise ValueError(f"Duplicate values for tag {value} in the string")

    return date, tags

def set_env(structure):
    """
    Recursively substitute environment variable placeholders in a structure.

    Placeholders are in the form {ENV.VARNAME, default = 'value'}.
    If the environment variable is not set, the default is used if provided.
    Works for dicts, lists, and strings.

    Args:
        structure (dict, list, or str): The structure to process.

    Returns:
        The structure with environment variables substituted.

    Raises:
        ValueError: If a variable is not set and no default is provided.

    Example:
        >>> import os
        >>> os.environ['FOO'] = 'bar'
        >>> set_env('Value: {ENV.FOO}')
        'Value: bar'
        >>> set_env('Value: {ENV.BAR, default = \'baz\'}')
        'Value: baz'
    """
    if isinstance(structure, dict):
        return {set_env(key): set_env(value) for key, value in structure.items()}
    elif isinstance(structure, list):
        return [set_env(value) for value in structure]
    elif isinstance(structure, str):
        pattern = r'(\{ENV\.([\w#\-\.]+)(?:\s*,\s*(?:default\s*=\s*)?\'(.*?)\'\s*)?\})'
        matches = re.findall(pattern, structure)
        if matches:
            for match in matches:
                var = match[1]
                default = match[2] if len(match[2]) > 0 else None
                value = os.getenv(var, default)

                if value is None:
                    raise ValueError(f"Environment variable {var} is not set and no default value provided")
            
                structure = structure.replace(match[0], value)
    
    return structure

def set_dataset(structure, obj_dict):
    """
    Recursively substitute dataset references in a structure using an object dictionary.

    Placeholders are in the form {obj, tag = 'value'}.
    The object (dataset) is looked up in obj_dict by key. If tag assignments are present,
    the dataset is updated with those tags.
    Works for dicts, lists, and strings.

    Args:
        structure (dict, list, or str): The structure to process.
        obj_dict (dict): Dictionary mapping object names to datasets.

    Returns:
        The structure with dataset references substituted.

    Example:
        >>> class Dummy:
        ...     def update(self, **tags):
        ...         self.tags = tags
        ...         return self
        ...
        >>> obj_dict = {'foo': Dummy()}
        >>> set_dataset('{foo, tag = \'bar\'}', obj_dict).tags['tag'] == 'bar'
        True
    """
    if isinstance(structure, dict):
        return {set_dataset(key, obj_dict): set_dataset(value, obj_dict) for key, value in structure.items()}
    elif isinstance(structure, list):
        return [set_dataset(value, obj_dict) for value in structure]
    elif isinstance(structure, str):
        pattern = r'{([\w#-\.]+)(?:\s*,\s*([\w#-\.]+\s*=\s*\'.*?\')+)?}'
        match = re.match(pattern, structure)
        if match:
            key= match.group(1)
            structure = obj_dict.get(key, structure)

            if len(match.groups()) > 1:
                tag_values = match.group(2)
                if tag_values:
                    tags = {}
                    tag_values_pattern = r'([\w#-\.]+)\s*=\s*\'(.*?)\''
                    for tag_values_match in re.finditer(tag_values_pattern, tag_values):
                        tags[tag_values_match.group(1)] = tag_values_match.group(2)

                    structure = structure.update(**tags)

    return structure

def substitute_values(structure, tag_dict, **kwargs):
    """
    Recursively substitute placeholders in a nested structure (dict, list, or str).

    All string values in the structure will have their placeholders replaced using
    `substitute_string`. The function traverses dictionaries and lists recursively.

    Args:
        structure (dict, list, or str): The structure containing placeholders.
        tag_dict (dict): Dictionary mapping tag names to values.
        **kwargs: Additional keyword arguments passed to `substitute_string`.

    Returns:
        The structure with all placeholders substituted.

    Example:
        >>> substitute_values({"file": "output_{date:%Y%m%d}.tif"}, {"date": "2024-02-20"})
        {'file': 'output_20240220.tif'}
        >>> substitute_values(["tile_{tile}"], {"tile": ["A", "B"]})
        ['tile_A', 'tile_B']
    """

    if isinstance(structure, dict):
        return {substitute_values(key, tag_dict, **kwargs): substitute_values(value, tag_dict, **kwargs) for key, value in structure.items()}
    elif isinstance(structure, list):
        return [substitute_values(value, tag_dict, **kwargs) for value in structure]
    elif isinstance(structure, str):
        return substitute_string(structure, tag_dict, **kwargs)
    else:
        return structure


def substitute_string(string, tag_dict, rec=False):
    """
    Substitute placeholders in a string with values from a tag dictionary.

    Placeholders are in the form `{tag}` or `{tag:format}`. If a value in `tag_dict`
    is a datetime string or object and a format is provided, it will be formatted accordingly.
    If the value is a list, all possible substitutions will be generated recursively.

    Args:
        string (str): The template string containing placeholders.
        tag_dict (dict): Dictionary mapping tag names to values.
        rec (bool, optional): If True, recursively substitute lists of values. Default is False.

    Returns:
        str or list[str]: The string with placeholders substituted, or a list of all possible
                          substitutions if any tag value is a list.

    Example:
        >>> substitute_string("file_{date:%Y%m%d}.tif", {"date": "2024-02-20"})
        'file_20240220.tif'
        >>> substitute_string("tile_{tile}", {"tile": ["A", "B"]})
        ['tile_A', 'tile_B']
    """

    if not isinstance(string, str):
        return string

    pattern = r'{([\w#-\.]+)(?::(.*?))?}'

    def replace_match(match, tag_dict):
        key = match.group(1)
        fmt = match.group(2)
        value = tag_dict.get(key)

        if value is None:
            return match.group(0)  # Return the original match if the key is not found

        raw_value = value
        if isinstance(value, str):
            try:
                value = get_date_from_str(value)
            except ValueError:
                value = value

        if isinstance(value, dt.datetime) and fmt:
            return value.strftime(fmt)
        elif fmt:
            return format(value, fmt)
        elif isinstance(raw_value, str):
            return raw_value
        else:
            return str(value)

    def generate_strings(string, tag_dict):
        matches = re.findall(pattern, string)
        if not matches:
            return string

        key = matches[0][0]
        fmt = matches[0][1]
        value = tag_dict.get(key)

        if isinstance(value, list):
            results = []
            for val in value:
                temp_dict = tag_dict.copy()
                temp_dict[key] = val
                this_replace = lambda m: replace_match(m, temp_dict)
                this_replacement = re.sub(pattern, this_replace, string, count=1)
                results.append(generate_strings(this_replacement, temp_dict))
            return results
        else:
            return re.sub(pattern, lambda m: replace_match(m, tag_dict), string)

    return generate_strings(string, tag_dict)

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