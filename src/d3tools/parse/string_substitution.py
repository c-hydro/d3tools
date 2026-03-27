import datetime as dt
import re

from ..timestepping.time_utils import get_date_from_str


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