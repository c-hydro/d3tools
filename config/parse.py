import datetime as dt
import re

from timestepping.time_utils import get_date_from_str

def substitute_values(structure, tag_dict, **kwargs):
    """
    replace the {tags} in the structure with the values in the tag_dict
    """

    if isinstance(structure, dict):
        return {substitute_values(key, tag_dict, **kwargs): substitute_values(value, tag_dict, **kwargs) for key, value in structure.items()}
    elif isinstance(structure, list):
        return [substitute_values(value, tag_dict, **kwargs) for value in structure]
    elif isinstance(structure, str):
        return substitute_string(structure, tag_dict, **kwargs)
    else:
        return structure

def substitute_string(string, tag_dict, rec = False):
    """
    Replace the {tags} in the string with the values in the tag_dict.
    Handles datetime objects with format specifiers.
    """
    def replace_match(match):
        key = match.group(1)
        fmt = match.group(2)
        value = tag_dict.get(key)

        if value is None:
            return match.group(0)  # No substitution, return the original tag

        if isinstance(value, str):
            try:
                value = get_date_from_str(value)
            except ValueError:
                pass

        if isinstance(value, dt.datetime) and fmt:
            return value.strftime(fmt)
        else:
            return str(value)

    pattern = re.compile(r'{(\w+)(?::(.*?))?}')

    while rec:
        new_string = pattern.sub(replace_match, string)
        if new_string == string:
            return new_string
        string = new_string

    return pattern.sub(replace_match, string)

def flatten_dict(nested_dict:dict, sep:str = '.', parent_key:str = '') -> dict:
    """
    Flatten a nested dictionary into a single level dictionary.
    for each nested key, it creates as many key:value pairs to ensure all combinations of the parent keys are present.
    parent keys are separated by '.' in the new key.
    e.g. {'a': {'b': 1, 'c': 2}} -> {'a.b': 1, 'b': 1, 'a.c': 2, 'c': 2}
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

def parse_options(options: dict, **kwargs):
    """
    Parse the options, using themselves as tags.
    """
    tags = flatten_dict(options, **kwargs)
    tags = substitute_values(tags, tags, rec=True)

    return substitute_values(options, tags, rec=True)