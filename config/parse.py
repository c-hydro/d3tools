import datetime as dt
import re

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