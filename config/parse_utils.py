import datetime as dt
import re

try:
    from ..timestepping.time_utils import get_date_from_str
except ImportError:
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

def substitute_string(string, tag_dict, rec=False):
    """
    Replace the {tags} in the string with the values in the tag_dict.
    Handles datetime objects with format specifiers.
    """

    if not isinstance(string, str):
        return string

    pattern = r'{([\w.]+)(?::(.*?))?}'

    def replace_match(match, tag_dict):
        key = match.group(1)
        fmt = match.group(2)
        value = tag_dict.get(key)

        if value is None:
            return match.group(0)  # Return the original match if the key is not found

        if isinstance(value, dt.datetime) and fmt:
            return value.strftime(fmt)
        elif fmt:
            return format(value, fmt)
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

def set_dataset(structure, obj_dict):
    """
    Replace the {obj, tag = 'value'} in the structure with the corresponding dataset in the obj_dict.
    the tag = 'value' is used to update the tags of the dataset.
    """
    if isinstance(structure, dict):
        return {set_dataset(key, obj_dict): set_dataset(value, obj_dict) for key, value in structure.items()}
    elif isinstance(structure, list):
        return [set_dataset(value, obj_dict) for value in structure]
    elif isinstance(structure, str):
        pattern = r'{([\w.]+)(?:\s*,\s*([\w.]+\s*=\s*\'.*?\')+)?}'
        match = re.match(pattern, structure)
        if match:
            key   = match.group(1)
            ds = obj_dict.get(key, structure)

            if len(match.groups()) > 1:
                tag_values = match.group(2)
                if tag_values:
                    tags = {}
                    tag_values_pattern = r'([\w.]+)\s*=\s*\'(.*?)\''
                    for tag_values_match in re.finditer(tag_values_pattern, tag_values):
                        tags[tag_values_match.group(1)] = tag_values_match.group(2)

                    ds = ds.update(**tags)

            return ds
        else:
            return structure
    else:
        return structure

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

def make_hashable(obj):
    """
    Convert a nested dictionary to a hashable object.
    """
    if isinstance(obj, dict):
        return ('dict',) + tuple((k, make_hashable(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return ('list',) +  tuple(make_hashable(v) for v in obj)
    else:
        return obj

def transform_back(obj):
    """
    Transform the hashable object back to its original form (list or dict).
    """
    if obj[0] == 'dict':
        return {k: transform_back(v) if isinstance(v, tuple) else v for k, v in obj[1:]}
    elif obj[0] == 'list':
        return [transform_back(v) if isinstance(v, tuple) else v for v in obj[1:]]
    else:
        return obj

def get_unique_values(values):
    unique_values = set()
    for value in values:
        unique_values.add(make_hashable(value))
    return [transform_back(value) if isinstance(value, tuple) else value for value in unique_values]

def extract_date_and_tags(string: str, string_pattern:str):
    pattern = string_pattern
    pattern = re.sub(r'\{(\w+)\}', r'(?P<\1>[^/]+)', pattern)
    pattern = pattern.replace('%Y', '(?P<year>\\d{4})')
    pattern = pattern.replace('%m', '(?P<month>\\d{2})')
    pattern = pattern.replace('%d', '(?P<day>\\d{2})')

    # get all the substituted names (i.e. the parts of the pattern that are between < and >)
    substituted_names = re.findall(r'(?<=<)\w+(?=>)', pattern)

    # if there are duplicate names, change them to avoid conflicts
    for name in set(substituted_names):
        count = substituted_names.count(name)
        if count > 1:
            for i in range(count-1):
                pattern = pattern.replace(f'(?P<{name}>', f'(?P<{name}{i}>', 1)

    # Match the string with the pattern
    match = re.match(pattern, string)
    if not match:
        raise ValueError("The string does not match the pattern")
    
    # Extract the date components
    if 'year' in substituted_names:
        year = int(match.group('year'))
    else:
        year = 1900
    
    if 'month' in substituted_names:
        month = int(match.group('month'))
    else:
        month = 1
    
    if 'day' in substituted_names:
        day = int(match.group('day'))
    else:
        day = 1

    date = dt.datetime(year, month, day)
    
    # Extract the other key-value pairs
    all_tags = match.groupdict()
    tags = {key: value for key, value in all_tags.items() if key in substituted_names and key not in ['year', 'month', 'day']}
    
    return date, tags