import datetime as dt
import re

# backward compatibility, these functions used to be in this module
from .string_substitution import substitute_string, substitute_values
from .structure_utils import flatten_dict, make_hashable, transform_back, get_unique_values, format_dict
from .special_substitutions import set_env, set_dataset

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