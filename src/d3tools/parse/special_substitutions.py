import re
import os

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
