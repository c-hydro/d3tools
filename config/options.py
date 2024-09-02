from .parse_utils import get_unique_values, flatten_dict, substitute_values, set_dataset
from ..data import Dataset

import json

class Options(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = Options(v)

    def __getattr__(self, item):
        key_paths = self.find_keys(item, get_all = True)
        values = []
        for key_path in key_paths:
            current = self
            for key in key_path:
                current = current[key]
            values.append(current)

        values = get_unique_values(values)
        if len(values) == 1:
            return values.pop()
        elif len(values) > 1:
            raise ValueError(f"Multiple values found: {values}")
        else:
            raise AttributeError(f"'Options' object has no attribute '{item}'")

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, item):
        try:
            del self[item]
        except KeyError:
            raise AttributeError(f"'Options' object has no attribute '{item}'")

    @classmethod
    def load(cls, path: str, **kwargs) -> dict:
        """
        Load the configuration from the specified path as a dictionary.
        """
        
        ext:str = path.split('.')[-1]

        if ext != 'json':
            raise ValueError(f"Unsupported file format: {ext}. Config file should be in JSON format.")

        with open(path, 'r') as file:
            config:dict = json.load(file)

        return Options(parse_options(config, **kwargs))

    def find_keys(self, key: str, get_all = False) -> list[str]:
        """
        Returns the tree of keys that contain the specified key.
        It returns a list of keys that end with the specified key.
        e.g. {'a': {'b': 1, 'c': 2}}, 'c' -> ['a','c']
        """
        def search(d, key, path=[]):
            paths_found = []
            if isinstance(d, dict):
                for k, v in d.items():
                    if k == key:
                        paths_found.append(path + [k])
                    else:
                        paths_found.extend(search(v, key, path + [k]))
            return paths_found

        paths = search(self, key)
        if get_all:
            return paths
        elif len(paths) == 1:
            return paths[0]
        elif len(paths) > 1:
            raise ValueError(f"Multiple keys found: {paths}")
        else:
            return []
        
    def get(self, key: list[str]|str, default = None, ignore_case = False):
        """
        Get the value of the specified key.
        If the key is a list, it will return the value of the first key that is found.
        """

        if ignore_case and isinstance(key, str):
            key = key.lower()
            for k, v in self.items():
                if k.lower() == key:
                    return v
            return default
        
        elif isinstance(key, list):
            value = None
            while not value and key:
                value = self.get(key.pop(0), None, ignore_case)
            
            if not value:
                return default
            else:
                return value