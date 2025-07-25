from ..parse import get_unique_values, flatten_dict, substitute_values, set_dataset, set_env
from ..data import Dataset
from .utils import load_jsons

class Options(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = Options(v)
            elif isinstance(v, list):
                self[k] = [Options(i) if isinstance(i, dict) else i for i in v]

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
    def load(cls, *paths: str, **kwargs) -> dict:
        """
        Load the configuration from the specified path as a dictionary.
        """
        
        config = load_jsons(*paths)

        config_options = Options(config)
        parsed_options = config_options.parse(**kwargs)

        return Options(parsed_options)

    def parse(self, **kwargs):
        """
        Parse the options, using the tags.
        And parse the datasets in the options.
        """

        self = Options(set_env(self))
        
        tags = self.get('tags', {}, ignore_case = True) #flatten_dict(self, **kwargs)
        tags = substitute_values(tags, tags, rec=True)
        parsed_options = Options(substitute_values(self, tags, rec=True))

        # parse datasets
        dataset_options, ds_key = parsed_options.get('datasets', {}, ignore_case=True, get_key=True)
        defaults = dataset_options.pop('__defaults__', None)
        for dsname, dsopt in dataset_options.items():
            dataset_options[dsname] = Dataset.from_options(dsopt, defaults)

        flat_dsoptions = flatten_dict({ds_key: dataset_options})
        parsed_options = set_dataset(parsed_options, flat_dsoptions)

        return parsed_options
    
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
        
    def get(self, key: list[str]|str, default = None, ignore_case = False, get_key = False):
        """
        Get the value of the specified key.
        If the key is a list, it will return the value of the first key that is found.
        """
        if isinstance(key, str):
            if ignore_case:
                key = key.lower()

            for k, v in self.items():
                _key = k.lower() if ignore_case else k
                if _key == key:
                    outvalue, outkey = v, k
                    break
            else:
                outvalue, outkey = default, None
        
        elif isinstance(key, list):
            v = None
            while not v and key:
                v, k = self.get(key.pop(0), None, ignore_case, get_key = True)
            
            if not v:
                outvalue, outkey = default, None
            else:
                outvalue, outkey = v, k

        if get_key:
            return outvalue, outkey
        else:
            return outvalue