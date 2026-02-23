from typing import Any

from .dataset import Dataset
from ...parse import KeyParser

class MemoryDataset(Dataset):
    type = 'memory'

    def __init__(self, key_pattern: str, keep_after_reading = False, **kwargs):
        self.key_pattern = key_pattern
        super().__init__(**kwargs)
        self.data_dict = {}
        self.keep_after_reading = keep_after_reading

    @property
    def key_pattern(self):
        return self._key_pattern

    @key_pattern.setter
    def key_pattern(self, key_pattern: str):
        self._key_pattern = key_pattern

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_key: str, **kwargs) -> Any:
        if self.keep_after_reading:
            return self.data_dict.get(input_key)
        else:
            return self.data_dict.pop(input_key)
    
    def _write_data(self, output: Any, output_key: str, **kwargs) -> None:
        #future: add append mode for tables and text 
        # currently they are not supported in MemoryDataset since the Mixins are not called here
        self.data_dict[output_key] = output

    def _rm_data(self, key: str) -> None:
        self.data_dict.pop(key)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path: str) -> bool:
        for key in self.data_dict.keys():
            if key.startswith(data_path):
                return True
        else:
            return False
    
    def _walk(self, prefix: str):
        for key in self.data_dict.keys():
            if key.startswith(prefix):
                yield key

    def _get_recreate_kwargs(self) -> dict:
        kwargs = super()._get_recreate_kwargs()
        kwargs.update({'keep_after_reading': self.keep_after_reading})
        return kwargs
    
    def update(self, in_place = False, **kwargs):
        new_self = super().update(in_place = in_place, **kwargs)

        key_parser = KeyParser(new_self.key_pattern)
        matching_keys = key_parser.get_matching_keys(self.available_keys)
        for key in matching_keys:
            new_self.data_dict[key] = self.data_dict.get(key)

        if in_place:
            self = new_self
            return self
        else:
            return new_self
