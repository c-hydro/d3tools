import xarray as xr
import pandas as pd

from .dataset import Dataset
from ..config.parse_utils import extract_date_and_tags

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
    def key_pattern(self, key_pattern):
        self._key_pattern = key_pattern

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_key):
        if self.keep_after_reading:
            return self.data_dict.get(input_key)
        else:
            return self.data_dict.pop(input_key)
    
    def _write_data(self, output: xr.DataArray|pd.DataFrame, output_key: str, **kwargs):
        self.data_dict[output_key] = output

    def _rm_data(self, key):
        self.data_dict.pop(key)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        for key in self.data_dict.keys():
            if key.startswith(data_path):
                return True
        else:
            return False
    
    def _walk(self, prefix):
        for key in self.data_dict.keys():
            if key.startswith(prefix):
                yield key
    
    def update(self, in_place = False, **kwargs):
        new_self = super().update(in_place = in_place, **kwargs)

        for key in self.available_keys:
            try: 
                extract_date_and_tags(key, new_self.key_pattern)
                new_self.data_dict[key] = self.data_dict.get(key)
            except ValueError:
                pass

        if in_place:
            self = new_self
            return self
        else:
            return new_self
