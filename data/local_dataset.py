import os
import rioxarray
import xarray as xr
import pandas as pd
import datetime as dt

try:
    from .dataset import Dataset
    from ..timestepping.timestep import TimeStep
    from ..config.parse_utils import extract_date_and_tags
    from .io_utils import write_to_file, read_from_file, rm_file
except ImportError:
    from dataset import Dataset
    from timestepping.timestep import TimeStep
    from tools.config.parse_utils import extract_date_and_tags
    from io_utils import write_to_file, read_from_file, rm_file

from typing import Optional

class LocalDataset(Dataset):
    type = 'local'

    def __init__(self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs):
        if path is not None:
            self.dir = path
        elif 'dir' in kwargs:
            self.dir = kwargs.pop('dir')
        elif 'key_pattern' in kwargs:
            self.dir = os.path.dirname(kwargs.get('key_pattern'))

        if filename is not None:
            self.file = filename
        elif 'file' in kwargs:
            self.file = kwargs.pop('file')
        elif 'key_pattern' in kwargs:
            self.file = os.path.basename(kwargs.pop('key_pattern'))

        super().__init__(**kwargs)

    @property
    def key_pattern(self):
        return os.path.join(self.dir, self.file)

    @key_pattern.setter
    def key_pattern(self, path):
        self.dir  = os.path.dirname(path)
        self.file = os.path.basename(path)

    def path(self, time: Optional[TimeStep] = None, **kwargs):
        return self.get_key(time, **kwargs)

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_path) -> xr.DataArray|xr.Dataset|pd.DataFrame:
        return read_from_file(input_path, self.format)
    
    def _write_data(self, output: xr.DataArray|pd.DataFrame, output_path: str) -> None:
        write_to_file(output, output_path, self.format)

    def _rm_data(self, path) -> None:
        rm_file(path)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        return os.path.exists(data_path)
    
    @property
    def available_keys(self) -> list[str]:
        dir = self.dir
        while '%' in dir or '{' in dir:
            dir = os.path.dirname(dir)
    
        files = []
        for root, _, filenames in os.walk(dir):
            for filename in filenames:
                this_key = os.path.join(root, filename)
                try:
                    extract_date_and_tags(this_key, self.key_pattern)
                    files.append(this_key)
                except ValueError:
                    pass

        return files