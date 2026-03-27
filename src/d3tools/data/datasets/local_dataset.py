import os
import rioxarray
import xarray as xr
import pandas as pd

from .dataset import Dataset
from ...timestepping import TimeStep
from ..io_utils import rm_file

from typing import Any, Optional

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
    def _read_data(self, input_path: str, **kwargs) -> Any:
        # _read_from_file is defined in format mixins,
        # so it will handle format-specific reading
        data = self._read_from_file(input_path, **kwargs)
        return data
    
    def _write_data(self, output: Any, output_path: str, **kwargs) -> None:
        # _write_to_file is defined in format mixins,
        # so it will handle format-specific writing
        self._write_to_file(output, output_path, **kwargs)

    def _rm_data(self, path: str) -> None:
        rm_file(path)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path: str) -> bool:
        return os.path.exists(data_path)
    
    def _walk(self, prefix: str):
        for root, _, filenames in os.walk(prefix):
            for filename in filenames:
                yield os.path.join(root, filename)