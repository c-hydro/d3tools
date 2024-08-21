import os
import rioxarray
import xarray as xr
import pandas as pd

try:
    from .dataset import Dataset
    from ..timestepping.timestep import TimeStep
except ImportError:
    from dataset import Dataset
    from timestepping.timestep import TimeStep

from typing import Optional

class LocalDataset(Dataset):
    type = 'local'

    def __init__(self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs):
        if path is not None:
            self.dir = path
        elif 'dir' in kwargs:
            self.dir = kwargs.pop('dir')
        elif 'key_pattern' in kwargs:
            self.dir = os.path.dirname(kwargs.pop('key_pattern'))

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
    def _read_data(self, input_path):
        # read the data from a csv
        if self.format == 'csv':
            data = pd.read_csv(input_path)

        # read the data from a geotiff
        elif self.format == 'geotiff':
            data = rioxarray.open_rasterio(input_path)

        # read the data from a netcdf
        elif self.format == 'netcdf':
            data = xr.open_dataset(input_path)
            # check if there is a single variable in the dataset
            if len(data.data_vars) == 1:
                data = data[list(data.data_vars)[0]]

        return data
    
    def _write_data(self, output: xr.DataArray|pd.DataFrame, output_path: str):
    # create the directory if it does not exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # save the data to a csv (we check the type of the data already before calling _write_data)
        if self.format == 'csv':
            output.to_csv(output_path)
        
        # save the data to a geotiff
        elif self.format == 'geotiff':
            output.rio.to_raster(output_path, compress = 'lzw')

        # save the data to a netcdf
        elif self.format == 'netcdf':
            output.to_netcdf(output_path)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        return os.path.exists(data_path)