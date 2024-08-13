import os
import rioxarray
import xarray as xr
import pandas as pd

from .dataset import Dataset

class LocalDataset(Dataset):
    type = 'local'

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