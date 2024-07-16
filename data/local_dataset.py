import os
import rioxarray
import xarray as xr

from .dataset import Dataset

class LocalDataset(Dataset):
    type = 'local'

    ## INPUT/OUTPUT METHODS
    def _read_data(self, input_path):
        data = rioxarray.open_rasterio(input_path)
        return data
    
    def _write_data(self, output: xr.DataArray, output_path: str):
        
        # save the data to a geotiff
        # create the directory if it does not exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        output.rio.to_raster(output_path, compress = 'lzw')

    ## METHODS TO CHECK DATA AVAILABILITY
    def _check_data(self, data_path) -> bool:
        return os.path.exists(data_path)