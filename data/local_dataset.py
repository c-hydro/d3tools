from typing import Optional
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


    # def update(self, in_place = False, **kwargs):
    #     if in_place:
    #         self.dir  = substitute_string(self.dir, kwargs)
    #         self.file = substitute_string(self.file, kwargs)
    #         self.path_pattern = self.path(**kwargs)
    #         self.tags.update(kwargs)
    #         return self
    #     else:
    #         new_path = substitute_string(self.dir, kwargs)
    #         new_file = substitute_string(self.file, kwargs)
    #         new_name = self.name
    #         new_format = self.format
    #         new_handler = LocalIOHandler(new_path, new_file, new_name, new_format)
    #         new_handler.template = self.template
    #         new_tags = self.tags.copy()
    #         new_tags.update(kwargs)
    #         new_handler.tags = new_tags
    #         return new_handler