import pandas as pd
import rioxarray as rxr
import xarray as xr
import numpy as np
import os
import json

from typing import Optional

def get_format_from_path(path: str) -> str:
    # get the file extension
    extension = path.split('.')[-1]

    # check if the file is a csv
    if extension == 'csv':
        return 'csv'

    # check if the file is a geotiff
    elif extension == 'tif' or extension == 'tiff':
        return 'geotiff'

    # check if the file is a netcdf
    elif extension == 'nc':
        return 'netcdf'
    
    elif extension == 'json':
        return 'json'
    
    elif extension == 'txt':
        return 'txt'
    
    elif extension == 'shp':
        return 'shp'

    raise ValueError(f'File format not supported: {extension}')

def read_from_file(path, format: Optional[str] = None) -> xr.DataArray|xr.Dataset|pd.DataFrame:

    if format is None:
        format = get_format_from_path(path)

    # read the data from a csv
    if format == 'csv':
        data = pd.read_csv(path)

    # read the data from a json
    elif format == 'json':
        with open(path, 'r') as f:
            data = json.load(f)

    # read the data from a txt file
    elif format == 'txt':
        with open(path, 'r') as f:
            data = f.readlines()

    # read the data from a shapefile
    elif format == 'shp':
        import geopandas as gpd
        data:gpd.GeoDataFrame = gpd.read_file(path)

    # read the data from a geotiff
    elif format == 'geotiff':
        data = rxr.open_rasterio(path)

    # read the data from a netcdf
    elif format == 'netcdf':
        data = xr.open_dataset(path)
        # check if there is a single variable in the dataset
        if len(data.data_vars) == 1:
            data = data[list(data.data_vars)[0]]

    return data

def write_to_file(data, path, format: Optional[str] = None) -> None:

    if format is None:
        format = get_format_from_path(path)

    os.makedirs(os.path.dirname(path), exist_ok = True)

    # write the data to a csv
    if format == 'csv':
        data.to_csv(path)

    # write the data to a json
    elif format == 'json':
        with open(path, 'w') as f:
            json.dump(data, f)

    # write the data to a geotiff
    elif format == 'geotiff':
        data.rio.to_raster(path, compress = 'LZW')

    # write the data to a netcdf
    elif format == 'netcdf':
        data.to_netcdf(path)

def rm_file(path) -> None:
    os.remove(path)

# DECORATOR TO MAKE THE FUNCTION BELOW WORK WITH XR.DATASET
def withxrds(func):
    def wrapper(*args, **kwargs):
        if isinstance(args[0], xr.Dataset):
            return xr.Dataset({var: func(args[0][var], **kwargs) for var in args[0]})
        else:
            return func(*args, **kwargs)
    return wrapper

## FUNCTIONS TO CLEAN DATA
@withxrds
def straighten_data(data: xr.DataArray) -> xr.DataArray:
    """
    Ensure that the data has descending latitudes.
    """
    
    try:
        y_dim = data.rio.y_dim
    except rxr.exceptions.MissingSpatialDimensionError:
        y_dim = None

    if y_dim is None:
        for dim in data.dims:
            if 'lat' in dim.lower() or 'y' in dim.lower():
                y_dim = dim
                break
    if data[y_dim].data[0] < data[y_dim].data[-1]:
        data = data.sortby(y_dim, ascending = False)

    return data

@withxrds
def reset_nan(data: xr.DataArray) -> xr.DataArray:
    """
    Make sure that the nodata value is set to np.nan for floats and to the maximum integer for integers.
    """
    data_type = data.dtype
    new_fill_value = np.nan if np.issubdtype(data_type, np.floating) else np.iinfo(data_type).max
    fill_value = data.attrs.get('_FillValue', None)

    if fill_value is None:
        data.attrs['_FillValue'] = new_fill_value
    elif not np.isclose(fill_value, new_fill_value, equal_nan = True):
        data = data.where(~np.isclose(data, fill_value, equal_nan = True), new_fill_value)
        data.attrs['_FillValue'] = new_fill_value

    return data

@withxrds
def set_type(data: xr.DataArray) -> xr.DataArray:
    """
    Make sure that the data is the smallest possible.
    """

    max_value = data.max()
    min_value = data.min()

    # check if output contains floats or integers
    if np.issubdtype(data.dtype, np.floating):
        if max_value < 2**31 and min_value > -2**31:
            data = data.astype(np.float32)
        else:
            data = data.astype(np.float64)
    elif np.issubdtype(data.dtype, np.integer):
        if min_value >= 0:
            if max_value <= 255:
                data = data.astype(np.uint8)
            elif max_value <= 65535:
                data = data.astype(np.uint16)
            elif max_value < 2**31:
                data = data.astype(np.uint32)
            else:
                data = data.astype(np.uint64)
        else:
            if max_value <= 127 and min_value >= -128:
                data = data.astype(np.int8)
            elif max_value <= 32767 and min_value >= -32768:
                data = data.astype(np.int16)
            elif max_value < 2**31 and min_value > -2**31:
                data = data.astype(np.int32)
            else:
                data = data.astype(np.int64)

    return reset_nan(data)