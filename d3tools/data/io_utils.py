
import rioxarray as rxr
import xarray as xr
import numpy as np
import os
import json
import datetime as dt

try:
    import pandas as pd
    import geopandas as gpd
except ImportError:
    pass

from typing import Optional

def check_data_format(data, format: str) -> None:
    """"
    Ensures that the data is compatible with the format of the dataset.
    """
    # add possibility to write a geopandas dataframe to a geojson or a shapefile
    if isinstance(data, gpd.GeoDataFrame):
        if format not in ['shp', 'json']:
            raise ValueError(f'Cannot write a geopandas dataframe to a {format} file.')
        
    elif isinstance(data, pd.DataFrame):
        if not format == 'csv':
            raise ValueError(f'Cannot write pandas dataframe to a {format} file.')
    elif isinstance(data, str):
        if format =='txt' or format == 'file':
            pass
        else:
            raise ValueError(f'Cannot write a string to a {format} file.')
    elif isinstance(data, dict):
        if not format == 'json':
            raise ValueError(f'Cannot write a dictionary to a {format} file.')
        
    elif isinstance(data, np.ndarray) or isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
        if format == 'csv':
            raise ValueError(f'Cannot write matrix data to a csv file.')
    
    if format == 'geotiff' and isinstance(data, xr.Dataset):
        raise ValueError(f'Cannot write a dataset to a geotiff file.')

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
    
    elif extension in ['json', 'geojson']:
        return 'json'
    
    elif extension == 'txt':
        return 'txt'
    
    elif extension == 'shp':
        return 'shp'
    
    elif extension in ['png', 'pdf']:
        return 'file'

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
            # understand if the data is actually in a geodataframe format
            if 'features' in data.keys():
                data = gpd.read_file(path)

    # read the data from a txt file
    elif format == 'txt':
        with open(path, 'r') as f:
            data = f.readlines()

    # read the data from a shapefile
    elif format == 'shp':
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

    # read the data from a png or pdf
    elif format == 'file':
        data = path

    return data

def write_to_file(data, path, format: Optional[str] = None, append = False) -> None:

    if format is None:
        format = get_format_from_path(path)

    dir = os.path.dirname(path)
    if len(dir) > 0:
        os.makedirs(os.path.dirname(path), exist_ok = True)
    if not os.path.exists(path):
        append = False

    # write the data to a csv
    if format == 'csv':
        if append:
            data.to_csv(path, mode = 'a', header = False)
        else:
            data.to_csv(path)

    # write the data to a json
    elif format == 'json':
        # write a dictionary to a json
        if isinstance(data, dict):
            for key in data.keys():
                if isinstance(data[key], np.ndarray):
                    data[key] = data[key].tolist
                elif isinstance(data[key], dt.datetime):
                    data[key] = data[key].isoformat()
            if append:
                with open(path, 'r') as f:
                    old_data = json.load(f)
                old_data = [old_data] if not isinstance(old_data, list) else old_data
                old_data.append(data)
                data = old_data
            with open(path, 'w') as f:
                json.dump(data, f, indent = 4)
        # write a geodataframe to a json
        elif isinstance(data, gpd.GeoDataFrame):
            data.to_file(path, driver = 'GeoJSON')

    # write a geodataframe to a shapefile
    elif format == 'shp':
        data.to_file(path)

    elif format == 'txt':
        if append:
            with open(path, 'a') as f:
                f.writelines(data)
        else:
            with open(path, 'w') as f:
                f.writelines(data)

    # write the data to a geotiff
    elif format == 'geotiff':
        data.rio.to_raster(path, compress = 'LZW')

    # write the data to a netcdf
    elif format == 'netcdf':
        data.to_netcdf(path)

    # write the data to a png or pdf (i.e. move the file)
    elif format == 'file':
        os.rename(data, path)

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