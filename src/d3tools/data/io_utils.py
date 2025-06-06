
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
    if isinstance(data, np.ndarray) or isinstance(data, xr.DataArray):
        if not format in ['geotiff', 'netcdf']:
            raise ValueError(f'Cannot write matrix data to a {format} file.')

    elif isinstance(data, xr.Dataset):
        if format not in ['netcdf']:
            raise ValueError(f'Cannot write a dataset to a {format} file.')
        
    elif isinstance(data, str):
        if format not in ['txt', 'file']:
            raise ValueError(f'Cannot write a string to a {format} file.')
        
    elif isinstance(data, dict):
        if format not in ['json']:
            raise ValueError(f'Cannot write a dictionary to a {format} file.')
        
    elif 'gpd' in globals() and isinstance(data, gpd.GeoDataFrame):
        if format not in ['shp', 'json']:
            raise ValueError(f'Cannot write a geopandas dataframe to a {format} file.')
                
    elif 'pd' in globals() and isinstance(data, pd.DataFrame):
        if format not in ['csv', 'parquet']:
            raise ValueError(f'Cannot write a pandas dataframe to a {format} file.')
    
    elif format not in ['file']:
        raise ValueError(f'Cannot write a {type(data)} to a {format} file.')

def get_format_from_path(path: str) -> str:
    # get the file extension
    extension = path.split('.')[-1]

    # check if the file is a csv
    if extension == 'csv':
        return 'csv'
    
    # check if the file is a parquet
    if extension == 'parquet':
        return 'parquet'

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
    
    elif extension in ['png', 'pdf', '']:
        return 'file'

    raise ValueError(f'File format not supported: {extension}')

def read_from_file(path, format: Optional[str] = None) -> xr.DataArray|xr.Dataset|pd.DataFrame:

    if format is None:
        format = get_format_from_path(path)

    # read the data from a csv
    if format == 'csv':
        data = pd.read_csv(path)

    # read the data from a parquet
    elif format == 'parquet':
        data = pd.read_parquet(path)

    # read the data from a json
    elif format == 'json':
        with open(path, 'r') as f:
            data = json.load(f)
            # understand if the data is actually in a geodataframe format
            if isinstance(data, dict) and 'features' in data.keys():
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
            data.to_csv(path, mode = 'a', header = False, index=False)
        else:
            data.to_csv(path, index=False)

    # write the data to a parquet
    elif format == 'parquet':
        if append:
            data.to_parquet(path, mode = 'a', header = False, index=False, compression='snappy')
        else:
            data.to_parquet(path, index=False, compression='snappy')

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
def reset_nan(data: xr.DataArray, nan_value = None) -> xr.DataArray:
    """
    Make sure that the nodata value is set to np.nan for floats and to the maximum integer for integers.
    """

    fill_value = nan_value or data.attrs.get('_FillValue', None)
    data_type = data.dtype

    if np.issubdtype(data_type, np.floating):
        new_fill_value = np.nan
    elif np.issubdtype(data_type, np.unsignedinteger):
        new_fill_value = np.iinfo(data_type).max
    elif np.issubdtype(data_type, np.integer):
        new_fill_value = np.iinfo(data_type).min

    data = change_nan(data, new_fill_value, fill_value)

    return data.astype(data_type)

@withxrds
def set_nan(data: xr.DataArray, nan_value = None) -> xr.DataArray:
    """
    Makes sure the nodata value is set to the nan_value provided (if available).
    """

    fill_value = data.attrs.get('_FillValue') # this should never be None based on how the rest of the code is written
    data_type = data.dtype
    new_fill_value = nan_value or fill_value

    # check that the fill value is compatible with the data type
    if np.issubdtype(data_type, np.integer) and np.isnan(new_fill_value):
        return reset_nan(data)

    data = change_nan(data, new_fill_value, fill_value)
    return data.astype(data_type)

@withxrds
def change_nan(data: xr.DataArray, new_nan, current_nan = None) -> xr.DataArray:
    if current_nan is not None and not np.isclose(current_nan, new_nan, equal_nan = True):
        data = data.where(~np.isclose(data, current_nan, equal_nan = True), new_nan)

    data.attrs['_FillValue'] = new_nan
    return data

@withxrds
def set_type(data: xr.DataArray, nan_value = None, read = True) -> xr.DataArray:
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
            elif max_value <= (2**32)-1:
                data = data.astype(np.uint32)
            else:
                data = data.astype(np.uint64)
                
            if nan_value is not None and not np.issubdtype(data.dtype, np.unsignedinteger):
                nan_value = None
        else:
            if max_value <= 127 and min_value >= -128:
                data = data.astype(np.int8)
            elif max_value <= 32767 and min_value >= -32768:
                data = data.astype(np.int16)
            elif max_value <= 2**31-1 and min_value >= -2**31:
                data = data.astype(np.int32)
            else:
                data = data.astype(np.int64)

            if nan_value is not None and not np.issubdtype(data.dtype, np.integer):
                nan_value = None
    
    if read:
        return reset_nan(data, nan_value)
    else:
        return set_nan(data, nan_value)