"""Helpers for detecting file formats and delegating read/write operations.

These utility functions centralize the mapping between file extensions and the
matching format mixin, and they provide thin wrappers used by dataset storage
classes during data I/O.
"""

import xarray as xr
import numpy as np
import os
import pandas as pd
import geopandas as gpd

from typing import Any, Optional

# these functions used to be in io_utils, but they are now in io_utils_raster.
# We import them here for backwards compatibility,
# but they should be imported from io_utils_raster in the future.
from .io_utils_raster import (
    save_raster_in_chunks,
    optimise_blocksizes,
    straighten_data,
    reset_nan,
    set_nan,
    change_nan,
    set_type
)

from .format_mixins import (
    RasterMixin,
    TableMixin,
    VectorMixin,
    PlainTextMixin,
    StructuredTextMixin,
    FileMixin
)

# Format → Mixin mapping
FORMAT_MIXIN_MAP = {
    'netcdf': RasterMixin,
    'geotiff': RasterMixin,
    'csv': TableMixin,
    'parquet': TableMixin,
    'shp': VectorMixin,
    'geojson': VectorMixin,
    'json': StructuredTextMixin,
    'txt': PlainTextMixin,
    'log': PlainTextMixin,
    'file': FileMixin,  # Generic file, no format processing
}


def check_data_format(data, format: str) -> None:
    """Validate that a value can be written to a dataset of the requested format.

    Args:
        data: Value to persist.
        format: Canonical format name such as ``netcdf`` or ``csv``.

    Raises:
        TypeError: If the object type is incompatible with the selected format.
    """
    if isinstance(data, np.ndarray) or isinstance(data, xr.DataArray):
        if format not in ['geotiff', 'netcdf']:
            raise TypeError(f'Cannot write matrix data to a {format} file.')

    elif isinstance(data, xr.Dataset):
        if format not in ['netcdf']:
            raise TypeError(f'Cannot write a dataset to a {format} file.')

    elif isinstance(data, str):
        if format not in ['txt', 'file']:
            raise TypeError(f'Cannot write a string to a {format} file.')

    elif isinstance(data, dict):
        if format not in ['json']:
            raise TypeError(f'Cannot write a dictionary to a {format} file.')

    elif 'gpd' in globals() and isinstance(data, gpd.GeoDataFrame):
        if format not in ['shp', 'geojson', 'json']:
            raise TypeError(f'Cannot write a geopandas dataframe to a {format} file.')

    elif 'pd' in globals() and isinstance(data, pd.DataFrame):
        if format not in ['csv', 'parquet']:
            raise TypeError(f'Cannot write a pandas dataframe to a {format} file.')

    elif format not in ['file']:
        raise TypeError(f'Cannot write a {type(data)} to a {format} file.')


def get_format_from_path(path: str) -> str:
    """Infer the canonical d3tools format name from a file path.

    Args:
        path: File path or key to inspect.

    Returns:
        The format label used by the dataset mixins.

    Raises:
        ValueError: If the extension is not supported.
    """
    extension = path.split('.')[-1]

    if extension == path or extension in ['png', 'pdf', 'jpg', 'jpeg']:
        return 'file'

    if extension == 'tif' or extension == 'tiff':
        return 'geotiff'

    if extension == 'nc':
        return 'netcdf'

    if extension not in FORMAT_MIXIN_MAP:
        raise ValueError(f'File format not supported: {extension}')

    return extension


def get_mixin_class_from_format(format: str):
    """Return the format mixin class that matches a canonical format label.

    Args:
        format: Canonical format name.

    Returns:
        The corresponding format mixin class.

    Raises:
        ValueError: If the format is not known.
    """
    _format_mixin = FORMAT_MIXIN_MAP.get(format, None)
    if _format_mixin is None:
        raise ValueError(f'Format {format} not supported.')

    return _format_mixin  # Return the class, not an instance

def get_mixin_from_format(format: str):
    """Instantiate the mixin for a given format and attach the format name.

    Args:
        format: Canonical format name.

    Returns:
        A format mixin instance configured for the requested format.
    """
    _format_mixin = get_mixin_class_from_format(format)
    format_mixin = _format_mixin()
    format_mixin.format = format

    return format_mixin


def ensure_directory_exists(path: str) -> None:
    """Create the parent directory for a file if it does not already exist.

    Args:
        path: Destination file path whose parent directory should exist.
    """
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)


def read_from_file(path, format: Optional[str] = None, **kwargs) -> Any:
    """Read a file using the appropriate format mixin.

    Args:
        path: Source file path.
        format: Optional explicit format override. If omitted, it is inferred
            from the filename extension.
        **kwargs: Format-specific read arguments forwarded to the mixin.

    Returns:
        The decoded object returned by the matching format mixin.
    """
    if format is None:
        format = get_format_from_path(path)

    format_mixin = get_mixin_from_format(format)
    return format_mixin._read_from_file(path, **kwargs)


def write_to_file(data, path, format: Optional[str] = None, append=False) -> None:
    """Write data to disk using the matching format mixin.

    Args:
        data: Data object to serialize.
        path: Destination file path.
        format: Optional explicit format override. If omitted, it is inferred
            from the filename extension.
        append: Whether the format backend supports appending to an existing file.
    """
    if format is None:
        format = get_format_from_path(path)

    format_mixin = get_mixin_from_format(format)
    format_mixin._write_to_file(data, path, append=append)


def rm_file(path) -> None:
    """Delete a file from the local filesystem.

    Args:
        path: File path to remove.
    """
    os.remove(path)
