"""Raster-oriented helpers for consistent xarray-based data cleaning and writing.

These utility functions normalize spatial metadata, convert nodata values, and
prepare raster arrays for GeoTIFF or NetCDF output in a format-aware way.
"""

import rioxarray as rxr
import xarray as xr
import numpy as np
import rasterio


def save_raster_as_cog(data: xr.DataArray, path: str) -> None:
    """Write a DataArray as a Cloud Optimized GeoTIFF.

    Args:
        data: Raster data to serialize.
        path: Output file path.
    """
    y_name = data.rio.y_dim
    x_name = data.rio.x_dim

    profile = {
        "driver": "COG",
        "height": data.sizes[y_name],
        "width": data.sizes[x_name],
        "count": 1,
        "dtype": str(data.dtype),
        "crs": data.rio.crs,
        "transform": data.rio.transform(),

        "compress": "LZW",
        "blocksize": 512,
        "overview_resampling": "average"
    }

    with rasterio.open(path, 'w', **profile) as dst:
        dst.write(data.values)

        # Convert all attrs to strings for GeoTIFF tags
        tags = {k: str(v) for k, v in data.attrs.items()}
        dst.update_tags(**tags)

        # Set nodata value if available
        dst.nodata = data.attrs.get('_FillValue', data.rio.nodata)


def save_raster_in_chunks(data: xr.DataArray, path: str, chunk_mb=128) -> None:
    """Write a raster to disk in tiled chunks sized to a target memory budget.

    Args:
        data: Raster data to write.
        path: Output file path.
        chunk_mb: Target chunk size in megabytes.
    """
    y_name = data.rio.y_dim
    x_name = data.rio.x_dim

    profile = {
        "driver": "GTiff",
        "height": data.sizes[y_name],
        "width": data.sizes[x_name],
        "count": data.shape[0] if "band" in data.dims or "bands" in data.dims else 1,
        "dtype": str(data.dtype),
        "crs": data.rio.crs,
        "transform": data.rio.transform(),
        "compress": "LZW"
    }

    blockxsize, blockysize = optimise_blocksizes(data, target_chunk_mb = chunk_mb)
    profile.update(blockxsize=blockxsize, blockysize=blockysize, tiled=True, BIGTIFF="YES")

    with rasterio.open(path, 'w', **profile) as dst:
        for ji, window in dst.block_windows(1):
            arr = data.isel(
                **{x_name: slice(window.col_off, window.col_off + window.width),
                   y_name: slice(window.row_off, window.row_off + window.height)}
            ).values
            # Ensure arr has the correct shape for rasterio (add band dimension if missing)
            if arr.ndim == 2: arr = arr[np.newaxis, :, :]
            dst.write(arr, window=window)

        # Convert all attrs to strings for GeoTIFF tags
        tags = {k: str(v) for k, v in data.attrs.items()}
        dst.update_tags(**tags)

        # Set nodata value if available
        dst.nodata = data.attrs.get('_FillValue', data.rio.nodata)

        # build overviews (pyramids)
        dst.build_overviews([2, 4, 8, 16], rasterio.enums.Resampling.average)
        dst.update_tags(ns='rio_overview', resampling='average')


def optimise_blocksizes(data: xr.DataArray, target_chunk_mb=128) -> tuple[int, int]:
    """Estimate aligned GeoTIFF block sizes that approach a target chunk size.

    Args:
        data: Raster data to size for writing.
        target_chunk_mb: Desired maximum chunk size in megabytes.

    Returns:
        A pair ``(blockxsize, blockysize)`` aligned to a 16-pixel multiple.
    """
    y_name = data.rio.y_dim
    x_name = data.rio.x_dim
    
    # Assume 2D spatial chunks (y, x) are last two dims
    chunk_map = dict(zip(data.dims, data.chunks))
    y_chunk = chunk_map.get(y_name, [None])[0]
    x_chunk = chunk_map.get(x_name, [None])[0]

    # Target chunk size in MB (adjustable based on your memory)
    bytes_per_element = data.dtype.itemsize
    current_chunk_size_mb = (y_chunk * x_chunk * bytes_per_element) / (1024**2)
    
    # Calculate multiplier to reach target size
    if current_chunk_size_mb < target_chunk_mb:
        multiplier = int(np.sqrt(target_chunk_mb / current_chunk_size_mb))
        multiplier = max(2, multiplier)
    else:
        multiplier = 1
    
    # Apply multiplier and round to multiple of 16
    blockxsize = max(16, ((x_chunk * multiplier)//16) * 16)
    blockysize = max(16, ((y_chunk * multiplier)//16) * 16)
    
    # # Cap at image dimensions
    # blockxsize = min(blockxsize, data.sizes[x_name])
    # blockysize = min(blockysize, data.sizes[y_name])

    return blockxsize, blockysize

# DECORATOR TO MAKE THE FUNCTIONS BELOW WORK WITH XR.DATASET
def withxrds(func):
    """Apply a DataArray function to each variable in an xarray Dataset.

    Args:
        func: Callable that accepts one DataArray and returns a transformed one.

    Returns:
        The transformed Dataset when the input is a Dataset, otherwise the direct
        result of ``func``.
    """
    def wrapper(*args, **kwargs):
        if isinstance(args[0], xr.Dataset):
            return xr.Dataset({var: func(args[0][var], **kwargs) for var in args[0]})
        return func(*args, **kwargs)
    return wrapper

## FUNCTIONS TO CLEAN DATA
@withxrds
def straighten_data(data: xr.DataArray) -> xr.DataArray:
    """Ensure raster coordinates are ordered from north to south.

    The helper sorts latitude or ``y`` coordinates in descending order when
    necessary so that spatial arrays are consistently oriented.

    Args:
        data: Raster DataArray or Dataset.

    Returns:
        A copy of the input with the vertical spatial dimension sorted.
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
def reset_nan(data: xr.DataArray, nan_value=None) -> xr.DataArray:
    """Reset the fill value for a raster to the floating NaN or integer sentinel.

    Args:
        data: Raster data to normalize.
        nan_value: Optional explicit fill value override.

    Returns:
        A DataArray whose ``_FillValue`` is set consistently with the dtype.
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
def set_nan(data: xr.DataArray, nan_value=None) -> xr.DataArray:
    """Set the raster fill value to a caller-specified sentinel.

    Args:
        data: Raster data to update.
        nan_value: Fill value to apply.

    Returns:
        Data with the new nodata value and metadata updated.
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
def change_nan(data: xr.DataArray, new_nan, current_nan=None) -> xr.DataArray:
    """Replace one nodata marker with another while preserving the dataset attrs.

    Args:
        data: Raster data to update.
        new_nan: Replacement fill value.
        current_nan: Existing fill value to replace. If omitted, only the
            ``_FillValue`` attribute is updated.

    Returns:
        Data with the requested fill value applied.
    """
    if current_nan is not None and not np.isclose(current_nan, new_nan, equal_nan=True):
        data = data.where(~np.isclose(data, current_nan, equal_nan=True), new_nan)

    data.attrs['_FillValue'] = new_nan
    return data


@withxrds
def set_type(data: xr.DataArray, nan_value=None, read=True) -> xr.DataArray:
    """Downcast a raster to the smallest safe integer or floating dtype.

    Args:
        data: Raster data to normalize.
        nan_value: Optional fill value to retain when converting integer arrays.
        read: If ``True``, use the read-path fill-value logic; otherwise use the
            write-path logic.

    Returns:
        Data converted to a compact dtype while preserving fill-value metadata.
    """

    finite_data = data.where(np.isfinite(data))
    max_value = finite_data.max(skipna=True)
    min_value = finite_data.min(skipna=True)

    if nan_value is not None:
        min_value = min(min_value, nan_value)
        max_value = max(max_value, nan_value)

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
    return set_nan(data, nan_value)