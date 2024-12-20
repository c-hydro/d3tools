from pyproj import CRS
import xarray as xr
from typing import Sequence

from .bounding_box import BoundingBox


def get_crs(datum: str|int|CRS) -> CRS:
    """
    Get the CRS object from the datum
    """

    if isinstance(datum, str):
        return CRS.from_string(datum)
    elif isinstance(datum, int):
        return CRS.from_epsg(datum)
    elif isinstance(datum, CRS):
        return datum
    else:
        raise ValueError(f'Unknown datum type: {datum}, please provide an EPSG code ("EPSG:#####") or a WKT string.')

def crop_to_bb(src: str|xr.DataArray|xr.Dataset,
               BBox: BoundingBox) -> xr.DataArray:
    """
    Cut a geotiff to a bounding box.
    """
    if isinstance(src, str):
        if src.endswith(".nc"):
            src_ds = xr.load_dataset(src, engine="netcdf4")
        elif src.endswith(".grib"):
            src_ds = xr.load_dataset(src, engine="cfgrib")
    elif isinstance(src, xr.DataArray) or isinstance(src, xr.Dataset):
        src_ds = src

    x_dim = src_ds.rio.x_dim
    lon_values = src_ds[x_dim].values
    if (lon_values > 180).any():
        new_lon_values = xr.where(lon_values > 180, lon_values - 360, lon_values)
        new = src_ds.assign_coords({x_dim:new_lon_values}).sortby(x_dim)
        src_ds = new.rio.set_spatial_dims(x_dim, new.rio.y_dim)

    # transform the bounding box to the geotiff projection
    if src_ds.rio.crs is not None:
        transformed_BBox = BBox.transform(src_ds.rio.crs.to_wkt())
    else:
        src_ds = src_ds.rio.write_crs(BBox.wkt_datum, inplace=True)
        transformed_BBox = BBox
    # otherwise, let's assume that the bounding box is already in the right projection
    #TODO: eventually fix this...

    # Crop the raster
    cropped = clip_xarray(src_ds, transformed_BBox)

    return cropped

def clip_xarray(input: xr.DataArray,
                bounds: tuple[float, float, float, float],
                ) -> xr.DataArray:

    bounds_buffered = buffer_bbox(bounds, -1e-6)
    input_clipped = input.rio.clip_box(*bounds_buffered)
    
    return input_clipped

def buffer_bbox(bbox: Sequence[float], buffer: int) -> Sequence[float]:
    """
    Buffer the bounding box, the buffer is in units of coordinates
    """
    left, bottom, right, top = bbox
    return (left   - buffer,
            bottom - buffer,
            right  + buffer,
            top    + buffer)