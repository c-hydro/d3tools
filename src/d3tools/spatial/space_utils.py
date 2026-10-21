"""Coordinate reference and raster-bound utilities used by spatial operations."""

from pyproj import CRS
import xarray as xr
from typing import Sequence

def get_crs(datum: str|int|CRS) -> CRS:
    """
    Convert a CRS string, EPSG integer, or ``pyproj.CRS`` to a CRS object.

    Args:
        datum: CRS input. String values are passed to ``CRS.from_string``;
            integers are interpreted as EPSG codes; ``pyproj.CRS`` instances are
            returned unchanged.

    Returns:
        A ``pyproj.CRS`` instance matching the supplied datum.

    Raises:
        ValueError: If the datum type is unsupported and cannot be converted to a
            valid WKT-based CRS.
    """
   
    if isinstance(datum, str):
        return CRS.from_string(datum)
    elif isinstance(datum, int):
        return CRS.from_epsg(datum)
    elif isinstance(datum, CRS):
        return datum
    elif hasattr(CRS, 'to_wkt'):
        return CRS.from_string(datum.to_wkt())
    else:
        raise ValueError(f'Unknown datum type: {datum}, please provide an EPSG code ("EPSG:#####") or a WKT string.')

def crop_to_bb(src: str|xr.DataArray|xr.Dataset,
               BBox: 'BoundingBox') -> xr.DataArray:
    """
    Crop raster data or a raster file to a bounding box.

    Args:
        src: Raster file path, ``xarray.DataArray``, or ``xarray.Dataset``.
        BBox: Bounding box to apply. If the source has a CRS, the bounds are
            transformed to that CRS; if it has no CRS, the source is assigned the
            bounding box CRS in place and the bounds are used directly.

    Returns:
        The clipped xarray object. Any x coordinates greater than 180 are shifted
        by -360 and sorted before clipping, which is useful for antimeridian-style
        longitude data.

    Note:
        If the source is missing a CRS, the source object is updated in place with
        ``rio.write_crs(..., inplace=True)`` before cropping.
    """
    if isinstance(src, str):
        from ..data.io_utils import read_from_file
        src_ds = read_from_file(src)
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
    cropped = clip_xarray(src_ds, transformed_BBox.bbox)

    return cropped

def clip_xarray(input: xr.DataArray,
                bounds: tuple[float, float, float, float],
                ) -> xr.DataArray:
    """
    Clip a raster-aware DataArray to bounds slightly inset on every side.

    Args:
        input: Raster-aware DataArray to clip.
        bounds: Bounds ordered as ``(left, bottom, right, top)``.

    Returns:
        The result of ``rio.clip_box`` using bounds contracted by ``1e-6``
        coordinate units on each side.
    """

    bounds_buffered = buffer_bbox(bounds, -1e-6)
    input_clipped = input.rio.clip_box(*bounds_buffered)
    
    return input_clipped

def buffer_bbox(bbox: Sequence[float], buffer: int) -> Sequence[float]:
    """
    Expand or contract a bounding box by a coordinate-unit distance.

    Args:
        bbox: Four values ordered as ``(left, bottom, right, top)``.
        buffer: Distance applied outward to each side; a negative value contracts
            the box.

    Returns:
        A tuple of adjusted bounds. The input sequence is not modified.
    """
    left, bottom, right, top = bbox
    return (left   - buffer,
            bottom - buffer,
            right  + buffer,
            top    + buffer)