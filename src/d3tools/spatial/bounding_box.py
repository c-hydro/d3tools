"""Bounding-box construction and coordinate-reference transformations."""

from typing import TYPE_CHECKING

import xarray as xr
import rioxarray as rxr
from pyproj import CRS, Transformer

from .space_utils import get_crs, buffer_bbox

if TYPE_CHECKING:
    from ..data import Dataset

class BoundingBox():
    """Store raster bounds together with their coordinate reference system.

    A bounding box keeps the left/bottom/right/top values in a CRS-aware tuple and
    exposes convenience methods for buffer expansion, transformation, and creation
    from raster-like xarray objects or files.
    """

    def __init__(self,
                 left: float, bottom: float, right: float, top: float,
                 datum: str|int|CRS = 'EPSG:4326',
                 buffer: float = 0.0,):
        """
        Create a bounding box and optionally expand it in coordinate units.

        Args:
            left: Left coordinate.
            bottom: Bottom coordinate.
            right: Right coordinate.
            top: Top coordinate.
            datum: Coordinate reference system as a CRS string, EPSG integer, or
                ``pyproj.CRS``. Defaults to EPSG:4326.
            buffer: Distance to expand each side of the bounds, in the CRS units.

        The bounds are stored in ``bbox`` as ``(left, bottom, right, top)``.
        """

        # datum should be able to accept both EPSG codes and WKT strings and should default to WGS84
        self.crs = get_crs(datum)

        # set the bounding box
        self.bbox = (left, bottom, right, top)

        # buffer the bounding box
        self.add_buffer(buffer)

    @property
    def epsg_code(self):
        """Return the CRS as an ``EPSG:<code>`` string, or ``EPSG:None`` if unavailable."""
        return f'EPSG:{self.crs.to_epsg()}'
    
    @property
    def wkt_datum(self):
        """Return the coordinate reference system as WKT text."""
        return self.crs.to_wkt()

    @staticmethod
    def from_xarray(data: xr.DataArray, buffer: float = 0.0):
        """
        Build a bounding box from an xarray object's raster bounds and CRS.

        Args:
            data: Raster-aware ``xarray.DataArray`` with bounds and CRS exposed
                through its ``rio`` accessor.
            buffer: Distance to expand each side of the bounds, in CRS units.

        Returns:
            A new bounding box using the raster bounds and CRS.
        """
        left, bottom, right, top = data.rio.bounds()

        return BoundingBox(left, bottom, right, top, datum = data.rio.crs, buffer = buffer)

    @staticmethod
    def from_dataset(dataset: "Dataset", buffer: float = 0.0):
        """
        Build a bounding box from the data returned by a dataset.

        Args:
            dataset: Dataset whose ``get_data()`` result is raster-aware.
            buffer: Distance to expand each side of the bounds, in CRS units.

        Returns:
            A new bounding box derived from the dataset data.
        """
        data:xr.DataArray = dataset.get_data()

        return BoundingBox.from_xarray(data, buffer)
    
    @staticmethod
    def from_file(grid_file, buffer: float = 0.0):
        """
        Build a bounding box from a raster file's bounds and CRS.

        Args:
            grid_file: Path accepted by ``rioxarray.open_rasterio``.
            buffer: Distance to expand each side of the bounds, in CRS units.

        Returns:
            A new bounding box derived from the opened raster.
        """

        # grid_data = gdal.Open(grid_file, gdalconst.GA_ReadOnly)

        # transform = grid_data.GetGeoTransform()
        # shape = (grid_data.RasterYSize, grid_data.RasterXSize)

        # #bbox in the form (min_lon, min_lat, max_lon, max_lat)
        # left   = transform[0] 
        # top    = transform[3]
        # right  = transform[0] + shape[1]*transform[1]
        # bottom = transform[3] + shape[0]*transform[5]

        # proj  = grid_data.GetProjection()

        # grid_data = None
        # return BoundingBox(left, bottom, right, top, datum = proj, buffer = buffer)

        data = rxr.open_rasterio(grid_file)
        return BoundingBox.from_xarray(data, buffer)

    def add_buffer(self, buffer: int) -> None:
        """
        Set the buffer distance and expand the stored bounds in coordinate units.

        Args:
            buffer: Distance to expand each side of the current bounds. Negative
                values contract the bounds.

        This updates both ``buffer`` and ``bbox`` on this instance.
        """
        self.buffer = buffer
        self.bbox = buffer_bbox(self.bbox, buffer)

    def transform(self, new_datum: str, inplace = False) -> None:
        """
        Transform the bounding box coordinates to another coordinate reference system.

        Args:
            new_datum: CRS identifier accepted by ``get_crs`` (typically an EPSG
                code or WKT string).
            inplace: If ``True``, replace this object's bounds and CRS in place and
                return ``None``. If ``False``, return a transformed copy and leave
                the current object unchanged.

        Returns:
            ``None`` when ``inplace`` is ``True``; otherwise a new
            ``BoundingBox`` expressed in ``new_datum``.

        Note:
            The method works by transforming all four corners and re-computing the
            bounding limits in the target CRS.
        """
        
        # figure out if we were given an EPSG code or a WKT string
        new_crs: CRS = get_crs(new_datum)

        # if the new datum is different to the current one, do nothing
        if not new_crs==self.crs:

            # Create a transformer to convert coordinates
            transformer = Transformer.from_crs(self.crs, new_crs, always_xy=True)

            # Transform the bounding box coordinates - because the image might be warped, we need to transform all 4 corners
            bl_x, bl_y = transformer.transform(self.bbox[0], self.bbox[1])
            tr_x, tr_y = transformer.transform(self.bbox[2], self.bbox[3])
            br_x, br_y = transformer.transform(self.bbox[2], self.bbox[1])
            tl_x, tl_y = transformer.transform(self.bbox[0], self.bbox[3])

            # get the new bounding box
            min_x = min(bl_x, tl_x)
            max_x = max(br_x, tr_x)
            min_y = min(bl_y, br_y)
            max_y = max(tl_y, tr_y)
        else:
            min_x, min_y, max_x, max_y = self.bbox

        if inplace:
            self.bbox = (min_x, min_y, max_x, max_y)
            self.crs  = new_crs
        else:
            return BoundingBox(min_x, min_y, max_x, max_y, datum = new_crs)