import xarray as xr
import rioxarray as rxr
from pyproj import CRS, Transformer

from .space_utils import get_crs, buffer_bbox
from ..data import Dataset

class BoundingBox():

    def __init__(self,
                 left: float, bottom: float, right: float, top: float,
                 datum: str|int|CRS = 'EPSG:4326',
                 buffer: float = 0.0,):
        """
        Create a bounding box object
        """

        # datum should be able to accept both EPSG codes and WKT strings and should default to WGS84
        self.crs = get_crs(datum)

        # set the bounding box
        self.bbox = (left, bottom, right, top)

        # buffer the bounding box
        self.add_buffer(buffer)

    @property
    def epsg_code(self):
        return f'EPSG:{self.crs.to_epsg()}'
    
    @property
    def wkt_datum(self):
        return self.crs.to_wkt()

    @staticmethod
    def from_xarray(data: xr.DataArray, buffer: float = 0.0):
        left, bottom, right, top = data.rio.bounds()

        return BoundingBox(left, bottom, right, top, datum = data.rio.crs, buffer = buffer)

    @staticmethod
    def from_dataset(dataset: Dataset, buffer: float = 0.0):
        data:xr.DataArray = dataset.get_data()

        return BoundingBox.from_xarray(data, buffer)
    
    @staticmethod
    def from_file(grid_file, buffer: float = 0.0):
        """
        Get attributes from grid_file
        We get the bounding box, crs, resolution, shape and transform of the grid.
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
        Buffer the bounding box, the buffer is in units of coordinates
        """
        self.buffer = buffer
        self.bbox = buffer_bbox(self.bbox, buffer)

    def transform(self, new_datum: str, inplace = False) -> None:
        """
        Transform the bounding box to a new datum
        new_datum: the new datum in the form of an EPSG code
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