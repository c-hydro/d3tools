"""
Raster-specific functionality mixin for Dataset classes.

This mixin provides template management and raster-specific operations
for datasets that store gridded/raster data (NetCDF, GeoTIFF, etc.).
"""
import numpy as np
import xarray as xr
import rioxarray as rxr
import os
import datetime as dt
from typing import Optional

from .base import FormatMixin
from ...spatial import TemplateManager

class RasterMixin(FormatMixin):
    """
    Mixin providing raster/gridded data functionality.
    
    This mixin adds:
    - Template management for spatial grids
    - Coordinate system handling
    - Raster-specific read/write operations
    
    Used by datasets that store spatially gridded data like NetCDF, GeoTIFF.
    [future] Separate into submixins for NetCDF vs GeoTIFF if needed.
    """
    
    def _init_format_properties(self):
        """Initialize raster-specific properties. Call from __init__."""
        self.template_manager = TemplateManager()
    
    def _read_from_file(self, path: str, chunk_threshold: int = 500, **kwargs) -> xr.DataArray | xr.Dataset:
        """Read raster data from a file using xarray or rioxarray.
        Args:
            path: Path to the raster file
            chunk_threshold: File size threshold (in MB) for using dask
            **kwargs: Additional arguments [unused in this method but passed for consistency]

        Returns:
            xarray DataArray or Dataset read from the raster file
        """

        # read the data from a geotiff
        if self.format == 'geotiff':
            # check the size of the file to decide whether to use dask or not
            file_size = os.path.getsize(path) / (1024**2)  # size in MB
            if file_size > chunk_threshold:  # threshold for using dask
                data = rxr.open_rasterio(path, chunks = {})
            else:
                data = rxr.open_rasterio(path)
        # read the data from a netcdf
        elif self.format == 'netcdf':
            data = xr.open_dataset(path)
            # check if there is a single variable in the dataset
            if len(data.data_vars) == 1:
                data = data[list(data.data_vars)[0]]
        return data

    def _format_after_read(self, data, **kwargs):
        """
        Post-process raster data after reading from storage.
        
        Handles coordinate straightening, nodata value conversion,
        and template application/creation.
        
        Args:
            data: Raw raster data from storage
            **kwargs: Additional arguments (as_is flag, etc.)
            
        Returns:
            Processed xarray ready for use
        """
        from ..io_utils_raster import straighten_data, set_type

        # Ensure data has descending latitudes
        data = straighten_data(data)
        
        # Convert nodata values
        data = set_type(data, self.nan_value, read=True)
        
        # Handle templates
        template_dict = self.get_template_dict(make_it=False, **kwargs)
        if template_dict is None:
            # Create template from data
            self.set_template(data, **kwargs)
        else:
            # Apply existing template to align coordinates
            attrs = data.attrs
            data = self.set_data_to_template(data, template_dict)
            data.attrs.update(attrs)
        
        # Add source metadata -> move this to a more general place in Dataset.read_data after format-specific processing
        if 'source_key' not in kwargs:
            data.attrs.update({'source_key': kwargs.get('full_key', "")})
        
        return data

    def _format_before_write(self, data, **kwargs):
        """
        Prepare raster data for writing with format-specific logic.
        
        Handles template creation/application, coordinate straightening,
        and nodata value management.
        
        Args:
            data: Raster data to prepare (xarray or numpy array)
            **kwargs: Additional arguments
            
        Returns:
            Prepared xarray ready for writing
        """
        from ..io_utils_raster import straighten_data, set_type
        
        # Ensure there is a template available
        try:
            template_dict = self.get_template_dict(**kwargs)
        except PermissionError:
            template_dict = None

        if template_dict is None:
            if isinstance(data, (xr.DataArray, xr.Dataset)):
                self.set_template(data, **kwargs)
                template_dict = self.get_template_dict(**kwargs, make_it=False)
            else:
                raise ValueError('Cannot write numpy array without a template.')
        
        # Apply template to data
        if isinstance(data, (xr.DataArray, xr.Dataset)):
            data = straighten_data(data)
            output = self.set_data_to_template(data, template_dict)
        else:
            output = self.set_data_to_template(data, template_dict)
            output = straighten_data(output)
        
        # Fix the type and nodata value
        output = set_type(output, self.nan_value, read=False)
        
        return output

    def _write_to_file(self, data: xr.DataArray, path: str, **kwargs):
        """Write raster data to a file using xarray.
        Args:
            data: xarray DataArray or Dataset prepared for writing
            path: Path to the output file
            **kwargs: Additional arguments [unused in this method but passed for consistency]

        """
        from ..io_utils import ensure_directory_exists
        ensure_directory_exists(path)
        
        if self.format == 'geotiff':
            if data.chunks is not None:
                # If data is chunked, save it in chunks
                from ..io_utils_raster import save_raster_in_chunks
                save_raster_in_chunks(data, path)
            else:
                # If not chunked, write directly
                data.rio.to_raster(path, compress='LZW', windowed=np.prod(data.shape) > 1e8)

        # write the data to a netcdf
        elif self.format == 'netcdf':
            data.to_netcdf(path)

    @property
    def _template(self) -> dict:
        """Backward compatibility property for accessing templates."""
        return self.template_manager._templates
    
    @_template.setter
    def _template(self, value: dict):
        """Backward compatibility property for setting templates."""
        self.template_manager._templates = value
    
    def get_template_dict(self, make_it: bool = True, **kwargs):
        """
        Get template dictionary for raster data.
        
        Templates define the spatial grid (coordinates, CRS, etc.) for raster data.
        If template doesn't exist and make_it=True, creates it from actual data.
        
        Args:
            make_it: Whether to create template if it doesn't exist
            **kwargs: Additional options (tile, etc.)
            
        Returns:
            Template dictionary or dict of templates (for tiled datasets)
        """
        # frop the file_version if it exists
        kwargs.pop('file_version', None)
        
        tile = kwargs.pop('tile', None)
        if tile is None:
            if self.has_tiles:
                template_dict = {}
                for tile in self.tile_names:
                    template_dict[tile] = self.get_template_dict(make_it=make_it, tile=tile, **kwargs)
                return template_dict
            else:
                tile = '__tile__'

        template_dict = self.template_manager.get(tile)
        if template_dict is None and make_it:
            if not self.has_time:
                data = self.get_data(as_is=True, **kwargs)
                self.set_template(data, tile=tile)

            else:
                # Use get_any_date instead of get_last_date - we don't care which file
                any_date = self.get_any_date(tile=tile, **kwargs)
                if any_date is not None:
                    data = self.get_data(time=any_date, tile=tile, as_is=True, **kwargs)
                else:
                    return None
            
            from ..io_utils_raster import straighten_data
            data = straighten_data(data)
            self.set_template(data, tile=tile)
            template_dict = self.get_template_dict(make_it=False, tile=tile, **kwargs)
        
        return template_dict
    
    def set_template(self, templatearray: xr.DataArray | xr.Dataset, **kwargs):
        """
        Set the spatial template for this raster dataset.
        
        Args:
            templatearray: xarray with coordinate system to use as template
            **kwargs: Additional options (tile, etc.)
        """
        tile = kwargs.get('tile', '__tile__')
        self.template_manager.set(templatearray, spatial_key=tile)

    @staticmethod
    def build_templatearray(template_dict: dict, data=None) -> xr.DataArray | xr.Dataset:
        """
        Build a template xarray.DataArray from a dictionary.
        
        Args:
            template_dict: Dictionary with template metadata
            data: Optional data array to apply template to
            
        Returns:
            xarray DataArray or Dataset with template applied
        """
        return TemplateManager.build_array(template_dict, data)

    @staticmethod
    def set_data_to_template(data: np.ndarray | xr.DataArray | xr.Dataset,
                             template_dict: dict) -> xr.DataArray | xr.Dataset:
        """
        Apply template to data array.
        
        Ensures data conforms to the spatial grid defined in template.
        
        Args:
            data: Numpy array or xarray to apply template to
            template_dict: Template metadata dictionary
            
        Returns:
            xarray with template coordinates applied
        """
        return TemplateManager.apply_to_data(data, template_dict)

    def set_metadata(self, data: xr.DataArray|xr.Dataset, **kwargs) -> xr.DataArray|xr.Dataset:
        """
        Add metadata to the xarray object.
        
        Args:
            data: xarray object to which metadata should be added
            **kwargs: Metadata key-value pairs to add
            
        Returns:
            xarray object with metadata attached
        """
        
        time = kwargs.pop('time', None)
        if time is not None:
            datatime = self.get_time_signature(time)
            kwargs['time'] = datatime.strftime('%Y-%m-%d')

        if hasattr(data, 'attrs'):
            if 'long_name' in data.attrs:
                data.attrs.pop('long_name')
            if 'time' in data.attrs:
                data.attrs.pop('time')
            kwargs.update(data.attrs)

        metadata = kwargs.copy()
        name = metadata.get('name', self.name)
        if 'long_name' in metadata:
            metadata.pop('long_name')

        metadata['time_produced'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data.attrs.update(metadata)

        if isinstance(data, xr.DataArray):
            data.name = name

        return data

    def update_metadata(self, data: xr.DataArray|xr.Dataset, **kwargs) -> xr.DataArray|xr.Dataset:
        """
        Update existing metadata on the xarray object.
        
        Args:
            data: xarray object whose metadata should be updated
            **kwargs: Metadata key-value pairs to update
            
        Returns:
            xarray object with updated metadata
        """
        attrs = data.attrs if hasattr(data, 'attrs') else {}
        attrs.update(kwargs)
        data.attrs = attrs
        return data
    
    def get_metadata(self, data: xr.DataArray|xr.Dataset, keys: Optional[list|str] = None) -> dict:
        """
        Retrieve metadata from the xarray object.
                
        Args:
            data: xarray object from which to retrieve metadata
            keys: Optional list of metadata keys to retrieve (if None, retrieve all)
            
        Returns:
            Metadata dictionary extracted from the xarray object
        """
        if hasattr(data, 'attrs'):
            metadata = data.attrs
            if keys is not None:
                if isinstance(keys, str):
                    keys = [keys]
                metadata = {k: v for k, v in metadata.items() if k in keys}
            return dict(metadata)
        else:
            return {}

    def validate_data(self, data: xr.DataArray|xr.Dataset, **kwargs) -> xr.DataArray|xr.Dataset:
        """
        Validate the data object in a format-specific way.
        
        This is a helper method that can be used to validate data before writing.
        The implementation will depend on the data structure.
        
        Args:
            data: Data object to validate
            **kwargs: Additional arguments for validation
            
        Returns:
            Validated data object
        """
        output = data.rio.write_nodata(data.attrs.get('_FillValue', self.nan_value))
        return output