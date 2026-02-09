"""
Raster-specific functionality mixin for Dataset classes.

This mixin provides template management and raster-specific operations
for datasets that store gridded/raster data (NetCDF, GeoTIFF, etc.).
"""
import numpy as np
import xarray as xr
import datetime as dt
from typing import Optional

from ...spatial import TemplateManager
from ..io_utils import straighten_data
from ...timestepping import TimeStep


class RasterMixin:
    """
    Mixin providing raster/gridded data functionality.
    
    This mixin adds:
    - Template management for spatial grids
    - Coordinate system handling
    - Raster-specific read/write operations
    
    Used by datasets that store spatially gridded data like NetCDF, GeoTIFF.
    """
    
    def _init_raster_properties(self):
        """Initialize raster-specific properties. Call from __init__."""
        self.template_manager = TemplateManager()
    
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
