"""
Template management for Dataset spatial consistency.

This module provides the TemplateManager class which handles spatial template
creation and application to ensure coordinate consistency across dataset files.
"""

from typing import Optional, Iterator
import numpy as np
import xarray as xr
from .io_utils import set_type


class TemplateManager:
    """
    Manages spatial templates for Dataset instances.
    
    Templates store spatial metadata (CRS, dimensions, coordinates) to ensure
    all files in a dataset have consistent spatial reference and alignment.
    
    Attributes:
        _templates: Dictionary mapping spatial keys to template dictionaries
    """
    
    def __init__(self):
        """Initialize an empty TemplateManager."""
        self._templates: dict[str, dict] = {}
    
    def get(self, spatial_key: str = '__tile__') -> Optional[dict]:
        """
        Get template dictionary for a spatial key.
        
        Args:
            spatial_key: Identifier for the spatial unit (tile name or '__tile__')
            
        Returns:
            Template dictionary if it exists, None otherwise
        """
        return self._templates.get(spatial_key)
    
    def set(self, templatearray: xr.DataArray | xr.Dataset, spatial_key: str = '__tile__') -> None:
        """
        Create and store template from sample data.
        
        Args:
            templatearray: Sample xarray DataArray or Dataset to extract template from
            spatial_key: Identifier for the spatial unit (tile name or '__tile__')
        """
        # Handle xr.Dataset by extracting first variable
        if isinstance(templatearray, xr.Dataset):
            vars = list(templatearray.data_vars)
            templatearray = templatearray[vars[0]]
        else:
            vars = None

        # Extract template metadata directly from the DataArray
        # Get the CRS and the nodata value
        crs = templatearray.attrs.get('crs', templatearray.rio.crs)

        if crs is not None:
            crs_wkt = crs.to_wkt()
        elif hasattr(templatearray, 'spatial_ref') and hasattr(templatearray.spatial_ref, 'crs_wkt'):
            crs_wkt = templatearray.spatial_ref.crs_wkt
        elif hasattr(templatearray, 'crs') and hasattr(templatearray.crs, 'crs_wkt'):
            crs_wkt = templatearray.crs.crs_wkt
        else:  # if all fails, assume EPSG:4326
            from pyproj import CRS
            crs_wkt = CRS.from_epsg(4326).to_wkt()

        self._templates[spatial_key] = {
            'crs': crs_wkt,
            '_FillValue': templatearray.attrs.get('_FillValue'),
            'dims_names': templatearray.dims,
            'spatial_dims': (templatearray.rio.x_dim, templatearray.rio.y_dim),
            'dims_starts': {},
            'dims_ends': {},
            'dims_lengths': {}
        }
        
        if vars is not None:
            self._templates[spatial_key]['variables'] = vars

        for dim in templatearray.dims:
            this_dim_values = templatearray[dim].data
            start = this_dim_values[0]
            end = this_dim_values[-1]
            length = len(this_dim_values)
            self._templates[spatial_key]['dims_starts'][dim] = float(start)
            self._templates[spatial_key]['dims_ends'][dim] = float(end)
            self._templates[spatial_key]['dims_lengths'][dim] = length
    
    def exists(self, spatial_key: Optional[str] = None) -> bool:
        """
        Check if template(s) exist.
        
        Args:
            spatial_key: Specific spatial key to check, or None to check if any exist
            
        Returns:
            True if template exists, False otherwise
        """
        if spatial_key is None:
            return len(self._templates) > 0
        return spatial_key in self._templates
    
    def get_all(self) -> dict[str, dict]:
        """
        Get all templates.
        
        Returns:
            Dictionary mapping spatial keys to template dictionaries
        """
        return self._templates.copy()
    
    def clear(self, spatial_key: Optional[str] = None) -> None:
        """
        Clear template(s).
        
        Args:
            spatial_key: Specific key to clear, or None to clear all
        """
        if spatial_key is None:
            self._templates.clear()
        else:
            self._templates.pop(spatial_key, None)
    
    def copy(self, deep: bool = False) -> 'TemplateManager':
        """
        Create a copy of this TemplateManager.
        
        Args:
            deep: If True, deep copy the templates. If False, shallow copy (shared reference)
            
        Returns:
            New TemplateManager instance
        """
        new_manager = TemplateManager()
        if deep:
            import copy
            new_manager._templates = copy.deepcopy(self._templates)
        else:
            new_manager._templates = self._templates.copy()
        return new_manager
    
    def iter_spatial_keys(self) -> Iterator[str]:
        """
        Iterate over all spatial keys.
        
        Yields:
            Spatial key strings
        """
        yield from self._templates.keys()
    
    @staticmethod
    def build_array(template_dict: dict, data=None) -> xr.DataArray:
        """
        Build a template xarray.DataArray from a template dictionary.
        
        Args:
            template_dict: Template dictionary with spatial metadata
            data: Optional numpy array data to populate. If None, filled with nodata value
            
        Returns:
            xarray.DataArray with template spatial structure
        """
        shape = [template_dict['dims_lengths'][dim] for dim in template_dict['dims_names']]
        if data is None:
            data = np.full(shape, template_dict['_FillValue'])
        else:
            data = data.reshape(shape)
        template = xr.DataArray(data, dims=template_dict['dims_names'])
        
        for dim in template_dict['dims_names']:
            start = template_dict['dims_starts'][dim]
            end = template_dict['dims_ends'][dim]
            length = template_dict['dims_lengths'][dim]
            template[dim] = np.linspace(start, end, length)

        template.attrs = {'crs': template_dict['crs'], '_FillValue': template_dict['_FillValue']}
        template = template.rio.set_spatial_dims(*template_dict['spatial_dims']).rio.write_crs(
            template_dict['crs']).rio.write_coordinate_system()

        return template
    
    @staticmethod
    def apply_to_data(data: np.ndarray | xr.DataArray | xr.Dataset,
                      template_dict: dict) -> xr.DataArray | xr.Dataset:
        """
        Apply template spatial structure to data.
        
        This replaces the coordinates and CRS of the input data with those from
        the template, ensuring spatial consistency.
        
        Args:
            data: Input data (numpy array, DataArray, or Dataset)
            template_dict: Template dictionary with spatial metadata
            
        Returns:
            Data with template spatial structure applied
        """
        if isinstance(data, xr.DataArray):
            data = TemplateManager.build_array(template_dict, data.values)
        elif isinstance(data, np.ndarray):
            data = TemplateManager.build_array(template_dict, data)
        elif isinstance(data, xr.Dataset):
            vars = template_dict['variables']
            template = TemplateManager.build_array(template_dict, data[vars[0]].values)
            data = xr.Dataset({var: template.copy(data=data[var]) for var in vars})
        
        return set_type(data, read=True)
