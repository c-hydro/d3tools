"""
Template management for Dataset spatial consistency.

This module provides the TemplateManager class which handles spatial template
creation and application to ensure coordinate consistency across dataset files.
"""

import datetime as dt

from typing import Optional, Iterator
import os
import json
import numpy as np
import xarray as xr
from pathlib import Path
import pandas as pd

from ..timestepping.timeperiods.timestep import estimate_timestep, TimeStep
from ..errors import TemplateValidationError, TemplateMemoryError

class TemplateManager:
    """
    Manages spatial templates for Dataset instances.
    
    Templates store spatial metadata (CRS, dimensions, coordinates) to ensure
    all files in a dataset have consistent spatial reference and alignment.
    
    Attributes:
        _templates: Dictionary mapping spatial keys to template dictionaries
        _cache_dir: Optional directory for disk caching of templates
        _validate: Whether to validate templates on creation
    """
    
    # Required keys for valid templates
    REQUIRED_KEYS = {'crs', '_FillValue', 'dims_names', 'spatial_dims', 
                     'dims_starts', 'dims_ends', 'dims_steps', 'dims_lengths'}
    
    def __init__(self, cache_dir: Optional[str] = None, validate: bool = True):
        """
        Initialize a TemplateManager.
        
        Args:
            cache_dir: Optional directory path for disk caching templates
            validate: Whether to validate templates on creation
        """
        self._templates: dict[str, dict] = {}
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._validate = validate
        
        if self._cache_dir:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, spatial_key: str = '__tile__', load_from_cache: bool = True) -> Optional[dict]:
        """
        Get template dictionary for a spatial key.
        
        Args:
            spatial_key: Identifier for the spatial unit (tile name or '__tile__')
            load_from_cache: If True and template not in memory, try loading from cache
            
        Returns:
            Template dictionary if it exists, None otherwise
        """
        # Check memory first
        if spatial_key in self._templates:
            return self._templates[spatial_key]
        
        # Try loading from cache if enabled
        if load_from_cache and self._cache_dir:
            if self.load_from_cache(spatial_key):
                return self._templates[spatial_key]
        
        return None
    
    def set(self, templatearray: xr.DataArray | xr.Dataset, spatial_key: str = '__tile__') -> None:
        """
        Create and store template from sample data.
        
        Args:
            templatearray: Sample xarray DataArray or Dataset to extract template from
            spatial_key: Identifier for the spatial unit (tile name or '__tile__')
            
        Raises:
            TemplateValidationError: If template validation fails
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

        template_dict = {
            'crs': crs_wkt,
            '_FillValue': templatearray.attrs.get('_FillValue'),
            'dims_names': templatearray.dims,
            'spatial_dims': (templatearray.rio.x_dim, templatearray.rio.y_dim),
            'dims_starts': {},
            'dims_ends': {},
            'dims_steps': {},
            'dims_lengths': {},
            'dims_values': {}
        }
        
        if vars is not None:
            template_dict['variables'] = vars

        for dim in templatearray.dims:
            this_dim_values = templatearray[dim].data
            length = len(this_dim_values)
            template_dict['dims_lengths'][dim] = length
            # check the type of the coordinate,
            # if numeric:
            if np.issubdtype(this_dim_values.dtype, np.number):
                start = this_dim_values[0]
                end = this_dim_values[-1]
                step  = this_dim_values[1] - this_dim_values[0] if len(this_dim_values) > 1 else 0
                template_dict['dims_starts'][dim] = float(start)
                template_dict['dims_ends'][dim] = float(end)
                template_dict['dims_steps'][dim] = float(step)
                template_dict['dims_values'][dim] = np.asarray(this_dim_values).tolist()
            # if datetime:
            elif np.issubdtype(this_dim_values.dtype, np.datetime64):
                # Convert to python datetimes: estimate_timestep and from_date
                # require datetime.datetime, not numpy.datetime64 / pd.Timestamp.
                # ISO strings are used for storage so json.dump works unchanged.
                py_dates = pd.DatetimeIndex(this_dim_values).to_pydatetime().tolist()
                start = py_dates[0]
                end   = py_dates[-1]
                step  = 'd'  # default to daily
                if len(py_dates) > 1:
                    step_ = estimate_timestep(py_dates)
                    if step_ is not None:
                        step = step_.unit
                template_dict['dims_starts'][dim] = start.isoformat()
                template_dict['dims_ends'][dim]   = end.isoformat()
                template_dict['dims_steps'][dim]  = step
        
        # Validate if enabled
        if self._validate:
            self._validate_template(template_dict, spatial_key)
        
        # Store in memory
        self._templates[spatial_key] = template_dict
        
        # Cache to disk if enabled
        if self._cache_dir:
            self._cache_template(spatial_key, template_dict)
    
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
        new_manager = TemplateManager(
            cache_dir=str(self._cache_dir) if self._cache_dir else None,
            validate=self._validate
        )
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
    
    def _validate_template(self, template_dict: dict, spatial_key: str) -> None:
        """
        Validate template dictionary has required keys and valid values.
        
        Args:
            template_dict: Template dictionary to validate
            spatial_key: Spatial key for error messages
            
        Raises:
            TemplateValidationError: If template is invalid
        """
        # Check required keys
        missing_keys = self.REQUIRED_KEYS - set(template_dict.keys())
        if missing_keys:
            raise TemplateValidationError(
                missing_keys=list(missing_keys),
                spatial_key=spatial_key
            )
        
        # Validate values
        invalid_keys = {}
        
        if template_dict['crs'] is None:
            invalid_keys['crs'] = "CRS cannot be None"
        
        if not template_dict['dims_names']:
            invalid_keys['dims_names'] = "Must have at least one dimension"
        
        if len(template_dict['spatial_dims']) != 2:
            invalid_keys['spatial_dims'] = f"Must have exactly 2 spatial dimensions, got {len(template_dict['spatial_dims'])}"
        
        # Check that all dims have start/end/length
        for dim in template_dict['dims_names']:
            if dim not in template_dict['dims_starts']:
                invalid_keys[f'dims_starts[{dim}]'] = "Missing"
            if dim not in template_dict['dims_ends']:
                invalid_keys[f'dims_ends[{dim}]'] = "Missing"
            if dim not in template_dict['dims_steps']:
                invalid_keys[f'dims_steps[{dim}]'] = "Missing"
            if dim not in template_dict['dims_lengths']:
                invalid_keys[f'dims_lengths[{dim}]'] = "Missing"
            elif template_dict['dims_lengths'][dim] <= 0:
                invalid_keys[f'dims_lengths[{dim}]'] = f"Must be positive, got {template_dict['dims_lengths'][dim]}"
        
        if invalid_keys:
            raise TemplateValidationError(
                invalid_keys=invalid_keys,
                spatial_key=spatial_key
            )
    
    def _cache_template(self, spatial_key: str, template_dict: dict) -> None:
        """
        Save template to disk cache.
        
        Args:
            spatial_key: Spatial key for the template
            template_dict: Template dictionary to cache
        """
        if not self._cache_dir:
            return
        
        cache_file = self._cache_dir / f"template_{spatial_key}.json"
        
        # Convert tuples to lists for JSON serialization
        serializable_dict = {}
        for key, value in template_dict.items():
            if isinstance(value, tuple):
                serializable_dict[key] = list(value)
            else:
                serializable_dict[key] = value
        
        with open(cache_file, 'w') as f:
            json.dump(serializable_dict, f, indent=2)
    
    def load_from_cache(self, spatial_key: str) -> bool:
        """
        Load template from disk cache.
        
        Args:
            spatial_key: Spatial key for the template
            
        Returns:
            True if template was loaded, False if not found in cache
        """
        if not self._cache_dir:
            return False
        
        cache_file = self._cache_dir / f"template_{spatial_key}.json"
        
        if not cache_file.exists():
            return False
        
        try:
            with open(cache_file, 'r') as f:
                template_dict = json.load(f)
            
            # Convert lists back to tuples where needed
            if 'dims_names' in template_dict and isinstance(template_dict['dims_names'], list):
                template_dict['dims_names'] = tuple(template_dict['dims_names'])
            if 'spatial_dims' in template_dict and isinstance(template_dict['spatial_dims'], list):
                template_dict['spatial_dims'] = tuple(template_dict['spatial_dims'])
            if 'variables' in template_dict and isinstance(template_dict['variables'], list):
                # variables should stay as list
                pass
            
            self._templates[spatial_key] = template_dict
            return True
        except (json.JSONDecodeError, KeyError, IOError):
            # Cache file corrupted or incompatible, ignore
            return False
    
    def clear_cache(self) -> None:
        """Clear all cached templates from disk."""
        if not self._cache_dir or not self._cache_dir.exists():
            return
        
        for cache_file in self._cache_dir.glob("template_*.json"):
            cache_file.unlink()
    
    @staticmethod
    def build_array(template_dict: dict, data:Optional[np.ndarray]=None) -> xr.DataArray:
        """
        Build a template xarray.DataArray from a template dictionary.
        
        Args:
            template_dict: Template dictionary with spatial metadata
            data: Optional numpy array or DataArray data to populate. If None, filled with nodata value
            
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
            step = template_dict['dims_steps'][dim]
            dim_values = template_dict.get('dims_values', {}).get(dim)
            if dim_values is not None:
                template[dim] = np.asarray(dim_values)
            elif isinstance(start, (int, float)):
                template[dim] = np.linspace(start, end, length)
            else:
                start_ts = TimeStep.from_unit(step).from_date(start)
                vals = [start_ts + i for i in range(length)]
                template[dim] = [val.start for val in vals]
                
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
        def align_to_template(array: xr.DataArray, template: xr.DataArray) -> xr.DataArray:
            """Align labelled data without losing cells to floating-point jitter.

            Spatial coordinates decoded independently from GRIB can differ from
            an otherwise identical template by a few ulps. Exact ``reindex``
            turns those cells into NaN. For equal-size numeric axes, snap only
            when the coordinates are equal within a very small fraction of the
            grid spacing; also handle the same axis in reverse order. Genuine
            grid differences still fall back to normal labelled reindexing.
            """
            template_dims = tuple(template.dims)
            aligned = array.transpose(*template_dims)

            for dim in template_dims:
                source = np.asarray(aligned[dim].values)
                target = np.asarray(template[dim].values)

                if source.shape == target.shape and np.issubdtype(source.dtype, np.number) and np.issubdtype(target.dtype, np.number):
                    if source.size > 1 and (np.issubdtype(source.dtype, np.floating) or np.issubdtype(target.dtype, np.floating)):
                        step = float(np.nanmedian(np.abs(np.diff(target.astype(float)))))
                        atol = max(step * 1e-8, 1e-10) if np.isfinite(step) else 1e-10
                    else:
                        atol = 0.0

                    if np.allclose(source, target, rtol=0.0, atol=atol, equal_nan=True):
                        aligned = aligned.assign_coords({dim: target})
                        continue

                    if source.size > 1 and np.allclose(source[::-1], target, rtol=0.0, atol=atol, equal_nan=True):
                        aligned = aligned.isel({dim: slice(None, None, -1)})
                        aligned = aligned.assign_coords({dim: target})
                        continue

                aligned = aligned.reindex({dim: target})

            return aligned

        if isinstance(data, xr.DataArray):
            attrs = data.attrs.copy()
            template = TemplateManager.build_array(template_dict)
            aligned = align_to_template(data, template)
            data = template.copy(data=aligned.data)
            data.attrs.update(attrs)
        elif isinstance(data, np.ndarray):
            data = TemplateManager.build_array(template_dict, data)
        elif isinstance(data, xr.Dataset):
            vars = template_dict['variables']
            template = TemplateManager.build_array(template_dict)
            das = {}
            for var in vars:
                aligned = align_to_template(data[var], template)
                da = template.copy(data=aligned.data)
                da.attrs.update(data[var].attrs)
                das[var] = da
            data = xr.Dataset(das)
        
        # Lazy import to avoid circular dependency
        from ..data.io_utils import set_type
        return set_type(data, read=True)
