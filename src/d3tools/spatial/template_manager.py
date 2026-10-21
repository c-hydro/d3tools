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
    Store, cache, and apply spatial templates for dataset data.

    Templates record spatial metadata such as CRS, dimension names, coordinate
    ranges, and fill value so arrays can be reconstructed with consistent spatial
    geometry and coordinate alignment. The manager can keep templates in memory,
    persist them to JSON cache files, and rebuild arrays or datasets from them.

    Attributes:
        REQUIRED_KEYS: Keys required by template validation.
        _templates: Mapping from spatial keys to template dictionaries.
        _cache_dir: Optional directory used for JSON template caching.
        _validate: Whether ``set`` validates extracted templates.
    """
    
    # Required keys for valid templates
    REQUIRED_KEYS = {'crs', '_FillValue', 'dims_names', 'spatial_dims', 
                     'dims_starts', 'dims_ends', 'dims_steps', 'dims_lengths'}
    
    def __init__(self, cache_dir: Optional[str] = None, validate: bool = True):
        """
        Initialize an empty template manager.
        
        Args:
            cache_dir: Optional directory for cached JSON templates. If provided,
                the directory is created when it does not exist.
            validate: Whether templates are validated when stored.

        Creating the manager may create ``cache_dir`` on disk.
        """
        self._templates: dict[str, dict] = {}
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._validate = validate
        
        if self._cache_dir:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, spatial_key: str = '__tile__', load_from_cache: bool = True) -> Optional[dict]:
        """
        Return the template for a spatial key, checking memory before disk.
        
        Args:
            spatial_key: Identifier for a spatial unit, defaulting to ``'__tile__'``.
            load_from_cache: If true and the key is absent from memory, attempt to
                load its JSON cache file when caching is configured.
            
        Returns:
            The stored template dictionary, or ``None`` if it is unavailable.

        A successful cache load also stores the template in memory.
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
        Extract spatial metadata from sample data and store it under a key.
        
        Args:
            templatearray: Sample ``xarray.DataArray`` or ``xarray.Dataset``. For
                a Dataset, metadata comes from its first data variable and all
                variable names are recorded.
            spatial_key: Identifier for a spatial unit, defaulting to ``'__tile__'``.
            
        Raises:
            TemplateValidationError: If validation is enabled and extracted
                metadata fails validation.

        The template is stored in memory and, when configured, written to the
        manager's JSON cache. A missing CRS is represented using EPSG:4326.
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
            'dims_lengths': {}
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
        Check whether a template key, or any in-memory template, exists.
        
        Args:
            spatial_key: Key to check, or ``None`` to check whether the in-memory
                template mapping is nonempty.
            
        Returns:
            ``True`` when the requested key exists, otherwise ``False``.

        This checks memory only and does not load from disk.
        """
        if spatial_key is None:
            return len(self._templates) > 0
        return spatial_key in self._templates
    
    def get_all(self) -> dict[str, dict]:
        """
        Return a shallow copy of the in-memory template mapping.
        
        Returns:
            A new dictionary mapping spatial keys to their template dictionaries.
            The nested template dictionaries are shared with this manager.
        """
        return self._templates.copy()
    
    def clear(self, spatial_key: Optional[str] = None) -> None:
        """
        Remove one in-memory template or clear all in-memory templates.
        
        Args:
            spatial_key: Key to remove, or ``None`` to clear every in-memory entry.

        This does not remove corresponding cache files from disk.
        """
        if spatial_key is None:
            self._templates.clear()
        else:
            self._templates.pop(spatial_key, None)
    
    def copy(self, deep: bool = False) -> 'TemplateManager':
        """
        Create another manager with the same cache and validation settings.
        
        Args:
            deep: If true, recursively copy stored templates. Otherwise copy only
                the outer mapping, sharing nested template dictionaries.
            
        Returns:
            A new manager. Its cache directory is the same path, and initializing
            it may create that directory if it no longer exists.
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
        Yield the keys currently stored in memory.
        
        Yields:
            Spatial key strings in dictionary insertion order.
        """
        yield from self._templates.keys()
    
    def _validate_template(self, template_dict: dict, spatial_key: str) -> None:
        """Validate required template keys and values, raising on invalid metadata."""
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
        """Write a template as JSON to the configured disk cache."""
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
        Load a cached JSON template into memory.
        
        Args:
            spatial_key: Spatial key for the template
            
        Returns:
            ``True`` if a readable cache file was loaded; otherwise ``False``.

        Invalid JSON, incompatible cache content, and I/O errors are ignored and
        return ``False``. Tuple-valued dimension metadata is restored from JSON lists.
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
        """Delete matching ``template_*.json`` files from the configured cache directory."""
        if not self._cache_dir or not self._cache_dir.exists():
            return
        
        for cache_file in self._cache_dir.glob("template_*.json"):
            cache_file.unlink()
    
    @staticmethod
    def build_array(template_dict: dict, data:Optional[np.ndarray]=None) -> xr.DataArray:
        """
        Reconstruct a raster-aware DataArray from a stored spatial template.

        Args:
            template_dict: Mapping containing CRS, fill value, dimension names,
                spatial dimensions, lengths, starts, ends, and steps.
            data: Optional NumPy array of values. If omitted, the array is filled
                with the template ``_FillValue``; provided data is reshaped to the
                template dimensions.

        Returns:
            A ``xarray.DataArray`` with reconstructed coordinates, CRS, spatial
            dimensions, and ``crs``/``_FillValue`` attributes.

        Note:
            Numeric dimensions are reconstructed with ``linspace``; datetime-like
            dimensions are rebuilt from their stored start and step.
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
            if isinstance(start, float):
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
        Replace supported input data coordinates and CRS using a template.

        Args:
            data: NumPy array, DataArray, or Dataset. Dataset variables are taken
                from the template's ``variables`` entry.
            template_dict: Template dictionary with spatial metadata.

        Returns:
            A DataArray or Dataset rebuilt with template coordinates and CRS and
            passed through ``data.io_utils.set_type``. Existing attributes are
            retained for DataArray inputs and their variables.

        Note:
            NumPy input is converted to a DataArray. For Dataset input, only the
            variables named by the template are rebuilt into the returned Dataset.
        """
        if isinstance(data, xr.DataArray):
            attrs = data.attrs.copy()
            data = TemplateManager.build_array(template_dict, data.data)
            data.attrs.update(attrs)
        elif isinstance(data, np.ndarray):
            data = TemplateManager.build_array(template_dict, data)
        elif isinstance(data, xr.Dataset):
            vars = template_dict['variables']
            template = TemplateManager.build_array(template_dict, data[vars[0]].data)
            das = {}
            for var in vars:
                da = template.copy(data=data[var])
                da.attrs.update(data[var].attrs)
                das[var] = da
            data = xr.Dataset(das)
        
        # Lazy import to avoid circular dependency
        from ..data.io_utils import set_type
        return set_type(data, read=True)
