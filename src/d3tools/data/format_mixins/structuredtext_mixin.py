"""
Structured text-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store structured text data
(JSON, YAML, XML - currently focused on JSON).
"""
import json
import datetime as dt
import numpy as np
from typing import Optional, Any, Dict
import geopandas as gpd

from .base import FormatMixin
from .vector_mixin import VectorMixin  # For GeoJSON detection and delegation

class StructuredTextMixin(FormatMixin):
    """
    Mixin for structured text (JSON) operations.
    
    Handles JSON files with serialization/deserialization and metadata.
    """

    json_warning_issued = False  # Class-level flag to track if warning has been issued
    
    def _init_format_properties(self):
        """
        Initialize structured text-specific properties.
        
        Called from Dataset.__init__ when format is JSON.
        """
        # JSON formats don't need special initialization
        pass

    def _read_from_file(self, path: str, **kwargs) -> Dict[str, Any]:
        """
        Read raw JSON data from a file.
        
        Args:
            path: Full path to the source JSON file (must be local, resolved by storage mixin)
            **kwargs: Additional arguments [unused in this method but passed for consistency]
        Returns:
            Raw dictionary data read from the JSON file
        """
        with open(path, 'r') as f:
            data = json.load(f)

        # check if data is actually a list of features (e.g., GeoJSON)
        if isinstance(data, dict) and 'features' in data.keys():
            self._set_format_to_geojson()

            # Initialize vector properties for GeoJSON handling
            VectorMixin._init_format_properties(self)  
            return VectorMixin._read_from_file(self,path)

        return data
    
    def _format_after_read(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Post-process structured text data after reading from storage.
        
        Args:
            data: Raw JSON/dict data from storage
            full_key: Full path/key to source file
            **kwargs: Additional arguments
            
        Returns:
            Processed dictionary ready for use
        """
        if self.format == 'geojson':
            # Delegate to VectorMixin for GeoJSON post-processing
            return VectorMixin._format_after_read(self, data, **kwargs)

        # Future: datetime parsing, schema validation
        return data
    
    def _format_before_write(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Prepare structured text data for writing with format-specific logic.
        
        Args:
            data: Dictionary/JSON data to prepare
            **kwargs: Additional arguments
            
        Returns:
            Prepared dictionary ready for JSON serialization
        """

        if self.format == 'geojson':
            # Delegate to VectorMixin for GeoJSON preparation
            return VectorMixin._format_before_write(self, data, **kwargs)

        # Prepare data for JSON serialization (convert datetime, numpy, etc.)
        def convert_value(value):
            """Recursively convert non-JSON-serializable types."""
            if isinstance(value, np.ndarray):
                return value.tolist()
            elif isinstance(value, (dt.datetime, dt.date)):
                return value.isoformat()
            elif isinstance(value, np.datetime64):
                return np.datetime_as_string(value)
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, (list, tuple)):
                return [convert_value(item) for item in value]
            else:
                return value
        
        prepared = convert_value(data)
        
        return prepared
    
    def _write_to_file(self, data: Dict[str, Any]|gpd.GeoDataFrame, path: str, append: bool = False, **kwargs) -> Any:
        """
        Write data to a file, optionally appending to existing data.
        
        Args:
            data: Dictionary/JSON data to write
            path: Path to the output file
            append: Whether to append to an existing file
            **kwargs: Additional arguments [unused in this method but passed for consistency]
            
        Returns:
            Combined data structure
        """
        from ..io_utils import ensure_directory_exists
        ensure_directory_exists(path)

        if isinstance(data, gpd.GeoDataFrame):
            self._set_format_to_geojson()

        if self.format == 'geojson':
            # Delegate to VectorMixin for GeoJSON writing
            return VectorMixin._write_to_file(self, data, path, append, **kwargs)

        # if append, open the existing file and append the new data to it
        if append:
            with open(path, 'r') as f:
                old_data = json.load(f)
            old_data = [old_data] if not isinstance(old_data, list) else old_data
            old_data.append(data)
            data = old_data
        
        # write the data to a (geo)json file
        with open(path, 'w') as f:
            json.dump(data, f, indent = 4)

    def set_metadata(self, data, time = None, time_format = '%Y-%m-%d', **kwargs):
        """
        Set metadata for the data.
        """
        if not isinstance(data, gpd.GeoDataFrame):
            return data
        
        self._set_format_to_geojson()
        if hasattr(data, 'attrs'):
            if 'long_name' in data.attrs:
                data.attrs.pop('long_name')
            kwargs.update(data.attrs)
        
        metadata = kwargs.copy()
        metadata['time_produced'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if time is not None:
            datatime = self.get_time_signature(time)
            metadata['time'] = datatime.strftime(time_format)

        name = metadata.get('name', self.name)
        if 'long_name' in metadata:
            metadata.pop('long_name')

        data.attrs.update(metadata)

        return data

    def _set_format_to_geojson(self):
        # Detected GeoJSON structure - delegate to VectorMixin
        if not self.json_warning_issued:
            import warnings
            warnings.warn(
                f"File '{self.key_pattern}' appears to be GeoJSON but has .json extension. "
                f"Most functionality should work, but some GeoJSON-specific features may not be available. "
                f"Consider renaming to .geojson or specifying format='geojson' explicitly.",
                UserWarning
            )
            self.json_warning_issued = True

        self.format = 'geojson'
