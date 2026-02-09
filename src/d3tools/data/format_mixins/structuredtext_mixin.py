"""
Structured text-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store structured text data
(JSON, YAML, XML - currently focused on JSON).
"""
import json
import datetime as dt
import numpy as np
from typing import Optional, Any, Dict

from .base import FormatMixin


class StructuredTextMixin(FormatMixin):
    """
    Mixin for structured text (JSON) operations.
    
    Handles JSON files with serialization/deserialization and metadata.
    """
    
    def _init_format_properties(self):
        """
        Initialize structured text-specific properties.
        
        Called from Dataset.__init__ when format is JSON.
        """
        # JSON formats don't need special initialization
        pass
    
    def _format_after_read(self, data: Dict[str, Any], full_key: str, **kwargs) -> Dict[str, Any]:
        """
        Post-process structured text data after reading from storage.
        
        Args:
            data: Raw JSON/dict data from storage
            full_key: Full path/key to source file
            **kwargs: Additional arguments
            
        Returns:
            Processed dictionary ready for use
        """
        # Future: datetime parsing, schema validation
        return data
    
    def _format_before_write(self, data: Dict[str, Any], time, time_format: str,
                     metadata: dict, **kwargs) -> Dict[str, Any]:
        """
        Prepare structured text data for writing with format-specific logic.
        
        Args:
            data: Dictionary/JSON data to prepare
            time: Timestamp
            time_format: Format string for time
            metadata: Metadata dictionary
            **kwargs: Additional arguments
            
        Returns:
            Prepared dictionary ready for JSON serialization
        """
        # Prepare data for JSON serialization (convert datetime, numpy, etc.)
        data = self._prepare_for_json_serialization(data)
        
        return data
    
    def _prepare_for_json_serialization(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare data for JSON serialization.
        
        Converts non-serializable types (datetime, numpy arrays) to JSON-compatible formats.
        
        Args:
            data: Data dictionary to prepare
            
        Returns:
            Dictionary with serializable values
        """
        prepared = {}
        for key, value in data.items():
            if isinstance(value, np.ndarray):
                prepared[key] = value.tolist()
            elif isinstance(value, (dt.datetime, dt.date)):
                prepared[key] = value.isoformat()
            elif isinstance(value, np.datetime64):
                prepared[key] = np.datetime_as_string(value)
            else:
                prepared[key] = value
        return prepared
    
    def _load_json_with_append(self, path: str, new_data: Dict[str, Any]) -> Any:
        """
        Load existing JSON and append new data.
        
        Args:
            path: Path to existing JSON file
            new_data: New data to append
            
        Returns:
            Combined data structure
        """
        try:
            with open(path, 'r') as f:
                old_data = json.load(f)
            
            # If existing data is not a list, make it one
            if not isinstance(old_data, list):
                old_data = [old_data]
            
            old_data.append(new_data)
            return old_data
        except (FileNotFoundError, json.JSONDecodeError):
            # If file doesn't exist or is invalid, just return new data
            return new_data
