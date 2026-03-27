"""
Base class for format mixins.

Defines the interface that all format mixins must implement for the Dataset class.
"""
from abc import ABC, abstractmethod
from typing import Any


class FormatMixin(ABC):
    """
    Abstract base class for format-specific functionality mixins.
    
    Format mixins handle data transformations specific to different file formats
    (raster, vector, table, text) and provide hooks in the Dataset read/write lifecycle.
    
    All format mixins must implement three methods:
    
    1. _init_format_properties(): Initialize format-specific state during Dataset.__init__
    2. _format_after_read(): Transform data after reading from storage
    3. _format_before_write(): Transform data before writing to storage
    
    The separation between format and storage concerns:
    - Format mixins handle: data transformations, validation, metadata, format-specific processing
    - Storage mixins handle: file I/O, network operations, path resolution
    
    Data flow:
        Read:  Storage retrieves → Format transforms (via _format_after_read)
        Write: Format prepares (via _format_before_write) → Storage writes
    """
    
    @abstractmethod
    def _init_format_properties(self):
        """
        Initialize format-specific properties.
        
        Called during Dataset.__init__ after basic properties are set.
        Use this to set up format-specific state (e.g., template managers for raster data).
        
        Example:
            For RasterMixin: Creates TemplateManager for spatial grids
            For VectorMixin: Could initialize spatial index
            For TableMixin: Could set default dtypes
        """
        pass
    
    @abstractmethod
    def _format_after_read(self, data: Any, full_key: str, **kwargs) -> Any:
        """
        Transform data immediately after reading from storage.
        
        This method is called by Dataset.read_data() after the storage layer
        has retrieved the raw data. Use it for format-specific post-processing.
        
        Args:
            data: Raw data from storage (_read_data result)
            full_key: Full path/key to the source file
            **kwargs: Additional context (as_is flag, etc.)
            
        Returns:
            Transformed data ready for application use
            
        Example transformations:
            Raster: Apply templates, straighten coordinates, convert nodata
            Vector: Validate CRS, repair geometries
            Table: Parse datetimes, set index
            Text: Decode, validate encoding
        """
        pass
    
    @abstractmethod
    def _format_before_write(self, data: Any, time, time_format: str, 
                            metadata: dict, **kwargs) -> Any:
        """
        Transform data before writing to storage.
        
        This method is called by Dataset.write_data() before the storage layer
        writes the data. Use it for format-specific preparation.
        
        Args:
            data: Application data to prepare for writing
            time: Timestamp for the data
            time_format: Format string for time serialization
            metadata: Metadata dictionary to attach
            **kwargs: Additional context (as_is flag, etc.)
            
        Returns:
            Transformed data ready for storage layer
            
        Example transformations:
            Raster: Apply template, set nodata values, add CRS
            Vector: Convert datetime columns for JSON, set metadata
            Table: Convert types, format for CSV/Parquet
            Text: Encode, serialize to JSON
        """
        pass
