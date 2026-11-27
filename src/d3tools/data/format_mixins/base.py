"""
Base class for format mixins.

Defines the interface that all format mixins must implement for the Dataset class.
"""
from typing import Any, Optional


class FormatMixin:
    """
    Base class for format-specific functionality mixins.
    
    Format mixins handle data transformations specific to different file formats
    (raster, vector, table, text) and provide hooks in the Dataset read/write lifecycle.
    
    All format mixins must implement three methods:
    
    1. _init_format_properties(): Initialize format-specific state during Dataset.__init__
    2. _read_file(): Read raw data from a file
    3. _format_after_read(): Transform data after reading from storage
    4. _format_before_write(): Transform data before writing to storage
    5. _write_file(): Write data to a file

    Additional methods that are defined here for all formats can be added as needed (e.g., validation, metadata handling).
    All these methods are defined here just returning the data,
    they can be overridden by the specific mixins to add format-specific logic.
    # metadata handling:
        - set_metadata(): method to set metadata
        - update_metadata(): method to update existing metadata
        - get_metadata(): method to retrieve metadata
    # validation:
        - validate_data(): method to validate data before writing
    
    The separation between format and storage concerns:
    - Format mixins handle: data transformations, validation, metadata, format-specific processing, local file I/O
    - Storage mixins handle: network operations, path resolution

    Data flow:
        Read:  Storage retrieves → Format transforms (via _format_after_read)
        Write: Format prepares (via _format_before_write) → Storage writes
    """

    format: str  # Format name
    
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
        raise NotImplementedError("Must implement _init_format_properties in format mixin")
    
    def _read_from_file(self, path: str, **kwargs) -> Any:
        """
        Read raw data from a file.
        
        This method is called by Dataset.read_data() after resolving the full path/key.
        Use it to read data using format-specific libraries (e.g., rasterio, geopandas).
        
        Args:
            path: Full path to the source file (must be local, resolved by storage mixin)
            **kwargs: Additional arguments (e.g., as_is flag)
            
        Returns:
            Raw data read from the file, to be processed by _format_after_read
        """
        raise NotImplementedError("Must implement _read_from_file in format mixin")

    def _format_after_read(self, data: Any, **kwargs) -> Any:
        """
        Transform data immediately after reading from storage.
        
        This method is called by Dataset.read_data() after the storage layer
        has retrieved the raw data. Use it for format-specific post-processing.
        
        Args:
            data: Raw data from storage (_read_data result)
            **kwargs: Additional context (as_is flag, etc.)
            
        Returns:
            Transformed data ready for application use
            
        Example transformations:
            Raster: Apply templates, straighten coordinates, convert nodata
            Vector: Validate CRS, repair geometries
            Table: Parse datetimes, set index
            Text: Decode, validate encoding
        """
        raise NotImplementedError("Must implement _format_after_read in format mixin")

    def _format_before_write(self, data: Any, **kwargs) -> Any:
        """
        Transform data before writing to storage.
        
        This method is called by Dataset.write_data() before the storage layer
        writes the data. Use it for format-specific preparation.
        
        Args:
            data: Application data to prepare for writing
            **kwargs: Additional context (as_is flag, etc.)
            
        Returns:
            Transformed data ready for storage layer
            
        Example transformations:
            Raster: Apply template, set nodata values, add CRS
            Vector: Convert datetime columns for JSON, set metadata
            Table: Convert types, format for CSV/Parquet
            Text: Encode, serialize to JSON
        """
        raise NotImplementedError("Must implement _format_before_write in format mixin")

    def _write_to_file(self, data: Any, path: str, **kwargs):
        """
        Write data to a file.
        
        This method is called by Dataset.write_data() after _format_before_write has
        prepared the data. Use it to write data using format-specific libraries.
        
        Args:
            data: Data prepared for writing (output of _format_before_write)
            path: Full path to the destination file (must be local, resolved by storage mixin)
            **kwargs: Additional arguments (e.g., as_is flag)
            
        Example:
            For RasterMixin: Use rioxarray to write GeoTIFF or xarray to write to netCDF
            For VectorMixin: Use geopandas to write GeoJSON
            For TableMixin: Use pandas to write CSV/Parquet
            For TextMixin: Write text or JSON files
        """
        raise NotImplementedError("Must implement _write_to_file in format mixin")

    def set_metadata(self, data: Any, **kwargs) -> Any:
        """
        Add metadata to the data object in a format-specific way.
        
        This is a helper method that can be used by _format_before_write to attach
        metadata to the data object before writing. The implementation will depend
        on the data structure (e.g., xarray Dataset attributes, GeoDataFrame attrs).
        
        Args:
            data: Data object to which metadata should be added
            **kwargs: Metadata key-value pairs to add
            
        Returns:
            Data object with metadata attached
        """
        # Default implementation does nothing, override in mixins as needed
        return data

    def update_metadata(self, data: Any, **kwargs) -> Any:
        """
        Update existing metadata on the data object.
        
        This is a helper method that can be used to update metadata on an existing
        data object. The implementation will depend on the data structure.
        
        Args:
            data: Data object whose metadata should be updated
            **kwargs: Metadata key-value pairs to update
            
        Returns:
            Data object with updated metadata
        """
        # Default implementation does nothing, override in mixins as needed
        return data
    
    def get_metadata(self, data: Any, keys: Optional[list|str] = None) -> dict:
        """
        Retrieve metadata from the data object.
        
        This is a helper method to extract metadata from a data object in a format-specific way.
        
        Args:
            data: Data object from which to retrieve metadata
            keys: Optional list of metadata keys to retrieve (if None, retrieve all)
            
        Returns:
            Metadata dictionary extracted from the data object
        """
        # Default implementation returns empty dict, override in mixins as needed
        return {}
    
    def validate_data(self, data: Any, **kwargs) -> Any:
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
        # Default implementation does nothing, override in RasterMixin as needed
        return data