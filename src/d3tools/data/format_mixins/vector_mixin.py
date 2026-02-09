"""
Vector-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store vector/geometry data
(Shapefiles, GeoJSON, GeoPackage, GeoDataFrames).
"""
import geopandas as gpd
import datetime as dt
import numpy as np
import json
from typing import Optional

from .base import FormatMixin


class VectorMixin(FormatMixin):
    """
    Mixin for vector/geometry-specific operations.
    
    Handles shapefile and GeoJSON formats with GeoDataFrame operations.
    """
    
    def _init_format_properties(self):
        """
        Initialize vector-specific properties.
        
        Called from Dataset.__init__ when format is vector-based.
        """
        # Vector formats don't need special initialization
        # Future: could add spatial indexing, CRS validation, etc.
        pass
    
    def _format_after_read(self, data: gpd.GeoDataFrame, full_key: str, **kwargs) -> gpd.GeoDataFrame:
        """
        Post-process vector data after reading from storage.
        
        Args:
            data: Raw GeoDataFrame from storage
            full_key: Full path/key to source file
            **kwargs: Additional arguments
            
        Returns:
            Processed GeoDataFrame ready for use
        """
        # Future: CRS validation, geometry repair, spatial indexing
        return data
    
    def _format_before_write(self, data: gpd.GeoDataFrame, time, time_format: str,
                     metadata: dict, **kwargs) -> gpd.GeoDataFrame:
        """
        Prepare vector data for writing with format-specific logic.
        
        Args:
            data: GeoDataFrame to prepare
            time: Timestamp
            time_format: Format string for time
            metadata: Metadata dictionary
            **kwargs: Additional arguments
            
        Returns:
            Prepared GeoDataFrame ready for writing
        """
        # Handle GeoDataFrame-specific metadata
        if isinstance(data, gpd.GeoDataFrame):
            data = self.set_metadata(data, time, time_format, **metadata)
        
        return data
    
    def _prepare_geodataframe_for_json(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Prepare GeoDataFrame for JSON serialization.
        
        Converts datetime columns to ISO format strings.
        
        Args:
            data: GeoDataFrame to prepare
            
        Returns:
            GeoDataFrame with serializable columns
        """
        data = data.copy()
        for col in data.columns:
            if len(data) > 0:
                first_val = data[col].iloc[0]
                if isinstance(first_val, np.datetime64):
                    data[col] = data[col].apply(lambda x: x.astype('O'))
                if isinstance(first_val, (dt.datetime, dt.date)):
                    data[col] = data[col].apply(lambda x: x.isoformat())
        return data
