"""
Table-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store tabular data
(CSV, Parquet, pandas DataFrames).
"""
import pandas as pd
from typing import Optional

from .base import FormatMixin


class TableMixin(FormatMixin):
    """
    Mixin for table/dataframe-specific operations.
    
    Handles CSV and Parquet formats with DataFrame operations.
    """
    
    def _init_format_properties(self):
        """
        Initialize table-specific properties.
        
        Called from Dataset.__init__ when format is table-based.
        """
        # Table formats don't need special initialization like templates
        # This is a placeholder for consistency with other mixins
        pass
    
    def _format_after_read(self, data: pd.DataFrame, full_key: str, **kwargs) -> pd.DataFrame:
        """
        Post-process table data after reading from storage.
        
        Args:
            data: Raw DataFrame from storage
            full_key: Full path/key to source file
            **kwargs: Additional arguments
            
        Returns:
            Processed DataFrame ready for use
        """
        # Tables typically don't need post-processing
        # Could add: datetime parsing, type conversion, index setting
        return data
    
    def _format_before_write(self, data: pd.DataFrame, time, time_format: str, 
                     metadata: dict, **kwargs) -> pd.DataFrame:
        """
        Prepare table data for writing with format-specific logic.
        
        Args:
            data: DataFrame to prepare
            time: Timestamp
            time_format: Format string for time
            metadata: Metadata dictionary
            **kwargs: Additional arguments
            
        Returns:
            Prepared DataFrame ready for writing
        """
        # Set metadata if data supports attrs (some pandas versions/backends)
        if hasattr(data, 'attrs'):
            data = self.set_metadata(data, time, time_format, **metadata)
        
        return data
