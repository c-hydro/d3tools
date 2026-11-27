"""
Table-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store tabular data
(CSV, Parquet, pandas DataFrames).
"""
import os
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

    def _read_from_file(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Read table data from a file.
        
        Args:
            path: Local path to the source table file
            **kwargs: Additional arguments [not used in this method but passed for consistency]

        Returns:
            DataFrame read from the table file
        """

        # future: use kwargs for options like encoding, delimiter, etc. for CSV
        # currently it conflicts with the tags that are passed as kwargs

        if self.format == 'csv':
            return pd.read_csv(path)
        elif self.format == 'parquet':
            return pd.read_parquet(path)

    def _format_after_read(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Post-process table data after reading from storage.
        
        Args:
            data: Raw DataFrame from storage
            **kwargs: Additional arguments
            
        Returns:
            Processed DataFrame ready for use
        """
        # Tables typically don't need post-processing
        # Could add: datetime parsing, type conversion, index setting
        return data
    
    def _format_before_write(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
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
        return data

    def _write_to_file(self, data: pd.DataFrame, path: str, append: bool = False, **kwargs):
        """
        Write table data to a file.
        
        Args:
            data: DataFrame prepared for writing
            path: Local path to the destination table file
            append: Whether to append to the file if it exists
            **kwargs: Additional arguments [unused in this method but passed for consistency]

        """

        # future: use kwargs for options like encoding, delimiter, etc. for CSV
        # currently it conflicts with the tags that are passed as kwargs

        from ..io_utils import ensure_directory_exists
        ensure_directory_exists(path)

        if self.format == 'csv':
            mode = 'a' if append else 'w'
            data.to_csv(path, mode = mode, index = False, header = not append)
        elif self.format == 'parquet':
            if append and os.path.exists(path):
                # For parquet, we need to read existing data and concatenate before writing
                # (there is no native append mode)
                existing_data = pd.read_parquet(path)
                data = pd.concat([existing_data, data], ignore_index=True)
            data.to_parquet(path, index = False, compression='snappy')