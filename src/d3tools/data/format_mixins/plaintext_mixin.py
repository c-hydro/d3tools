"""
Plain text-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store plain text data
(TXT files, logs, unstructured text).
"""
from typing import Optional

from .base import FormatMixin


class PlainTextMixin(FormatMixin):
    """
    Mixin for plain text file operations.
    
    Handles .txt files with simple read/write operations.
    """
    
    def _init_format_properties(self):
        """
        Initialize plain text-specific properties.
        
        Called from Dataset.__init__ when format is plain text.
        """
        # Plain text formats don't need special initialization
        pass
    
    def _format_after_read(self, data, full_key: str, **kwargs):
        """
        Post-process plain text data after reading from storage.
        
        Args:
            data: Raw text data from storage
            full_key: Full path/key to source file
            **kwargs: Additional arguments
            
        Returns:
            Processed text data ready for use
        """
        # Plain text typically doesn't need post-processing
        return data
    
    def _format_before_write(self, data, time, time_format: str, 
                     metadata: dict, **kwargs):
        """
        Prepare plain text data for writing with format-specific logic.
        
        Args:
            data: Text data to prepare (string or text-like)
            time: Timestamp
            time_format: Format string for time
            metadata: Metadata dictionary
            **kwargs: Additional arguments
            
        Returns:
            Prepared text data ready for writing
        """
        # Plain text typically doesn't need preparation
        # Could add timestamp prefixes or metadata headers here if needed
        return data
