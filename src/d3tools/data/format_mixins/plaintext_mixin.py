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
    
    def _read_from_file(self, path: str, **kwargs) -> list[str]: 
        """
        Read plain text data from a file.
        
        Args:
            path: Local path to the source text file
            **kwargs: Additional arguments [not used in this method but passed for consistency]

        Returns:
            List of lines read from the text file
        """
        with open(path, 'r') as f:
            data = f.readlines()
        
        return data

    def _format_after_read(self, data: list[str], **kwargs) -> list[str]:
        """
        Post-process plain text data after reading from storage.
        
        Args:
            data: Raw text data from storage
            
        Returns:
            Processed text data ready for use
        """
        # Plain text typically doesn't need post-processing
        return data
    
    def _format_before_write(self, data: list[str], **kwargs) -> list[str]:
        """
        Prepare plain text data for writing with format-specific logic.
        
        Args:
            data: Text data to prepare (string or text-like)
            **kwargs: Additional arguments
            
        Returns:
            Prepared text data ready for writing
        """
        # Plain text typically doesn't need preparation
        # Could add timestamp prefixes or metadata headers here if needed
        return data
    
    def _write_to_file(self, data: list[str], path: str, append : bool = False, **kwargs):
        """
        Write plain text data to a file.
        
        Args:
            data: Text data prepared for writing (output of _format_before_write)
            path: Local path to the destination text file
            append: Whether to append to the file (True) or overwrite (False)
            **kwargs: Additional arguments [unused in this method but passed for consistency]

        Example:
            data = ["Line 1\n", "Line 2\n"]
            path = "/tmp/output.txt"
        """
        from ..io_utils import ensure_directory_exists
        ensure_directory_exists(path)

        mode = 'a' if append else 'w'
        with open(path, mode) as f:
            f.writelines(data)
