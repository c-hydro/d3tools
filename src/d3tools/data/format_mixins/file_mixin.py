"""
Plain text-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that should not be opened, nus simply handled as files
(PNG images, PDF documents).
"""
import os
from typing import Optional
from .base import FormatMixin

class FileMixin(FormatMixin):
    """
    Mixin for file operations.
    
    Handles files that should not be opened, such as PNG images and PDF documents.
    """
    
    def _init_format_properties(self):
        """
        Initialize file-specific properties.
        
        Called from Dataset.__init__ when format is file-based.
        """
        # File formats don't need special initialization
        pass
    
    def _read_from_file(self, path: str, **kwargs) -> list[str]: 
        """
        Read for these files simply returns the file path.
        
        Args:
            path: Local path to the source file

        Returns:
            List of lines read from the text file
        """
        return path

    def _format_after_read(self, data: list[str], **kwargs) -> list[str]:
        """
        Post-process files after reading from storage.
        
        Args:
            data: Raw data from storage
            
        Returns:
            Processed data ready for use
        """
        # No post-processing needed for file paths
        return data
    
    def _format_before_write(self, data: list[str], **kwargs) -> list[str]:
        """
        Prepare data for writing with format-specific logic.
        
        Args:
            data: Text data to prepare (string or text-like)
            **kwargs: Additional arguments
            
        Returns:
            Prepared data ready for writing
        """
        # No preparation needed for file paths
        return data
    
    def _write_to_file(self, data: list[str], path: str, **kwargs):
        """
        Write data to a file in the case of files means to copy the file over.
        
        Args:
            data: file prepared for writing (output of _format_before_write)
            path: Local path to the destination text file
            **kwargs: Additional arguments (e.g., encoding)

        """

        from ..io_utils import ensure_directory_exists
        ensure_directory_exists(path)

        # write the data to a png or pdf (i.e. move the file)
        os.rename(data, path)
