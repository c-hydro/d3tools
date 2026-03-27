"""
DatasetThumbnailManager - Handles thumbnail generation for individual datasets.

This class encapsulates thumbnail creation logic that was previously embedded
in the Dataset class, separating concerns and making Dataset simpler.

Note: This is dataset-specific. Future workflow-level thumbnail management
will use a separate ThumbnailManager class.
"""

import os
import xarray as xr
import geopandas as gpd
from typing import Optional, Union


class DatasetThumbnailManager:
    """
    Manages thumbnail generation for dataset outputs.
    
    Holds configuration for colors, destination, overlays, etc. and provides
    a simple interface for generating thumbnails from data.
    """
    
    def __init__(self, colors, destination, overlay=None, **options):
        """
        Initialize thumbnail manager.
        
        Args:
            colors: Dataset or dict of Datasets pointing to color definition files
            destination: Dataset defining where thumbnails should be saved
            overlay: Optional Dataset for overlay data
            **options: Additional thumbnail options (field, annotation, etc.)
        """
        self.colors = colors
        self.destination = destination
        self.overlay = overlay
        self.options = options
    
    @classmethod
    def from_dict(cls, config: dict, dataset_factory=None):
        """
        Create DatasetThumbnailManager from a configuration dictionary.
        
        Args:
            config: Dictionary with 'colors', 'destination', and optional 'overlay' keys
            dataset_factory: Function to parse string paths into Dataset objects.
                           Should have signature: func(path_str, **defaults) -> Dataset
        
        Returns:
            ThumbnailManager instance or None if config is invalid
        """
        if 'colors' not in config or 'destination' not in config:
            return None
        
        colors = config['colors']
        destination = config['destination']
        overlay = config.get('overlay')
        
        # Parse string paths to Datasets if factory is provided
        if dataset_factory is not None:
            colors = dataset_factory(colors)
            
            if isinstance(destination, str):
                destination = dataset_factory(destination)
            
            if overlay is not None and isinstance(overlay, str):
                overlay = dataset_factory(overlay)
        
        # Extract other options
        other_opts = {k: v for k, v in config.items() 
                     if k not in ['colors', 'destination', 'overlay']}
        
        return cls(colors, destination, overlay, **other_opts)
    
    def make_thumbnail(self, data: Union[xr.DataArray, dict, gpd.GeoDataFrame], **kwargs):
        """
        Generate a thumbnail from data.
        
        Args:
            data: Data to visualize (DataArray, dict of DataArrays, or GeoDataFrame)
            **kwargs: Additional context (time, tags, etc.) for substitution
        
        Returns:
            Thumbnail object with saved thumbnail file path
        """
        from .thumbnail import Thumbnail
        
        options = self.options.copy()
        
        col_def = self.colors.update(**kwargs)
        if isinstance(data, dict):
            data = data['']
        elif isinstance(data, gpd.GeoDataFrame):
            field = options.pop('field', 'value')
            data = data[['geometry', field]].rename(columns={field: 'value'})
        thumbnail = Thumbnail(data, col_def)
        
        # Determine destination path
        destination_path = self.destination.get_key(**kwargs)
        
        # Save thumbnail
        thumbnail.save(destination_path, **options)
        
        return thumbnail
