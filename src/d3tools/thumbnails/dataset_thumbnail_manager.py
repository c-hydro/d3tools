"""Dataset-level thumbnail management.

This module keeps the configuration needed to render a single dataset's
thumbnail output: the color definitions, output destination, and optional
overlay or annotation settings. It wraps the lower-level ``Thumbnail``
renderer and exposes a small, dataset-oriented API for callers that already
resolve output file names through ``Dataset`` objects.
"""

import os
import xarray as xr
import geopandas as gpd
from typing import Optional, Union


class DatasetThumbnailManager:
    """Manage thumbnail generation for one dataset output.

    The manager stores the dataset references that define the color map,
    destination path, and optional overlay, then turns a data object into a
    saved thumbnail through the shared ``Thumbnail`` renderer.
    """
    
    def __init__(self, colors, destination, overlay=None, **options):
        """
        Initialize a manager with dataset references and rendering options.
        
        Args:
            colors: Dataset or mapping of Datasets used to resolve color files.
            destination: Dataset used to resolve output paths.
            overlay: Optional Dataset used as an overlay for generated images.
            **options: Thumbnail rendering options, such as ``field``,
                ``annotation``, and ``dpi``. ``field`` is consumed when the
                input is a GeoDataFrame; other options are passed to save.
        """
        self.colors = colors
        self.destination = destination
        self.overlay = overlay
        self.options = options
    
    @classmethod
    def from_dict(cls, config: dict, dataset_factory=None):
        """
        Build a manager from a mapping with ``colors`` and ``destination``.
        
        Args:
            config: Mapping containing ``colors`` and ``destination``, with
                optional ``overlay`` and thumbnail options.
            dataset_factory (Callable | None): Optional callable applied to ``colors`` and to
                ``destination`` or ``overlay`` only when those latter values
                are strings or dictionaries.
        
        Returns:
            DatasetThumbnailManager | None: A manager with remaining config entries stored as options, or
            ``None`` if either required key is absent.
        """
        if 'colors' not in config or 'destination' not in config:
            return None
        
        colors = config['colors']
        destination = config['destination']
        overlay = config.get('overlay')
        
        # Parse string paths to Datasets if factory is provided
        if dataset_factory is not None:
            colors = dataset_factory(colors)
            
            if isinstance(destination, (str, dict)):
                destination = dataset_factory(destination)
            
            if overlay is not None and isinstance(overlay, (str, dict)):
                overlay = dataset_factory(overlay)
        
        # Extract other options
        other_opts = {k: v for k, v in config.items() 
                     if k not in ['colors', 'destination', 'overlay']}
        
        return cls(colors, destination, overlay, **other_opts)
    
    def make_thumbnail(self, data: Union[xr.DataArray, dict, gpd.GeoDataFrame], **kwargs):
        """
        Generate, save, and return a thumbnail for dataset output.

        ``kwargs`` are used to resolve the color and destination datasets and
        are also passed to ``colors.update`` and ``destination.get_key``.
        Dictionary input selects its
        empty-string entry. GeoDataFrame input selects the configured
        ``field`` (default ``value``), keeps geometry, and renames that field
        to ``value`` before constructing the thumbnail. A configured overlay
        is added unless the ``overlay`` option already exists.
        
        Args:
            data: Raster DataArray, mapping whose ``''`` entry is a DataArray,
                or GeoDataFrame containing geometry and the selected value field.
            **kwargs (Any): Context values used when resolving dataset paths.
        
        Returns:
            Thumbnail: The ``Thumbnail`` instance after its ``save`` method has written
            the resolved destination.
        """
        from .thumbnail import Thumbnail
        
        options = self.options.copy()
        if self.overlay is not None and 'overlay' not in options:
            options['overlay'] = self.overlay
        
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
