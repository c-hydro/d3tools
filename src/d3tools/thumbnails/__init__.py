"""Thumbnail generation utilities for dataset outputs.

This package exposes the public classes used to render single thumbnails,
combine many raster thumbnails into one output, and manage per-dataset
thumbnail creation from configured data sources.
"""

from .thumbnail import *
from .thumbnail_collection import *
from .dataset_thumbnail_manager import DatasetThumbnailManager

__all__ = ['Thumbnail', 'ThumbnailCollection', 'DatasetThumbnailManager']