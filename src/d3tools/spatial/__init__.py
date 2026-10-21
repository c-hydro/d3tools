"""Spatial helpers for CRS-aware raster bounds and template-based alignment.

The package re-exports the primary public API used by dataset and workflow code:
``BoundingBox`` for raster bounds and coordinate transforms, ``TemplateManager``
for rebuilding spatial metadata, and the CRS/cropping helpers from
``space_utils``.
"""

from .bounding_box import BoundingBox
from .space_utils import *
from .template_manager import TemplateManager
