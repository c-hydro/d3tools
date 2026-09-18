import numpy as np
import rioxarray
import xarray as xr
import os
from typing import Optional

import geopandas as gpd
from pandas.api.types import is_numeric_dtype

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from ..data import Dataset
from .colors import parse_colors, keep_used_colors, create_colormap

#TODO TEST
class Thumbnail:
    DEFAULT_VECTOR_SHORT_SIDE = 900
    MAX_VECTOR_LONG_SIDE = 2400

    def __init__(self, data:str|xr.DataArray|gpd.GeoDataFrame, color_definition_file:str):
        if isinstance(data, xr.DataArray):
            self.src = data
            self.type = 'raster'

        elif isinstance(data, gpd.GeoDataFrame):
            self.src = data
            self.type = 'vector'

        elif isinstance(data, str):
            self.raster_file = data
            self.src = rioxarray.open_rasterio(data)
            self.type = 'raster'

        else:
            raise TypeError(f"Unsupported data type: {type(data)}. Expected str, xr.DataArray, or gpd.GeoDataFrame.")

        # parse the color definition file
        self.txt_file = color_definition_file
        all_breaks, self.all_colors, all_labels = parse_colors(self.txt_file)
        self.bins = self._get_bins(all_breaks)
        self.nan_class = len(self.bins) + 1

        if self.type == 'vector':
            if self.src.empty:
                raise ValueError("Cannot create a thumbnail from an empty GeoDataFrame.")
            if 'value' not in self.src.columns:
                raise ValueError("GeoDataFrame thumbnail input must include a 'value' column.")
            if not is_numeric_dtype(self.src['value']):
                raise ValueError("GeoDataFrame thumbnail 'value' column must be numeric.")

            self.crs = self.src.crs
            self.missing_mask = self.src['value'].isna().to_numpy()
            self.allnan = bool(np.all(self.missing_mask))
            self.digital_src = self.discretize(all_breaks)

            self.breaks = np.unique(self.digital_src["value_discrete"])

            bounds = tuple(self.src.total_bounds)
            self.shape = self._shape_from_bounds(bounds)
            self.extent = self._extent_from_bounds(bounds)

        elif self.type == 'raster':
            self.src = self.src.squeeze()
            self.img = np.asarray(self.src.data)
            if self.img.ndim != 2:
                raise ValueError("Raster thumbnail input must be a single band. Select one band before creating a Thumbnail.")

            self.nan_value = self.src.rio.nodata
            self.missing_mask = self._missing_mask(self.img, self.nan_value)
            self.allnan = bool(np.all(self.missing_mask))

            self.transform = self.src.rio.transform()
            self.shape = self.img.shape
            self.crs = self.src.rio.crs

            self.extent = (self.transform[2], self.transform[2] + self.transform[0]*self.img.shape[1],
                           self.transform[5] + self.transform[4]*self.img.shape[0], self.transform[5])

            self.digital_img = self.discretize(all_breaks)
            self.breaks = np.unique(self.digital_img)
                
        has_nans = bool(np.any(self.missing_mask))

        self.all_labels = all_labels.copy()
        all_labels.append('nan')
        self.labels = [all_labels[i] for i in range(min(self.breaks), max(self.breaks)+1)]

        if self.allnan:
            self.colors = []
            self.colormap = None
        else:
            self.colors = keep_used_colors(self.breaks, self.all_colors)
            self.colormap = create_colormap(self.colors, include_nan = has_nans)

    def _get_bins(self, breaks: list) -> list[float]:
        if len(breaks) == 0:
            return []
        if breaks[-1] == 'inf':
            return [float(pos) for pos in breaks[:-1]]
        return [float(pos) for pos in breaks]

    def _missing_mask(self, data: np.ndarray, nodata) -> np.ndarray:
        try:
            missing = np.isnan(data)
        except TypeError:
            missing = np.zeros(data.shape, dtype=bool)

        if nodata is not None:
            missing = missing | np.isclose(data, nodata, equal_nan=True)

        return missing

    def _shape_from_bounds(self, bounds: tuple[float, float, float, float]) -> tuple[int, int]:
        x_min, y_min, x_max, y_max = bounds
        width = x_max - x_min
        height = y_max - y_min

        if not np.isfinite(width) or not np.isfinite(height) or width <= 0 or height <= 0:
            return (self.DEFAULT_VECTOR_SHORT_SIDE, self.DEFAULT_VECTOR_SHORT_SIDE)

        if width >= height:
            shape_height = self.DEFAULT_VECTOR_SHORT_SIDE
            shape_width = int(np.ceil(self.DEFAULT_VECTOR_SHORT_SIDE * width / height))
        else:
            shape_width = self.DEFAULT_VECTOR_SHORT_SIDE
            shape_height = int(np.ceil(self.DEFAULT_VECTOR_SHORT_SIDE * height / width))

        long_side = max(shape_height, shape_width)
        if long_side > self.MAX_VECTOR_LONG_SIDE:
            scale = self.MAX_VECTOR_LONG_SIDE / long_side
            shape_height = max(1, int(round(shape_height * scale)))
            shape_width = max(1, int(round(shape_width * scale)))

        return (shape_height, shape_width)

    def _extent_from_bounds(self, bounds: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        x_min, y_min, x_max, y_max = bounds
        width = x_max - x_min
        height = y_max - y_min

        if not np.isfinite(width) or width <= 0:
            x_center = x_min if np.isfinite(x_min) else 0
            x_min = x_center - 0.5
            x_max = x_center + 0.5
        if not np.isfinite(height) or height <= 0:
            y_center = y_min if np.isfinite(y_min) else 0
            y_min = y_center - 0.5
            y_max = y_center + 0.5

        return (x_min, x_max, y_min, y_max)

    def discretize(self, breaks: list, nan_value = np.nan):
        # Create an array of bins from the positions
        if len(self.bins) == 0:
            return np.zeros_like(self.img)
        else:
            bins = self.bins

        if self.type == 'raster':
            # Discretize the raster values
            raster_discrete = np.digitize(self.img, bins, right = True)

            # Set the nan values to the last bin
            raster_discrete = np.where(self.missing_mask, self.nan_class, raster_discrete)

            return raster_discrete

        elif self.type == 'vector':
            # Discretize the vector 'value' column
            values = self.src['value'].to_numpy()
            vector_discrete = np.digitize(values, bins, right=True)
            # Set nan values to the last bin
            vector_discrete = np.where(self.missing_mask, self.nan_class, vector_discrete)
            result = self.src.copy()
            result['value_discrete'] = vector_discrete
            return result

    def make_image(self,
                   size: Optional[float] = None,
                   dpi: Optional[float] = None):
        
        target_dpi = 150
        target_inches = 6
        
        min_dim = min(self.shape)
        if size is None and dpi is None:
            dpi = max(min_dim / target_inches, target_dpi) 
            size = target_inches / (min_dim / dpi)
        elif size is None:
            size = target_inches / (min_dim / dpi)
        elif dpi is None:
            dpi = min_dim * size / target_inches
        
        height, width = (sz * size for sz in self.shape)

        fig_width_in_inches = width / dpi
        fig_height_in_inches = height / dpi
        self.dpi = dpi
        self.size_in_inches = (fig_width_in_inches, fig_height_in_inches)

        # Create a figure with a single subplot
        fig, ax = plt.subplots(figsize=(fig_width_in_inches, fig_height_in_inches), dpi=dpi)

        # Plot the TIFF file
        if self.type == 'raster':    
            im = ax.imshow(self.digital_img, cmap=self.colormap, extent=self.extent, interpolation='nearest')
        # or the vector data
        elif self.type == 'vector':
            #ax.set_facecolor([0.5, 0.5, 0.5, 1.0])
            im = self.digital_src.plot(column='value_discrete', ax=ax, cmap=self.colormap, linewidth=0, legend=False)
            ax.set_xlim(self.extent[0], self.extent[1])
            ax.set_ylim(self.extent[2], self.extent[3])

        self.ax = ax
        self.fig = fig
        self.im = im

    def make_no_data_image(self, dpi: Optional[float] = None):
        if dpi is None:
            dpi = 150

        self.dpi = dpi
        self.size_in_inches = (4, 4)
        fig, ax = plt.subplots(figsize=self.size_in_inches, dpi=dpi)
        ax.set_facecolor([0.5, 0.5, 0.5, 1.0])
        ax.text(0.5, 0.5, "No data", ha='center', va='center', transform=ax.transAxes)
        ax.axis('off')

        self.ax = ax
        self.fig = fig

    def add_overlay(self, shp_file: str|Dataset, **kwargs):

        if isinstance(shp_file, str):
            shapes:gpd.GeoDataFrame = gpd.read_file(shp_file)
        else:
            shapes = shp_file.get_data()
        
        shapes = shapes.to_crs(self.crs.to_string())

        if 'facecolor' not in kwargs:
            kwargs['facecolor'] = 'none'
        if 'edgecolor' not in kwargs:
            kwargs['edgecolor'] = 'black'
        if 'linewidth' not in kwargs:
            kwargs['linewidth'] = 0.5

        shapes.boundary.plot(ax=self.ax, **kwargs)

        #Preserve the aspect ratio and the extent
        self.ax.set_aspect('equal')
        self.ax.set_xlim(self.extent[0], self.extent[1])
        self.ax.set_ylim(self.extent[2], self.extent[3])

    def add_annotation(self, text:str, **kwargs):

        if 'xycoords' not in kwargs:
            kwargs['xycoords'] = 'axes fraction'
        if 'fontsize' not in kwargs:
            width_in_inches = self.size_in_inches[0]
            # 1 point = 1/72 inch, we assume letters are 75% as wide as they are tall
            optimal_fontsize = min((width_in_inches) / (len(text) * 0.75 * 1/72), 20)

            # if the text is too tall, we reduce the font size to fit it in 1/10 of the height
            height_in_inches = self.size_in_inches[1]
            if optimal_fontsize * 1/72 > height_in_inches * 1/10:
                optimal_fontsize = (height_in_inches * 1/10) / (1/72)
            
            kwargs['fontsize'] = optimal_fontsize
        if 'fontfamily' not in kwargs:
            kwargs['fontfamily'] = 'sans-serif'
        if 'xy' not in kwargs:
            kwargs['xy'] = (0.02, 0.02)
            kwargs['ha'] = 'left'
            kwargs['va'] = 'bottom'

        if 'color' not in kwargs:
            kwargs['color'] = 'black'
        if 'backgroundcolor' not in kwargs:
            kwargs['backgroundcolor'] = 'none'

        self.ax.annotate(text, clip_on = True, **kwargs)

    def add_legend(self, **kwargs):

        if 'loc' not in kwargs:
            kwargs['loc'] = 'upper right'
        if 'bbox_to_anchor' not in kwargs:
            kwargs['bbox_to_anchor'] = (1, 1)
        if 'borderaxespad' not in kwargs:
            kwargs['borderaxespad'] = 0

        colors_normalized = [np.array(color, dtype=int) / 255. for color in self.all_colors]
        patches = [mpatches.Patch(color=color, label=label) for color, label in zip(colors_normalized, self.all_labels)]

        self.fig.legend(handles=patches, **kwargs)

    def save(self, destination:str, **kwargs):
        self.thumbnail_file = destination
        #breakpoint()
        if self.allnan:
            dpi = kwargs.pop('dpi', None)
            self.make_no_data_image(dpi)
            self.fig.tight_layout(pad=0)
            self.fig.patch.set_facecolor([0.5, 0.5, 0.5, 1.0])

            parent = os.path.dirname(destination)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self.fig.savefig(destination, dpi=self.dpi, bbox_inches='tight', pad_inches=0)
            plt.close(self.fig)
            return destination

        if "shape" in kwargs:
            self.shape = kwargs['shape']

        if not hasattr(self, 'fig'):
            size = kwargs.get('size', None)
            dpi  = kwargs.pop('dpi', None)
            self.make_image(size, dpi)

        if 'overlay' in kwargs:
            if isinstance(kwargs['overlay'], dict):
                self.add_overlay(**kwargs.pop('overlay'))
            elif isinstance(kwargs['overlay'], Dataset) or isinstance(kwargs['overlay'], str):
                self.add_overlay(kwargs['overlay'])
            elif kwargs['overlay'] == False or kwargs['overlay'] is None:
                pass
        
        if 'annotation' in kwargs:
            if isinstance(kwargs['annotation'], dict):
                annotation_opts = kwargs.pop('annotation')
                if 'text' not in annotation_opts:
                    if 'source_key' in self.src.attrs:
                        text = os.path.basename(self.src.attrs['source_key'])
                    elif hasattr(self, 'raster_file'):
                        text = os.path.basename(self.raster_file)
                else:
                    text = annotation_opts.pop('text')
                self.add_annotation(text, **annotation_opts)
            elif isinstance(kwargs['annotation'], str):
                self.add_annotation(kwargs['annotation'])
            elif kwargs['annotation'] == False or kwargs['annotation'] is None or kwargs['annotation'].lower == 'none':
                pass
        elif 'source_key' in self.src.attrs:
            annotation_txt = os.path.basename(self.src.attrs['source_key'])
            self.add_annotation(annotation_txt)
        elif hasattr(self, 'raster_file'):
            annotation_txt = os.path.basename(self.raster_file)
            self.add_annotation(annotation_txt)

        if 'legend' in kwargs:
            if isinstance(kwargs['legend'], dict):
                self.add_legend(**kwargs.pop('legend'))
            elif kwargs['legend'] == False or kwargs['legend'] is None or kwargs['legend'].lower == 'none':
                pass
        else:
            self.add_legend()

        self.ax.axis('off')
        self.fig.tight_layout(pad=0)
        self.fig.patch.set_facecolor([0.5, 0.5, 0.5, 1.0])
    
        parent = os.path.dirname(destination)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.fig.savefig(destination, dpi=self.dpi, bbox_inches='tight', pad_inches=0)
        plt.close(self.fig)
        return destination