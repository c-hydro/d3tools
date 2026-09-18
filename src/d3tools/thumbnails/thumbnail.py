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

            self.bounds = tuple(self.src.total_bounds)
            self.shape = self._shape_from_bounds(self.bounds)
            self.extent = self._extent_from_bounds(self.bounds)

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

    def _positive_int_option(self, option_name: str, value: Optional[int]) -> int:
        if value is None:
            if option_name == 'vector_short_side':
                return self.DEFAULT_VECTOR_SHORT_SIDE
            return self.MAX_VECTOR_LONG_SIDE

        value = int(value)
        if value <= 0:
            raise ValueError(f"{option_name} must be greater than zero.")
        return value

    def _shape_from_bounds(self,
                           bounds: tuple[float, float, float, float],
                           short_side: Optional[int] = None,
                           max_long_side: Optional[int] = None) -> tuple[int, int]:
        short_side = self._positive_int_option('vector_short_side', short_side)
        max_long_side = self._positive_int_option('vector_max_long_side', max_long_side)

        x_min, y_min, x_max, y_max = bounds
        width = x_max - x_min
        height = y_max - y_min

        if not np.isfinite(width) or not np.isfinite(height) or width <= 0 or height <= 0:
            return (short_side, short_side)

        if width >= height:
            shape_height = short_side
            shape_width = int(np.ceil(short_side * width / height))
        else:
            shape_width = short_side
            shape_height = int(np.ceil(short_side * height / width))

        long_side = max(shape_height, shape_width)
        if long_side > max_long_side:
            scale = max_long_side / long_side
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
        if self.type == 'vector' and size is None and dpi is None:
            dpi = target_dpi
            size = 1
        elif size is None and dpi is None:
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

    def _infer_annotation_text(self) -> Optional[str]:
        if 'source_key' in self.src.attrs:
            return os.path.basename(self.src.attrs['source_key'])
        if hasattr(self, 'raster_file'):
            return os.path.basename(self.raster_file)
        return None

    def _annotation_text_and_options(self, annotation) -> tuple[Optional[str], dict]:
        if annotation is False or annotation is None:
            return None, {}

        if isinstance(annotation, str):
            if annotation.strip() == '' or annotation.strip().lower() == 'none':
                return None, {}
            return annotation, {}

        if isinstance(annotation, dict):
            annotation_opts = annotation.copy()
            text = annotation_opts.pop('text', None)
            if text is None:
                text = self._infer_annotation_text()
            if text is None:
                return None, annotation_opts
            if not isinstance(text, str):
                raise TypeError("Thumbnail annotation text must be a string.")
            if text.strip() == '' or text.strip().lower() == 'none':
                return None, annotation_opts
            return text, annotation_opts

        raise TypeError("Thumbnail annotation must be a string, dict, False, or None.")

    def _legend_options(self, legend) -> Optional[dict]:
        if legend is False or legend is None:
            return None

        if legend is True:
            return {}

        if isinstance(legend, str):
            if legend.strip().lower() == 'none':
                return None
            raise ValueError("Thumbnail legend string option must be 'none'.")

        if isinstance(legend, dict):
            return legend.copy()

        raise TypeError("Thumbnail legend must be True, False, None, 'none', or a dict.")

    def _overlay_args_and_options(self, overlay) -> tuple[object, dict] | tuple[None, None]:
        if overlay is False or overlay is None:
            return None, None

        if isinstance(overlay, dict):
            overlay_opts = overlay.copy()
            if 'shp_file' not in overlay_opts:
                raise ValueError("Thumbnail overlay options must include 'shp_file'.")
            shp_file = overlay_opts.pop('shp_file')
            return shp_file, overlay_opts

        if isinstance(overlay, (str, Dataset, gpd.GeoDataFrame)):
            return overlay, {}

        raise TypeError("Thumbnail overlay must be a string, Dataset, GeoDataFrame, dict, False, or None.")

    def add_overlay(self, shp_file: str|Dataset, **kwargs):

        if isinstance(shp_file, str):
            shapes:gpd.GeoDataFrame = gpd.read_file(shp_file)
        elif isinstance(shp_file, gpd.GeoDataFrame):
            shapes = shp_file.copy()
        else:
            shapes = shp_file.get_data()
        
        if self.crs is None:
            raise ValueError("Thumbnail source CRS is required to add an overlay.")
        if shapes.crs is None:
            raise ValueError("Overlay CRS is required to add an overlay.")

        shapes = shapes.to_crs(self.crs)

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
        if 'borderaxespad' not in kwargs:
            kwargs['borderaxespad'] = 0
        # for compatibility with older scripts that might have this option
        if 'bbox_to_anchor' in kwargs:
            kwargs.pop('bbox_to_anchor')

        colors_normalized = [np.array(color, dtype=int) / 255. for color in self.all_colors]
        patches = [mpatches.Patch(color=color, label=label) for color, label in zip(colors_normalized, self.all_labels)]

        self.ax.legend(handles=patches, **kwargs)

    def _close_figure(self):
        if hasattr(self, 'fig'):
            plt.close(self.fig)
        for attr in ('fig', 'ax', 'im'):
            if hasattr(self, attr):
                delattr(self, attr)

    def save(self, destination:str, **kwargs):
        self.thumbnail_file = destination
        self._close_figure()
        try:
            if self.allnan:
                dpi = kwargs.pop('dpi', None)
                self.make_no_data_image(dpi)
            else:
                vector_short_side = kwargs.pop('vector_short_side', None)
                vector_max_long_side = kwargs.pop('vector_max_long_side', None)
                if "shape" in kwargs:
                    self.shape = kwargs['shape']
                elif self.type == 'vector':
                    self.shape = self._shape_from_bounds(
                        self.bounds,
                        short_side=vector_short_side,
                        max_long_side=vector_max_long_side,
                    )

                size = kwargs.get('size', None)
                dpi  = kwargs.pop('dpi', None)
                self.make_image(size, dpi)

                if 'overlay' in kwargs:
                    overlay_src, overlay_opts = self._overlay_args_and_options(kwargs['overlay'])
                    if overlay_src is not None:
                        self.add_overlay(overlay_src, **overlay_opts)
                
                if 'annotation' in kwargs:
                    annotation_txt, annotation_opts = self._annotation_text_and_options(kwargs['annotation'])
                    if annotation_txt is not None:
                        self.add_annotation(annotation_txt, **annotation_opts)
                else:
                    annotation_txt = self._infer_annotation_text()
                    if annotation_txt is not None:
                        self.add_annotation(annotation_txt)

                if 'legend' in kwargs:
                    legend_opts = self._legend_options(kwargs['legend'])
                    if legend_opts is not None:
                        self.add_legend(**legend_opts)
                else:
                    self.add_legend()

            self.ax.axis('off')
            self.fig.tight_layout(pad=0)
            self.fig.patch.set_facecolor([0.5, 0.5, 0.5, 1.0])
        
            parent = os.path.dirname(destination)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self.fig.savefig(destination, dpi=self.dpi, bbox_inches='tight', pad_inches=0)
            return destination
        finally:
            self._close_figure()