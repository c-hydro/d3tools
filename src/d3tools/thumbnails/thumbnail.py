import numpy as np
import rioxarray
import xarray as xr
import os
from typing import Optional

import geopandas as gpd

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from ..data import Dataset
from .colors import parse_colors, keep_used_colors, create_colormap

#TODO TEST
class Thumbnail:
    def __init__(self, raster:str|xr.DataArray, color_definition_file:str):
        if isinstance(raster, xr.DataArray):
            self.src = raster
        elif isinstance(raster, str):
            self.raster_file = raster
            self.src = rioxarray.open_rasterio(raster)

        # check if the raster is all nan
        if np.all(np.isclose(self.src.data, self.src.rio.nodata, equal_nan=True)):
            self.allnan = True
        else:
            self.allnan = False

            self.transform = self.src.rio.transform()
            self.img = self.src.data.squeeze()
            self.nan_value = self.src.rio.nodata
            self.shape = self.img.shape
            self.crs = self.src.rio.crs

            self.extent = (self.transform[2], self.transform[2] + self.transform[0]*self.img.shape[1],
                        self.transform[5] + self.transform[4]*self.img.shape[0], self.transform[5])

            self.txt_file = color_definition_file
            all_breaks, self.all_colors, all_labels = parse_colors(color_definition_file)

            self.digital_img = self.discretize_raster(all_breaks)
            self.breaks = np.unique(self.digital_img)
            self.colors = keep_used_colors(self.breaks, self.all_colors)
            self.all_labels = all_labels
            
            all_labels.append('nan')
            self.labels = [all_labels[i] for i in range(min(self.breaks), max(self.breaks)+1)]

            self.colormap = create_colormap(self.colors)

    def discretize_raster(self, breaks: list, nan_value = np.nan):
        # Create an array of bins from the positions
        if len(breaks) == 0:
            return np.zeros_like(self.img)
        elif breaks[-1] == 'inf':
            bins = [float(pos) for pos in breaks[:-1]]
        else:
            bins = [float(pos) for pos in breaks]

        # Discretize the raster values
        raster_discrete = np.digitize(self.img, bins, right = True)

        # Set the nan values to the last bin
        raster_discrete = np.where(np.isclose(self.img, nan_value, equal_nan= True), len(bins) + 1, raster_discrete)

        return raster_discrete

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
        im = ax.imshow(self.digital_img, cmap=self.colormap, extent=self.extent, interpolation='nearest')

        self.ax = ax
        self.fig = fig
        self.im = im

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
           return

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
    
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        self.fig.savefig(destination, dpi=self.dpi, bbox_inches='tight', pad_inches=0)
        plt.close(self.fig)