import numpy as np
import rioxarray
import xarray as xr
import os
from typing import Optional

import matplotlib.pyplot as plt

from . import colors as col

#TODO TEST
class Thumbnail:
    def __init__(self, raster:str|xr.DataArray, color_definition_file:str):
        if isinstance(raster, xr.DataArray):
            self.src = raster
        elif isinstance(raster, str):
            self.src = rioxarray.open_rasterio(raster)

        self.transform = self.src.rio.transform()
        self.img = self.src.data.squeeze()
        self.nan_value = self.src.rio.nodata
        self.shape = self.img.shape
        self.crs = self.src.rio.crs

        self.extent = (self.transform[2], self.transform[2] + self.transform[0]*self.img.shape[1],
                       self.transform[5] + self.transform[4]*self.img.shape[0], self.transform[5])

        self.txt_file = color_definition_file
        all_breaks, all_colors, all_labels = col.parse_txt(color_definition_file)

        self.digital_img = self.discretize_raster(all_breaks)
        self.breaks = np.unique(self.digital_img)
        self.colors = col.keep_used_colors(self.breaks, all_colors)
        
        all_labels.append('nan')
        self.labels = [all_labels[i] for i in range(min(self.breaks), max(self.breaks)+1)]

        self.colormap = col.create_colormap(self.colors)

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
        
        min_dim = min(self.shape)
        if size is None and dpi is None:
            dpi = max(min_dim / 6, 100) 
            size = 6 / (min_dim / dpi)
        elif size is None:
            size = 6 / (min_dim / dpi)
        elif dpi is None:
            dpi = min_dim * size / 6
        
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

    def add_overlay(self, shp_file: str, **kwargs):
        import geopandas as gpd

        shapes:gpd.GeoDataFrame = gpd.read_file(shp_file)
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
            kwargs['xycoords'] = 'axes points'
        if 'fontsize' not in kwargs:
            width_in_inches = self.size_in_inches[0]
            # 1 point = 1/72 inch, we assume letters are 0.55 times as wide as they are tall
            # and look for the optimal font size that allows to write the text in 3/4 of the width of the image
            optimal_fontsize = min((width_in_inches * 3/4) / (len(text) * 0.55 * 1/72), 20)

            # if the text is too tall, we reduce the font size to fit it in 1/5 of the height
            height_in_inches = self.size_in_inches[1]
            if optimal_fontsize * 1/72 > height_in_inches * 1/5:
                optimal_fontsize = (height_in_inches * 1/5) / (1/72)
            
            kwargs['fontsize'] = optimal_fontsize
        if 'xy' not in kwargs:
            kwargs['xy'] = (6, kwargs['fontsize']/3)
        if 'color' not in kwargs:
            kwargs['color'] = 'black'
        if 'backgroundcolor' not in kwargs:
            kwargs['backgroundcolor'] = 'white'

        self.ax.annotate(text, annotation_clip=False, **kwargs)

    def add_legend(self, **kwargs):

        if 'loc' not in kwargs:
            kwargs['loc'] = 'upper right'
        if 'bbox_to_anchor' not in kwargs:
            kwargs['bbox_to_anchor'] = (0.99, 0.99)
        if 'borderaxespad' not in kwargs:
            kwargs['borderaxespad'] = 0

        import matplotlib.patches as mpatches
        colors_normalized = [np.array(color, dtype=int) / 255. for color in self.colors]
        patches = [mpatches.Patch(color=color, label=label) for color, label in zip(colors_normalized, self.labels)]

        self.fig.legend(handles=patches, **kwargs)

    def save(self, file:str, **kwargs):

        if not hasattr(self, 'fig'):
            size = kwargs.get('size', None)
            dpi  = kwargs.pop('dpi', None)
            self.make_image(size, dpi)

        if 'overlay' in kwargs:
            if isinstance(kwargs['overlay'], dict):
                self.add_overlay(**kwargs.pop('overlay'))
            elif isinstance(kwargs['overlay'], str):
                self.add_overlay(kwargs['overlay'])
            elif kwargs['overlay'] == False or kwargs['overlay'] is None or kwargs['overlay'].lower == 'none' :
                pass

        if 'annotation' in kwargs:
            if isinstance(kwargs['annotation'], dict):
                self.add_annotation(**kwargs.pop('annotation'))
            elif isinstance(kwargs['annotation'], str):
                self.add_annotation(kwargs['annotation'])
            elif kwargs['annotation'] == False or kwargs['annotation'] is None or kwargs['annotation'].lower == 'none':
                pass
        elif hasattr(self, 'raster_file'):
            self.add_annotation(f'{os.path.basename(self.raster_file)}')

        if 'legend' in kwargs:
            if isinstance(kwargs['legend'], dict):
                self.add_legend(**kwargs.pop('legend'))
            elif kwargs['legend'] == False or kwargs['legend'] is None or kwargs['legend'].lower == 'none':
                pass
        else:
            self.add_legend()

        self.ax.axis('off')
        self.fig.tight_layout(pad=0)

        os.makedirs(os.path.dirname(file), exist_ok=True)
        self.fig.savefig(file, dpi=self.dpi, bbox_inches='tight', pad_inches=0)

        plt.close(self.fig)

        self.thumbnail_file = file