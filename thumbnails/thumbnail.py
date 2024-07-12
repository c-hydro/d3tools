import numpy as np
import rioxarray
import os

import matplotlib.pyplot as plt

from . import colors as col

#TODO TEST
class Thumbnail:
    def __init__(self, raster_file:str, color_definition_file:str):
        self.raster_file = raster_file
        with rioxarray.open_rasterio(raster_file) as src:
            self.src = src
            self.transform = src.rio.transform()
            self.img = src.data.squeeze()
            self.nan_value = src.rio.nodata
            self.flip = src.y.data[-1] > src.y.data[0]
            self.shape = self.img.shape
            self.crs = src.rio.crs

        self.extent = (self.transform[2], self.transform[2] + self.transform[0]*self.img.shape[1],
                       self.transform[5] + self.transform[4]*self.img.shape[0], self.transform[5])

        self.txt_file = color_definition_file
        all_breaks, all_colors = col.parse_txt(color_definition_file)

        self.digital_img = self.discretize_raster(all_breaks)
        self.breaks = np.unique(self.digital_img)
        self.colors = col.keep_used_colors(self.breaks, all_colors)

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

    def make_image(self, size = 1, dpi = 150):
     
        height, width = (sz * size for sz in self.shape)
        fig_width_in_inches = width / dpi
        fig_height_in_inches = height / dpi
        self.dpi = dpi

        # Create a figure with a single subplot
        fig, ax = plt.subplots(figsize=(fig_width_in_inches, fig_height_in_inches), dpi=dpi)

        # Plot the TIFF file
        im = ax.imshow(self.digital_img, cmap=self.colormap, extent=self.extent, interpolation='nearest')

        self.ax = ax
        self.fig = fig
        self.im = im

    def add_overlay(self, shp_file = None, **kwargs):
        import geopandas as gpd
        if shp_file is None:
            shp_file = 'thumbnails/countries/countries.shp'

        countries = gpd.read_file(shp_file)
        countries = countries.to_crs(self.crs.to_string())

        if 'facecolor' not in kwargs:
            kwargs['facecolor'] = 'none'
        if 'edgecolor' not in kwargs:
            kwargs['edgecolor'] = 'black'
        if 'linewidth' not in kwargs:
            kwargs['linewidth'] = 0.5

        countries.boundary.plot(ax=self.ax, **kwargs)

        # Preserve the aspect ratio and the extent
        self.ax.set_aspect('equal')
        self.ax.set_xlim(self.extent[0], self.extent[1])
        self.ax.set_ylim(self.extent[2], self.extent[3])

    def add_annotation(self, text:str, **kwargs):
        
        if 'xy' not in kwargs:
            kwargs['xy'] = (0, 0)
        if 'xycoords' not in kwargs:
            kwargs['xycoords'] = 'axes fraction'
        if 'fontsize' not in kwargs:
            kwargs['fontsize'] = 12
        if 'color' not in kwargs:
            kwargs['color'] = 'black'
        if 'backgroundcolor' not in kwargs:
            kwargs['backgroundcolor'] = 'white'

        self.ax.annotate(text, **kwargs)

    def save(self, file:str, **kwargs):

        if not hasattr(self, 'fig'):
            size = kwargs.pop('size') if 'size' in kwargs else None
            dpi  = kwargs.pop('dpi') if 'dpi' in kwargs else None
            self.make_image(size, dpi)

        if 'overlay' in kwargs:
            if isinstance(kwargs['overlay'], dict):
                self.add_overlay(**kwargs.pop('overlay'))
            elif kwargs['overlay'] == False or kwargs['overlay'] is None or kwargs['overlay'].lower == 'none' :
                pass
        else:
            self.add_overlay()

        if 'annotation' in kwargs:
            if isinstance(kwargs['annotation'], dict):
                self.add_annotation(**kwargs.pop('annotation'))
            elif kwargs['annotation'] == False or kwargs['annotation'] is None or kwargs['annotation'].lower == 'none':
                pass
        else:
            self.add_annotation(f'{os.path.basename(self.raster_file)}')

        if self.flip:
            self.ax.invert_yaxis()

        self.ax.axis('off')
        self.fig.tight_layout(pad=0)
        self.fig.savefig(file, dpi=self.dpi)

        plt.close(self.fig)

        self.thumbnail_file = file