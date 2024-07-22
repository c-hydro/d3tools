import numpy as np
import rioxarray
import xarray as xr
import os

import matplotlib.pyplot as plt

from . import colors as col

#TODO TEST
class Thumbnail:
    def __init__(self, raster:str|xr.DataArray, color_definition_file:str):
        if isinstance(raster, xr.DataArray):
            self.src = raster
        elif isinstance(raster, str):
            self.src = rioxarray.open_rasterio(raster)
        
        # let's force the image to be flipped in the y coordinate
        self.flip = self.src.y.data[-1] > self.src.y.data[0]
        if not self.flip:
            ydim = self.src.rio.y_dim
            self.src = self.src.sortby('y', ascending=True)

        self.transform = self.src.rio.transform()
        self.img = self.src.data.squeeze()
        self.nan_value = self.src.rio.nodata
        self.shape = self.img.shape
        self.crs = self.src.rio.crs

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

    def add_overlay(self, shp_file: str, **kwargs):
        import geopandas as gpd

        shapes = gpd.read_file(shp_file)
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
            size = kwargs.get('size', 1)
            dpi  = kwargs.pop('dpi', 150)
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

        self.ax.invert_yaxis()

        self.ax.axis('off')
        self.fig.tight_layout(pad=0)

        os.makedirs(os.path.dirname(file), exist_ok=True)
        self.fig.savefig(file, dpi=self.dpi)

        plt.close(self.fig)

        self.thumbnail_file = file