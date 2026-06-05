"""
Vector-specific functionality mixin for Dataset classes.

This mixin provides functionality for datasets that store vector/geometry data
(Shapefiles, GeoJSON, GeoPackage, GeoDataFrames).
"""
import geopandas as gpd
import datetime as dt
import numpy as np
import json
from typing import Optional

from .base import FormatMixin

class VectorMixin(FormatMixin):
    """
    Mixin for vector/geometry-specific operations.
    
    Handles shapefile and GeoJSON formats with GeoDataFrame operations.
    """
    
    def _init_format_properties(self):
        """
        Initialize vector-specific properties.
        
        Called from Dataset.__init__ when format is vector-based.
        """
        # Vector formats don't need special initialization
        # Future: could add spatial indexing, CRS validation, etc.
        pass
    
    def _read_from_file(self, path: str, **kwargs) -> gpd.GeoDataFrame:
        """
        Read vector data from a file using geopandas.

        Args:
            path: Path to the vector file
            **kwargs: Additional arguments for geopandas read function

        Returns:
            GeoDataFrame read from the vector file
        """

        data = gpd.read_file(path, **kwargs)

        # if the format is geojson, check if there is metadata
        # and if there is, add it to the GeoDataFrame attributes
        if self.format == 'geojson':
            with open(path, 'r') as f:
                json_data = json.load(f)
            if 'metadata' in json_data:
                data.attrs = json_data['metadata']

        return data

    def _format_after_read(self, data: gpd.GeoDataFrame, **kwargs) -> gpd.GeoDataFrame:
        """
        Post-process vector data after reading from storage.
        
        Args:
            data: Raw GeoDataFrame from storage
            **kwargs: Additional arguments
            
        Returns:
            Processed GeoDataFrame ready for use
        """
        # Future: CRS validation, geometry repair, spatial indexing
        return data
    
    def _format_before_write(self, data: gpd.GeoDataFrame, **kwargs) -> gpd.GeoDataFrame:
        """
        Prepare vector data for writing with format-specific logic.
        
        Args:
            data: GeoDataFrame to prepare
            **kwargs: Additional arguments
            
        Returns:
            Prepared GeoDataFrame ready for writing
        """
        # if the format is geojson, we need to convert time columns to stings
        if self.format == 'geojson':
            for col in data.columns:
                if len(data) > 0:
                    first_val = data[col].iloc[0]
                    if isinstance(first_val, np.datetime64):
                        data[col] = data[col].apply(lambda x: x.astype('O'))
                    if isinstance(first_val, (dt.datetime, dt.date)):
                        data[col] = data[col].apply(lambda x: x.isoformat())

        return data
    
    def _write_to_file(self, data: gpd.GeoDataFrame, path: str, append: bool = False, **kwargs):
        """
        Write vector data to a file using geopandas.
        
        Args:
            data: GeoDataFrame to write
            path: Path to the output file
            append: Whether to append to an existing file
            **kwargs: Additional arguments for geopandas.GeoDataFrame.to_file or json.dump
        """
        
        from ..io_utils import ensure_directory_exists
        ensure_directory_exists(path)
        
        if self.format == 'shp':
            mode = 'a' if append else 'w'
            data.to_file(path, driver='ESRI Shapefile', mode=mode, **kwargs)

        elif self.format == 'geojson':
            # in terory this will work just fine, but in the past we have saved
            # geojson files differently, so we keep doing as we have in the past
            # for consistency with old files.
            # Future: consider switching to standard GeoJSON writing.
            # mode = 'a' if append else 'w'
            # data.to_file(path, driver='GeoJSON', mode=mode, **kwargs)

            # ensure time columns are converted to strings
            for col in data.columns:
                if isinstance(data[col].iloc[0], np.datetime64):
                    data[col] = data[col].apply(lambda x: x.astype('O'))
                if isinstance(data[col].iloc[0], (dt.datetime, dt.date)):
                    data[col] = data[col].apply(lambda x: x.isoformat())

            # convert the GeoDataFrame into a dictionary
            dict_data = json.loads(data.to_json())
            # if there is metadata, add it to the dictionary
            if data.attrs:
                dict_data['metadata'] = data.attrs
            data = dict_data

            # if append, open the existing file and append the new data to it
            if append:
                with open(path, 'r') as f:
                    old_data = json.load(f)
                old_data = [old_data] if not isinstance(old_data, list) else old_data
                old_data.append(data)
                data = old_data
            
            # write the data to a (geo)json file
            default_kwargs = {'indent': 4}
            default_kwargs.update(kwargs)
            with open(path, 'w') as f:
                json.dump(data, f, **default_kwargs)

    def set_metadata(self, data: gpd.GeoDataFrame, **kwargs) -> gpd.GeoDataFrame:
        """
        Add metadata to the GeoDataFrame object.
        
        Args:
            data: GeoDataFrame object to which metadata should be added
            **kwargs: Metadata key-value pairs to add
            
        Returns:
            GeoDataFrame object with metadata attached
        """

        time = kwargs.pop('time', None)
        if time is not None:
            datatime = self.get_time_signature(time)
            kwargs['time'] = datatime.strftime('%Y-%m-%d')

        if hasattr(data, 'attrs'):
            if 'time' in data.attrs:
                data.attrs.pop('time')
            kwargs.update(data.attrs)
        
        metadata = kwargs.copy()
        metadata['time_produced'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        data.attrs.update(metadata)

        return data

    def update_metadata(self, data: gpd.GeoDataFrame, **kwargs) -> gpd.GeoDataFrame:
        """
        Update existing metadata on the GeoDataFrame object.
        
        Args:
            data: GeoDataFrame object whose metadata should be updated
            **kwargs: Metadata key-value pairs to update
            
        Returns:
            GeoDataFrame object with updated metadata
        """
        attrs = data.attrs if hasattr(data, 'attrs') else {}
        attrs.update(kwargs)
        data.attrs = attrs
        return data
    
    def get_metadata(self, data: gpd.GeoDataFrame, keys: Optional[list|str] = None) -> dict:
        """
        Retrieve metadata from the GeoDataFrame object.
                
        Args:
            data: GeoDataFrame object from which to retrieve metadata
            keys: Optional list of metadata keys to retrieve (if None, retrieve all)
            
        Returns:
            Metadata dictionary extracted from the GeoDataFrame object
        """
        if hasattr(data, 'attrs'):
            metadata = data.attrs
            if keys is not None:
                if isinstance(keys, str):
                    keys = [keys]
                metadata = {k: v for k, v in metadata.items() if k in keys}
            return dict(metadata)
        else:
            return {}