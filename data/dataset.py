from typing import Optional, Generator, Callable
import datetime as dt
import numpy as np
import rioxarray as rxr
import xarray as xr
import pandas as pd

from abc import ABC, ABCMeta, abstractmethod
import os
import re

try:
    from ..timestepping import TimeRange, Day, ViirsModisTimeStep, Dekad, Month, Year
    from ..timestepping.timestep import TimeStep
    from ..config.parse_utils import substitute_string, extract_date_and_tags
    from .io_utils import get_format_from_path, straighten_data, reset_nan, set_type
except ImportError:
    from timestepping import TimeRange, Day, ViirsModisTimeStep, Dekad, Month, Year
    from timestepping.timestep import TimeStep
    from tools.config.parse_utils import substitute_string, extract_date_and_tags
    from io_utils import get_format_from_path, straighten_data, reset_nan, set_type

def withcases(func):
    def wrapper(*args, **kwargs):
        if 'cases' in kwargs:
            cases = kwargs.pop('cases')
            if cases is not None:
                return [func(*args, **case['tags'], **kwargs) for case in cases]
        else:
            return func(*args, **kwargs)
    return wrapper

class DatasetMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'type' in attrs:
            cls.subclasses[attrs['type']] = cls

class Dataset(ABC, metaclass=DatasetMeta):
    _defaults = {'type': 'local',
                 'time_signature' : 'end'}

    def __init__(self, **kwargs):

        if 'name' in kwargs:
            self.name   = kwargs.pop('name')
        else:
            basename_noext  = '.'.join(os.path.basename(self.key_pattern).split('.')[:-1])
            basename_nodate = basename_noext.replace('%Y', '').replace('%m', '').replace('%d', '')
            if basename_nodate.endswith('_'):
                basename_nodate = basename_nodate[:-1]
            elif basename_nodate.startswith('_'):
                basename_nodate = basename_nodate[1:]
            elif '__' in basename_nodate:
                basename_nodate = basename_nodate.replace('__', '_')

            self.name = basename_nodate

        if 'format' in kwargs:
            self.format = kwargs.pop('format')
        else:
            self.format = get_format_from_path(self.key_pattern)

        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        if 'thumbnail' in kwargs:
            self.thumb_opts = kwargs.pop('thumbnail')

        if 'notification' in kwargs:
            self.notif_opts = kwargs.pop('notification')

        if 'log' in kwargs:
            self.log_opts = kwargs.pop('log')

        if 'tile_names' in kwargs:
            self.tile_names = kwargs.pop('tile_names')

        self._template = {}
        self.options = kwargs
        self.tags = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"
    
    def update(self, in_place = False, **kwargs):
        new_name = substitute_string(self.name, kwargs)
        new_key_pattern = substitute_string(self.key_pattern, kwargs)

        if in_place:
            self.name = new_name
            self.key_pattern = self.get_key(**kwargs)
            self.tags.update(kwargs)
            return self
        else:
            new_options = self.options.copy()
            new_options.update({'key_pattern': new_key_pattern, 'name': new_name})
            new_dataset = self.__class__(**new_options)

            new_dataset._template = self._template
            if hasattr(self, '_tile_names'):
                new_dataset._tile_names = self._tile_names

            new_dataset.time_signature = self.time_signature
            
            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags
            return new_dataset

    def copy(self, template = False):
        new_dataset = self.update()
        if template:
            new_dataset._template = self._template
        return new_dataset

    ## CLASS METHODS FOR FACTORY
    @classmethod
    def from_options(cls, options: dict):
        type = options.pop('type', None)
        type = cls.get_type(type)
        Subclass: 'Dataset' = cls.get_subclass(type)
        return Subclass(**options)

    @classmethod
    def get_subclass(cls, type: str):
        type = cls.get_type(type)
        Subclass: 'Dataset'|None = cls.subclasses.get(type)
        if Subclass is None:
            raise ValueError(f"Invalid type of dataset: {type}")
        return Subclass
    
    @classmethod
    def get_type(cls, type: Optional[str] = None):
        if type is not None:
            return type
        elif hasattr(cls, 'type'):
            return cls.type
        else:
            return cls._defaults['type']
    
    ## PROPERTIES
    @property
    def format(self):
        return self._format
    
    @format.setter
    def format(self, value):
        self._format = value

    @property
    def has_tiles (self):
        return '{tile}' in self.key_pattern

    @property
    def tile_names(self):
        if not self.has_tiles:
            self._tile_names = ['__tile__']
        
        if not hasattr(self, '_tile_names') or self._tile_names is None:
            self._tile_names = self.available_tags.get('tile')

        return self._tile_names
    
    @tile_names.setter
    def tile_names(self, value):
        if isinstance(value, str):
            self._tile_names = self.get_tile_names_from_file(value)
        elif isinstance(value, list) or isinstance(value, tuple):
            self._tile_names = list(value)
        else:
            raise ValueError('Invalid tile names.')
        
    def get_tile_names_from_file(self, filename: str) -> list[str]:
        with open(filename, 'r') as f:
            return [l.strip() for l in f.readlines()]

    @property
    def ntiles(self):
        return len(self.tile_names)

    @property
    def key_pattern(self):
        raise NotImplementedError

    @key_pattern.setter
    def key_pattern(self, value):
        raise NotImplementedError

    @property
    def available_keys(self):
        raise NotImplementedError

    @property
    def is_static(self):
        return not '{' in self.key_pattern and not '%' in self.key_pattern

    @property
    def available_tags(self):
        return self.get_available_tags()

    def get_available_tags(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        updated_self = self.update(**kwargs)

        if isinstance(time, TimeStep):
            time = self.get_time_signature(time)

        all_keys = updated_self.available_keys
        all_tags = {}
        all_dates = set()
        for key in all_keys:
            this_date, this_tags = extract_date_and_tags(key, self.key_pattern)
            if time is not None and this_date != time:
                continue

            for tag in this_tags:
                if tag not in all_tags:
                    all_tags[tag] = set()
                all_tags[tag].add(this_tags[tag])
            all_dates.add(this_date)
        
        all_tags = {tag: list(all_tags[tag]) for tag in all_tags}
        all_tags['time'] = list(all_dates)

        return all_tags

    def estimate_timestep(self) -> TimeStep:
        from scipy.stats import mode
        all_times = self.available_tags['time']
        all_times.sort()
        all_diff = [(all_times[i+1] - all_times[i]).days for i in range(len(all_times)-1)]
        step_length = mode(all_diff).mode
        
        if np.isclose(step_length, 1):
            return Day
        elif np.isclose(step_length, 8):
            return ViirsModisTimeStep
        elif np.isclose(step_length, 10):
            return Dekad
        elif 30 <= step_length <= 31:
            return Month
        elif 365 <= step_length <= 366:
            return Year
        else:
            raise None

    def get_last_date(self) -> dt.datetime:
        all_times = self['time']
        if len(all_times) == 0:
            return None
        return max(all_times)

    def get_last_ts(self) -> TimeStep:
        last_date = self.get_last_date()
        if last_date is None:
            return None
        timestep = self.estimate_timestep()
        if timestep is None:
            return None

        if self.time_signature == 'end+1':
            return timestep.from_date(last_date) -1
        else:
            return timestep.from_date(last_date)

    ## TIME-SIGNATURE MANAGEMENT
    @property
    def time_signature(self):
        if not hasattr(self, '_time_signature'):
            self._time_signature = self._defaults['time_signature']
        
        return self._time_signature
        
    @time_signature.setter
    def time_signature(self, value):
        if value not in ['start', 'end', 'end+1']:
            raise ValueError(f"Invalid time signature: {value}")
        self._time_signature = value

    def get_time_signature(self, timestep: Optional[TimeStep | dt.datetime]) -> dt.datetime:
        if timestep is None:
            return None
        if isinstance(timestep, dt.datetime):
            time = timestep
            # calculating the length in this way is not perfect,
            # but should work given that timesteps are always requested in order
            if hasattr(self, 'previous_requested_time'):
                length = (time - self.previous_requested_time).days
            else:
                length = None
            self.previous_requested_time = time
        else:
            time_signature = self.time_signature
            if time_signature == 'start':
                time = timestep.start
            elif time_signature == 'end':
                time = timestep.end
            elif time_signature == 'end+1':
                time = (timestep+1).start
            length = timestep.get_length()
            self.previous_requested_time = time

        key_without_tags = re.sub(r'\{[^}]*\}', '', self.key_pattern)
        hasyear = '%Y' in key_without_tags

        # change the date to 28th of February if it is the 29th of February,
        # but only if no year is present in the path (i.e. this is a parameter)
        # and the length is greater than 1 (i.e. not a daily timestep)
        if not hasyear and time.month == 2 and time.day == 29:
            if length is not None and length > 1:
                time = time.replace(day = 28)
        
        return time

    ## INPUT/OUTPUT METHODS
    def get_data(self, time: Optional[dt.datetime|TimeStep] = None, as_is = False, **kwargs):
        full_key = self.get_key(time, **kwargs)

        if self.format in ['csv', 'json', 'txt', 'shp']:
            if self._check_data(full_key):
                return self._read_data(full_key)

        if self.check_data(time, **kwargs):
            data = self._read_data(full_key)
            if as_is:
                return data

            # ensure that the data has descending latitudes
            data = straighten_data(data)

            # make sure the nodata value is set to np.nan for floats and to the max int for integers
            data = reset_nan(data)

        # if the data is not available, try to calculate it from the parents
        elif hasattr(self, 'parents') and self.parents is not None:
            data = self.make_data(time, **kwargs)
            if as_is:
                return data

        else:
            raise ValueError(f'Could not resolve data from {full_key}.')

        # if there is no template for the dataset, create it from the data
        template_dict = self.get_template_dict(make_it=False, **kwargs)
        if template_dict is None:
            #template = self.make_templatearray_from_data(data)
            self.set_template(data, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            data = self.set_data_to_template(data, template_dict)
            data.attrs.update(attrs)
        
        data.attrs.update({'source_key': full_key})
        return data
    
    @abstractmethod
    def _read_data(self, input_key:str):
        raise NotImplementedError
    
    def check_data_for_writing(self, data: xr.DataArray|xr.Dataset|np.ndarray|pd.DataFrame|str|dict):
        """"
        Ensures that the data is compatible with the format of the dataset.
        """
        if isinstance(data, pd.DataFrame):
            if not self.format == 'csv':
                raise ValueError(f'Cannot write pandas dataframe to a {self.format} file.')
        elif isinstance(data, str):
            if not self.format == 'txt':
                raise ValueError(f'Cannot write a string to a {self.format} file.')
        elif isinstance(data, dict):
            if not self.format == 'json':
                raise ValueError(f'Cannot write a dictionary to a {self.format} file.')
        elif isinstance(data, np.ndarray) or isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
            if self.format == 'csv':
                raise ValueError(f'Cannot write matrix data to a csv file.')
        
        if self.format == 'geotiff' and isinstance(data, xr.Dataset):
            raise ValueError(f'Cannot write a dataset to a geotiff file.')

    def write_data(self, data: xr.DataArray|xr.Dataset|np.ndarray|pd.DataFrame,
                   time: Optional[dt.datetime|TimeStep] = None,
                   time_format: str = '%Y-%m-%d',
                   metadata = {},
                   **kwargs):

        self.check_data_for_writing(data)

        output_file = self.get_key(time, **kwargs)

        if self.format in ['csv', 'json', 'txt']:
            append = kwargs.pop('append', False)
            self._write_data(data, output_file, append = append)
            return
        
        # if data is a numpy array, ensure there is a template available
        template_dict = self.get_template_dict(**kwargs, make_it=False)
        if template_dict is None:
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                #templatearray = self.make_templatearray_from_data(data)
                self.set_template(data, **kwargs)
                template_dict = self.get_template_dict(**kwargs, make_it=False)
            else:
                raise ValueError('Cannot write numpy array without a template.')
            
        output = self.set_data_to_template(data, template_dict)
        output = set_type(output)
        output = straighten_data(output)
        output.attrs['source_key'] = output_file

        # if necessary generate the thubnail
        if 'parents' in metadata:
            parents = metadata.pop('parents')
        else:
            parents = {}

        timestamp = self.get_time_signature(time)
        if hasattr(self, 'thumb_opts'):
            parents[''] = output
            thumb_opts = self.thumb_opts.copy()
            if 'destination' in thumb_opts:
                destination = thumb_opts.pop('destination')
                destination = timestamp.strftime(substitute_string(destination, kwargs))
            else:
                destination = output_file
            thumbnail_file = self.make_thumbnail(data = parents,
                                                 options = thumb_opts,
                                                 destination = destination,
                                                 **kwargs)
            self.thumbnail_file = thumbnail_file

        # add the metadata
        attrs = data.attrs if hasattr(data, 'attrs') else {}
        output.attrs.update(attrs)
        name = substitute_string(self.name, kwargs)
        metadata['name'] = name
        output = self.set_metadata(output, time, time_format, **metadata)

        # write the data
        self._write_data(output, output_file)
        
        if hasattr(self, 'log_opts'):
            if isinstance(self.log_opts, str|Dataset):
                log_output = self.log_opts
                log_opts = {}
            elif isinstance(self.log_opts, dict):
                log_opts = self.log_opts.copy()
                log_output = log_opts.pop('file')
            other_to_log = {}
            other_to_log['source_key'] = output_file
            if any(k != '' for k in parents):
                other_to_log['parents'] = []
                for key, value in parents.items():
                    other_to_log['parents'] = value.attrs.get('source_key', key)
            if hasattr(self, 'thumbnail_file'):
                other_to_log['thumbnail'] = self.thumbnail_file
            log_dict = self.get_log(output, time, kwargs, other_to_log, **log_opts)
            tags = kwargs.copy()
            tags['now'] = dt.datetime.now()
            if isinstance(log_output, str):
                log_output = timestamp.strftime(substitute_string(log_output, tags))
            elif isinstance(log_output, Dataset):
                log_output = log_output.update(time, **tags)
            self.write_log(log_dict, log_output, time, **kwargs)
        
        if hasattr(self, 'notif_opts'):
            for key, value in self.notif_opts.items():
                if isinstance(value, str):
                    self.notif_opts[key] = timestamp.strftime(substitute_string(value, kwargs))
            
            self.notif_opts['metadata'] = output.attrs
            self.notify(self.notif_opts.copy())

    def copy_data(self, new_key_pattern, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        data = self.get_data(time, **kwargs)
        timestamp = self.get_time_signature(time)
        if timestamp is None:
            new_key = substitute_string(new_key_pattern, kwargs)
        else:
            new_key = timestamp.strftime(substitute_string(new_key_pattern, kwargs))
        self._write_data(data, new_key)

    def rm_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        key = self.get_key(time, **kwargs)
        self._rm_data(key)

    def move_data(self, new_key_pattern, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        self.copy_data(new_key_pattern, time, **kwargs)
        self.rm_data(time, **kwargs)

    @abstractmethod
    def _write_data(self, output: xr.DataArray, output_key: str):
        raise NotImplementedError
    
    @abstractmethod
    def _rm_data(self, key: str):
        raise NotImplementedError

    def make_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        if not hasattr(self, 'parents') or self.parents is None:
            raise ValueError(f'No parents for {self.name}')
        
        parent_data = {name: parent.get_data(time, **kwargs) for name, parent in self.parents.items()}
        data = self.fn(**parent_data)
        self.write_data(data, time, **kwargs)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _get_times(self, time_range: TimeRange, **kwargs) -> Generator[dt.datetime, None, None]:
        all_times = self.update(**kwargs).available_tags.get('time', [])
        all_times.sort()
        for time in all_times:
            if time_range.contains(time):
                yield time
        
        if len(all_times) == 0 and hasattr(self, 'parents') and self.parents is not None:
            parent_times = [parent.get_times(time_range, **kwargs) for parent in self.parents.values()]
            all_times = parent_times[0]
            all_times.sort()
            for time in all_times:
                if all(time in parent_times[i] for i in range(1, len(parent_times))):
                    yield time

    @withcases
    def get_times(self, time_range: TimeRange, **kwargs) -> list[dt.datetime]:
        """
        Get a list of times between two dates.
        """
        return list(self._get_times(time_range, **kwargs))
        
    @withcases
    def check_data(self, time: Optional[TimeStep|dt.datetime] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time.
        """
        updated_self:Dataset = self.update(**kwargs)
        if 'tile' in kwargs:
            full_key = updated_self.get_key(time)
            return self._check_data(full_key)
        
        for tile in updated_self.tile_names:
            full_key = updated_self.get_key(time, tile = tile)
            if not self._check_data(full_key):
                return False
        else:
            return True
    
    @withcases
    def find_times(self, times: list[TimeStep|dt.datetime], id = False, rev = False, **kwargs) -> list[TimeStep] | list[int]:
        """
        Find the times for which data is available.
        """
        all_ids = list(range(len(times)))
        ids = [i for i in all_ids if self.check_data(times[i], **kwargs)] or []
        if rev:
            ids = [i for i in all_ids if i not in ids] or []

        if id:
            return ids
        else:
            return [times[i] for i in ids]

    @withcases
    def find_tiles(self, time: Optional[TimeStep|dt.datetime] = None, rev = False,**kwargs) -> list[str]:
        """
        Find the tiles for which data is available.
        """
        all_tiles = self.tile_names
        available_tiles = [tile for tile in all_tiles if self.check_data(time, tile = tile, **kwargs)]
        if not rev:
            return available_tiles
        else:
            return [tile for tile in all_tiles if tile not in available_tiles]

    @abstractmethod
    def _check_data(self, data_key) -> bool:
        raise NotImplementedError

    def get_start(self, **kwargs) -> dt.datetime:
        """
        Get the start of the available data.
        """
        time_start = dt.datetime(1900, 1, 1)
        time_end = dt.datetime.now()
        for time in self._get_times(TimeRange(time_start, time_end), **kwargs):
            return time

    ## METHODS TO MANIPULATE THE DATASET
    def get_key(self, time: Optional[TimeStep|dt.datetime] = None, **kwargs):
        
        time = self.get_time_signature(time)
        raw_key = substitute_string(self.key_pattern, kwargs)
        key = time.strftime(raw_key) if time is not None else raw_key
        return key

    def set_parents(self, parents:dict[str:'Dataset'], fn:Callable):
        self.parents = parents
        self.fn = fn

    ## METHODS TO MANIPULATE THE TEMPLATE
    def get_template_dict(self, make_it:bool = True, **kwargs):
        tile = kwargs.pop('tile', None)
        if tile is None:
            if self.has_tiles:
                template_dict = {}
                for tile in self.tile_names:
                    template_dict[tile] = self.get_template_dict(make_it = make_it, tile = tile, **kwargs)
                return template_dict
            else:
                tile = '__tile__'

        template_dict = self._template.get(tile, None)
        if template_dict is None and make_it:
            start_time = self.get_start(**kwargs)
            if start_time is not None:
                start_data = self.get_data(time = start_time, tile = tile, **kwargs)
                #templatearray = self.make_templatearray_from_data(start_data)
                self.set_template(start_data, tile = tile)
                template_dict = self.get_template_dict(make_it = False, **kwargs)
        
        return template_dict
    
    def set_template(self, templatearray: xr.DataArray|xr.Dataset, **kwargs):
        tile = kwargs.get('tile', '__tile__')
        # save in self._template the minimum that is needed to recreate the template
        # get the crs and the nodata value, these are the same for all tiles
        crs = templatearray.attrs.get('crs')
        if crs is not None:
            crs_wkt = crs.to_wkt()
        else:
            crs_wkt = templatearray.spatial_ref.crs_wkt

        self._template[tile] = {'crs': crs_wkt,
                                '_FillValue' : templatearray.attrs.get('_FillValue'),
                                'dims_names' : templatearray.dims,
                                'spatial_dims' : (templatearray.rio.x_dim, templatearray.rio.y_dim),
                                'dims_starts': {},
                                'dims_ends': {},
                                'dims_lengths': {}}
        
        if isinstance(templatearray, xr.Dataset):
            self._template[tile]['variables'] = list(templatearray.data_vars)
        
        for dim in templatearray.dims:
            this_dim_values = templatearray[dim].data
            start = this_dim_values[0]
            end = this_dim_values[-1]
            length = len(this_dim_values)
            self._template[tile]['dims_starts'][dim] = float(start)
            self._template[tile]['dims_ends'][dim] = float(end)
            self._template[tile]['dims_lengths'][dim] = length

    @staticmethod
    def build_templatearray(template_dict: dict) -> xr.DataArray|xr.Dataset:
        """
        Build a template xarray.DataArray from a dictionary.
        """
        shape = [template_dict['dims_lengths'][dim] for dim in template_dict['dims_names']]
        template = xr.DataArray(np.full(shape, template_dict['_FillValue']), dims = template_dict['dims_names'])
        
        for dim in template_dict['dims_names']:
            start  = template_dict['dims_starts'][dim]
            end    = template_dict['dims_ends'][dim]
            length = template_dict['dims_lengths'][dim]
            template[dim] = np.linspace(start, end, length)

        template.attrs = {'crs': template_dict['crs'], '_FillValue': template_dict['_FillValue']}
        template = template.rio.set_spatial_dims(*template_dict['spatial_dims']).rio.write_coordinate_system()

        if 'variables' in template_dict:
            template_ds = xr.Dataset({var: template.copy() for var in template_dict['variables']})
            return template_ds
        
        return template

    @staticmethod
    def set_data_to_template(data: np.ndarray|xr.DataArray|xr.Dataset,
                             template_dict: dict) -> xr.DataArray|xr.Dataset:
        
        if isinstance(data, xr.DataArray):
            x_dim, y_dim = template_dict['spatial_dims']
            crs = template_dict['crs']
            data = data.rio.set_spatial_dims(x_dim, y_dim).rio.write_crs(crs).rio.write_coordinate_system()

            for dim in template_dict['dims_names']:
                start  = template_dict['dims_starts'][dim]
                end    = template_dict['dims_ends'][dim]
                length = template_dict['dims_lengths'][dim]
                data[dim] = np.linspace(start, end, length)
            data = data.transpose(*template_dict['dims_names'])

            nodata_value = template_dict['_FillValue']
            data.attrs['_FillValue'] = nodata_value

        elif isinstance(data, np.ndarray):
            template = Dataset.build_templatearray(template_dict)
            data = template.copy(data = data)
        elif isinstance(data, xr.Dataset):
            all_data = [Dataset.set_data_to_template(data[var], template_dict) for var in template['variables']]
            data = xr.merge(all_data)
        
        return data

    def set_metadata(self, data: xr.DataArray|xr.Dataset,
                     time: Optional[TimeStep|dt.datetime] = None,
                     time_format: str = '%Y-%m-%d', **kwargs) -> xr.DataArray:
        """
        Set metadata for the data.
        """
        metadata = {}        
        if hasattr(data, 'attrs'):
            if 'long_name' in data.attrs:
                data.attrs.pop('long_name')
            metadata.update(data.attrs)
        
        metadata.update(kwargs)
        metadata['time_produced'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if time is not None:
            datatime = self.get_time_signature(time)
            metadata['time'] = datatime.strftime(time_format)

        name = metadata.get('name', self.name)
        if 'long_name' in metadata:
            metadata.pop('long_name')

        data.attrs.update(metadata)

        if isinstance(data, xr.DataArray):
            data.name = name

        return data

    ## THUMBNAIL METHODS
    @staticmethod
    def make_thumbnail(data: xr.DataArray|dict[str,xr.DataArray], options: dict, destination: str, **kwargs):
        try:
            from ..thumbnails import Thumbnail, ThumbnailCollection
        except ImportError:
            from thumbnails import Thumbnail, ThumbnailCollection

        if 'colors' in options:
            colors = options.pop('colors')
        else:
            return

        if isinstance(colors, str|Dataset):
            col_def = substitute_string(colors, kwargs) if isinstance(colors, str) else colors.update(**kwargs)
            if isinstance(data, dict):
                data = data['']
            this_thumbnail = Thumbnail(data, col_def)
            destination = destination.replace('.tif', '.png')

        elif isinstance(colors, dict):
            keys = list(colors.keys())
            col_defs = []
            for key in keys:
                col_def = substitute_string(colors[key], kwargs) if isinstance(colors[key], str) else colors[key].update(**kwargs)
                col_defs.append(col_def)
            data      = list(data[key] for key in keys)
            this_thumbnail = ThumbnailCollection(data, col_defs)
            destination = destination.replace('.tif', '.pdf')
            
        this_thumbnail.save(destination, **options)
        return destination

    ## NOTIFICATION METHODS
    def notify(self, notification_options: dict):
        try:
            from ..notification import EmailNotification
        except ImportError:
            from notification import EmailNotification

        from_address = notification_options.pop('from', None)
        email_client = notification_options.pop('email_client', None)
        email_login_env = notification_options.pop('email_login_env', None)
        email_pwd_env = notification_options.pop('email_pwd_env', None)

        notification = EmailNotification(from_address, email_client, email_login_env, email_pwd_env)

        if hasattr(self, 'thumbnail_file'):
            notification.attach(self.thumbnail_file)

        recipients = notification_options.pop('to')
        subject = notification_options.pop('subject')
        notification.send(recipients, subject, **notification_options)

    ## LOGGING METHODS
    def get_log(self,data: xr.DataArray, time: TimeStep|dt.datetime, tags: dict, other: dict[str, xr.DataArray], **log_options) -> dict:
        log_dict = {}
        log_dict['name'] = self.name

        for key, value in tags.items():
            log_dict[key] = value

        if isinstance(time, dt.datetime):
            time = time.strftime('%Y-%m-%d')
            log_dict['timestamp'] = time
        elif(isinstance(time, TimeStep)):
            log_dict['timestamp'] = self.get_time_signature(time).strftime('%Y-%m-%d')
            log_dict['timestep_start'] = time.start.strftime('%Y-%m-%d')
            log_dict['timestep_end'] = time.end.strftime('%Y-%m-%d')

        log_dict['time_added'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for key, value in other.items():
            log_dict[key] = value

        log_dict['data_checks'] = {k: str(v) for k,v in self.qc_checks(data).items()}
        return log_dict

    @staticmethod
    def write_log(log_dict, log_ds, time, **kwargs):
        if isinstance(log_ds, str):
            log_ds = Dataset.from_options({'type': 'local', 'key_pattern': log_ds})
        
        if log_ds.format == 'txt':
            # convert the log_dict to a string
            log_str = '---'
            for key, value in log_dict.items():
                log_str += f'{key}: {value}\n'
            log_str += '---'
            log_output = log_str
        elif log_ds.format == 'json':
            log_output = log_dict
    
        log_ds.write_data(log_output, time, append = True, **kwargs)

    @staticmethod
    def qc_checks(data: xr.DataArray) -> dict:
        """
        Perform quality checks on the data.
        - max and min values
        - percentage and absolute number of NaNs
        - percentage and absolute number of zeros
        - sum of values
        - sum of absolute values
        """
        data = data.values
        qc_dict = {}
        qc_dict['max'] = np.nanmax(data)
        qc_dict['min'] = np.nanmin(data)
        qc_dict['nans'] = int(np.sum(np.isnan(data)))
        qc_dict['nans_pc'] = qc_dict['nans'] / data.size
        qc_dict['zeros'] = int(np.sum(data == 0))
        qc_dict['zeros_pc'] = qc_dict['zeros'] / (data.size - qc_dict['nans'])
        qc_dict['sum'] = np.nansum(data)
        qc_dict['sum_abs'] = np.nansum(np.abs(data))

        for key, value in qc_dict.items():
            if not isinstance(value, int):
                qc_dict[key] = round(float(value), 4)

        return qc_dict
    