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
    from ..timestepping import TimeRange
    from ..timestepping.timestep import TimeStep
    from ..config.parse import substitute_string, extract_date_and_tags
    from ..config.options import Options
except ImportError:
    from timestepping import TimeRange
    from timestepping.timestep import TimeStep
    from config.parse import substitute_string, extract_date_and_tags
    from config.options import Options

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
            self.format = os.path.basename(self.key_pattern).split('.')[-1]

        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        if 'thumbnail' in kwargs:
            self.thumb_opts = kwargs.pop('thumbnail')

        if 'notification' in kwargs:
            self.notif_opts = kwargs.pop('notification')

        self._template = {}
        self.options = Options(kwargs)
        self.tags = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    @property
    def has_tiles (self):
        return '{tile}' in self.key_pattern

    @property
    def tile_names(self):
        return self.available_tags.get('tile', ['__tile__'])
    
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
    def available_tags(self):
        all_keys = self.available_keys
        all_tags = {}
        all_dates = set()
        for key in all_keys:
            this_date, this_tags = extract_date_and_tags(key, self.key_pattern)
            for tag in this_tags:
                if tag not in all_tags:
                    all_tags[tag] = set()
                all_tags[tag].add(this_tags[tag])
            all_dates.add(this_date)
        
        all_tags = {tag: list(all_tags[tag]) for tag in all_tags}
        all_tags['time'] = list(all_dates)

        return all_tags

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
            
            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags
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
        if value in ['tif', 'tiff']:
            self._format = 'geotiff'
        elif value in ['nc', 'netcdf']:
            self._format = 'netcdf'
        elif value in ['csv']:
            self._format = 'csv'
        else:
            raise ValueError(f"Invalid format: {value}, only geotiff and csv are currently supported")

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

        key_without_tags = re.sub(r'\{.*\}', '', self.key_pattern)
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

        if self.format == 'csv' and self.check_data(full_key):
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
        template = self.get_template(make_it=False, **kwargs)
        if template is None:
            template = self.make_templatearray_from_data(data)
            self.set_template(template, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            data = self.set_data_to_template(data, template)
            data.attrs.update(attrs)
        
        data.attrs.update({'source_key': full_key})
        return data
    
    @abstractmethod
    def _read_data(self, input_key:str):
        raise NotImplementedError
    
    def check_data_for_writing(self, data: xr.DataArray|xr.Dataset|np.ndarray|pd.DataFrame):
        """"
        Ensures that the data is compatible with the format of the dataset.
        """
        if isinstance(data, pd.DataFrame):
            if not self.format == 'csv':
                raise ValueError(f'Cannot write pandas dataframe to a {self.format} file.')
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

        if self.format == 'csv':
            self._write_data(data, output_file)
            return

        # if data is a numpy array, ensure there is a template available
        template = self.get_template(**kwargs, make_it=False)
        if template is None:
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                templatearray = self.make_templatearray_from_data(data)
                self.set_template(templatearray, **kwargs)
                template = self.get_template(**kwargs, make_it=False)
            else:
                raise ValueError('Cannot write numpy array without a template.')

        output = self.set_data_to_template(data, template)
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
            if 'destination' in self.thumb_opts:
                destination = self.thumb_opts.pop('destination')
                destination = timestamp.strftime(substitute_string(destination, kwargs))
            else:
                destination = output_file
            thumbnail_file = self.make_thumbnail(data = parents,
                                                 options = self.thumb_opts,
                                                 destination = destination,
                                                 **kwargs)
            self.thumbnail_file = thumbnail_file

        # add the metadata
        attrs = data.attrs if hasattr(data, 'attrs') else {}
        output.attrs.update(attrs)
        name = substitute_string(self.name, kwargs)
        metadata['name'] = name
        output = self.set_metadata(output, time, time_format, **metadata)

        if hasattr(self, 'notif_opts'):
            for key, value in self.notif_opts.items():
                if isinstance(value, str):
                    self.notif_opts[key] = timestamp.strftime(substitute_string(value, kwargs))
            
            self.notif_opts['metadata'] = output.attrs
            self.notify(self.notif_opts)

        # write the data
        self._write_data(output, output_file)

    @abstractmethod
    def _write_data(self, output: xr.DataArray, output_key: str):
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
        for time in all_times:
            if time_range.contains(time):
                yield time
        
        if len(all_times) == 0 and hasattr(self, 'parents') and self.parents is not None:
            parent_times = [parent.get_times(time_range, **kwargs) for parent in self.parents.values()]
            for time in parent_times[0]:
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
        all_keys = updated_self.available_keys
        for tile in updated_self.tile_names:
            full_key = updated_self.get_key(time, tile = tile)
            if full_key not in all_keys:
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
    def get_template(self, make_it:bool = True, **kwargs):
        tile = kwargs.pop('tile', '__tile__')
        template_dict = self._template.get(tile, None)
        start_time = self.get_start(**kwargs)
        if template_dict is None and start_time is not None and make_it:
            start_data = self.get_data(time = start_time, tile = tile, **kwargs)
            templatearray = self.make_templatearray_from_data(start_data)
            self.set_template(templatearray, tile = tile)
        elif template_dict is not None:
            templatearray = self.build_templatearray(template_dict)
        else:
            templatearray = None
        
        return templatearray
    
    def set_template(self, templatearray: xr.DataArray|xr.Dataset, **kwargs):
        tile = kwargs.get('tile', '__tile__')
        # save in self._template the minimum that is needed to recreate the template
        # get the crs and the nodata value, these are the same for all tiles
        self._template[tile] = {'crs': templatearray.attrs.get('crs'),
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
    def make_templatearray_from_data(data: xr.DataArray|xr.Dataset) -> xr.DataArray|xr.Dataset:
        """
        Make a template xarray.DataArray from a given xarray.DataArray.
        """
        nodata_value = data.attrs.get('_FillValue', np.nan)

        if isinstance(data, xr.Dataset):
            vararrays = [Dataset.make_templatearray_from_data(data[var]) for var in data]
            template  = xr.merge(vararrays)
        else:
            template = data.copy(data = np.full(data.shape, nodata_value))

        # clear all attributes
        template.attrs = {}
        template.encoding = {}
        
        # make crs and nodata explicit as attributes
        template.attrs = {'crs': data.rio.crs.to_wkt(),
                          '_FillValue': nodata_value}

        return template

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
                             template: xr.DataArray|xr.Dataset) -> xr.DataArray|xr.Dataset:
        
        if isinstance(data, xr.DataArray):
            data = data.rio.set_spatial_dims(template.rio.x_dim, template.rio.y_dim).rio.write_coordinate_system()
            data = data.transpose(*template.dims)
            output = template.copy(data = data)
        elif isinstance(data, np.ndarray):
            output = template.copy(data = data)
        elif isinstance(data, xr.Dataset):
            all_outputs = [Dataset.set_data_to_template(data[var], template[var]) for var in template]
            output = xr.merge(all_outputs)
        
        return output

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

        if isinstance(colors, str):
            col_file = substitute_string(colors, kwargs)
            if isinstance(data, dict):
                data = data['']
            this_thumbnail = Thumbnail(data, col_file)
            destination = destination.replace('.tif', '.png')

        elif isinstance(colors, dict):
            keys = list(colors.keys())
            col_files = list(substitute_string(colors[key], kwargs) for key in keys)
            data      = list(data[key] for key in keys)
            this_thumbnail = ThumbnailCollection(data, col_files)
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

## FUNCTIONS TO MANIPULATE THE DATA

# DECORATOR TO MAKE THE FUNCTION WORK WITH XR.DATASET
def withxrds(func):
    def wrapper(*args, **kwargs):
        if isinstance(args[0], xr.Dataset):
            return xr.Dataset({var: func(args[0][var], **kwargs) for var in args[0]})
        else:
            return func(*args, **kwargs)
    return wrapper

@withxrds
def straighten_data(data: xr.DataArray) -> xr.DataArray:
    """
    Ensure that the data has descending latitudes.
    """
    
    try:
        y_dim = data.rio.y_dim
    except rxr.exceptions.MissingSpatialDimensionError:
        y_dim = None

    if y_dim is None:
        for dim in data.dims:
            if 'lat' in dim.lower() or 'y' in dim.lower():
                y_dim = dim
                break
    if data[y_dim].data[0] < data[y_dim].data[-1]:
        data = data.sortby(y_dim, ascending = False)

    return data

@withxrds
def reset_nan(data: xr.DataArray) -> xr.DataArray:
    """
    Make sure that the nodata value is set to np.nan for floats and to the maximum integer for integers.
    """

    data_type = data.dtype
    new_fill_value = np.nan if np.issubdtype(data_type, np.floating) else np.iinfo(data_type).max
    fill_value = data.attrs.get('_FillValue', new_fill_value)

    data = data.where(~np.isclose(data, fill_value, equal_nan = True), new_fill_value)
    data.attrs['_FillValue'] = new_fill_value

    return data

@withxrds
def set_type(data: xr.DataArray) -> xr.DataArray:
    """
    Make sure that the data is the smallest possible.
    """

    max_value = data.max()
    min_value = data.min()

    # check if output contains floats or integers
    if np.issubdtype(data.dtype, np.floating):
        if max_value < 2**31 and min_value > -2**31:
            data = data.astype(np.float32)
        else:
            data = data.astype(np.float64)
    elif np.issubdtype(data.dtype, np.integer):
        if min_value >= 0:
            if max_value <= 255:
                data = data.astype(np.uint8)
            elif max_value <= 65535:
                data = data.astype(np.uint16)
            elif max_value < 2**31:
                data = data.astype(np.uint32)
            else:
                data = data.astype(np.uint64)
        else:
            if max_value <= 127 and min_value >= -128:
                data = data.astype(np.int8)
            elif max_value <= 32767 and min_value >= -32768:
                data = data.astype(np.int16)
            elif max_value < 2**31 and min_value > -2**31:
                data = data.astype(np.int32)
            else:
                data = data.astype(np.int64)

    return reset_nan(data)