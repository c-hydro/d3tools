from typing import Optional, Generator, Callable
import datetime as dt
import numpy as np
import xarray as xr
from abc import ABC, ABCMeta, abstractmethod
from functools import cached_property
import os
import re

try:
    from ..timestepping import TimeRange
    from ..timestepping.timestep import TimeStep
    from ..config.parse import substitute_string
    from ..config.options import Options
except ImportError:
    from timestepping import TimeRange
    from timestepping.timestep import TimeStep
    from config.parse import substitute_string
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

    def __init__(self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs):
        if path is not None:
            self.dir = path
        elif 'dir' in kwargs:
            self.dir = kwargs.pop('dir')

        if filename is not None:
            self.file = filename
        elif 'file' in kwargs:
            self.file = kwargs.pop('file')
        
        self.path_pattern = os.path.join(self.dir, self.file)

        if 'name' in kwargs:
            self.name   = kwargs.pop('name')
        else:
            basename_noext  = '.'.join(os.path.basename(self.file).split('.')[:-1])
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
            self.format = os.path.basename(self.file).split('.')[-1]

        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        if 'thumbnail' in kwargs:
            self.thumb_opts = kwargs.pop('thumbnail')

        self._template = {}
        self.options = Options(kwargs)
        self.tags = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def update(self, in_place = False, **kwargs):
        new_path = substitute_string(self.dir, kwargs)
        new_file = substitute_string(self.file, kwargs)
        new_name = substitute_string(self.name, kwargs)

        if in_place:
            self.dir  = new_path
            self.file = new_file
            self.name = new_name
            self.path_pattern = self.path(**kwargs)
            self.tags.update(kwargs)
            return self
        else:
            new_options = self.options.copy()
            new_options.update({'dir': new_path, 'file': new_file, 'name': new_name})
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
        else:
            raise ValueError(f"Invalid format: {value}, only geotiff is currently supported")

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
        else:
            time_signature = self.time_signature
            if time_signature == 'start':
                time = timestep.start
            elif time_signature == 'end':
                time = timestep.end
            elif time_signature == 'end+1':
                time = (timestep+1).start

        path_without_tags = re.sub(r'\{.*\}', '', self.path_pattern)
        hasyear = '%Y' in path_without_tags

        if not hasyear and time.month == 2 and time.day == 29:
            time = time.replace(day = 28)
        
        return time

    ## INPUT/OUTPUT METHODS
    def get_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        full_path = self.path(time, **kwargs)

        if self.check_data(time, **kwargs):
            data = self._read_data(full_path)

            # ensure that the data has descending latitudes
            data = straighten_data(data)

            # make sure the nodata value is set to np.nan for floats and to the max int for integers
            data = reset_nan(data)
        
        # if the data is not available, try to calculate it from the parents
        elif hasattr(self, 'parents') and self.parents is not None:
            data = self.make_data(time, **kwargs)
                
        else:
            raise ValueError(f'Could not resolve data from {full_path}.')
        
        # if there is no template for the dataset, create it from the data
        if self.get_template(make_it=False, **kwargs) is None:
            template = self.make_template_from_data(data)
            self.set_template(template, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            data = self.get_template(make_it=False, **kwargs).copy(data = data)
            data.attrs.update(attrs)

        return data
    
    @abstractmethod
    def _read_data(self, input_path:str):
        raise NotImplementedError
    
    def write_data(self, data: xr.DataArray|np.ndarray,
                   time: Optional[dt.datetime|TimeStep] = None,
                   time_format: str = '%Y-%m-%d',
                   metadata = {},
                   **kwargs):
        
        # if data is a numpy array, enure there is a template available
        template = self.get_template(**kwargs, make_it=False)
        if template is None:
            if isinstance(data, xr.DataArray):
                template = self.make_templatearray_from_data(data)
                self.set_template(template, **kwargs)
            else:
                raise ValueError('Cannot write numpy array without a template.')

        # if data is None, just use the template
        if data is None or data.size == 0:
            output = template
        else:
            output = template.copy(data = data)

        # add metadata
        attrs = data.attrs if hasattr(data, 'attrs') else {}
        output.attrs.update(attrs)
        name = substitute_string(self.name, kwargs)
        metadata['name'] = name
        output = self.set_metadata(output, time, time_format, **metadata)

        # make sure the data is the smallest possible
        output = set_type(output)

        # ensure that the data has descending latitudes
        output = straighten_data(output)

        # check if there is a thumbnail to be saved and save it
        output_file = self.path(time, **kwargs)
        if hasattr(self, 'thumb_opts'):
            try:
                from ..thumbnails import Thumbnail
            except ImportError:
                from thumbnails import Thumbnail
            thumb_opts = self.thumb_opts
            if 'colors' in thumb_opts:
                col_file = substitute_string(thumb_opts['colors'], kwargs)
                this_thumbnail = Thumbnail(output, col_file)
                this_thumbnail.save(output_file.replace('.tif', '.png'), annotation = os.path.basename(output_file), **thumb_opts)

        # write the data
        self._write_data(output, output_file)

    @abstractmethod
    def _write_data(self, output: xr.DataArray, output_path: str):
        raise NotImplementedError

    def make_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        if not hasattr(self, 'parents') or self.parents is None:
            raise ValueError(f'No parents for {self.name}')
        
        parent_data = {name: parent.get_data(time, **kwargs) for name, parent in self.parents.items()}
        data = self.fn(**parent_data)
        self.write_data(data, time, **kwargs)

    ## METHODS TO CHECK DATA AVAILABILITY
    def _get_times(self, time_range: TimeRange, **kwargs) -> Generator[dt.datetime, None, None]:
        for timestep in time_range.days:
            time = timestep.start
            if self.check_data(time, **kwargs):
                yield time
            elif hasattr(self, 'parents') and self.parents is not None:
                if all(parent.check_data(time, **kwargs) for parent in self.parents.values()):
                    yield time

    @withcases
    def get_times(self, time_range: TimeRange, **kwargs) -> list[dt.datetime]:
        """
        Get a list of times between two dates.
        """
        return list(self._get_times(time_range, **kwargs))

    @withcases
    def check_data(self, time: Optional[TimeStep] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time.
        """
        full_path = self.path(time, **kwargs)
        return self._check_data(full_path)
    
    @withcases
    def find_times(self, times: list[TimeStep], id = False, rev = False, **kwargs) -> list[TimeStep] | list[int]:
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
    def _check_data(self, data_path) -> bool:
        raise NotImplementedError

    @cached_property
    def start(self):
        """
        Get the start of the available data.
        """
        time_start = dt.datetime(1900, 1, 1)
        time_end = dt.datetime.now()
        for time in self._get_times(TimeRange(time_start, time_end)):
            return time

    ## METHODS TO MANIPULATE THE DATASET
    def path(self, time: Optional[TimeStep] = None, **kwargs):
        
        time = self.get_time_signature(time)

        raw_path = substitute_string(self.path_pattern, kwargs)
        path = time.strftime(raw_path) if time is not None else raw_path
        return path

    def set_parents(self, parents:dict[str:'Dataset'], fn:Callable):
        self.parents = parents
        self.fn = fn

    ## METHODS TO MANIPULATE THE TEMPLATE
    def get_template(self, make_it:bool = True, **kwargs):

        tile = kwargs.pop('tile', '__tile__')
        template_dict = self._template.get(tile, None)
        if template_dict is None and self.start is not None and make_it:
            start_data = self.get_data(time = self.start, tile = tile, **kwargs)
            templatearray = self.make_templatearray_from_data(start_data)
            self.set_template(templatearray, tile = tile)
        elif template_dict is not None:
            templatearray = self.build_templatearray(template_dict)
        else:
            templatearray = None
        
        return templatearray
    
    def set_template(self, templatearray: xr.DataArray, **kwargs):
        tile = kwargs.get('tile', '__tile__')
        # save in self._template the minimum that is needed to recreate the template
        # get the crs and the nodata value, these are the same for all tiles
        self._template[tile] = {'crs': templatearray.attrs.get('crs'),
                                '_FillValue' : templatearray.attrs.get('_FillValue'),
                                'dims_names' : templatearray.dims,
                                'dims_starts': {},
                                'dims_ends': {},
                                'dims_lengths': {}}
        
        for dim in templatearray.dims:
            this_dim_values = templatearray[dim].data
            start = this_dim_values[0]
            end = this_dim_values[-1]
            length = len(this_dim_values)
            self._template[tile]['dims_starts'][dim] = float(start)
            self._template[tile]['dims_ends'][dim] = float(end)
            self._template[tile]['dims_lengths'][dim] = length

    @staticmethod
    def make_templatearray_from_data(data: xr.DataArray) -> xr.DataArray:
        """
        Make a template xarray.DataArray from a given xarray.DataArray.
        """

        nodata_value = data.attrs.get('_FillValue', np.nan)

        # make a copy of the data, but fill it with NaNs
        template = data.copy(data = np.full(data.shape, nodata_value))

        # clear all attributes
        template.attrs = {}
        template.encoding = {}

        # make crs and nodata explicit as attributes
        template.attrs = {'crs': data.rio.crs.to_wkt(),
                          '_FillValue': nodata_value}

        return template

    @staticmethod
    def build_templatearray(template_dict: dict) -> xr.DataArray:
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

        return template

    def set_metadata(self, data: xr.DataArray,
                     time: Optional[TimeStep|dt.datetime],
                     time_format: str, **kwargs) -> xr.DataArray:
        """
        Set metadata for the data.
        """
        metadata = {}
        
        metadata.update(kwargs)
        if hasattr(data, 'attrs'):
            metadata.update(data.attrs)
        
        metadata['time_produced'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if time is not None:
            datatime = self.get_time_signature(time)
            metadata['time'] = datatime.strftime(time_format)

        name = metadata.get('name', self.name)
        if 'long_name' in metadata:
            metadata.pop('long_name')

        data.attrs.update(metadata)
        data.name = name

        return data
    
def straighten_data(data: xr.DataArray) -> xr.DataArray:
    """
    Ensure that the data has descending latitudes.
    """
    y_dim = data.rio.y_dim
    if y_dim is None:
        for dim in data.dims:
            if 'lat' in dim.lower() | 'y' in dim.lower():
                y_dim = dim
                break
    if data[y_dim].data[0] < data[y_dim].data[-1]:
        data = data.sortby(y_dim, ascending = False)

    return data

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

    return data