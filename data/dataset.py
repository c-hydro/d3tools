from typing import Optional, Generator, Callable
import datetime as dt
import numpy as np
import xarray as xr
from abc import ABC, ABCMeta, abstractmethod
from functools import cached_property
import os

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
            self.name = '.'.join(os.path.basename(self.file).split('.')[:-1])

        if 'format' in kwargs:
            self.format = kwargs.pop('format')
        else:
            self.format = os.path.basename(self.file).split('.')[-1]

        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')

        if 'ntiles' in kwargs:
            self._template = [None]*kwargs.pop('ntiles')
        else:
            self._template = [None]

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
            self.time_signature = self._defaults['time_signature']
        
        return self._time_signature
        
    @time_signature.setter
    def time_signature(self, value):
        if value not in ['start', 'end', 'end+1']:
            raise ValueError(f"Invalid time signature: {value}")
        self._time_signature = value

    def get_time_signature(self, time: TimeStep) -> dt.datetime:
        time_signature = self.time_signature
        if time_signature == 'start':
            this_time = time.start
        elif time_signature == 'end':
            this_time = time.end
        elif time_signature == 'end+1':
            this_time = (time+1).start

        if '%Y' not in self.path_pattern and this_time.month == 2 and this_time.day == 29:
            this_time = this_time.replace(day = 28)
        
        return this_time

    ## INPUT/OUTPUT METHODS
    def get_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        full_path = self.path(time, **kwargs)

        if self.check_data(time, **kwargs):
            data = self._read_data(full_path, **kwargs)

            # ensure that the data has descending latitudes
            data = straighten_data(data)

            # make sure the nodata value is set to np.nan
            data = reset_nan(data)
        
        # if the data is not available, try to calculate it from the parents
        elif hasattr(self, 'parents') and self.parents is not None:
            data = self.make_data(time, **kwargs)
                
        else:
            raise ValueError(f'Could not resolve data from {full_path}.')
        
        # if there is no template for the dataset, create it from the data
        if self.get_template(**kwargs) is None:
            template = self.make_template_from_data(data)
            self.set_template(template, **kwargs)
        else:
            # otherwise, update the data in the template
            # (this will make sure there is no errors in the coordinates due to minor rounding)
            attrs = data.attrs
            data = self.get_template(**kwargs).copy(data = data)
            data.attrs.update(attrs)

        return data
    
    @abstractmethod
    def _read_data(self, input_path:str):
        raise NotImplementedError
    
    def write_data(self, data: xr.DataArray,
                   time: Optional[dt.datetime|TimeStep] = None,
                   time_format: str = '%Y-%m-%d',
                   tags = {},
                   **kwargs):
        
        if data is None or data.size == 0:
            output = self.get_template(**tags)
        else:
            output = self.get_template(**tags).copy(data = data)

        # add metadata
        output = self.set_metadata(output, time, time_format, **kwargs)

        output_file = self.path(time, **tags)
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

    def get_times(self, time_range: TimeRange, **kwargs) -> list[dt.datetime]:
        """
        Get a list of times between two dates.
        """
        return list(self._get_times(time_range, **kwargs))

    def check_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time.
        """
        full_path = self.path(time, **kwargs)
        return self._check_data(full_path)
    
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
    def path(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        
        if isinstance(time, TimeStep):
            time: dt.datetime = self.get_time_signature(time)

        raw_path = substitute_string(self.path_pattern, kwargs)
        path = time.strftime(raw_path) if time is not None else raw_path
        return path

    def set_parents(self, parents:dict[str:'Dataset'], fn:Callable):
        self.parents = parents
        self.fn = fn

    ## METHODS TO MANIPULATE THE TEMPLATE
    def get_template(self, **kwargs):

        tile = kwargs.get('tile', 0)
        return self._template[tile]
        # if self._template[tile] is not None:
        #     return self._template[tile]
        # else:
        #     return None
        #     try:
        #         start_data = self.get_data(time = self.start, **kwargs)
        #         template = self.make_template_from_data(start_data)
        #         self._template[tile] = template
        #     except ValueError:
        #         template = None
        
        # return template

    def set_template(self, template: xr.DataArray, **kwargs):
        tile = kwargs.get('tile', 0)
        self._template[tile] = template

    def set_metadata(self, data: xr.DataArray,
                     time: Optional[dt.datetime|TimeStep],
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

        metadata['name'] = self.name
        if 'long_name' in metadata:
            metadata.pop('long_name')

        data.attrs.update(metadata)
        data.name = self.name

    @staticmethod
    def make_template_from_data(data: xr.DataArray) -> xr.DataArray:
        """
        Make a template xarray.DataArray from a given xarray.DataArray.
        """
        # make a copy of the data, but fill it with NaNs
        template = data.copy(data = np.full(data.shape, np.nan))

        # clear all attributes
        template.attrs = {}
        template.encoding = {}

        # make crs and nodata explicit as attributes
        template.attrs = {'crs': data.rio.crs.to_wkt(),
                          '_FillValue': np.nan}

        return template
    
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
    if data[y_dim][0] < data[y_dim][-1]:
        data = data.sortby(y_dim, ascending = False)

    return data

def reset_nan(data: xr.DataArray) -> xr.DataArray:
    """
    Make sure that the nodata value is set to np.nan.
    """

    if '_FillValue' in data.attrs and not np.isnan(data.attrs['_FillValue']):
        data = data.where(data != data.attrs['_FillValue'])
        data.attrs['_FillValue'] = np.nan

    return data