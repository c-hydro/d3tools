from typing import Optional, Generator, Callable
import datetime as dt
import numpy as np
import xarray as xr
import geopandas as gpd
#import atexit

from abc import ABC, ABCMeta, abstractmethod
import os
import re

import tempfile

from ..timestepping import TimeRange, Month, TimeStep, estimate_timestep, TimeWindow
from ..parse import substitute_string, extract_date_and_tags
from .io_utils import get_format_from_path, straighten_data, set_type, check_data_format
from ..exit import run_at_exit_first, rm_at_exit

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

        # subsitute "now" with the current time
        self.key_pattern = substitute_string(self.key_pattern, {'now': dt.datetime.now()})

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

        if 'aggregation' in kwargs:
            self.agg = TimeWindow.from_str(kwargs.pop('aggregation'))

        if 'timestep' in kwargs:
            self.timestep = TimeStep.from_unit(kwargs.pop('timestep'))
            if hasattr(self, 'agg'):
                self.timestep = self.timestep.with_agg(self.agg)

        if 'notification' in kwargs:
            self.notif_opts = kwargs.pop('notification')
            run_at_exit_first(self.notify)

        if 'thumbnail' in kwargs:
            self.thumb_opts = self.parse_thumbnail_options(kwargs.pop('thumbnail'))

        if 'log' in kwargs:
            self.log_opts = self.parse_log_options(kwargs.pop('log'))

        if 'tile_names' in kwargs:
            self.tile_names = kwargs.pop('tile_names')

        if 'nan_value' in kwargs:
            self.nan_value = kwargs.pop('nan_value')
        else:
            self.nan_value = None

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

            if hasattr(self, 'parents') and self.parents is not None:
                new_parents = {k:p.update(**kwargs) for k,p in self.parents.items()}
                self.parents = new_parents

            return self
        else:
            new_options = self.options.copy()
            new_options.update({'key_pattern': new_key_pattern, 'name': new_name})
            new_dataset = self.__class__(**new_options)

            new_dataset._template = self._template
            if hasattr(self, '_tile_names'):
                new_dataset._tile_names = self._tile_names

            new_dataset.time_signature = self.time_signature
            if hasattr(self, 'timestep') and self.timestep is not None:
                new_dataset.timestep = self.timestep
            if hasattr(self, 'agg'):
                new_dataset.agg = self.agg

            if hasattr(self, 'parents') and self.parents is not None:
                new_dataset.parents = {k:p.update(**kwargs) for k,p in self.parents.items()}
                new_dataset.fn = self.fn
            
            new_tags = self.tags.copy()
            new_tags.update(kwargs)
            new_dataset.tags = new_tags
            new_dataset.nan_value = self.nan_value
            return new_dataset

    def copy(self, template = False):
        new_dataset = self.update()
        if template:
            new_dataset._template = self._template
        if hasattr(self, 'log_opts'):
            new_dataset.log_opts = self.log_opts
        if hasattr(self, 'thumb_opts'):
            new_dataset.thumb_opts = self.thumb_opts
        if hasattr(self, 'notif_opts'):
            new_dataset.notif_opts = self.notif_opts
        return new_dataset

    ## CLASS METHODS FOR FACTORY
    @classmethod
    def from_options(cls, options: dict, defaults: dict = None):
        defaults = defaults or {}
        new_options = defaults.copy()
        new_options.update(options)

        type = new_options.pop('type', None)
        type = cls.get_type(type)
        Subclass: 'Dataset' = cls.get_subclass(type)

        return Subclass(**new_options)

    @classmethod
    def get_subclass(cls, type: str):
        type = cls.get_type(type)
        Subclass: 'Dataset'|None = cls.subclasses.get(type.lower())
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
    def has_version(self):
        return '{file_version}' in self.key_pattern

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
        return self.get_available_keys()
    
    @property
    def agg(self):
        return self._agg

    @agg.setter
    def agg(self, value):
        self._agg = value
        if hasattr(self, 'timestep') and self.timestep is not None:
            self.timestep = self.timestep.with_agg(value)

    @property
    def timestep(self):
        return self._timestep
    
    @timestep.setter
    def timestep(self, value):
        self._timestep = value
        if hasattr(self, 'agg'):
            self._timestep = self._timestep.with_agg(self.agg)

    def get_available_keys(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        
        if isinstance(time, TimeRange):
            months = time.months
            if len(months) > 1:
                files = []
                for month in months:
                    t_start =  max(month.start, time.start)
                    t_end   =  min(month.end, time.end)
                    files.extend(self.get_available_keys(TimeRange(t_start, t_end), **kwargs))
                return files    

        prefix = self.get_prefix(time, **kwargs)
        if not self._check_data(prefix):
            return []
        if isinstance(time, dt.datetime):
            time = TimeRange(time, time)

        key_pattern = self.get_key(time = None, **kwargs)
        files = []
        for file in self._walk(prefix):
            try:
                this_time, _ = extract_date_and_tags(file, key_pattern)
                if time is None or (time is not None and time.contains(this_time)) or not self.has_time:
                    files.append(file)
            except ValueError:
                pass
        
        return files

    def _walk(self, prefix: str) -> Generator[str, None, None]:
        raise NotImplementedError

    @property
    def is_static(self):
        return not '{' in self.key_pattern and not self.has_time

    @property
    def has_time(self):
        return '%' in self.key_pattern

    @property
    def available_tags(self):
        return self.get_available_tags()

    def get_prefix(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        if not isinstance(time, TimeRange):
            prefix = self.get_key(time = time, **kwargs)
        else:
            start = time.start
            end = time.end
            prefix = self.get_key(time = None, **kwargs)
            if start.year == end.year:
                prefix = prefix.replace('%Y', str(start.year))
                if start.month == end.month:
                    prefix = prefix.replace('%m', f'{start.month:02d}')
                    if start.day == end.day:
                        prefix = prefix.replace('%d', f'{start.day:02d}')

        prefix = os.path.dirname(prefix)
        while '%' in prefix or '{' in prefix:
            prefix = os.path.dirname(prefix)
        
        return prefix

    def get_available_tags(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        all_keys = self.get_available_keys(time, **kwargs)
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

    def get_last_date(self, now = None, n = 1, lim = dt.datetime(1900,1,1), **kwargs) -> dt.datetime|list[dt.datetime]|None:
        if now is None:
            now = dt.datetime.now()
        
        # the most efficient way, I think is to search my month
        this_month = Month(now.year, now.month)
        last_date = []
        while len(last_date) < n:
            this_month_times = self.get_times(this_month, **kwargs)
            if len(this_month_times) > 0:
                valid_time = [t for t in this_month_times if t <= now]
                valid_time.sort(reverse = True)
                last_date.extend(valid_time)
            elif this_month.start < lim:
                break

            this_month = this_month - 1
            
        if len(last_date) == 0:
            return None
        if n == 1:
            return last_date[0]
        else:
            return last_date

    def get_last_ts(self, **kwargs) -> TimeStep:

        last_date = self.get_last_date(**kwargs)
        if last_date is None:
            return None
        
        if hasattr(self, 'timestep') and self.timestep is not None:
            timestep = self.timestep
        else:
            kwargs.pop('now', None)
            other_dates = self.get_last_date(now = last_date, n = 3, **kwargs)
            timestep = estimate_timestep(other_dates)
            if timestep is None:
                return None

        if self.time_signature == 'end+1':
            return timestep.from_date(last_date) -1
        else:
            return timestep.from_date(last_date)

    def estimate_timestep(self, date_sample = None, **kwargs) -> TimeStep:
        if hasattr(self, 'timestep') and self.timestep is not None:
            return self.timestep
        
        if date_sample is None or len(date_sample) == 0:
            date_sample = self.get_last_date(n = 8, **kwargs)
        elif len(date_sample) < 5:
            other_dates = self.get_last_date(n = 8 - len(date_sample), now = min(date_sample), **kwargs)  or []
            date_sample = other_dates + date_sample

        timestep = estimate_timestep(date_sample)
        if timestep is not None and hasattr(self, 'agg'):
            timestep = timestep.with_agg(self.agg)
        
        self.timestep = timestep
        return timestep

    def get_first_date(self, start = None, n = 1, **kwargs) -> dt.datetime|list[dt.datetime]|None:
        if start is None:
            start = dt.datetime(1900, 1, 1)

        end = self.get_last_date(**kwargs)
        if end is None:
            return None
        
        start_month = Month(start.year, start.month)
        end_month   = Month(end.year, end.month)

        # first look for a suitable time to start the search
        while True:
            midpoint = start_month.start + (end_month.end - start_month.start) / 2
            mid_month = Month(midpoint.year, midpoint.month)
            mid_month_times = self.get_times(mid_month, **kwargs)
            # if we do actually find some times in the month
            if len(mid_month_times) > 0:

                    # end goes to midpoint
                    end_month = mid_month
            # if we didn't find any times in the month 
            else:
                # we start from the midpoint this time
                start_month = mid_month

            if start_month + 1 == end_month:
                break
        
        first_date = []
        while len(first_date) < n and start_month.end <= end:
            this_month_times = self.get_times(start_month, **kwargs)
            valid_time = [t for t in this_month_times if t >= start]
            valid_time.sort()
            first_date.extend(valid_time)

            start_month = start_month + 1

        if len(first_date) == 0:
            return None
        if n == 1:
            return first_date[0]
        else:
            return first_date

    def get_first_ts(self, **kwargs) -> TimeStep:

        first_date = self.get_first_date(**kwargs)
        if first_date is None:
            return None
        
        if hasattr(self, 'timestep') and self.timestep is not None:
            timestep = self.timestep
        else:
            other_dates = self.get_first_date(start = first_date, n = 8, **kwargs)
            timestep = estimate_timestep(other_dates)
            if timestep is None:
                return None

        if self.time_signature == 'end+1':
            return timestep.from_date(first_date) -1
        else:
            return timestep.from_date(first_date)

    def get_start(self, agg=True, **kwargs) -> dt.datetime:
        """
        Get the start of the available data.
        """
        first_ts = self.get_first_ts(**kwargs)
        if first_ts is not None:
            if agg:
                return first_ts.agg_range.start
            else:
                return first_ts.start
        else:
            return self.get_first_date(**kwargs)

    def is_subdataset(self, other: 'Dataset') -> bool:
        key = self.get_key(time = dt.datetime(1900,1,1))
        try:
            extract_date_and_tags(key, other.key_pattern)
            return True
        except ValueError:
            return False

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
            if hasattr(self, 'timestep') and self.timestep is not None: 
                length = self.timestep.from_date(time).get_length()
            elif hasattr(self, 'previous_requested_time'):
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
        
        # progressively remove the non-used tags from the time
        if '%s' not in key_without_tags:
            time = time.replace(second = 0)
            if '%M' not in key_without_tags:
                time = time.replace(minute = 0)
                if '%H' not in key_without_tags:
                    time = time.replace(hour = 0)
                    if '%d' not in key_without_tags:
                        time = time.replace(day = 1)
                        if '%m' not in key_without_tags:
                            time = time.replace(month = 1)

        return time

    ## INPUT/OUTPUT METHODS
    def get_data(self, time: Optional[dt.datetime|TimeStep] = None, as_is = False, **kwargs):
        # if this is a versioned file, and the version is not specified, get the latest version
        if self.has_version and 'file_version' not in kwargs:
            available_versions = self.get_available_tags(time, **kwargs).get('file_version')
            if available_versions is not None:
                available_versions.sort()
                kwargs['file_version'] = available_versions[-1]

        full_key = self.get_key(time, **kwargs)

        if self.format in ['csv', 'json', 'txt', 'shp', 'parquet']:
            if self._check_data(full_key):
                return self._read_data(full_key)
            else:
                raise ValueError(f'Could not resolve data from {full_key}.')
            
        if self._check_data(full_key):
            data = self._read_data(full_key)

            if as_is:
                return data
            
            # ensure that the data has descending latitudes
            data = straighten_data(data)

            # make sure the nodata value is set to np.nan for floats and to the max int for integers
            data = set_type(data, self.nan_value, read = True)

        # if the data is not available, try to calculate it from the parents
        elif hasattr(self, 'parents') and self.parents is not None:
            data = self.make_data(time, **kwargs)
            if as_is:
                return data
            data = straighten_data(data)
            data = set_type(data, self.nan_value, read = True)

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

    def write_data(self, data,
                   time: Optional[dt.datetime|TimeStep] = None,
                   time_format: str = '%Y-%m-%d',
                   metadata = None,
                   as_is = False,
                   **kwargs):

        if metadata is None: metadata = {}
        check_data_format(data, self.format)

        output_file = self.get_key(time, **kwargs)

        if self.format in ['csv', 'json', 'txt', 'shp', 'parquet']:
            append = kwargs.pop('append', False)
            self._write_data(data, output_file, append = append)

            if hasattr(self, 'thumb_opts') and self.thumb_opts is not None:
                thumb_opts = self.thumb_opts.copy()

                destination = thumb_opts.pop('destination')
                if 'annotation' in thumb_opts and 'text' not in thumb_opts['annotation']:
                    thumb_opts['annotation']['text'] = os.path.basename(output_file)
                else:
                    thumb_opts['annotation'] = {'text': os.path.basename(output_file)}
                self.make_thumbnail(data = data,
                                    options = thumb_opts,
                                    destination = destination,
                                    time = time, **kwargs)

            return
        
        if self.format == 'file':
            self._write_data(data, output_file)
            return
        
        if as_is:
            output = data
            output = output.rio.write_nodata(output.attrs.get('_FillValue', self.nan_value))
        else:
        # if data is a numpy array, ensure there is a template available
            try:
                template_dict = self.get_template_dict(**kwargs)
            except PermissionError:
                template_dict = None

            if template_dict is None:
                if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                    #templatearray = self.make_templatearray_from_data(data)
                    self.set_template(data, **kwargs)
                    template_dict = self.get_template_dict(**kwargs, make_it=False)
                else:
                    raise ValueError('Cannot write numpy array without a template.')
            
            # if the data is an xarray, straighen it before setting it to the template
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                data = straighten_data(data)
                output = self.set_data_to_template(data, template_dict)
            # if the data is a numpy array, set it to the template and then straighten it (which should be unnecessary)
            else:
                output = self.set_data_to_template(data, template_dict)
                output = straighten_data(output)
            
            # fix the type and the nodata value
            output = set_type(output, self.nan_value, read = False)
            
        output.attrs['source_key'] = output_file
        # if necessary generate the thubnail
        if 'parents' in metadata:
            parents = metadata.pop('parents')
        else:
            parents = {}

        if hasattr(self, 'thumb_opts') and self.thumb_opts is not None:
            parents[''] = output
            thumb_opts = self.thumb_opts.copy()

            destination = thumb_opts.pop('destination')
            thumbnail = self.make_thumbnail(data = parents,
                                            options = thumb_opts,
                                            destination = destination,
                                            time = time, **kwargs)
            thumbnail_file = thumbnail.thumbnail_file
        else:
            thumbnail_file = None

        # add the metadata
        old_attrs = data.attrs if hasattr(data, 'attrs') else {}
        new_attrs = output.attrs
        old_attrs.update(new_attrs)
        output.attrs = old_attrs
        
        name = substitute_string(self.name, kwargs)
        metadata['name'] = str(name)
        output = self.set_metadata(output, time, time_format, **metadata)
        # write the data
        self._write_data(output, output_file)
        
        # get the info for the logs
        other_to_log = {}
        other_to_log['source_key'] = output_file
        if thumbnail_file is not None:
            other_to_log['thumbnail'] = thumbnail_file
        if hasattr(self, 'log_opts'):
            log_dict = self.get_log(output, options = self.log_opts, time = time, **kwargs, **other_to_log)
            log_opts = self.log_opts.copy()
            log_output = log_opts.pop('output')
            self.write_log(log_dict, log_output, time, **kwargs)
        
        if hasattr(self, 'notif_opts'):
            log_dict = self.get_log(output, time = time, **kwargs, **other_to_log)
            this_layer = {'tags' : kwargs, 'time' : time, 'log' : log_dict, 'thumbnail' : thumbnail_file}
            if 'layers' in self.notif_opts:
                self.notif_opts['layers'].append(this_layer)
            else:
                self.notif_opts['layers'] = [this_layer]

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
        if self.type != 'memory':
            self.write_data(data, time, **kwargs)
        return data

    ## METHODS TO CHECK DATA AVAILABILITY
    def _get_times(self, time_range: TimeRange, **kwargs) -> Generator[dt.datetime, None, None]:

        all_times = self.get_available_tags(time_range, **kwargs)['time']
        all_times.sort()
        for time in all_times:
            if time_range.contains(time):
                yield time
        
        if hasattr(self, 'parents') and self.parents is not None:
            parent_times = [set(parent.get_times(time_range, **kwargs)) for parent in self.parents.values()]
            # get the intersection of all times
            parent_times = set.intersection(*parent_times)
            for time in parent_times:
                if time not in all_times and time_range.contains(time):
                    yield time

    @withcases
    def get_times(self, time_range: TimeRange, **kwargs) -> list[dt.datetime]:
        """
        Get a list of times between two dates.
        """
        return list(self._get_times(time_range, **kwargs))

    def get_timesteps(self, time_range: TimeRange, **kwargs) -> list[TimeStep]:

        timestep = self.estimate_timestep()
        window = TimeWindow(1, timestep.unit)

        if self.time_signature == 'start':
            _time_range = time_range.extend(window, before = True)
        elif self.time_signature.startswith('end'):
            _time_range = time_range.extend(window, before = False)
            if self.time_signature == 'end+1':
                _time_range = time_range.extend(TimeWindow(1, 'd'), before = False)

        times = self.get_times(_time_range, **kwargs)
        if self.time_signature == 'end+1':
            times = [t - dt.timedelta(days = 1) for t in times]
        
        timesteps = [timestep.from_date(t) for t in times]
        for ts in timesteps:
            if ts.start > time_range.end + dt.timedelta(minutes=1439) or ts.end < time_range.start:
                timesteps.remove(ts)

        return timesteps

    @withcases
    def check_data(self, time: Optional[TimeStep|dt.datetime] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time.
        """
        # if this is a versioned file, and the version is not specified, get the latest version
        if self.has_version and 'file_version' not in kwargs:
            available_versions = self.get_available_tags(time, **kwargs).get('file_version')
            if available_versions is not None:
                available_versions.sort()
                kwargs['file_version'] = available_versions[-1]

        if 'tile' in kwargs:
            full_key = self.get_key(time, **kwargs)
            if self._check_data(full_key):
                return True
            elif hasattr(self, 'parents') and self.parents is not None:
                return all([parent.check_data(time, **kwargs) for parent in self.parents.values()])
            else:
                return False

        for tile in self.tile_names:
            if not self.check_data(time, tile = tile, **kwargs):
                return False
        else:
            return True
    
    @withcases
    def find_times(self, times: list[TimeStep|dt.datetime], id = False, rev = False, **kwargs) -> list[TimeStep] | list[int]:
        """
        Find the times for which data is available.
        """
        all_ids = list(range(len(times)))

        time_signatures = [self.get_time_signature(t) for t in times]
        tr = TimeRange(min(time_signatures), max(time_signatures))

        all_times = self.get_available_tags(tr, **kwargs).get('time', [])

        ids = [i for i in all_ids if time_signatures[i] in all_times] or []
        if rev:
            ids = [i for i in all_ids if i not in ids] or []

        if id:
            return ids
        else:
            return [times[i] for i in ids]

    @withcases
    def find_tiles(self, time: Optional[TimeStep|dt.datetime] = None, rev = False, **kwargs) -> list[str]:
        """
        Find the tiles for which data is available.
        """
        all_tiles = self.tile_names
        available_tiles = self.get_available_tags(time, **kwargs).get('tile', [])
        
        if not rev:
            return [tile for tile in all_tiles if tile in available_tiles]
        else:
            return [tile for tile in all_tiles if tile not in available_tiles]

    @abstractmethod
    def _check_data(self, data_key) -> bool:
        raise NotImplementedError

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
            if not self.has_time:
                data = self.get_data(as_is = True, **kwargs)
                self.set_template(data, tile = tile)

            else:
                first_date = self.get_first_date(tile = tile, **kwargs)
                if first_date is not None:
                    data = self.get_data(time = first_date, tile = tile, as_is=True, **kwargs)
                else:
                    return None
            
            data = straighten_data(data)
            #templatearray = self.make_templatearray_from_data(start_data)
            self.set_template(data, tile = tile)
            template_dict = self.get_template_dict(make_it = False, tile = tile, **kwargs)
        
        return template_dict
    
    def set_template(self, templatearray: xr.DataArray|xr.Dataset, **kwargs):
        tile = kwargs.get('tile', '__tile__')

        if isinstance(templatearray, xr.Dataset):
            vars = list(templatearray.data_vars)
            templatearray = templatearray[vars[0]]
        else:
            vars = None

        with tempfile.TemporaryDirectory() as tmpdir:
            # write the template to a temporary file
            templatearray.rio.to_raster(os.path.join(tmpdir, 'template.tif'), driver = 'GTiff')
            # read the template back in
            templatearray = xr.open_dataarray(os.path.join(tmpdir, 'template.tif'))
            # this ensures that the template is in the same format as the data that will be read later

            # close the file to make sure the temporary folder is deleted
            templatearray.close()

        # save in self._template the minimum that is needed to recreate the template
        # get the crs and the nodata value, these are the same for all tiles
        crs = templatearray.attrs.get('crs', templatearray.rio.crs)

        if crs is not None:
            crs_wkt = crs.to_wkt()
        elif hasattr(templatearray, 'spatial_ref') and hasattr(templatearray.spatial_ref, 'crs_wkt'):
            crs_wkt = templatearray.spatial_ref.crs_wkt
        elif hasattr(templatearray, 'crs') and hasattr(templatearray.crs, 'crs_wkt'):
            crs_wkt = templatearray.crs.crs_wkt
        else: # if all fails, assume EPSG:4326
            from pyproj import CRS
            crs_wkt = CRS.from_epsg(4326).to_wkt()

        self._template[tile] = {'crs': crs_wkt,
                                '_FillValue' : templatearray.attrs.get('_FillValue'),
                                'dims_names' : templatearray.dims,
                                'spatial_dims' : (templatearray.rio.x_dim, templatearray.rio.y_dim),
                                'dims_starts': {},
                                'dims_ends': {},
                                'dims_lengths': {}}
        
        if vars is not None:
            self._template[tile]['variables'] = vars

        for dim in templatearray.dims:
            this_dim_values = templatearray[dim].data
            start = this_dim_values[0]
            end = this_dim_values[-1]
            length = len(this_dim_values)
            self._template[tile]['dims_starts'][dim] = float(start)
            self._template[tile]['dims_ends'][dim] = float(end)
            self._template[tile]['dims_lengths'][dim] = length

    @staticmethod
    def build_templatearray(template_dict: dict, data = None) -> xr.DataArray|xr.Dataset:
        """
        Build a template xarray.DataArray from a dictionary.
        """

        shape = [template_dict['dims_lengths'][dim] for dim in template_dict['dims_names']]
        if data is None:
            data = np.full(shape, template_dict['_FillValue'])
        else:
            data = data.reshape(shape)
        template = xr.DataArray(data, dims = template_dict['dims_names'])
        
        for dim in template_dict['dims_names']:
            start  = template_dict['dims_starts'][dim]
            end    = template_dict['dims_ends'][dim]
            length = template_dict['dims_lengths'][dim]
            template[dim] = np.linspace(start, end, length)

        template.attrs = {'crs': template_dict['crs'], '_FillValue': template_dict['_FillValue']}
        template = template.rio.set_spatial_dims(*template_dict['spatial_dims']).rio.write_crs(template_dict['crs']).rio.write_coordinate_system()

        return template

    @staticmethod
    def set_data_to_template(data: np.ndarray|xr.DataArray|xr.Dataset,
                             template_dict: dict) -> xr.DataArray|xr.Dataset:
        
        if isinstance(data, xr.DataArray):
            #data = straighten_data(data)
            data = Dataset.build_templatearray(template_dict, data.values)
        elif isinstance(data, np.ndarray):
            data = Dataset.build_templatearray(template_dict, data)
        elif isinstance(data, xr.Dataset):
            vars = template_dict['variables']
            template = Dataset.build_templatearray(template_dict, data[vars[0]].values)
            data = xr.Dataset({var: template.copy(data = data[var]) for var in vars})
        
        return set_type(data, read = True)

    def set_metadata(self, data: xr.DataArray|xr.Dataset,
                     time: Optional[TimeStep|dt.datetime] = None,
                     time_format: str = '%Y-%m-%d', **kwargs) -> xr.DataArray:
        """
        Set metadata for the data.
        """
     
        if hasattr(data, 'attrs'):
            if 'long_name' in data.attrs:
                data.attrs.pop('long_name')
            kwargs.update(data.attrs)
        
        metadata = kwargs.copy()
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

    def parse_as_ds(self, value) -> 'Dataset':
        if isinstance(value, str):
            return Dataset.from_options({"key_pattern":value}, defaults = self._creation_kwargs.copy())
        else:
            return value

    ## THUMBNAIL METHODS

    def parse_thumbnail_options(self, thumbnail_options: dict) -> dict:
        if 'colors' not in thumbnail_options or 'destination' not in thumbnail_options:
            #TODO add a warning
            return None
        else:
            colors = thumbnail_options.get('colors')
            destination = thumbnail_options.get('destination')

        if isinstance(colors, dict):
            thumbnail_options['colors'] = {key: self.parse_as_ds(colors[key]) for key in colors}
        else:
            thumbnail_options['colors'] = self.parse_as_ds(colors)

        thumbnail_options['destination'] = self.parse_as_ds(destination)

        if 'overlay' in thumbnail_options:
            thumbnail_options['overlay'] = self.parse_as_ds(thumbnail_options['overlay'])
        
        return thumbnail_options

    @staticmethod
    def make_thumbnail(data: xr.DataArray|dict[str,xr.DataArray]|gpd.GeoDataFrame, options: dict, destination: 'Dataset', **kwargs):
        try:
            from ..thumbnails import Thumbnail, ThumbnailCollection
        except ImportError:
            from thumbnails import Thumbnail, ThumbnailCollection

        colors = options.pop('colors')
        if isinstance(colors, dict):
            col_defs       = [v.update(**kwargs) for v in colors.values()]
            data           = list(data[k] for k in colors.keys())
            this_thumbnail = ThumbnailCollection(data, col_defs)
        else:
            col_def = colors.update(**kwargs)
            if isinstance(data, dict):
                data = data['']
            elif isinstance(data, gpd.GeoDataFrame):
                field = options.pop('field')
                data = data[['geometry', field]].rename(columns = {field: 'value'})
            this_thumbnail = Thumbnail(data, col_def)
        
        destination_path = destination.get_key(**kwargs)
        if hasattr(destination, 'tmp_dir'):
            if destination_path.startswith('/'):
                _path = destination_path[1:]
            else:
                _path = destination_path
            tmp_destination = os.path.join(destination.tmp_dir, _path)
            this_thumbnail.save(tmp_destination, **options)
            destination.write_data(tmp_destination, **kwargs)
        else:
            this_thumbnail.save(destination_path, **options)
        
        return this_thumbnail

    ## NOTIFICATION METHODS
    def notify(self):
        try:
            from ..notification import EmailNotification
        except ImportError:
            from notification import EmailNotification

        notification_options = self.notif_opts.copy()
        layers = notification_options.pop('layers', None)
        if layers is None:
            return

        from_address = notification_options.pop('from', None)
        email_client = notification_options.pop('email_client', None)
        email_login_env = notification_options.pop('email_login_env', None)
        email_pwd_env = notification_options.pop('email_pwd_env', None)

        notification = EmailNotification(from_address, email_client, email_login_env, email_pwd_env)

        log_list = []
        layers_string = ""
        attachement_size = 0
        other_thubmnails = []
        for layer in layers:
            tags = layer.pop('tags')
            tags_string = ':' + ', '.join([f'{k}: {v}' for k,v in tags.items() if not (isinstance(v, str) and len(v) == 0)])
            time = layer.pop('time')
            log_dict = layer.pop('log')
            log_list.append(log_dict)
            thumbnail_file = layer.pop('thumbnail')
            if thumbnail_file is not None:
                this_size = os.path.getsize(thumbnail_file)
                if attachement_size + this_size < 25e6:
                    notification.attach(thumbnail_file)
                    attachement_size += this_size
                    attachment_string = '[thumbnail attached]'
                else:
                    other_thubmnails.append(thumbnail_file)
                    attachment_string = f'\n    [thumbnail: {thumbnail_file}]'
            else:
                attachment_string = ''
            layers_string += f" - {time} {tags_string} {attachment_string}\n"

        header = 'Hello,\nthis is an automatic notification.\nSome new data is available and I thought you should know.\n'
        main = f'\n\nHere is a list of the newly available layers for the dataset {self.name}:\n{layers_string}'
        attach_str = 'Attached is a log file with more information for each layer.'
        if len(other_thubmnails) > 0:
            attach_str += 'Due to the size of the attachments, some thumbnails are not attached but are available for download.'
        footer = '\n\nBest regards,\nYour friendly data provider.'

        body = header + main + attach_str + footer

        import json
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdirname:
            now = dt.datetime.now()
            log_file = os.path.join(tmpdirname, f'{self.name}_{now:%Y%m%d_%H%M%S}.json')
            with open(log_file, 'w') as f:
                json.dump(log_list, f, indent = 4)
            notification.attach(log_file)

            recipients = notification_options.pop('to')
            subject = notification_options.pop('subject', f'[AUTOMATIC NOTIFICATION] {self.name} : new data available')
            notification.send(recipients, subject, body = body)

            rm_at_exit(tmpdirname)

    ## LOGGING METHODS
    def parse_log_options(self, value) -> dict:
        # if value is a Dataset already it will have a "key_pattern" attribute
        if isinstance(value, Dataset):
            log_output= value.update(now = dt.datetime.now())
            log_opts = {'output' : log_output}
        # if it is a string, we use that as the key_pattern and the default from the creation kwargs of this dataset
        elif isinstance(value, str):
            log_output_file = substitute_string(value, {'now': dt.datetime.now()})
            log_output = Dataset.from_options({"key_pattern":log_output_file},
                                                defaults = self._creation_kwargs.copy())
            log_opts = {'output' : log_output}
        # if it is a dictionary, we use the "file" key as the "key_pattern" and the rest are options for the log
        elif isinstance(value, dict):
            log_output_file = substitute_string(value.pop('file'), {'now': dt.datetime.now()})
            log_output = Dataset.from_options({'key_pattern' : log_output_file},
                                                defaults = self._creation_kwargs.copy())
            log_opts['output'] = log_output
        return log_opts

    def get_log(self, data: xr.DataArray, options = None, **kwargs) -> dict:
        log_dict = {}

        metadata = data.attrs

        log_dict['dataset'] = self.name
        log_dict['source_key'] = metadata.get('source_key', kwargs.get('source_key', None))
        log_dict['thumbnail'] = kwargs.get('thumbnail', None)
        log_dict['time'] = metadata.get('time', kwargs.get('time', None))
        log_dict['time_produced'] = metadata.get('time_produced', dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        kwargs.update(metadata)
        for k, v in kwargs.items():
            if k not in log_dict:
                log_dict[k] = v
        
        kwargs.update(metadata)

        # format the log_dict correctly
        final_log_dict = {}
        for key, value in log_dict.items():
            if isinstance(value, TimeStep):
                final_log_dict[key] = self.get_time_signature(value).strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(value, dt.datetime):
                final_log_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, str) and len(value.strip()) > 0 and not value.startswith('__'):
                final_log_dict[key] = value
            elif isinstance(value, int): 
                final_log_dict[key] = str(value)
            elif isinstance(value, float):
                final_log_dict[key] = str(round(value, 4))

        final_log_dict['data_checks'] = {k: str(v) for k,v in self.qc_checks(data).items()}

        return final_log_dict

    @staticmethod
    def write_log(log_dict, log_ds, time, **kwargs):
        
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
    def qc_checks(data: xr.DataArray|xr.Dataset) -> dict:
        """
        Perform quality checks on the data.
        - max and min values
        - percentage and absolute number of NaNs
        - percentage and absolute number of zeros
        - sum of values
        - sum of absolute values
        """
        if isinstance(data, xr.Dataset):
            full_dict = {}
            var_list = list(data.data_vars.values())
            for var in var_list:
                full_dict[var.name] = Dataset.qc_checks(var)

            qc_dict = {}
            for k, d in full_dict.items():
                for k_, v_ in d.items():
                    qc_dict[f'{k}_{k_}'] = v_
            
            return qc_dict

        data = data.values
        qc_dict = {}
        qc_dict['max'] = np.nanmax(data)
        qc_dict['min'] = np.nanmin(data)
        qc_dict['nans'] = int(np.sum(np.isnan(data)))
        qc_dict['nans_pc'] = qc_dict['nans'] / data.size * 100
        qc_dict['zeros'] = int(np.sum(data == 0))
        if (data.size - qc_dict['nans']) != 0:
            qc_dict['zeros_pc'] = qc_dict['zeros'] / (data.size - qc_dict['nans']) * 100
        else:
            qc_dict['zeros_pc'] = 0
        qc_dict['sum'] = np.nansum(data)
        qc_dict['sum_abs'] = np.nansum(np.abs(data))

        for key, value in qc_dict.items():
            if not isinstance(value, int):
                qc_dict[key] = round(float(value), 4)

        return qc_dict
    