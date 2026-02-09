from typing import Optional, Generator, Callable
import datetime as dt
import numpy as np
import xarray as xr
import geopandas as gpd
#import atexit

from abc import ABC, ABCMeta, abstractmethod
import os
import re

from ..timestepping import TimeRange, Month, TimeStep, estimate_timestep, TimeWindow
from ..parse import substitute_string, extract_date_and_tags
from .io_utils import get_format_from_path, straighten_data, set_type, check_data_format
from .template_manager import TemplateManager

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

        if 'thumbnail' in kwargs:
            self.thumbnail = kwargs.pop('thumbnail')

        if 'log' in kwargs:
            self.log = kwargs.pop('log')

        if 'tile_names' in kwargs:
            self.tile_names = kwargs.pop('tile_names')

        if 'nan_value' in kwargs:
            self.nan_value = kwargs.pop('nan_value')
        else:
            self.nan_value = None

        self.template_manager = TemplateManager()
        self.options = kwargs
        self.tags = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"
    
    @property
    def _template(self) -> dict:
        """Backward compatibility property for accessing templates."""
        return self.template_manager._templates
    
    @_template.setter
    def _template(self, value: dict):
        """Backward compatibility property for setting templates."""
        self.template_manager._templates = value

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

            new_dataset.template_manager = self.template_manager
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
            new_dataset.template_manager = self.template_manager
        if hasattr(self, 'log'):
            new_dataset.log = self.log
        if hasattr(self, 'thumbnail'):
            new_dataset.thumbnail = self.thumbnail
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
        
        # Parse manager configs if they're dicts/strings (not already manager objects)
        # Create dataset_factory once for use by both managers
        dataset_factory = lambda cfg: cls.from_options(cfg) if isinstance(cfg, dict) else cls.from_options({'path': os.path.dirname(cfg), 'file': os.path.basename(cfg)})
        
        # Helper to parse manager config if needed
        def parse_manager_if_needed(config, manager_class, check_method):
            """Parse config into manager if it's not already a manager object."""
            if config is None:
                return None
            # Check if already a manager by testing for a characteristic method
            if hasattr(config, check_method):
                return config
            # Not a manager - parse it
            return manager_class.from_dict(config, dataset_factory)
        
        if 'thumbnail' in new_options:
            from ..thumbnails import DatasetThumbnailManager
            new_options['thumbnail'] = parse_manager_if_needed(
                new_options['thumbnail'], 
                DatasetThumbnailManager, 
                'make_thumbnail'
            )
        
        if 'log' in new_options:
            from ..logging import DatasetLogManager
            new_options['log'] = parse_manager_if_needed(
                new_options['log'], 
                DatasetLogManager, 
                'write_log'
            )

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
                        prefix = prefix.replace('%j', f'{start.timetuple().tm_yday:03d}')  # Substitute %j if present

        prefix = os.path.dirname(prefix)
        while '%' in prefix or '{' in prefix:
            prefix = os.path.dirname(prefix)
        
        return prefix

    def get_available_tags(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        if self.time_signature == 'end+1' and time is not None:
            if isinstance(time, dt.datetime):
                time = time + dt.timedelta(days = 1)
            elif isinstance(time, TimeRange):
                time = TimeRange(time.start + dt.timedelta(days = 1), time.end + dt.timedelta(days = 1))
        
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

        if self.time_signature == 'end+1':
            all_tags['time'] = [t - dt.timedelta(days = 1) for t in all_tags['time']]
            all_tags['time'].sort()

        return all_tags

    def get_last_date(self, now = None, n = 1, lim = None, **kwargs) -> dt.datetime|list[dt.datetime]|None:
        if now is None:
            now = dt.datetime.now()
        
        # Find ANY date first using exponential backoff
        any_date = self.get_any_date(now=now, lim=lim, **kwargs)
        if any_date is None:
            return None
        
        # Binary search between any_date and now to find the last date
        start_month = Month(any_date.year, any_date.month)
        end_month = Month(now.year, now.month)
        last_month_with_data = start_month
        
        while start_month <= end_month:
            # Calculate midpoint
            months_diff = (end_month.start.year - start_month.start.year) * 12 + \
                         (end_month.start.month - start_month.start.month)
            
            if months_diff <= 1:
                # Adjacent or same months - we're done
                break
            
            mid_months = months_diff // 2
            mid_month = start_month + mid_months
            
            # Check if mid month has data
            mid_times = self.get_times(mid_month, **kwargs)
            if len(mid_times) > 0:
                # Data exists at midpoint, search forward
                last_month_with_data = mid_month
                start_month = mid_month
            else:
                # No data at midpoint, search backward
                end_month = mid_month - 1
        
        # Now collect n dates from last_month_with_data and nearby months
        last_date = []
        search_month = last_month_with_data
        end_search = Month(now.year, now.month)
        
        # Search forward from last known month
        while search_month <= end_search and len(last_date) < n * 3:  # Get extra to ensure we have enough
            month_times = self.get_times(search_month, **kwargs)
            if len(month_times) > 0:
                valid_time = [t for t in month_times if t <= now]
                last_date.extend(valid_time)
            search_month = search_month + 1
        
        # Sort and take the most recent n
        last_date.sort(reverse=True)
        last_date = last_date[:n]
        
        if len(last_date) == 0:
            return None
        if n == 1:
            return last_date[0]
        else:
            return last_date

    def get_any_date(self, now=None, lim=None, **kwargs) -> dt.datetime|None:
        """
        Find ANY available date quickly (used for template extraction).
        Much faster than get_last_date when you don't care which file.
        
        Returns immediately on first match.
        """
        if now is None:
            now = dt.datetime.now()
        
        # Default limit: 5 years back (reasonable for most datasets)
        if lim is None:
            lim = now - dt.timedelta(days=5*365)
        
        # Find any month with data using exponential backoff
        search_month = Month(now.year, now.month)
        months_back = 0
        jump_sizes = [1, 2, 3, 6, 12, 24, 48, 96]  # Exponentially increasing jumps
        
        for jump in jump_sizes:
            if search_month.start < lim:
                break
            
            month_times = self.get_times(search_month, **kwargs)
            if len(month_times) > 0:
                valid_times = [t for t in month_times if t <= now]
                return valid_times[0] if valid_times else None
            
            months_back += jump
            search_month = Month(now.year, now.month) - months_back  

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
                    if all(tag not in key_without_tags for tag in ('%d', '%j')):
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

            if as_is or self.type == 'memory':
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

            if isinstance(data, gpd.GeoDataFrame):
                data = self.set_metadata(data, time, time_format, **metadata)

            self._write_data(data, output_file, append = append)
            self._make_thumbnail(data, time, output_file, **kwargs)

            return
        
        if self.format == 'file':
            self._write_data(data, output_file)
            return
        
        if as_is or self.type == 'memory':
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
        
        # Generate thumbnail if configured
        if 'parents' in metadata:
            parents = metadata.pop('parents')
        else:
            parents = {}
        parents[''] = output
        thumbnail_file = self._make_thumbnail(parents, time, **kwargs)

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
        
        # Write log if configured
        self._make_log(output, output_file, thumbnail_file, time, **kwargs)

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
    
    def _make_thumbnail(self, data, time, output_file=None, **kwargs):
        """
        Helper method to generate thumbnail if manager is configured.
        
        Args:
            data: Data to visualize
            time: Timestamp
            output_file: Optional output file path for annotation
            **kwargs: Additional context
            
        Returns:
            Thumbnail file path if generated, None otherwise
        """
        if not hasattr(self, 'thumbnail') or self.thumbnail is None:
            return None
        
        # Add output file annotation if not already set and output_file is provided
        if output_file is not None:
            if 'annotation' in self.thumbnail.options and 'text' not in self.thumbnail.options['annotation']:
                self.thumbnail.options['annotation']['text'] = os.path.basename(output_file)
            elif 'annotation' not in self.thumbnail.options:
                self.thumbnail.options['annotation'] = {'text': os.path.basename(output_file)}
        
        thumbnail = self.thumbnail.make_thumbnail(data=data, time=time, **kwargs)
        return thumbnail.thumbnail_file
    
    def _make_log(self, output, output_file, thumbnail_file, time, **kwargs):
        """
        Helper method to write log if manager is configured.
        
        Args:
            output: Output data that was written
            output_file: Path where data was written
            thumbnail_file: Path to thumbnail if generated
            time: Timestamp
            **kwargs: Additional context
        """
        if not hasattr(self, 'log') or self.log is None:
            return
        
        log_dict = self.log.get_log(
            self.name,
            output,
            self.get_time_signature,
            source_key=output_file,
            thumbnail=thumbnail_file,
            time=time,
            **kwargs
        )
        self.log.write_log(log_dict, time, **kwargs)

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
            end = time_range.end
            if end.hour == 0 and end.minute == 0:
                end = end + dt.timedelta(minutes = 1439)
            if ts.start > end or ts.end < time_range.start:
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

        template_dict = self.template_manager.get(tile)
        if template_dict is None and make_it:
            if not self.has_time:
                data = self.get_data(as_is = True, **kwargs)
                self.set_template(data, tile = tile)

            else:
                # Use get_any_date instead of get_last_date - we don't care which file
                any_date = self.get_any_date(tile = tile, **kwargs)
                if any_date is not None:
                    data = self.get_data(time = any_date, tile = tile, as_is=True, **kwargs)
                else:
                    return None
            
            data = straighten_data(data)
            #templatearray = self.make_templatearray_from_data(start_data)
            self.set_template(data, tile = tile)
            template_dict = self.get_template_dict(make_it = False, tile = tile, **kwargs)
        
        return template_dict
    
    def set_template(self, templatearray: xr.DataArray|xr.Dataset, **kwargs):
        tile = kwargs.get('tile', '__tile__')
        self.template_manager.set(templatearray, spatial_key=tile)

    @staticmethod
    def build_templatearray(template_dict: dict, data = None) -> xr.DataArray|xr.Dataset:
        """
        Build a template xarray.DataArray from a dictionary.
        """
        return TemplateManager.build_array(template_dict, data)

    @staticmethod
    def set_data_to_template(data: np.ndarray|xr.DataArray|xr.Dataset,
                             template_dict: dict) -> xr.DataArray|xr.Dataset:
        return TemplateManager.apply_to_data(data, template_dict)

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
