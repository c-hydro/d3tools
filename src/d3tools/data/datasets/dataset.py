from typing import Optional, Generator, Callable
import datetime as dt
import xarray as xr

from abc import ABCMeta, abstractmethod
import os

from ...timestepping import TimeRange, TimeStep, TimeWindow
from ...parse import substitute_string, extract_date_and_tags, KeyParser
from ..io_utils import get_format_from_path, check_data_format, get_mixin_class_from_format, read_from_file
from ..data_catalogue import DataCatalogue

# Cache for dynamically created classes (avoids recreating same class combinations)
_CLASS_CACHE = {}

class DatasetMeta(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        elif 'type' in attrs:
            cls.subclasses[attrs['type']] = cls

class Dataset(metaclass=DatasetMeta):
    _defaults = {'type': 'local',
                 'time_signature' : 'end'}

    # region: INITIALIZATION AND CONFIGURATION METHODS
    def _get_or_derive_name(self, kwargs: dict) -> str:
        """Get name from kwargs or derive from key_pattern."""
        if 'name' in kwargs:
            return kwargs.pop('name')
        
        # Derive name from key_pattern by removing extension and date placeholders
        basename_noext = '.'.join(os.path.basename(self.key_pattern).split('.')[:-1])
        basename_nodate = basename_noext.replace('%Y', '').replace('%m', '').replace('%d', '')
        
        # Clean up extra underscores iteratively until no more changes
        while True:
            cleaned = basename_nodate
            
            # Remove trailing underscore
            if cleaned.endswith('_'):
                cleaned = cleaned[:-1]
            
            # Remove leading underscore
            if cleaned.startswith('_'):
                cleaned = cleaned[1:]
            
            # Collapse double underscores
            if '__' in cleaned:
                cleaned = cleaned.replace('__', '_')
            
            # If no changes were made, we're done
            if cleaned == basename_nodate:
                break
            basename_nodate = cleaned
        
        return basename_nodate
    
    def _get_or_detect_format(self, kwargs: dict) -> str:
        """Get format from kwargs or detect from key_pattern."""
        if 'format' in kwargs:
            return kwargs.pop('format')
        return get_format_from_path(self.key_pattern)
    
    def _setup_temporal_properties(self, kwargs: dict) -> None:
        """Setup timestep, aggregation, and time_signature properties."""
        # Set time signature (default from class defaults)
        if 'time_signature' in kwargs:
            self.time_signature = kwargs.pop('time_signature')
        
        # Set aggregation window
        if 'aggregation' in kwargs:
            self.agg = TimeWindow.from_str(kwargs.pop('aggregation'))
        
        # Set timestep and link with aggregation if present
        if 'timestep' in kwargs:
            self.timestep = TimeStep.from_unit(kwargs.pop('timestep'))
            if hasattr(self, 'agg'):
                self.timestep = self.timestep.with_agg(self.agg)
    
    def _set_optional_attributes(self, kwargs: dict) -> None:
        """Set optional dataset attributes from kwargs."""
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

    def __init__(self, **kwargs):
        """Initialize Dataset with configuration.
        
        Args:
            name: Dataset name (derived from key_pattern if not provided)
            format: Data format (detected from key_pattern if not provided)
            time_signature: Time reference ('end', 'start', 'end+1')
            timestep: Time step unit (e.g., 'daily', 'monthly')
            aggregation: Aggregation window for timestep
            thumbnail: DatasetThumbnailManager or config dict
            log: DatasetLogManager or config dict
            tile_names: List of tile names or path to file
            nan_value: Value to use for missing data
            **kwargs: Additional options stored in self.options
        """
        # Substitute "now" with the current time in key_pattern
        self.key_pattern = substitute_string(self.key_pattern, {'now': dt.datetime.now()})
        
        # Setup core attributes
        self.name = self._get_or_derive_name(kwargs)
        self.format = self._get_or_detect_format(kwargs)
        
        # Setup temporal properties
        self._setup_temporal_properties(kwargs)
        
        # Setup optional features
        self._set_optional_attributes(kwargs)
        
        # Initialize format-specific properties (e.g., template manager for raster)
        self._init_format_properties()
        
        # Initialize data catalogue for query and validation operations
        self.catalogue = DataCatalogue(self)
        
        # Store remaining options and initialize tags
        self.options = kwargs
        self.tags = {}

        # Store kwargs used for initialization (excluding options) for potential use in update()
        self._creation_kwargs = {
            'type': self.type,
            'name': self.name,
            'format': self.format,
            'time_signature': self.time_signature,
            'nan_value': self.nan_value,
        }
        self._creation_kwargs.update(self.options)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"
    # endregion

    # region: CLASS METHODS FOR FACTORY
    def __new__(cls, *args, **kwargs):
        """Create Dataset instance of the appropriate subclass based on type."""
        # Step 1: Detect format BEFORE creating instance
        format = kwargs.get('format', None)
        
        if format is None:
            # If format not explicitly provided, try to detect from key_pattern
            key_pattern = kwargs.get('key_pattern', None)
            if key_pattern is None:
                if len(args) > 0:
                    key_pattern = args[0]
                else:
                    dir  = kwargs.get('dir', None) or kwargs.get('path', '')
                    file = kwargs.get('file', '')  or kwargs.get('filename', '')
                    key_pattern = os.path.join(dir, file)
            format = get_format_from_path(key_pattern)
        
        # Step 2: Select mixin
        format_mixin = get_mixin_class_from_format(format)

        # Step 3: Create dynamic class (cached for performance)
        cache_key = (cls, format_mixin)
        if cache_key not in _CLASS_CACHE:
            # Use DatasetMeta as the metaclass for the dynamic class
            _CLASS_CACHE[cache_key] = DatasetMeta(
                cls.__name__,  # Still called "LocalDataset"
                (format_mixin, cls),  # MRO: Mixin first, then LocalDataset
                {'__module__': cls.__module__}
            )
        
        DynamicClass = _CLASS_CACHE[cache_key]
        
        # Step 4: Create instance of dynamic class
        instance = object.__new__(DynamicClass)
        
        # Store the original storage class so update()/copy() work correctly
        instance._original_class = cls
        
        return instance

    @classmethod
    def from_options(cls, options: dict, defaults: dict = None):
        """
        Create a Dataset from a configuration dictionary.
        
        This is a user-friendly factory method that delegates to the
        centralized dataset parser.
        
        Args:
            options: Configuration dictionary
            defaults: Optional default values to merge with options
            
        Returns:
            Dataset instance of the appropriate subclass
            
        Example:
            >>> config = {
            ...     'type': 'local',
            ...     'path': '/data',
            ...     'file': 'output.tif',
            ...     'thumbnail': {...},
            ...     'log': '/logs/output.txt'
            ... }
            >>> dataset = Dataset.from_options(config)
        """
        # Delegate to centralized parser
        from ...config.parsers import dataset_from_config
        return dataset_from_config(options, defaults)

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
    # endregion

    # region: METHODS TO COPY, UPDATE AND COMPARE DATASETS
    def update(self, in_place=False, preserve_temporal=True, preserve_spatial=True, **kwargs):
        """Create an updated copy of this dataset with substituted placeholders.
        
        Args:
            in_place: If True, mutate self instead of creating new instance
            preserve_temporal: If True, preserve timestep and aggregation attributes
            preserve_spatial: If True, preserve spatial attributes (only used by RasterMixin)
            **kwargs: Placeholder substitutions for key_pattern, name, and tags
            
        Returns:
            Updated Dataset instance (or None if in_place=True)
        """
        new_name = substitute_string(self.name, kwargs)
        new_key_pattern = substitute_string(self.key_pattern, kwargs)

        if in_place:
            # Minimal in-place update: just mutate key attributes
            self.name = new_name
            self.key_pattern = new_key_pattern

            self._post_update_init(self, preserve_temporal=preserve_temporal, preserve_spatial=preserve_spatial, update_kwargs=kwargs)
            return None  # Explicitly return None to indicate mutation
        
        # Create new instance from _creation_kwargs
        new_options = self._creation_kwargs.copy()
        new_options.update({'key_pattern': new_key_pattern, 'name': new_name})
        
        # Use original storage class, not the dynamic class (avoids MRO conflicts)
        original_class = getattr(self, '_original_class', self.__class__)
        new_dataset = original_class(**new_options)
        
        # Call post-update initialization hook (handles subclass & mixin-specific logic)
        new_dataset._post_update_init(
            source_dataset=self,
            preserve_temporal=preserve_temporal,
            preserve_spatial=preserve_spatial,
            update_kwargs=kwargs
        )
        
        return new_dataset
    
    def _post_update_init(self, source_dataset: 'Dataset', 
                         preserve_temporal: bool = True,
                         preserve_spatial: bool = True,
                         update_kwargs: dict = None):
        """Hook called after update() creates new instance.
        
        Subclasses and mixins should override this to handle their specific
        attributes. Always call super()._post_update_init() first to ensure
        proper MRO handling.
        
        Args:
            source_dataset: The original dataset being updated
            preserve_temporal: Whether to preserve temporal attributes
            preserve_spatial: Whether to preserve spatial attributes (only relevant for RasterMixin)
            update_kwargs: The kwargs passed to update() for substitution
        """
        update_kwargs = update_kwargs or {}
        
        # Preserve temporal attributes if requested
        if preserve_temporal:
            if hasattr(source_dataset, 'timestep') and source_dataset.timestep is not None:
                self.timestep = source_dataset.timestep
            if hasattr(source_dataset, 'agg'):
                self.agg = source_dataset.agg
        
        # Preserve derived dataset relationships
        if hasattr(source_dataset, 'parents') and source_dataset.parents is not None:
            self.parents = {k: p.update(**update_kwargs) for k, p in source_dataset.parents.items()}
            if hasattr(source_dataset, 'fn'):
                self.fn = source_dataset.fn
        
        # Always preserve/copy tags (merged with update_kwargs)
        new_tags = source_dataset.tags.copy()
        new_tags.update(update_kwargs)
        self.tags = new_tags

    def copy(self, preserve_temporal=True, preserve_spatial=True):
        """Create an independent copy sharing managers but with separate tags.
        
        Args:
            preserve_temporal: If True, preserve timestep and aggregation
            preserve_spatial: If True, preserve spatial attributes
            
        Returns:
            New Dataset instance
        """
        new_dataset = self.update(
            preserve_temporal=preserve_temporal,
            preserve_spatial=preserve_spatial
        )
        
        # Share log and thumbnail managers (copy-specific behavior)
        if hasattr(self, 'log'):
            new_dataset.log = self.log
        if hasattr(self, 'thumbnail'):
            new_dataset.thumbnail = self.thumbnail
        
        return new_dataset

    def is_subdataset(self, other: 'Dataset') -> bool:
        key = self.get_key(time = dt.datetime(1900,1,1))
        try:
            extract_date_and_tags(key, other.key_pattern)
            return True
        except ValueError:
            return False

    # endregion
    
    # region: PROPERTIES
        # format
    @property
    def format(self):
        return self._format
    
    @format.setter
    def format(self, value):
        self._format = value

        # timestepping and time-signature
    @property
    def has_time(self):
        return '%' in self.key_pattern
    
    @property
    def timestep(self):
        return self._timestep
    
    @timestep.setter
    def timestep(self, value):
        self._timestep = value
        if hasattr(self, 'agg'):
            self._timestep = self._timestep.with_agg(self.agg)

    @property
    def agg(self):
        return self._agg

    @agg.setter
    def agg(self, value):
        self._agg = value
        if hasattr(self, 'timestep') and self.timestep is not None:
            self.timestep = self.timestep.with_agg(value)

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

        # tile management

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
            self._tile_names = [l.strip() for l in read_from_file(value)]
        elif isinstance(value, list) or isinstance(value, tuple):
            self._tile_names = list(value)
        else:
            raise ValueError('Invalid tile names.')

    @property
    def ntiles(self):
        return len(self.tile_names)

        # tags

    @property
    def is_static(self):
        return not '{' in self.key_pattern and not self.has_time

    @property
    def has_version(self):
        return '{file_version}' in self.key_pattern

    @property
    def available_tags(self):
        return self.get_available_tags()
    
        # availability
    
    @property
    def available_keys(self):
        return self.get_available_keys() 
    # endregion

    # region: METHODS TO PARSE KEY PATTERN WITH TIME AND TAGS
    def get_key(self, time: Optional[TimeStep|dt.datetime] = None, **kwargs):
        """Render dataset key/path from time and tag values.

        This method delegates rendering to ``KeyParser`` and keeps Dataset-level
        compatibility by resolving/normalizing time through
        ``get_time_signature()`` first.

        Args:
            time: Optional datetime or timestep input.
            **kwargs: Tag substitutions for placeholders in ``self.key_pattern``.

        Returns:
            Concrete key/path string.
        """
        key_parser = KeyParser(self.key_pattern)
        parsed_time = self.get_time_signature(time)
        return key_parser.render(time=parsed_time, tags=kwargs)

    def get_time_signature(self, timestep: Optional[TimeStep | dt.datetime]) -> dt.datetime:
        """Resolve and normalize time according to dataset signature/pattern.

        Behavior:
        - Resolve datetime anchor from ``self.time_signature`` when input is a
          ``TimeStep``.
        - Infer an effective step length for datetime inputs using either
          ``self.timestep`` or ``previous_requested_time`` (legacy fallback).
        - Normalize precision/leap-day handling against ``self.key_pattern``.

        Args:
            timestep: Datetime, timestep, or ``None``.

        Returns:
            Normalized datetime aligned to dataset key semantics, or ``None``.
        """
        if timestep is None:
            return None
        
        key_parser = KeyParser(self.key_pattern)
        time = key_parser.resolve_time(timestep, time_signature=self.time_signature)

        if isinstance(timestep, dt.datetime):
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
            length = timestep.get_length()
            self.previous_requested_time = time

        return key_parser.normalize_time(time, step_length=length)
    # endregion

    # region: INPUT/OUTPUT METHODS

    # These are the main methods for getting and writing data, which handle the logic of checking availability,
    def get_data(self, time: Optional[dt.datetime|TimeStep] = None, as_is = False, **kwargs):
        
        # if this is a versioned file, and the version is not specified, get the latest version
        if self.has_version and 'file_version' not in kwargs:
            available_versions = self.get_available_tags(time, **kwargs).get('file_version')
            if available_versions is not None:
                available_versions.sort()
                kwargs['file_version'] = available_versions[-1]

        # parse the full key with the time and tags
        full_key = self.get_key(time, **kwargs)

        # first check that the data is available
        if self._check_data(full_key):
            # if so, read it
            raw_data = self._read_data(full_key)
        # if not, check if it has parents to inherit from
        elif hasattr(self, 'parents') and self.parents is not None:
            raw_data = self.make_data(time, **kwargs)
        # if the data is not available and there are no parents, raise an error
        else:
            raise FileNotFoundError(f'Could not resolve data from {full_key}.')

        # if we are not reading the data as is, we need to process it
        if as_is or self.type == 'memory':
            return raw_data
        else:
            # self._format_after_read is implemented in the mixins to handle any
            # format-specific processing after reading
            return self._format_after_read(raw_data, full_key = full_key, time = time, **kwargs)
    
    def write_data(self, data,
                   time: Optional[dt.datetime|TimeStep] = None,
                   metadata = None,
                   as_is = False,
                   **kwargs):

        if metadata is None: metadata = {}

        # check the data format (this will check if the type of the data is compatible with the dataset format)
        check_data_format(data, self.format)

        # get the full output key with the time and tags
        output_file = self.get_key(time, **kwargs)

        data = self.validate_data(data)
        # check if we need to prepare the data before writing
        if as_is or self.type == 'memory':
            output = data
        else:
            output = self._format_before_write(data, **kwargs)

        # make the thubnail if configured (this is also a bit of a hack since thumbnails only make sense for certain formats,
        # but we want to allow the user to configure it on any dataset)
        thumbnail_file = self._make_thumbnail(output, time, output_file, **kwargs)

        # fix the metadata
        old_md = self.get_metadata(data)#.attrs if hasattr(data, 'attrs') else {}
        new_md = self.get_metadata(output)#output.attrs
        old_md.update(new_md)
        
        name = substitute_string(self.name, kwargs)
        metadata['name'] = str(name)
        # remove time from metadata and old_md if present
        metadata.pop('time', None)
        old_md.pop('time', None)
        output = self.set_metadata(output, time=time, **old_md, **metadata)
        
        # write the data
        self._write_data(output, output_file, append = kwargs.get('append', False))
        
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
    # endregion
    
    # region: METHODS TO QUERY THE CATALOGUE FOR AVAILABLE DATA AND TIMES [DELEGATED TO CATALOGUE]
    def get_prefix(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        """Get the directory prefix for file discovery. Delegates to catalogue."""
        return self.catalogue.get_prefix(time=time, **kwargs)
    
    def get_available_keys(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        """Get list of available file keys/paths. Delegates to catalogue."""
        return self.catalogue.get_available_keys(time=time, **kwargs)

    def get_available_tags(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs):
        """Extract all unique tags and times from available files. Delegates to catalogue."""
        return self.catalogue.get_available_tags(time=time, **kwargs)

    def _get_times(self, time_range: TimeRange, **kwargs) -> Generator[dt.datetime, None, None]:
        """Generate times within a time range. Delegates to catalogue."""
        return self.catalogue._get_times(time_range, **kwargs)

    def estimate_timestep(self, date_sample = None, **kwargs) -> TimeStep:
        """Estimate the dataset's timestep from a sample of dates. Delegates to catalogue."""
        return self.catalogue.estimate_timestep(date_sample, **kwargs)

    def get_times(self, time_range: TimeRange, **kwargs) -> list[dt.datetime]:
        """Get a list of times between two dates. Delegates to catalogue."""
        return self.catalogue.get_times(time_range, **kwargs)

    def get_timesteps(self, time_range: TimeRange, **kwargs) -> list[TimeStep]:
        """Get a list of TimeStep objects within a time range. Delegates to catalogue."""
        return self.catalogue.get_timesteps(time_range, **kwargs)

    def get_any_date(self, now=None, lim=None, **kwargs) -> dt.datetime|None:
        """Find ANY available date quickly. Delegates to catalogue."""
        return self.catalogue.get_any_date(now=now, lim=lim, **kwargs) 

    def get_last_date(self, now = None, n = 1, lim = None, **kwargs) -> dt.datetime|list[dt.datetime]|None:
        """Find the most recent available date(s). Delegates to catalogue."""
        return self.catalogue.get_last_date(now=now, n=n, lim=lim, **kwargs)

    def get_last_ts(self, **kwargs) -> TimeStep:
        """Get the most recent timestep. Delegates to catalogue."""
        return self.catalogue.get_last_ts(**kwargs)

    def get_first_date(self, start = None, n = 1, **kwargs) -> dt.datetime|list[dt.datetime]|None:
        """Find the earliest available date(s). Delegates to catalogue."""
        return self.catalogue.get_first_date(start=start, n=n, **kwargs)

    def get_first_ts(self, **kwargs) -> TimeStep:
        """Get the earliest timestep. Delegates to catalogue."""
        return self.catalogue.get_first_ts(**kwargs)

    def get_start(self, agg=True, **kwargs) -> dt.datetime:
        """Get the start of the available data. Delegates to catalogue."""
        return self.catalogue.get_start(agg=agg, **kwargs)
    
    def check_data(self, time: Optional[TimeStep|dt.datetime] = None, **kwargs) -> bool:
        """Check if data is available for a given time. Delegates to catalogue."""
        return self.catalogue.check_data(time, **kwargs)
    
    def find_times(self, times: list[TimeStep|dt.datetime], id = False, rev = False, **kwargs) -> list[TimeStep] | list[int]:
        """Find the times for which data is available. Delegates to catalogue."""
        return self.catalogue.find_times(times, id=id, rev=rev, **kwargs)

    def find_tiles(self, time: Optional[TimeStep|dt.datetime] = None, rev = False, **kwargs) -> list[str]:
        """Find the tiles for which data is available. Delegates to catalogue."""
        return self.catalogue.find_tiles(time, rev=rev, **kwargs)
    
    # endregion
    
    # region: HELPER METHODS FOR THUMBNAIL AND LOG MANAGERS
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
    # endregion

    # region: METHODS TO MAKE DATA FROM PARENTS
    def set_parents(self, parents:dict[str:'Dataset'], fn:Callable):
        self.parents = parents
        self.fn = fn

    def make_data(self, time: Optional[dt.datetime|TimeStep] = None, **kwargs):
        if not hasattr(self, 'parents') or self.parents is None:
            raise ValueError(f'No parents for {self.name}')
        
        parent_data = {name: parent.get_data(time, **kwargs) for name, parent in self.parents.items()}
        data = self.fn(**parent_data)
        if self.type != 'memory':
            self.write_data(data, time, **kwargs)
        return data
    # endregion

    # region: ABSTRACT METHODS TO BE IMPLEMENTED IN SUBCLASSES FOR DATA I/O AND DISCOVERY
    @property
    def key_pattern(self):
        raise NotImplementedError

    @key_pattern.setter
    def key_pattern(self, value):
        raise NotImplementedError
    
    @abstractmethod
    def _read_data(self, input_key:str):
        raise NotImplementedError
    
    @abstractmethod
    def _write_data(self, output: xr.DataArray, output_key: str):
        raise NotImplementedError

    @abstractmethod
    def _rm_data(self, key: str):
        raise NotImplementedError
    
    @abstractmethod
    def _walk(self, prefix: str) -> Generator[str, None, None]:
        raise NotImplementedError

    @abstractmethod
    def _check_data(self, data_key) -> bool:
        raise NotImplementedError
    # endregion
