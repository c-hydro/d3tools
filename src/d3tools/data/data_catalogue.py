"""
Data Catalogue - Unified query and validation interface for dataset files.

This manager handles all operations related to querying the dataset catalogue:
- File discovery (get_available_keys, get_available_tags)
- Boundary discovery (get_last_date, get_first_date)
- Time enumeration (get_times, get_timesteps)
- Timestep inference (estimate_timestep)
- Validation (check_data, find_times, find_tiles)

All operations share the same mental model: querying what exists in the catalogue.
"""
from typing import Optional, Generator, TYPE_CHECKING
import datetime as dt

from ..timestepping import TimeRange, Month, TimeStep, estimate_timestep
from ..cases.utils import withcases
from ..parse import KeyParser

if TYPE_CHECKING:
    from .datasets import Dataset


class DataCatalogue:
    """
    Unified query and validation interface for available dataset files.
    
    Responsibilities:
    - Enumerate available files and their properties
    - Find temporal boundaries (first/last dates)
    - List times within ranges
    - Infer dataset timestep from samples
    - Validate existence of specific items
    
    All methods query the catalogue using different patterns:
    - Enumeration: "What times exist in this range?"
    - Point query: "Does THIS specific time exist?"
    - Batch query: "Which of THESE times exist?"
    """
    
    def __init__(self, dataset: 'Dataset'):
        """
        Initialize the data catalog.
        
        Args:
            dataset: Parent Dataset instance for accessing properties and methods
        """
        self.dataset = dataset
    
    def __repr__(self):
        return f"DataCatalogue({self.dataset.name})"

    def _to_storage_time(self, time: Optional[dt.datetime | TimeRange]) -> Optional[dt.datetime | TimeRange]:
        """Map logical/query time into storage-domain time.

        This helper delegates signature-specific conversion to ``KeyParser`` and
        supports both scalar datetimes and ``TimeRange`` inputs.

        Args:
            time: Logical/query datetime or range supplied by caller.

        Returns:
            Storage-domain datetime/range used for key matching, or ``None``.
        """
        if time is None:
            return None

        key_parser = KeyParser(self.dataset.key_pattern)
        if isinstance(time, dt.datetime):
            return key_parser.to_storage_time(time, self.dataset.time_signature)
        if isinstance(time, TimeRange):
            return TimeRange(
                key_parser.to_storage_time(time.start, self.dataset.time_signature),
                key_parser.to_storage_time(time.end, self.dataset.time_signature),
            )
        return time

    def _from_storage_time(self, time: dt.datetime) -> dt.datetime:
        """Map storage-domain datetime back to logical/query datetime.

        Args:
            time: Datetime extracted from storage key/path.

        Returns:
            Logical/query datetime expected by catalogue consumers.
        """
        key_parser = KeyParser(self.dataset.key_pattern)
        return key_parser.from_storage_time(time, self.dataset.time_signature)
    
    @withcases
    def get_prefix(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs) -> str:
        """
        Get the directory prefix for file discovery.
        
        Resolves the key pattern to a directory path, substituting date components
        when a TimeRange is provided to narrow the search space.
        
        Args:
            time: Optional datetime or TimeRange to partially resolve dates
            **kwargs: Additional tag substitutions
            
        Returns:
            Directory path prefix for file traversal
            
        Example:
            key_pattern = '/data/%Y/%m/file_%Y%m%d.tif'
            time = TimeRange(2021-01-01, 2021-01-31)
            → returns '/data/2021/01'
        """
        key_pattern = KeyParser(self.dataset.key_pattern)
        return key_pattern.prefix(time=time, tags=kwargs)
    
    @withcases
    def get_available_keys(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs) -> list[str]:
        """
        Get list of available file keys/paths.
        
        Traverses the file system to discover files matching the key pattern
        within the specified time range.
        
        Args:
            time: Optional datetime or TimeRange to filter files
            **kwargs: Additional tag substitutions
            
        Returns:
            List of file paths matching the pattern
            
        Example:
            >>> dataset.catalogue.get_available_keys(
            ...     time=TimeRange(2021-01-01, 2021-01-31)
            ... )
            ['/data/2021/01/file_20210101.tif', '/data/2021/01/file_20210102.tif', ...]
        """
        # Handle multi-month TimeRange by splitting into per-month queries
        if isinstance(time, TimeRange):
            months = time.months
            if len(months) > 1:
                files = []
                for month in months:
                    t_start = max(month.start, time.start)
                    month_end = month.end.replace(hour=23, minute=59, second=59)
                    t_end = min(month_end, time.end)
                    files.extend(self.get_available_keys(TimeRange(t_start, t_end), **kwargs))
                return files
        
        # Get directory prefix to search
        prefix = self.get_prefix(time, **kwargs)
        if not self.dataset._check_data(prefix):
            return []
        
        # Convert single datetime to TimeRange for consistent handling
        if isinstance(time, dt.datetime):
            time = TimeRange(time, time)
        
        # Get key pattern for matching
        key_pattern = self.dataset.get_key(time=None, **kwargs)
        key_parser = KeyParser(key_pattern)
        
        # Walk directory and filter matching files
        files = []
        for file in self.dataset._walk(prefix):
            try:
                parsed = key_parser.match(file)
                this_time = parsed.time
                if time is None or (time is not None and time.contains(this_time)) or not self.dataset.has_time:
                    files.append(file)
            except ValueError:
                pass
        
        return files
    
    @withcases
    def get_available_tags(self, time: Optional[dt.datetime|TimeRange] = None, **kwargs) -> dict[str, list]:
        """
        Extract all unique tags and times from available files.
        
        Discovers files and extracts their date/tag values, returning a dictionary
        mapping tag names to lists of unique values found.
        
        Args:
            time: Optional datetime or TimeRange to filter files
            **kwargs: Additional tag substitutions
            
        Returns:
            Dictionary with tag names as keys and lists of unique values.
            Always includes 'time' key with list of datetimes.
            
        Example:
            >>> dataset.catalogue.get_available_tags()
            {
                'time': [datetime(2021, 1, 1), datetime(2021, 1, 2), ...],
                'tile': ['h18v04', 'h19v04'],
                'variable': ['temp', 'precip']
            }
        """
        time = self._to_storage_time(time)
        
        # Get all available files
        all_keys = self.get_available_keys(time, **kwargs)
        key_parser = KeyParser(self.dataset.key_pattern)
        
        # Extract tags from each file
        all_tags = {}
        all_dates = set()
        for key in all_keys:
            parsed = key_parser.match(key)
            this_date = self._from_storage_time(parsed.time)
            this_tags = parsed.tags
            
            for tag in this_tags:
                if tag not in all_tags:
                    all_tags[tag] = set()
                all_tags[tag].add(this_tags[tag])
            all_dates.add(this_date)
        
        # Convert sets to sorted lists
        all_tags = {tag: list(all_tags[tag]) for tag in all_tags}
        all_tags['time'] = list(all_dates)
        all_tags['time'].sort()
        
        return all_tags

    def _get_times(self, time_range: TimeRange, **kwargs) -> Generator[dt.datetime, None, None]:
        """
        Generate times within a time range (internal generator version).
        
        Yields times from both the dataset's own files and from parent datasets
        (if any). Parent times are only included if they exist in ALL parents
        (intersection).
        
        Args:
            time_range: Time range to search within
            **kwargs: Additional tag filters
            
        Yields:
            datetime objects within the range
        """
        # Get times from this dataset's files
        all_times = self.get_available_tags(time_range, **kwargs)['time']
        all_times.sort()
        for time in all_times:
            if time_range.contains(time):
                yield time
        
        # Add times from parent datasets (intersection of all parents)
        if hasattr(self.dataset, 'parents') and self.dataset.parents is not None:
            parent_times = [set(parent.get_times(time_range, **kwargs)) for parent in self.dataset.parents.values()]
            # Get the intersection of all times
            parent_times = set.intersection(*parent_times)
            for time in parent_times:
                if time not in all_times and time_range.contains(time):
                    yield time

        # Add times from fallback dataset if available (union with main dataset)
        if hasattr(self.dataset, 'fallback') and self.dataset.fallback is not None:
            fallback_times = set(self.dataset.fallback.get_times(time_range,  **kwargs))
            for time in fallback_times:
                if time not in all_times and time_range.contains(time):
                    yield time

    @withcases
    def get_times(self, time_range: TimeRange, **kwargs) -> list[dt.datetime]:
        """
        Get a list of times between two dates.
        
        Returns all available datetime values within the specified range.
        If the dataset has parents, includes times that exist in ALL parents
        (useful for composite datasets).
        
        Args:
            time_range: Time range to search within
            **kwargs: Additional tag filters (handled by @withcases decorator in Dataset)
            
        Returns:
            Sorted list of datetime objects
            
        Example:
            >>> catalog.get_times(TimeRange('2024-01-01', '2024-01-31'))
            [datetime(2024, 1, 1), datetime(2024, 1, 2), ...]
        """
        return list(self._get_times(time_range, **kwargs))

    @withcases
    def get_timesteps(self, time_range: TimeRange, **kwargs) -> list[TimeStep]:
        """
        Get a list of TimeStep objects within a time range.
        
        Converts discovered times to TimeStep objects using the dataset's
        estimated timestep. Candidate discovery range is expanded through
        ``KeyParser.expand_overlap_range`` so start/end anchor semantics are
        applied consistently in one place.
        
        Args:
            time_range: Time range to search within
            **kwargs: Additional tag filters
            
        Returns:
            List of TimeStep objects that overlap the requested range.
            
        Example:
            >>> catalog.get_timesteps(TimeRange('2024-01-01', '2024-01-31'))
            [TimeStep('2024-01-01', freq='d', agg=1), ...]
        """
        timestep = self.estimate_timestep(**kwargs)
        key_parser = KeyParser(self.dataset.key_pattern)
        _time_range = key_parser.expand_overlap_range(
            time_range=time_range,
            timestep_unit=timestep.unit,
            time_signature=self.dataset.time_signature,
        )

        times = self.get_times(_time_range, **kwargs)
        
        # Convert to timesteps
        timesteps = [timestep.from_date(t) for t in times]
        
        # Filter to ensure timesteps overlap with requested range
        end = time_range.end
        if end.hour == 0 and end.minute == 0:
            end = end + dt.timedelta(minutes = 1439)
        timesteps = [ts for ts in timesteps if not (ts.start > end or ts.end < time_range.start)]

        return timesteps

    def estimate_timestep(self, date_sample=None, **kwargs) -> TimeStep:
        """
        Estimate the dataset's timestep from a sample of dates.
        
        Tries to infer the temporal resolution by examining spacing between
        available dates. Uses get_last_date to find recent dates if no sample
        is provided. Caches result in dataset.timestep.
        
        Args:
            date_sample: Optional list of dates to analyze. If None or too small,
                        fetches recent dates automatically.
            **kwargs: Additional tag filters for date discovery
            
        Returns:
            Estimated TimeStep, or None if cannot be determined
            
        Example:
            >>> catalog.estimate_timestep()
            TimeStep(freq='d', agg=1)  # Daily data
        """
        # Use cached timestep if available
        if hasattr(self.dataset, 'timestep') and self.dataset.timestep is not None:
            return self.dataset.timestep
        
        # Get sample dates if not provided
        if date_sample is None or len(date_sample) == 0:
            date_sample = self.get_last_date(n=8, **kwargs)
        elif len(date_sample) < 5:
            other_dates = self.get_last_date(n=8 - len(date_sample), now=min(date_sample), **kwargs) or []
            date_sample = other_dates + date_sample

        # Estimate using helper function
        timestep = estimate_timestep(date_sample)
        
        # Apply aggregation window if set
        if timestep is not None and hasattr(self.dataset, 'agg') and self.dataset.agg is not None:
            timestep = timestep.with_agg(self.dataset.agg)
        
        # Cache the result
        self.dataset.timestep = timestep
        return timestep

    def _binary_search_boundary_month(self, start_month: Month, end_month: Month, direction: int, **kwargs) -> Month:
        """
        Binary search to find a boundary month (first or last) containing data within a range.
        
        Args:
            start_month: Earliest month to search from
            end_month: Latest month to search to
            direction: Search direction: +1 for last (search forward), -1 for first (search backward)
            **kwargs: Additional tag filters
            
        Returns:
            Boundary month with data found in the range
        """
        # Track the known boundary month
        boundary_month = start_month if direction > 0 else end_month
        
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
                # Data exists at midpoint
                boundary_month = mid_month
                if direction > 0:
                    # Searching for last: move start forward
                    start_month = mid_month
                else:
                    # Searching for first: move end backward
                    end_month = mid_month
            else:
                # No data at midpoint
                if direction > 0:
                    # Searching for last: move end backward
                    end_month = mid_month - 1
                else:
                    # Searching for first: move start forward
                    start_month = mid_month + 1
        
        if direction < 0:
            # if searching for first, ensure we return the earliest month with data
            # this could be boundary_month or start_month depending on the final check
            if len(self.get_times(start_month, **kwargs)) > 0:
                return start_month
        else:
            if len(self.get_times(end_month, **kwargs)) > 0:
                return end_month
             
        return boundary_month

    @withcases
    def get_last_date(self, now=None, n=1, lim=None, **kwargs) -> dt.datetime | list[dt.datetime] | None:
        """
        Find the most recent available date(s).
        
        Uses an optimized binary search algorithm:
        1. First finds ANY date quickly using exponential backoff
        2. Then binary searches between that date and now to find the last month with data
        3. Finally collects the n most recent dates from that region
        
        Args:
            now: Reference time (defaults to current time)
            n: Number of recent dates to return
            lim: Lower time limit for search (defaults to 5 years ago)
            **kwargs: Additional tag filters
            
        Returns:
            Single datetime if n=1, list of datetimes if n>1, or None if no data found
            
        Example:
            >>> catalog.get_last_date()
            datetime(2024, 2, 15)
            >>> catalog.get_last_date(n=3)
            [datetime(2024, 2, 15), datetime(2024, 2, 14), datetime(2024, 2, 13)]
        """
        if now is None:
            # Add buffer to include forecast data (+1 month for now, this will need to be adjusted because it is a bit hacky)
            now = dt.datetime.now() + dt.timedelta(days=31) 
        
        # Find ANY date first using exponential backoff
        any_date = self.get_any_date(now=now, lim=lim, **kwargs)
        if any_date is None:
            return None
        
        # Binary search between any_date and now to find the last month with data
        start_month = Month(any_date.year, any_date.month)
        end_month = Month(now.year, now.month)
        last_month_with_data = self._binary_search_boundary_month(start_month, end_month, direction=+1, **kwargs)
        
        # Now collect n dates from last_month_with_data and nearby months
        last_date = []
        search_month = last_month_with_data
        end_search   = last_month_with_data - 12*5 if lim is None else Month.from_date(lim)
        
        # Search forward from last known month
        while search_month >= end_search and len(last_date) < n:
            month_times = self.get_times(search_month, **kwargs)
            if len(month_times) > 0:
                valid_time = [t for t in month_times if t <= now]
                last_date.extend(valid_time)
            search_month = search_month - 1
        
        # Sort and take the most recent n
        last_date.sort(reverse=True)
        last_date = last_date[:n]
        
        if len(last_date) == 0:
            return None
        if n == 1:
            return last_date[0]
        else:
            return last_date

    def get_any_date(self, now=None, lim=None, **kwargs) -> dt.datetime | None:
        """
        Find ANY available date quickly (used for template extraction).
        
        Much faster than get_last_date when you don't care which file.
        Uses exponential backoff to quickly find a month with data. If that fails,
        falls back to a linear monthly scan to ensure sparse data isn't missed.
        
        Args:
            now: Reference time (defaults to current time)
            lim: Lower time limit for search (defaults to 5 years ago)
            **kwargs: Additional tag filters
            
        Returns:
            First datetime found, or None if no data exists
            
        Example:
            >>> catalog.get_any_date()
            datetime(2024, 1, 15)  # Some recent date, not necessarily the last
        """
        if now is None:
            # Add buffer to include forecast data (+1 month for now, this will need to be adjusted because it is a bit hacky)
            now = dt.datetime.now() + dt.timedelta(days=31) 
        
        # Default limit: 5 years back (reasonable for most datasets)
        if lim is None:
            lim = now - dt.timedelta(days=5*365)
        
        # Phase 1: Fast exponential backoff for typical cases
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
        
        # Phase 2: Linear fallback for sparse/old datasets that were skipped
        # Start from where we left off or from 'now', go back month by month
        search_month = Month(now.year, now.month)
        lim_month = Month(lim.year, lim.month)
        
        while search_month >= lim_month:
            month_times = self.get_times(search_month, **kwargs)
            if len(month_times) > 0:
                valid_times = [t for t in month_times if t <= now]
                return valid_times[0] if valid_times else None
            search_month = search_month - 1
        
        # No data found
        return None

    @withcases
    def get_first_date(self, start=None, n=1, **kwargs) -> dt.datetime | list[dt.datetime] | None:
        """
        Find the earliest available date(s).
        
        Uses an optimized binary search algorithm:
        1. First finds ANY date quickly using exponential backoff
        2. Then binary searches between start and that date to find the first month with data
        3. Finally collects the n earliest dates from that region
        
        Args:
            start: Earliest date to consider (defaults to 1900-01-01)
            n: Number of dates to return
            **kwargs: Additional tag filters
            
        Returns:
            Single datetime if n=1, list of datetimes if n>1, or None if no data found
            
        Example:
            >>> catalog.get_first_date()
            datetime(2020, 1, 1)
            >>> catalog.get_first_date(n=3)
            [datetime(2020, 1, 1), datetime(2020, 1, 2), datetime(2020, 1, 3)]
        """
        if start is None:
            start = dt.datetime(1900, 1, 1)

        # Find ANY date first using exponential backoff
        any_date = self.get_any_date(**kwargs)
        if any_date is None:
            return None
        
        # Binary search between start and any_date to find the first month with data
        start_month = Month(start.year, start.month)
        end_month = Month(any_date.year, any_date.month)
        first_month_with_data = self._binary_search_boundary_month(start_month, end_month, direction=-1, **kwargs)
        
        # Collect n dates starting from the first month with data
        first_date = []
        search_month = first_month_with_data
        # Set a reasonable end limit (any_date's month + buffer)
        end_limit = end_month + 12*5  # Look up to 5 year past any_date
        
        while len(first_date) < n and search_month <= end_limit:
            this_month_times = self.get_times(search_month, **kwargs)
            if len(this_month_times) > 0:
                valid_time = [t for t in this_month_times if t >= start]
                valid_time.sort()
                first_date.extend(valid_time)
            search_month = search_month + 1

        # Take only n dates
        first_date = first_date[:n]
        
        if len(first_date) == 0:
            return None
        if n == 1:
            return first_date[0]
        else:
            return first_date

    @withcases
    def get_last_ts(self, **kwargs) -> TimeStep:
        """
        Get the most recent timestep.
        
        Finds the last available date and converts it to a TimeStep object.
        Estimates the timestep if not already set. Handles time_signature
        adjustments for datasets with 'end+1' convention.
        
        Args:
            **kwargs: Additional tag filters (can include 'now' parameter)
            
        Returns:
            TimeStep object for the most recent data, or None if no data found
            
        Example:
            >>> catalog.get_last_ts()
            TimeStep('2024-02-15', freq='d', agg=1)
        """
        last_date = self.get_last_date(**kwargs)
        if last_date is None:
            return None
        
        if hasattr(self.dataset, 'timestep') and self.dataset.timestep is not None:
            timestep = self.dataset.timestep
        else:
            kwargs.pop('now', None)
            other_dates = self.get_last_date(now=last_date, n=3, **kwargs)
            timestep = estimate_timestep(other_dates)
            if timestep is None:
                return None

        logical_date = self._from_storage_time(last_date)
        return timestep.from_date(logical_date)

    @withcases
    def get_first_ts(self, **kwargs) -> TimeStep:
        """
        Get the earliest timestep.
        
        Finds the first available date and converts it to a TimeStep object.
        Estimates the timestep if not already set. Handles time_signature
        adjustments for datasets with 'end+1' convention.
        
        Args:
            **kwargs: Additional tag filters (can include 'start' parameter)
            
        Returns:
            TimeStep object for the earliest data, or None if no data found
            
        Example:
            >>> catalog.get_first_ts()
            TimeStep('2020-01-01', freq='d', agg=1)
        """
        first_date = self.get_first_date(**kwargs)
        if first_date is None:
            return None
        
        if hasattr(self.dataset, 'timestep') and self.dataset.timestep is not None:
            timestep = self.dataset.timestep
        else:
            other_dates = self.get_first_date(start=first_date, n=8, **kwargs)
            timestep = estimate_timestep(other_dates)
            if timestep is None:
                return None

        logical_date = self._from_storage_time(first_date)
        return timestep.from_date(logical_date)

    @withcases
    def get_start(self, agg=False, **kwargs) -> dt.datetime | None:
        """
        Get the start of the available data.
        
        Returns the earliest date for which data is available. If agg=True and
        the dataset has an aggregation window, returns the start of the first
        aggregation period instead.
        
        Args:
            agg: Whether to return the start of the aggregation period (if applicable)
            **kwargs: Additional tag filters
        Returns:
            Earliest datetime available, or None if no data exists
        Example:
            >>> catalog.get_start()
            datetime(2020, 1, 31)
            >>> catalog.get_start(agg=True)
            datetime(2019, 1, 1)  # Start of aggregation period for data with 1-month aggregation
        """

        first_ts = self.get_first_ts(**kwargs)
        if first_ts is not None:
            if agg:
                return first_ts.agg_range.start
            else:
                return first_ts.start
        else:
            return self.get_first_date(**kwargs)

    @withcases
    def check_data(self, time: Optional[dt.datetime] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time and tags.
        
        Validates whether a specific time and tag combination exists in the catalogue.
        Supports versioned files (selects latest version if not specified) and 
        parent datasets (checks if all parents have data).
        
        Note: This method is wrapped with @withcases decorator in Dataset for 
        handling multiple cases at once.
        
        Args:
            time: Optional datetime to check. If None, checks all tiles.
            **kwargs: Tag filters (e.g., tile='h18v04', variable='temp')
            
        Returns:
            True if data exists, False otherwise
            
        Example:
            >>> catalogue.check_data(datetime(2024, 1, 1), tile='h18v04')
            True
            >>> catalogue.check_data(datetime(2024, 1, 1))  # Checks all tiles
            False  # Only returns True if ALL tiles exist
        """
        
        # Handle versioned files - get latest version if not specified
        if self.dataset.has_version and 'file_version' not in kwargs:
            available_versions = self.get_available_tags(time, **kwargs).get('file_version')
            if available_versions is not None:
                available_versions.sort()
                kwargs['file_version'] = available_versions[-1]

        # If specific tile is requested, check that tile
        if 'tile' in kwargs:
            return self._find_data_source(time, **kwargs) > 0

        # If no tile specified, check all tiles (returns True only if ALL exist)
        for tile in self.dataset.tile_names:
            if self._find_data_source(time, tile=tile, **kwargs) == 0:
                return False
        return True

    def _find_data_source(self, time: Optional[dt.datetime] = None, **kwargs) -> bool:
        """
        Check if data is available for a given time and tags and returns its source:
        0. data not found
        1. main dataset
        2. parent datasets (if any)
        3. fallback dataset (if any) 
        
        """
        
        # Handle versioned files - get latest version if not specified
        if self.dataset.has_version and 'file_version' not in kwargs:
            available_versions = self.get_available_tags(time, **kwargs).get('file_version')
            if available_versions is not None:
                available_versions.sort()
                kwargs['file_version'] = available_versions[-1]

        full_key = self.dataset.get_key(time, **kwargs)
        if self.dataset._check_data(full_key):
            return 1
        
        # Check parent datasets if available
        if hasattr(self.dataset, 'parents') and self.dataset.parents is not None:
            if all([parent.catalogue.check_data(time, **kwargs) for parent in self.dataset.parents.values()]):
                return 2
        
        # Check fallback datasets if available
        if hasattr(self.dataset, 'fallback') and self.dataset.fallback is not None:
            if self.dataset.fallback.check_data(time, **kwargs):
                return 3
        
        return 0

    @withcases
    def find_times(self, times: list[dt.datetime], id: bool = False, rev: bool = False, **kwargs) -> list[dt.datetime] | list[int]:
        """
        Find which times from a list are available in the catalogue.
        
        Efficiently filters a list of times to find which ones have data available.
        Useful for batch validation before processing.
        
        Note: This method is wrapped with @withcases decorator in Dataset for 
        handling multiple cases at once.
        
        Args:
            times: List of datetime or TimeStep objects to check
            id: If True, return indices instead of times
            rev: If True, return times that DON'T exist (reverse filter)
            **kwargs: Tag filters for validation
            
        Returns:
            List of times (or indices) that exist (or don't exist if rev=True)
            
        Example:
            >>> times = [datetime(2024, 1, i) for i in range(1, 32)]
            >>> catalogue.find_times(times, tile='h18v04')
            [datetime(2024, 1, 1), datetime(2024, 1, 3), ...]  # Only available times
            
            >>> catalogue.find_times(times, id=True, rev=True)
            [1, 5, 10]  # Indices of MISSING times
        """
        from ..timestepping import TimeStep, TimeRange
        
        if len(times) == 0:
            return []
        all_ids = list(range(len(times)))

        # Convert all times to their signature representation
        time_signatures = [self.dataset.get_time_signature(t) for t in times]
        tr = TimeRange(min(time_signatures), max(time_signatures))

        # Get all available times in the range
        all_times = self.get_available_tags(tr, **kwargs).get('time', [])

        # Find which times are available
        ids = [i for i in all_ids if time_signatures[i] in all_times] or []
        if rev:
            # Reverse: return times that DON'T exist
            ids = [i for i in all_ids if i not in ids] or []

        if id:
            return ids
        else:
            return [times[i] for i in ids]

    @withcases
    def find_tiles(self, time: Optional[dt.datetime] = None, rev: bool = False, **kwargs) -> list[str]:
        """
        Find which tiles are available for a given time.
        
        Returns a filtered list of tile names that have data available.
        Useful for determining which tiles to process.
        
        Note: This method is wrapped with @withcases decorator in Dataset for 
        handling multiple cases at once.
        
        Args:
            time: Optional datetime to check tiles for
            rev: If True, return tiles that DON'T exist (reverse filter)
            **kwargs: Additional tag filters
            
        Returns:
            List of tile names that exist (or don't exist if rev=True)
            
        Example:
            >>> catalogue.find_tiles(datetime(2024, 1, 1))
            ['h18v04', 'h19v04', 'h20v04']  # Only tiles with data
            
            >>> catalogue.find_tiles(datetime(2024, 1, 1), rev=True)
            ['h21v04']  # Tiles WITHOUT data
        """
        all_tiles = self.dataset.tile_names
        available_tiles = self.get_available_tags(time, **kwargs).get('tile', [])
        
        if not rev:
            return [tile for tile in all_tiles if tile in available_tiles]
        else:
            return [tile for tile in all_tiles if tile not in available_tiles]
