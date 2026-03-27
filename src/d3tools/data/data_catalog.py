"""
Data Catalog - Discovery and enumeration of available dataset files.

This manager handles all operations related to discovering what data exists:
- File discovery (get_available_keys, get_available_tags)
- Boundary discovery (get_last_date, get_first_date)
- Time enumeration (get_times, get_timesteps)
- Timestep inference (estimate_timestep)
"""
from typing import Optional, Generator, TYPE_CHECKING
import datetime as dt
import os

from ..timestepping import TimeRange, Month, TimeStep, estimate_timestep, TimeWindow

if TYPE_CHECKING:
    from .dataset import Dataset


class DataCatalog:
    """
    Manages discovery and enumeration of available dataset files.
    
    Responsibilities:
    - Enumerate available files and their properties
    - Find temporal boundaries (first/last dates)
    - List times within ranges
    - Infer dataset timestep from samples
    
    All methods here are about DISCOVERING what exists, not validating
    specific items (that's DataChecker's job).
    """
    
    def __init__(self, dataset: 'Dataset'):
        """
        Initialize the data catalog.
        
        Args:
            dataset: Parent Dataset instance for accessing properties and methods
        """
        self.dataset = dataset
    
    def __repr__(self):
        return f"DataCatalog({self.dataset.name})"
    
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
        if not isinstance(time, TimeRange):
            prefix = self.dataset.get_key(time=time, **kwargs)
        else:
            start = time.start
            end = time.end
            prefix = self.dataset.get_key(time=None, **kwargs)
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
            >>> dataset.catalog.get_available_keys(
            ...     time=TimeRange(2021-01-01, 2021-01-31)
            ... )
            ['/data/2021/01/file_20210101.tif', '/data/2021/01/file_20210102.tif', ...]
        """
        from ..parse import extract_date_and_tags
        
        # Handle multi-month TimeRange by splitting into per-month queries
        if isinstance(time, TimeRange):
            months = time.months
            if len(months) > 1:
                files = []
                for month in months:
                    t_start = max(month.start, time.start)
                    t_end = min(month.end, time.end)
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
        
        # Walk directory and filter matching files
        files = []
        for file in self.dataset._walk(prefix):
            try:
                this_time, _ = extract_date_and_tags(file, key_pattern)
                if time is None or (time is not None and time.contains(this_time)) or not self.dataset.has_time:
                    files.append(file)
            except ValueError:
                pass
        
        return files
    
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
            >>> dataset.catalog.get_available_tags()
            {
                'time': [datetime(2021, 1, 1), datetime(2021, 1, 2), ...],
                'tile': ['h18v04', 'h19v04'],
                'variable': ['temp', 'precip']
            }
        """
        from ..parse import extract_date_and_tags
        
        # Handle time_signature adjustment
        if self.dataset.time_signature == 'end+1' and time is not None:
            if isinstance(time, dt.datetime):
                time = time + dt.timedelta(days=1)
            elif isinstance(time, TimeRange):
                time = TimeRange(time.start + dt.timedelta(days=1), time.end + dt.timedelta(days=1))
        
        # Get all available files
        all_keys = self.get_available_keys(time, **kwargs)
        
        # Extract tags from each file
        all_tags = {}
        all_dates = set()
        for key in all_keys:
            this_date, this_tags = extract_date_and_tags(key, self.dataset.key_pattern)
            
            for tag in this_tags:
                if tag not in all_tags:
                    all_tags[tag] = set()
                all_tags[tag].add(this_tags[tag])
            all_dates.add(this_date)
        
        # Convert sets to sorted lists
        all_tags = {tag: list(all_tags[tag]) for tag in all_tags}
        all_tags['time'] = list(all_dates)
        
        # Adjust times for end+1 signature
        if self.dataset.time_signature == 'end+1':
            all_tags['time'] = [t - dt.timedelta(days=1) for t in all_tags['time']]
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

    def get_timesteps(self, time_range: TimeRange, **kwargs) -> list[TimeStep]:
        """
        Get a list of TimeStep objects within a time range.
        
        Converts discovered times to TimeStep objects using the dataset's
        estimated timestep. Handles time_signature adjustments and filters
        to ensure timesteps actually overlap with the requested range.
        
        Args:
            time_range: Time range to search within
            **kwargs: Additional tag filters
            
        Returns:
            List of TimeStep objects within the range
            
        Example:
            >>> catalog.get_timesteps(TimeRange('2024-01-01', '2024-01-31'))
            [TimeStep('2024-01-01', freq='d', agg=1), ...]
        """
        timestep = self.estimate_timestep(**kwargs)
        window = TimeWindow(1, timestep.unit)

        # Adjust time range based on time signature
        if self.dataset.time_signature == 'start':
            _time_range = time_range.extend(window, before=True)
        elif self.dataset.time_signature.startswith('end'):
            _time_range = time_range.extend(window, before=False)
            if self.dataset.time_signature == 'end+1':
                _time_range = time_range.extend(TimeWindow(1, 'd'), before=False)

        times = self.get_times(_time_range, **kwargs)
        
        # Adjust for end+1 signature
        if self.dataset.time_signature == 'end+1':
            times = [t - dt.timedelta(days=1) for t in times]
        
        # Convert to timesteps
        timesteps = [timestep.from_date(t) for t in times]
        
        # Filter to ensure timesteps overlap with requested range
        for ts in timesteps:
            end = time_range.end
            if end.hour == 0 and end.minute == 0:
                end = end + dt.timedelta(minutes=1439)
            if ts.start > end or ts.end < time_range.start:
                timesteps.remove(ts)

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
        
        return boundary_month

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
            now = dt.datetime.now()
        
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
            now = dt.datetime.now()
        
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
        end_limit = end_month + 12  # Look up to 1 year past any_date
        
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

        if self.dataset.time_signature == 'end+1':
            return timestep.from_date(last_date) - 1
        else:
            return timestep.from_date(last_date)

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

        if self.dataset.time_signature == 'end+1':
            return timestep.from_date(first_date) - 1
        else:
            return timestep.from_date(first_date)

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