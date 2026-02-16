"""
Tests for DataCatalog class.

Tests the data catalog manager that handles discovery and enumeration
of available dataset files.
"""
import pytest
import datetime as dt
from d3tools.data.local_dataset import LocalDataset
from d3tools.data.data_catalog import DataCatalog
from d3tools.timestepping import TimeRange, TimeStep


class TestDataCatalogInitialization:
    """Test DataCatalog initialization and integration with Dataset."""
    
    def test_catalog_created_on_dataset_init(self, tmp_path):
        """Test that catalog is automatically created when Dataset is initialized."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file='test.tif'
        )
        
        assert hasattr(dataset, 'catalog')
        assert isinstance(dataset.catalog, DataCatalog)
    
    def test_catalog_has_dataset_reference(self, tmp_path):
        """Test that catalog has reference to parent dataset."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file='test.tif'
        )
        
        assert dataset.catalog.dataset is dataset
    
    def test_catalog_repr(self, tmp_path):
        """Test catalog string representation."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file='test_data.tif'
        )
        
        assert repr(dataset.catalog) == "DataCatalog(test_data)"
    
    def test_catalog_created_for_different_dataset_types(self, tmp_path):
        """Test that catalog is created for all dataset types."""
        # LocalDataset
        local_ds = LocalDataset(path=str(tmp_path), file='test.tif')
        assert hasattr(local_ds, 'catalog')
        
        # Could add more dataset types here as we test them
        # S3Dataset, MemoryDataset, etc.


class TestGetPrefix:
    """Test get_prefix method for directory path resolution."""
    
    def test_get_prefix_with_datetime(self, tmp_path):
        """Test get_prefix with a single datetime."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y' / '%m'),
            file='data_%Y%m%d.tif'
        )
        
        time = dt.datetime(2021, 3, 15)
        prefix = dataset.catalog.get_prefix(time=time)
        
        # Should return the directory part only
        assert prefix == str(tmp_path / '2021' / '03')
    
    def test_get_prefix_with_timerange_same_year(self, tmp_path):
        """Test get_prefix with TimeRange in same year."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y' / '%m'),
            file='data_%Y%m%d.tif'
        )
        
        time_range = TimeRange(
            dt.datetime(2021, 3, 1),
            dt.datetime(2021, 3, 31)
        )
        prefix = dataset.catalog.get_prefix(time=time_range)
        
        # Should resolve year and month
        assert prefix == str(tmp_path / '2021' / '03')
    
    def test_get_prefix_with_timerange_same_day(self, tmp_path):
        """Test get_prefix with TimeRange on same day."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y' / '%m' / '%d'),
            file='data_%Y%m%d.tif'
        )
        
        time_range = TimeRange(
            dt.datetime(2021, 3, 15, 0, 0),
            dt.datetime(2021, 3, 15, 23, 59)
        )
        prefix = dataset.catalog.get_prefix(time=time_range)
        
        # Should resolve year, month, and day
        assert prefix == str(tmp_path / '2021' / '03' / '15')
    
    def test_get_prefix_with_timerange_different_months(self, tmp_path):
        """Test get_prefix with TimeRange spanning multiple months."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y' / '%m'),
            file='data_%Y%m%d.tif'
        )
        
        time_range = TimeRange(
            dt.datetime(2021, 3, 1),
            dt.datetime(2021, 5, 31)
        )
        prefix = dataset.catalog.get_prefix(time=time_range)
        
        # Should only resolve year (not month, since they differ)
        assert prefix == str(tmp_path / '2021')
    
    def test_get_prefix_strips_unresolved_patterns(self, tmp_path):
        """Test that unresolved date patterns are stripped from prefix."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y' / '%m'),
            file='data_%Y%m%d.tif'
        )
        
        # No time provided - patterns should be stripped
        prefix = dataset.catalog.get_prefix()
        
        # Should go up until no more date patterns
        assert prefix == str(tmp_path)
        assert '%Y' not in prefix
        assert '%m' not in prefix
    
    def test_get_prefix_with_tags(self, tmp_path):
        """Test get_prefix with tag substitution."""
        dataset = LocalDataset(
            path=str(tmp_path / '{region}' / '%Y'),
            file='data_%Y%m%d.tif'
        )
        
        time = dt.datetime(2021, 3, 15)
        prefix = dataset.catalog.get_prefix(time=time, region='EU')
        
        # Should substitute tag and resolve year
        assert prefix == str(tmp_path / 'EU' / '2021')
    
    def test_dataset_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_prefix delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y'),
            file='data_%Y%m%d.tif'
        )
        
        time = dt.datetime(2021, 3, 15)
        
        # Both should return same result
        catalog_result = dataset.catalog.get_prefix(time=time)
        dataset_result = dataset.get_prefix(time=time)
        
        assert catalog_result == dataset_result


class TestGetAvailableKeys:
    """Test get_available_keys method for file discovery."""
    
    def test_get_available_keys_no_files(self, tmp_path):
        """Test get_available_keys when no files exist."""
        dataset = LocalDataset(
            path=str(tmp_path / 'data'),
            file='file_%Y%m%d.tif'
        )
        
        keys = dataset.catalog.get_available_keys()
        assert keys == []
    
    def test_get_available_keys_with_files(self, tmp_path):
        """Test get_available_keys discovers existing files."""
        data_dir = tmp_path / 'data'
        data_dir.mkdir()
        
        # Create some test files
        (data_dir / 'file_20210101.tif').touch()
        (data_dir / 'file_20210102.tif').touch()
        (data_dir / 'file_20210103.tif').touch()
        
        dataset = LocalDataset(
            path=str(data_dir),
            file='file_%Y%m%d.tif'
        )
        
        keys = dataset.catalog.get_available_keys()
        
        assert len(keys) == 3
        assert str(data_dir / 'file_20210101.tif') in keys
        assert str(data_dir / 'file_20210102.tif') in keys
        assert str(data_dir / 'file_20210103.tif') in keys
    
    def test_get_available_keys_with_timerange_filter(self, tmp_path):
        """Test get_available_keys filters by TimeRange."""
        data_dir = tmp_path / 'data'
        data_dir.mkdir()
        
        # Create files for January and February
        (data_dir / 'file_20210101.tif').touch()
        (data_dir / 'file_20210115.tif').touch()
        (data_dir / 'file_20210201.tif').touch()
        (data_dir / 'file_20210215.tif').touch()
        
        dataset = LocalDataset(
            path=str(data_dir),
            file='file_%Y%m%d.tif'
        )
        
        # Filter to only January
        time_range = TimeRange(dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 31))
        keys = dataset.catalog.get_available_keys(time=time_range)
        
        assert len(keys) == 2
        assert str(data_dir / 'file_20210101.tif') in keys
        assert str(data_dir / 'file_20210115.tif') in keys
        assert str(data_dir / 'file_20210201.tif') not in keys
    
    def test_get_available_keys_with_datetime(self, tmp_path):
        """Test get_available_keys with single datetime."""
        data_dir = tmp_path / 'data'
        data_dir.mkdir()
        
        (data_dir / 'file_20210101.tif').touch()
        (data_dir / 'file_20210102.tif').touch()
        
        dataset = LocalDataset(
            path=str(data_dir),
            file='file_%Y%m%d.tif'
        )
        
        # Query specific date
        keys = dataset.catalog.get_available_keys(time=dt.datetime(2021, 1, 1))
        
        assert len(keys) == 1
        assert str(data_dir / 'file_20210101.tif') in keys
    
    def test_get_available_keys_ignores_non_matching_files(self, tmp_path):
        """Test that non-matching files are ignored."""
        data_dir = tmp_path / 'data'
        data_dir.mkdir()
        
        (data_dir / 'file_20210101.tif').touch()
        (data_dir / 'other_file.txt').touch()
        (data_dir / 'file_invalid.tif').touch()
        
        dataset = LocalDataset(
            path=str(data_dir),
            file='file_%Y%m%d.tif'
        )
        
        keys = dataset.catalog.get_available_keys()
        
        # Only the properly formatted file should be found
        assert len(keys) == 1
        assert str(data_dir / 'file_20210101.tif') in keys
    
    def test_get_available_keys_multi_month_range(self, tmp_path):
        """Test get_available_keys with multi-month TimeRange."""
        data_dir = tmp_path / 'data' / '%Y' / '%m'
        data_dir.mkdir(parents=True)
        
        # Create subdirectories and files
        jan_dir = tmp_path / 'data' / '2021' / '01'
        feb_dir = tmp_path / 'data' / '2021' / '02'
        jan_dir.mkdir(parents=True)
        feb_dir.mkdir(parents=True)
        
        (jan_dir / 'file_20210115.tif').touch()
        (feb_dir / 'file_20210215.tif').touch()
        
        dataset = LocalDataset(
            path=str(tmp_path / 'data' / '%Y' / '%m'),
            file='file_%Y%m%d.tif'
        )
        
        # Query spanning both months
        time_range = TimeRange(dt.datetime(2021, 1, 1), dt.datetime(2021, 2, 28))
        keys = dataset.catalog.get_available_keys(time=time_range)
        
        assert len(keys) == 2
    
    def test_get_available_keys_with_tags(self, tmp_path):
        """Test get_available_keys with tag substitution in kwargs."""
        # Create directories for different regions
        eu_dir = tmp_path / 'data' / 'EU'
        us_dir = tmp_path / 'data' / 'US'
        eu_dir.mkdir(parents=True)
        us_dir.mkdir(parents=True)
        
        # Create files in each region
        (eu_dir / 'file_20210101.tif').touch()
        (eu_dir / 'file_20210102.tif').touch()
        (us_dir / 'file_20210101.tif').touch()
        
        dataset = LocalDataset(
            path=str(tmp_path / 'data' / '{region}'),
            file='file_%Y%m%d.tif'
        )
        
        # Query with region tag
        keys = dataset.catalog.get_available_keys(region='EU')
        
        assert len(keys) == 2
        assert all('EU' in key for key in keys)
        assert not any('US' in key for key in keys)
        
        # Query different region
        keys_us = dataset.catalog.get_available_keys(region='US')
        assert len(keys_us) == 1
        assert 'US' in keys_us[0]
    
    def test_dataset_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_available_keys delegates to catalog."""
        data_dir = tmp_path / 'data'
        data_dir.mkdir()
        (data_dir / 'file_20210101.tif').touch()
        
        dataset = LocalDataset(
            path=str(data_dir),
            file='file_%Y%m%d.tif'
        )
        
        # Both should return same result
        catalog_result = dataset.catalog.get_available_keys()
        dataset_result = dataset.get_available_keys()
        
        assert catalog_result == dataset_result


class TestGetAvailableTags:
    """Test suite for get_available_tags method."""
    
    def test_get_available_tags_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_available_tags delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create mock files
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240102.tif").touch()
        
        result = dataset.get_available_tags()
        
        # Verify returns dict with time key
        assert isinstance(result, dict)
        assert 'time' in result

    def test_get_available_tags_with_tags(self, tmp_path):
        """Test get_available_tags extracts tag values."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{product}.tif"
        )
        
        # Create files with different product tags
        (tmp_path / "data_20240101_A.tif").touch()
        (tmp_path / "data_20240102_B.tif").touch()
        (tmp_path / "data_20240103_A.tif").touch()
        
        result = dataset.get_available_tags()
        
        # Verify tags extracted
        assert 'product' in result
        assert set(result['product']) == {'A', 'B'}
        assert len(result['time']) == 3

    def test_get_available_tags_with_time_range(self, tmp_path):
        """Test get_available_tags filters by time range."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create files
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240102.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2))
        result = dataset.get_available_tags(time=time_range)
        
        # Should only include first two dates
        assert len(result['time']) == 2

    def test_get_available_tags_end_plus_one(self, tmp_path):
        """Test get_available_tags handles time_signature='end+1'."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif",
            time_signature="end+1"
        )
        
        # Create files (note: filenames are +1 day ahead)
        (tmp_path / "data_20240102.tif").touch()  # Represents 2024-01-01
        (tmp_path / "data_20240103.tif").touch()  # Represents 2024-01-02
        
        result = dataset.get_available_tags()
        
        # Should adjust times back by 1 day
        assert dt.datetime(2024, 1, 1) in result['time']
        assert dt.datetime(2024, 1, 2) in result['time']

    def test_get_available_tags_end_plus_one_with_datetime(self, tmp_path):
        """Test get_available_tags with time_signature='end+1' and datetime query."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif",
            time_signature="end+1"
        )
        
        # Create files
        (tmp_path / "data_20240102.tif").touch()  # Represents 2024-01-01
        (tmp_path / "data_20240103.tif").touch()  # Represents 2024-01-02
        
        # Query for 2024-01-01 (should look for 20240102)
        result = dataset.get_available_tags(time=dt.datetime(2024, 1, 1))
        
        # Should find and adjust back
        assert len(result['time']) == 1
        assert dt.datetime(2024, 1, 1) in result['time']

    def test_get_available_tags_multiple_tags(self, tmp_path):
        """Test get_available_tags with multiple tag types."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{product}_{version}.tif"
        )
        
        # Create files with multiple tags
        (tmp_path / "data_20240101_A_v1.tif").touch()
        (tmp_path / "data_20240101_A_v2.tif").touch()
        (tmp_path / "data_20240102_B_v1.tif").touch()
        
        result = dataset.get_available_tags()
        
        # Verify all tags extracted
        assert 'product' in result
        assert 'version' in result
        assert set(result['product']) == {'A', 'B'}
        assert set(result['version']) == {'v1', 'v2'}
        assert len(result['time']) == 2

    def test_get_available_tags_empty(self, tmp_path):
        """Test get_available_tags with no matching files."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        result = dataset.get_available_tags()
        
        # Should return empty dict with time key
        assert result == {'time': []}

class TestGetTimes:
    """Test suite for get_times and _get_times methods."""
    
    def test_get_times_basic(self, tmp_path):
        """Test get_times returns list of times in range."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create files
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240102.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 3))
        result = dataset.get_times(time_range)
        
        assert len(result) == 3
        assert all(isinstance(t, dt.datetime) for t in result)
    
    def test_get_times_filters_by_range(self, tmp_path):
        """Test get_times only returns times within the range."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create files
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240102.tif").touch()
        (tmp_path / "data_20240105.tif").touch()
        
        # Request only first two
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2))
        result = dataset.get_times(time_range)
        
        assert len(result) == 2
        assert dt.datetime(2024, 1, 5) not in result
    
    def test_get_times_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_times delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 1))
        
        # Both should return same result
        catalog_result = dataset.catalog.get_times(time_range)
        dataset_result = dataset.get_times(time_range)
        
        assert catalog_result == dataset_result


class TestGetTimesteps:
    """Test suite for get_timesteps method."""
    
    def test_get_timesteps_basic(self, tmp_path):
        """Test get_timesteps returns TimeStep objects."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create daily files
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 5))
        result = dataset.get_timesteps(time_range, now = dt.datetime(2024, 1, 6))
        
        assert len(result) > 0
        assert all(isinstance(ts, TimeStep) for ts in result)
        assert all(ts.length == 1 and ts.unit == 'd' for ts in result)
    
    def test_get_timesteps_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_timesteps delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 4):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 3))
        
        # Both should return same result
        catalog_result = dataset.catalog.get_timesteps(time_range, now = dt.datetime(2024, 1, 6))
        dataset_result = dataset.get_timesteps(time_range, now = dt.datetime(2024, 1, 6))
        
        assert catalog_result == dataset_result


class TestEstimateTimestep:
    """Test suite for estimate_timestep method."""
    
    def test_estimate_timestep_daily(self, tmp_path):
        """Test estimate_timestep detects daily data."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create daily files
        for day in range(1, 10):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        timestep = dataset.estimate_timestep(now = dt.datetime(2024, 1, 31))
        
        assert timestep is not None
        assert timestep.unit == 'd'
    
    def test_estimate_timestep_uses_cache(self, tmp_path):
        """Test estimate_timestep returns cached value if available."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif",
            timestep='m'
        )
        
        # Even with no files, should return cached timestep
        timestep = dataset.estimate_timestep()
        
        assert timestep is not None
        assert timestep.unit == 'm'
    
    def test_estimate_timestep_with_sample(self, tmp_path):
        """Test estimate_timestep accepts date sample."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Provide explicit sample
        dates = [
            dt.datetime(2024, 1, 31),
            dt.datetime(2024, 2, 29),
            dt.datetime(2024, 3, 31),
        ]
        
        timestep = dataset.estimate_timestep(date_sample=dates)
        
        assert timestep is not None
        assert timestep.unit == 'm'
    
    def test_estimate_timestep_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.estimate_timestep delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 10):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        # Both should return same result
        catalog_result = dataset.catalog.estimate_timestep(now = dt.datetime(2024, 1, 31))
        dataset_result = dataset.estimate_timestep(now = dt.datetime(2024, 1, 31))
        
        assert catalog_result == dataset_result

class TestGetLastDate:
    """Test suite for get_last_date method."""
    
    def test_get_last_date_basic(self, tmp_path):
        """Test get_last_date finds most recent date."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create files
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240105.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        result = dataset.get_last_date()
        
        assert result == dt.datetime(2024, 1, 5)
    
    def test_get_last_date_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_last_date delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        now = dt.datetime(2024, 1, 31)
        catalog_result = dataset.catalog.get_last_date(now=now)
        dataset_result = dataset.get_last_date(now=now)
        
        assert catalog_result == dataset_result


class TestGetFirstTs:
    """Test suite for get_first_ts method."""
    
    def test_get_first_ts_basic(self, tmp_path):
        """Test get_first_ts returns earliest timestep."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        result = dataset.get_first_ts()
        
        assert result is not None
        assert isinstance(result, TimeStep)
    
    def test_get_first_ts_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_first_ts delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        catalog_result = dataset.catalog.get_first_ts()
        dataset_result = dataset.get_first_ts()
        
        assert catalog_result == dataset_result


class TestGetAnyDate:
    """Test suite for get_any_date method."""
    
    def test_get_any_date_basic(self, tmp_path):
        """Test get_any_date finds some available date."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240115.tif").touch()
        
        result = dataset.get_any_date()
        
        assert result == dt.datetime(2024, 1, 15)
    
    def test_get_any_date_returns_none_when_no_data(self, tmp_path):
        """Test get_any_date returns None when no data exists."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        result = dataset.get_any_date()
        
        assert result is None
    
    def test_get_any_date_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_any_date delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        
        catalog_result = dataset.catalog.get_any_date()
        dataset_result = dataset.get_any_date()
        
        assert catalog_result == dataset_result


class TestGetFirstDate:
    """Test suite for get_first_date method."""
    
    def test_get_first_date_basic(self, tmp_path):
        """Test get_first_date finds earliest date."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create files
        (tmp_path / "data_20240105.tif").touch()
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        result = dataset.get_first_date()
        
        assert result == dt.datetime(2024, 1, 1)
    
    def test_get_first_date_multiple(self, tmp_path):
        """Test get_first_date returns n earliest dates."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        result = dataset.get_first_date(n=3, )
        
        assert len(result) == 3
        assert result[0] == dt.datetime(2024, 1, 1)
        assert result[1] == dt.datetime(2024, 1, 2)
    
    def test_get_first_date_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_first_date delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        catalog_result = dataset.catalog.get_first_date()
        dataset_result = dataset.get_first_date()
        
        assert catalog_result == dataset_result


class TestGetLastTs:
    """Test suite for get_last_ts method."""
    
    def test_get_last_ts_basic(self, tmp_path):
        """Test get_last_ts returns most recent timestep."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        result = dataset.get_last_ts()
        
        assert result is not None
        assert isinstance(result, TimeStep)
    
    def test_get_last_ts_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_last_ts delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        now = dt.datetime(2024, 1, 31)
        catalog_result = dataset.catalog.get_last_ts(now=now)
        dataset_result = dataset.get_last_ts(now=now)
        
        assert catalog_result == dataset_result


class TestGetStart:
    """Test suite for get_start method."""
    
    def test_get_start_basic(self, tmp_path):
        """Test get_start returns earliest available time."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        result = dataset.get_start()
        
        assert result is not None
        assert isinstance(result, dt.datetime)
    
    def test_get_start_delegates_to_catalog(self, tmp_path):
        """Test that Dataset.get_start delegates to catalog."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        for day in range(1, 6):
            (tmp_path / f"data_202401{day:02d}.tif").touch()
        
        catalog_result = dataset.catalog.get_start()
        dataset_result = dataset.get_start()
        
        assert catalog_result == dataset_result

