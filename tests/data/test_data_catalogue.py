"""
Tests for DataCatalogue class.

Tests the data catalogue manager that handles discovery, enumeration,
and validation of available dataset files.
"""
import pytest
import datetime as dt
from d3tools.data import LocalDataset
from d3tools.data.data_catalogue import DataCatalogue
from d3tools.timestepping import TimeRange, TimeStep


class TestDataCatalogueInitialization:
    """Test DataCatalogue initialization and integration with Dataset."""
    
    def test_catalogue_created_on_dataset_init(self, tmp_path):
        """Test that catalogue is automatically created when Dataset is initialized."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file='test.tif'
        )
        
        assert hasattr(dataset, 'catalogue')
        assert isinstance(dataset.catalogue, DataCatalogue)
    
    def test_catalogue_has_dataset_reference(self, tmp_path):
        """Test that catalogue has reference to parent dataset."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file='test.tif'
        )
        
        assert dataset.catalogue.dataset is dataset
    
    def test_catalogue_repr(self, tmp_path):
        """Test catalogue string representation."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file='test_data.tif'
        )
        
        assert repr(dataset.catalogue) == "DataCatalogue(test_data)"
    
    def test_catalogue_created_for_different_dataset_types(self, tmp_path):
        """Test that catalogue is created for all dataset types."""
        # LocalDataset
        local_ds = LocalDataset(path=str(tmp_path), file='test.tif')
        assert hasattr(local_ds, 'catalogue')
        
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
        prefix = dataset.catalogue.get_prefix(time=time)
        
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
        prefix = dataset.catalogue.get_prefix(time=time_range)
        
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
        prefix = dataset.catalogue.get_prefix(time=time_range)
        
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
        prefix = dataset.catalogue.get_prefix(time=time_range)
        
        # Should only resolve year (not month, since they differ)
        assert prefix == str(tmp_path / '2021')
    
    def test_get_prefix_strips_unresolved_patterns(self, tmp_path):
        """Test that unresolved date patterns are stripped from prefix."""
        dataset = LocalDataset(
            path=str(tmp_path / '%Y' / '%m'),
            file='data_%Y%m%d.tif'
        )
        
        # No time provided - patterns should be stripped
        prefix = dataset.catalogue.get_prefix()
        
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
        prefix = dataset.catalogue.get_prefix(time=time, region='EU')
        
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
        catalog_result = dataset.catalogue.get_prefix(time=time)
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
        
        keys = dataset.catalogue.get_available_keys()
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
        
        keys = dataset.catalogue.get_available_keys()
        
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
        keys = dataset.catalogue.get_available_keys(time=time_range)
        
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
        keys = dataset.catalogue.get_available_keys(time=dt.datetime(2021, 1, 1))
        
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
        
        keys = dataset.catalogue.get_available_keys()
        
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
        keys = dataset.catalogue.get_available_keys(time=time_range)
        
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
        keys = dataset.catalogue.get_available_keys(region='EU')
        
        assert len(keys) == 2
        assert all('EU' in key for key in keys)
        assert not any('US' in key for key in keys)
        
        # Query different region
        keys_us = dataset.catalogue.get_available_keys(region='US')
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
        catalog_result = dataset.catalogue.get_available_keys()
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
        catalog_result = dataset.catalogue.get_times(time_range)
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
        catalog_result = dataset.catalogue.get_timesteps(time_range, now = dt.datetime(2024, 1, 6))
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
        catalog_result = dataset.catalogue.estimate_timestep(now = dt.datetime(2024, 1, 31))
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
        catalog_result = dataset.catalogue.get_last_date(now=now)
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
        
        catalog_result = dataset.catalogue.get_first_ts()
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
        
        
        catalog_result = dataset.catalogue.get_any_date()
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
        
        catalog_result = dataset.catalogue.get_first_date()
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
        catalog_result = dataset.catalogue.get_last_ts(now=now)
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
        
        catalog_result = dataset.catalogue.get_start()
        dataset_result = dataset.get_start()
        
        assert catalog_result == dataset_result


class TestCheckData:
    """Test suite for check_data method."""
    
    def test_check_data_file_exists(self, tmp_path):
        """Test check_data returns True when file exists."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        result = dataset.check_data(dt.datetime(2024, 1, 1))
        assert result is True
    
    def test_check_data_file_missing(self, tmp_path):
        """Test check_data returns False when file doesn't exist."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        result = dataset.check_data(dt.datetime(2024, 1, 1))
        assert result is False
    
    def test_check_data_with_tile(self, tmp_path):
        """Test check_data with specific tile."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        
        (tmp_path / "data_20240101_h18v04.tif").touch()
        
        # Specific tile exists
        assert dataset.check_data(dt.datetime(2024, 1, 1), tile='h18v04') is True
        
        # Other tile doesn't exist
        assert dataset.check_data(dt.datetime(2024, 1, 1), tile='h19v04') is False
    
    def test_check_data_all_tiles(self, tmp_path):
        """Test check_data without tile checks all tiles."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        # Only one tile exists
        (tmp_path / "data_20240101_h18v04.tif").touch()
        
        # Should return False since not all tiles exist
        result = dataset.check_data(dt.datetime(2024, 1, 1))
        assert result is False
        
        # Create second tile
        (tmp_path / "data_20240101_h19v04.tif").touch()
        
        # Now should return True
        result = dataset.check_data(dt.datetime(2024, 1, 1))
        assert result is True
    
    def test_check_data_with_versioned_file(self, tmp_path):
        """Test check_data selects latest version when not specified."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{file_version}.tif"
        )
        
        (tmp_path / "data_20240101_v1.tif").touch()
        (tmp_path / "data_20240101_v2.tif").touch()
        (tmp_path / "data_20240101_v3.tif").touch()
        
        # Should check for latest version (v3)
        result = dataset.check_data(dt.datetime(2024, 1, 1))
        assert result is True
    
    def test_check_data_with_specific_version(self, tmp_path):
        """Test check_data with explicit version specification."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{file_version}.tif"
        )
        
        (tmp_path / "data_20240101_v2.tif").touch()
        
        # Check for specific version
        assert dataset.check_data(dt.datetime(2024, 1, 1), file_version='v2') is True
        assert dataset.check_data(dt.datetime(2024, 1, 1), file_version='v1') is False
    
    def test_check_data_delegates_to_catalogue(self, tmp_path):
        """Test that Dataset.check_data delegates to catalogue."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        # Both should return same result
        catalogue_result = dataset.catalogue.check_data(dt.datetime(2024, 1, 1))
        dataset_result = dataset.check_data(dt.datetime(2024, 1, 1))
        
        assert catalogue_result == dataset_result


class TestFindTimes:
    """Test suite for find_times method."""
    
    def test_find_times_filters_available(self, tmp_path):
        """Test find_times returns only times that exist."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # Create some files
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        (tmp_path / "data_20240105.tif").touch()
        
        # Query for range including missing dates
        times_to_check = [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),  # Missing
            dt.datetime(2024, 1, 3),
            dt.datetime(2024, 1, 4),  # Missing
            dt.datetime(2024, 1, 5),
        ]
        
        result = dataset.find_times(times_to_check)
        
        assert len(result) == 3
        assert dt.datetime(2024, 1, 1) in result
        assert dt.datetime(2024, 1, 3) in result
        assert dt.datetime(2024, 1, 5) in result
        assert dt.datetime(2024, 1, 2) not in result
        assert dt.datetime(2024, 1, 4) not in result
    
    def test_find_times_returns_indices(self, tmp_path):
        """Test find_times can return indices instead of times."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        times_to_check = [
            dt.datetime(2024, 1, 1),  # Index 0
            dt.datetime(2024, 1, 2),  # Index 1 - missing
            dt.datetime(2024, 1, 3),  # Index 2
        ]
        
        result = dataset.find_times(times_to_check, id=True)
        
        assert result == [0, 2]
    
    def test_find_times_reverse_filter(self, tmp_path):
        """Test find_times with rev=True returns missing times."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        times_to_check = [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),
            dt.datetime(2024, 1, 3),
        ]
        
        result = dataset.find_times(times_to_check, rev=True)
        
        # Should return only the missing time
        assert len(result) == 1
        assert dt.datetime(2024, 1, 2) in result
    
    def test_find_times_reverse_with_indices(self, tmp_path):
        """Test find_times with both rev=True and id=True."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        times_to_check = [
            dt.datetime(2024, 1, 1),  # Index 0 - exists
            dt.datetime(2024, 1, 2),  # Index 1 - missing
            dt.datetime(2024, 1, 3),  # Index 2 - missing
        ]
        
        result = dataset.find_times(times_to_check, id=True, rev=True)
        
        # Should return indices of missing times
        assert result == [1, 2]
    
    def test_find_times_with_tags(self, tmp_path):
        """Test find_times with tag filters."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        
        (tmp_path / "data_20240101_h18v04.tif").touch()
        (tmp_path / "data_20240102_h18v04.tif").touch()
        # 20240103 missing for h18v04
        
        times_to_check = [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),
            dt.datetime(2024, 1, 3),
        ]
        
        result = dataset.find_times(times_to_check, tile='h18v04')
        
        assert len(result) == 2
        assert dt.datetime(2024, 1, 1) in result
        assert dt.datetime(2024, 1, 2) in result
        assert dt.datetime(2024, 1, 3) not in result
    
    def test_find_times_empty_list(self, tmp_path):
        """Test find_times with empty input list."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        result = dataset.find_times([])
        assert result == []
    
    def test_find_times_all_missing(self, tmp_path):
        """Test find_times when all times are missing."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        # No files created
        times_to_check = [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),
        ]
        
        result = dataset.find_times(times_to_check)
        assert result == []
    
    def test_find_times_delegates_to_catalogue(self, tmp_path):
        """Test that Dataset.find_times delegates to catalogue."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        
        times_to_check = [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)]
        
        # Both should return same result
        catalogue_result = dataset.catalogue.find_times(times_to_check)
        dataset_result = dataset.find_times(times_to_check)
        
        assert catalogue_result == dataset_result


class TestFindTiles:
    """Test suite for find_tiles method."""
    
    def test_find_tiles_returns_available(self, tmp_path):
        """Test find_tiles returns only tiles that exist."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04', 'h20v04']
        
        # Create files for only some tiles
        (tmp_path / "data_20240101_h18v04.tif").touch()
        (tmp_path / "data_20240101_h20v04.tif").touch()
        
        result = dataset.find_tiles(dt.datetime(2024, 1, 1))
        
        assert len(result) == 2
        assert 'h18v04' in result
        assert 'h20v04' in result
        assert 'h19v04' not in result
    
    def test_find_tiles_reverse_filter(self, tmp_path):
        """Test find_tiles with rev=True returns missing tiles."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04', 'h20v04']
        
        # Create files for only some tiles
        (tmp_path / "data_20240101_h18v04.tif").touch()
        
        result = dataset.find_tiles(dt.datetime(2024, 1, 1), rev=True)
        
        # Should return missing tiles
        assert len(result) == 2
        assert 'h19v04' in result
        assert 'h20v04' in result
        assert 'h18v04' not in result
    
    def test_find_tiles_all_available(self, tmp_path):
        """Test find_tiles when all tiles exist."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        (tmp_path / "data_20240101_h18v04.tif").touch()
        (tmp_path / "data_20240101_h19v04.tif").touch()
        
        result = dataset.find_tiles(dt.datetime(2024, 1, 1))
        
        assert len(result) == 2
        assert 'h18v04' in result
        assert 'h19v04' in result
    
    def test_find_tiles_none_available(self, tmp_path):
        """Test find_tiles when no tiles exist."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        # No files created
        result = dataset.find_tiles(dt.datetime(2024, 1, 1))
        
        assert result == []
    
    def test_find_tiles_with_additional_tags(self, tmp_path):
        """Test find_tiles with additional tag filters."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}_{variable}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        # Create files for specific variable
        (tmp_path / "data_20240101_h18v04_temp.tif").touch()
        (tmp_path / "data_20240101_h19v04_precip.tif").touch()
        
        # Query for specific variable
        result = dataset.find_tiles(dt.datetime(2024, 1, 1), variable='temp')
        
        assert len(result) == 1
        assert 'h18v04' in result
        assert 'h19v04' not in result
    
    def test_find_tiles_without_time(self, tmp_path):
        """Test find_tiles can work without specific time."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        (tmp_path / "data_h18v04.tif").touch()
        
        result = dataset.find_tiles()
        
        assert len(result) == 1
        assert 'h18v04' in result
    
    def test_find_tiles_delegates_to_catalogue(self, tmp_path):
        """Test that Dataset.find_tiles delegates to catalogue."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04']
        
        (tmp_path / "data_20240101_h18v04.tif").touch()
        
        # Both should return same result
        catalogue_result = dataset.catalogue.find_tiles(dt.datetime(2024, 1, 1))
        dataset_result = dataset.find_tiles(dt.datetime(2024, 1, 1))
        
        assert catalogue_result == dataset_result


class TestCatalogueWithCases:
    """Integration tests for @withcases decorator on catalogue methods."""
    
    def test_find_times_with_cases(self, tmp_path):
        """Test find_times expands cases correctly for multiple tiles."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        # Create files for different tiles
        (tmp_path / "data_20240101_h18v04.tif").touch()
        (tmp_path / "data_20240102_h18v04.tif").touch()
        (tmp_path / "data_20240101_h19v04.tif").touch()
        # 20240102 missing for h19v04
        
        times_to_check = [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),
        ]
        
        # Define cases for different tiles
        cases = [
            {'tags': {'tile': 'h18v04'}},
            {'tags': {'tile': 'h19v04'}},
        ]
        
        # Use cases to check times for both tiles
        result = dataset.find_times(times_to_check, cases=cases)
        
        # Should return list of results, one per case
        assert isinstance(result, list)
        assert len(result) == 2
        
        # h18v04 has both times
        assert len(result[0]) == 2
        
        # h19v04 has only first time
        assert len(result[1]) == 1
        assert result[1][0] == dt.datetime(2024, 1, 1)
    
    def test_check_data_with_cases(self, tmp_path):
        """Test check_data expands cases correctly."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{tile}.tif"
        )
        
        (tmp_path / "data_20240101_h18v04.tif").touch()
        # h19v04 missing
        
        cases = [
            {'tags': {'tile': 'h18v04'}},
            {'tags': {'tile': 'h19v04'}},
        ]
        
        result = dataset.check_data(dt.datetime(2024, 1, 1), cases=cases)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] is True   # h18v04 exists
        assert result[1] is False  # h19v04 missing
    
    def test_find_tiles_with_cases(self, tmp_path):
        """Test find_tiles expands cases correctly for different times."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{variable}_{tile}.tif"
        )
        dataset._tile_names = ['h18v04', 'h19v04']
        
        # Different tiles available for different dates/variables
        (tmp_path / "data_20240101_temp_h18v04.tif").touch()
        (tmp_path / "data_20240101_temp_h19v04.tif").touch()
        (tmp_path / "data_20240102_temp_h18v04.tif").touch()
        (tmp_path / "data_20240102_prec_h19v04.tif").touch()
        (tmp_path / "data_20240102_prec_h18v04.tif").touch()
        # h19v04 missing for 20240102
        
        # Use cases to check different times
        cases = [
            {'tags': {'variable': 'temp'}},
            {'tags': {'variable': 'prec'}},
        ]
        
        # Check tiles for Jan 1 (both exist)
        result_jan1 = dataset.find_tiles(dt.datetime(2024, 1, 1), cases=cases)
        assert 'h18v04' in result_jan1[0]
        assert 'h19v04' in result_jan1[0]
        assert len(result_jan1[1]) == 0  # No prec tiles on Jan 1
        
        # Check tiles for Jan 2 (only h18v04 exists)
        result_jan2 = dataset.find_tiles(dt.datetime(2024, 1, 2), cases=cases)
        assert 'h18v04' in result_jan2[0]
        assert len(result_jan2[1]) == 2  # both tiles for prec on Jan 2
    
    def test_get_times_with_cases(self, tmp_path):
        """Test get_times expands cases correctly for different tags."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{variable}.tif"
        )
        
        # Create files for different variables
        (tmp_path / "data_20240101_temp.tif").touch()
        (tmp_path / "data_20240102_temp.tif").touch()
        (tmp_path / "data_20240101_prec.tif").touch()
        # precip missing for 20240102
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2))
        
        cases = [
            {'tags': {'variable': 'temp'}},
            {'tags': {'variable': 'prec'}},
        ]
        
        result = dataset.get_times(time_range, cases=cases)
        
        assert isinstance(result, list)
        assert len(result) == 2
        
        # temp has both times
        assert len(result[0]) == 2
        
        # precip has only first time
        assert len(result[1]) == 1
    
    def test_get_timesteps_with_cases(self, tmp_path):
        """Test get_timesteps expands cases correctly."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d_{variable}.tif"
        )
        
        # Create files for different variables
        for day in range(1, 4):
            (tmp_path / f"data_202401{day:02d}_temp.tif").touch()
            (tmp_path / f"data_202401{day:02d}_prec.tif").touch()
        
        time_range = TimeRange(dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 3))
        
        cases = [
            {'tags': {'variable': 'temp'}},
            {'tags': {'variable': 'prec'}},
        ]
        
        result = dataset.get_timesteps(time_range, cases=cases, now=dt.datetime(2024, 1, 10))
        
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Both variables should have timesteps
        assert all(len(ts_list) > 0 for ts_list in result)
        assert all(all(isinstance(ts, TimeStep) for ts in ts_list) for ts_list in result)
    
    def test_cases_none_behaves_normally(self, tmp_path):
        """Test that cases=None doesn't change method behavior."""
        dataset = LocalDataset(
            path=str(tmp_path),
            file="data_%Y%m%d.tif"
        )
        
        (tmp_path / "data_20240101.tif").touch()
        (tmp_path / "data_20240103.tif").touch()
        
        times_to_check = [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),
            dt.datetime(2024, 1, 3),
        ]
        
        # With cases=None should behave like normal call
        result = dataset.find_times(times_to_check, cases=None)
        
        # Should return list of times (not list of lists)
        assert isinstance(result, list)
        assert len(result) == 2
        assert dt.datetime(2024, 1, 1) in result
        assert dt.datetime(2024, 1, 3) in result
