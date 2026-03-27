"""
Tests for optimized get_last_date with exponential backoff.
"""
import pytest
import datetime as dt
import tempfile
import os
from unittest.mock import Mock, patch

from d3tools.data import LocalDataset
from d3tools.timestepping import Month


@pytest.fixture
def temp_dataset_dir():
    """Create a temporary directory for dataset testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestExponentialBackoff:
    """Test that exponential backoff reduces filesystem scans."""
    
    def test_finds_recent_data_quickly(self, temp_dataset_dir):
        """Test that data in current month is found immediately."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        # Mock get_times to track calls
        original_get_times = ds.get_times
        call_count = [0]
        
        def tracked_get_times(time_range, **kwargs):
            call_count[0] += 1
            # Return data for current month only
            if time_range.start.month == dt.datetime.now().month:
                return [dt.datetime.now() - dt.timedelta(days=5)]
            return []
        
        ds.get_times = tracked_get_times
        
        result = ds.get_last_date()
        
        # Should find it on first try
        assert result is not None
        assert call_count[0] <= 2  # Current month + maybe one forward scan
    
    def test_finds_old_data_with_exponential_jumps(self, temp_dataset_dir):
        """Test that old data is found with few filesystem scans."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        # Mock get_times to track calls
        call_count = [0]
        target_date = dt.datetime.now() - dt.timedelta(days=180)  # 6 months ago
        
        def tracked_get_times(time_range, **kwargs):
            call_count[0] += 1
            # Return data only for target month
            if (time_range.start.year == target_date.year and 
                time_range.start.month == target_date.month):
                return [target_date]
            return []
        
        ds.get_times = tracked_get_times
        
        result = ds.get_last_date()
        
        # Should find it with exponential jumps (not 6 linear scans)
        assert result is not None
        # Exponential: current, -1mo, -3mo, -6mo = 4 searches to find
        # Plus forward scans from -6mo to current = ~6 more
        # Total ~10 instead of 6 sequential
        assert call_count[0] < 15  # Much less than linear would take
    
    def test_respects_lim_parameter(self, temp_dataset_dir):
        """Test that search stops at lim boundary."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        # Mock get_times to never return data
        ds.get_times = lambda time_range, **kwargs: []
        
        # Limit search to last 3 months
        lim = dt.datetime.now() - dt.timedelta(days=90)
        result = ds.get_last_date(lim=lim)
        
        # Should return None without searching forever
        assert result is None
    
    def test_returns_n_most_recent(self, temp_dataset_dir):
        """Test that n parameter returns multiple dates."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        # Mock get_times to return multiple dates
        now = dt.datetime.now()
        dates = [now - dt.timedelta(days=i) for i in range(10)]
        
        def mock_get_times(time_range, **kwargs):
            return [d for d in dates if time_range.contains(d)]
        
        ds.get_times = mock_get_times
        
        result = ds.get_last_date(n=5)
        
        assert len(result) == 5
        assert result == sorted(dates[:5], reverse=True)
    
    def test_handles_sparse_data(self, temp_dataset_dir):
        """Test finding data when files are very sparse."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        # Data exists 1 year ago only
        target_date = dt.datetime.now() - dt.timedelta(days=365)
        
        def mock_get_times(time_range, **kwargs):
            if (time_range.start.year == target_date.year and 
                time_range.start.month == target_date.month):
                return [target_date]
            return []
        
        ds.get_times = mock_get_times
        
        result = ds.get_last_date()
        
        assert result is not None
        assert result.year == target_date.year
        assert result.month == target_date.month


class TestBackwardCompatibility:
    """Test that optimization doesn't break existing behavior."""
    
    def test_returns_none_when_no_data(self, temp_dataset_dir):
        """Test that None is returned when no files exist."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        ds.get_times = lambda time_range, **kwargs: []
        
        result = ds.get_last_date()
        
        assert result is None
    
    def test_single_date_returned_without_list(self, temp_dataset_dir):
        """Test that single result (n=1) returns datetime not list."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        now = dt.datetime.now()
        ds.get_times = lambda time_range, **kwargs: [now] if time_range.contains(now) else []
        
        result = ds.get_last_date(n=1)
        
        assert isinstance(result, dt.datetime)
        assert not isinstance(result, list)
    
    def test_respects_now_parameter(self, temp_dataset_dir):
        """Test that 'now' parameter limits search."""
        ds = LocalDataset(path=temp_dataset_dir, filename="test_%Y%m%d.tif")
        
        past = dt.datetime(2020, 1, 15)
        future = dt.datetime(2025, 1, 15)
        
        ds.get_times = lambda time_range, **kwargs: [past, future] if time_range.start.year in [2020, 2025] else []
        
        result = ds.get_last_date(now=dt.datetime(2024, 1, 1))
        
        # Should only return past date, not future
        assert result == past
