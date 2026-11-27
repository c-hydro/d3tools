"""Comprehensive tests for time_parsing module."""

import datetime
import pytest
from d3tools.timestepping.time_parsing import get_date_from_str


class TestGetDateFromStr:
    """Test suite for get_date_from_str function."""

    def test_get_date_from_str_iso8601(self):
        """Test ISO 8601 format parsing."""
        assert get_date_from_str('2024-02-20') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('2024-12-31') == datetime.datetime(2024, 12, 31)
        assert get_date_from_str('2024-01-01') == datetime.datetime(2024, 1, 1)

    def test_get_date_from_str_compact(self):
        """Test compact format (YYYYMMDD) parsing."""
        assert get_date_from_str('20240220') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('20241231') == datetime.datetime(2024, 12, 31)
        assert get_date_from_str('20240101') == datetime.datetime(2024, 1, 1)

    def test_get_date_from_str_european(self):
        """Test European date format parsing."""
        assert get_date_from_str('20/02/2024') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('31/12/2024') == datetime.datetime(2024, 12, 31)
        assert get_date_from_str('01-02-2024') == datetime.datetime(2024, 2, 1)
        assert get_date_from_str('01.02.2024') == datetime.datetime(2024, 2, 1)

    def test_get_date_from_str_with_time(self):
        """Test date-time format parsing."""
        assert get_date_from_str('2024-02-20 13:45') == datetime.datetime(2024, 2, 20, 13, 45)
        assert get_date_from_str('2024-02-20 13:45:30') == datetime.datetime(2024, 2, 20, 13, 45, 30)
        assert get_date_from_str('2024-02-20 23') == datetime.datetime(2024, 2, 20, 23, 0)

    def test_get_date_from_str_month_names(self):
        """Test parsing with month names."""
        assert get_date_from_str('20 Feb 2024') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('20 February 2024') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('2024 Feb 20') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('2024 February 20') == datetime.datetime(2024, 2, 20)

    def test_get_date_from_str_end_of_day(self):
        """Test end-of-day adjustment."""
        dt = get_date_from_str('2024-02-20', end=True)
        assert dt.hour == 23 and dt.minute == 59 and dt.second == 59
        
        # Only missing components should be set to end
        dt = get_date_from_str('2024-02-20 13:45', end=True)
        assert dt.hour == 13 and dt.minute == 45 and dt.second == 59

        dt = get_date_from_str('2024-02-20 13', end=True)
        assert dt.hour == 13 and dt.minute == 59 and dt.second == 59

    def test_get_date_from_str_with_explicit_format(self):
        """Test parsing with explicit format specification."""
        dt = get_date_from_str('20/02/2024', format='%d/%m/%Y')
        assert dt == datetime.datetime(2024, 2, 20)
        
        dt = get_date_from_str('2024-Feb-20', format='%Y-%b-%d')
        assert dt == datetime.datetime(2024, 2, 20)

    def test_get_date_from_str_invalid(self):
        """Test that invalid strings raise ValueError."""
        with pytest.raises(ValueError, match='Cannot parse date string'):
            get_date_from_str('notadate')
        
        with pytest.raises(ValueError):
            get_date_from_str('2024-13-01')  # Invalid month
        
        with pytest.raises(ValueError):
            get_date_from_str('2024-02-30')  # Invalid day

    def test_get_date_from_str_leap_year(self):
        """Test leap year date parsing."""
        # Leap year
        assert get_date_from_str('2024-02-29') == datetime.datetime(2024, 2, 29)
        
        # Non-leap year should fail
        with pytest.raises(ValueError):
            get_date_from_str('2023-02-29')

    def test_get_date_from_str_edge_cases(self):
        """Test edge cases."""
        # First day of year
        assert get_date_from_str('2024-01-01') == datetime.datetime(2024, 1, 1)
        
        # Last day of year
        assert get_date_from_str('2024-12-31') == datetime.datetime(2024, 12, 31)
        
        # Midnight time
        assert get_date_from_str('2024-02-20 00:00:00') == datetime.datetime(2024, 2, 20, 0, 0, 0)
