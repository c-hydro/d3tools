"""Comprehensive tests for TimeRange class."""

import datetime
import pytest
from d3tools.timestepping import TimeRange, TimeWindow, Month, Year, Dekad, Day, Hour


class TestTimeRangeInit:
    """Test suite for TimeRange initialization."""

    def test_init_basic(self):
        """Test basic TimeRange initialization."""
        start = datetime.datetime(2024, 1, 1)
        end = datetime.datetime(2024, 12, 31)
        range_obj = TimeRange(start, end)
        
        assert range_obj.start == start
        assert range_obj.end == end

    def test_init_inherits_from_timeperiod(self):
        """Test that TimeRange has TimePeriod functionality."""
        range_obj = TimeRange('2024-01-01', '2024-12-31')
        assert range_obj.start == datetime.datetime(2024, 1, 1)
        assert range_obj.end == datetime.datetime(2024, 12, 31, 23, 59, 59)


class TestTimeRangeProperties:
    """Test suite for TimeRange timestep properties."""

    def test_months_property(self):
        """Test the months property."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))
        months = range_obj.months
        
        assert len(months) == 12
        assert all(isinstance(m, Month) for m in months)
        assert months[0].month == 1
        assert months[-1].month == 12

    def test_years_property(self):
        """Test the years property."""
        range_obj = TimeRange(datetime.datetime(2022, 1, 1), datetime.datetime(2024, 12, 31))
        years = range_obj.years
        
        assert len(years) == 3
        assert all(isinstance(y, Year) for y in years)
        assert years[0].year == 2022
        assert years[-1].year == 2024

    def test_dekads_property(self):
        """Test the dekads property."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        dekads = range_obj.dekads
        
        assert len(dekads) == 3  # 3 dekads in January
        assert all(isinstance(d, Dekad) for d in dekads)

    def test_days_property(self):
        """Test the days property."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 10))
        days = range_obj.days
        
        assert len(days) == 10
        assert all(isinstance(d, Day) for d in days)

    def test_hours_property(self):
        """Test the hours property."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1, 0), datetime.datetime(2024, 1, 1, 5))
        hours = range_obj.hours
        
        assert len(hours) >= 5
        assert all(isinstance(h, Hour) for h in hours)

    def test_viirstimes_property(self):
        """Test the viirstimes property."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 2, 28))
        viirs = range_obj.viirstimes
        
        assert len(viirs) > 0
        # VIIRS periods start every 8 days


class TestTimeRangeGetTimesteps:
    """Test suite for TimeRange.get_timesteps method."""

    def test_get_timesteps_by_string(self):
        """Test getting timesteps using string frequency."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))
        
        # Monthly
        timesteps = range_obj.get_timesteps('m')
        assert len(timesteps) == 12
        assert all(isinstance(ts, Month) for ts in timesteps)
        
        # Yearly
        timesteps = range_obj.get_timesteps('y')
        assert len(timesteps) == 1
        assert isinstance(timesteps[0], Year)
        
        # Dekadly
        timesteps = range_obj.get_timesteps('t')
        assert len(timesteps) == 36

    def test_get_timesteps_by_int(self):
        """Test getting timesteps using integer (timesteps per year)."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))
        
        # 12 timesteps per year (monthly)
        timesteps = range_obj.get_timesteps(12)
        assert len(timesteps) == 12
        
        # 1 timestep per year (yearly)
        timesteps = range_obj.get_timesteps(1)
        assert len(timesteps) == 1

    def test_get_timesteps_with_agg(self):
        """Test getting timesteps with aggregation window."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 3, 31))
        
        timesteps = range_obj.get_timesteps('m', agg='7d')
        assert len(timesteps) == 3
        for ts in timesteps:
            assert ts.agg_window is not None
            assert ts.agg_window.size == 7
            assert ts.agg_window.unit == 'd'

    def test_get_timesteps_unsupported_frequency_raises_error(self):
        """Test that unsupported frequency raises ValueError."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))
        
        with pytest.raises(ValueError, match='not recognized'):
            range_obj.get_timesteps('invalid')
        
        with pytest.raises(TypeError):
            range_obj.get_timesteps(3.5)


class TestTimeRangeGetTimestepsLike:
    """Test suite for TimeRange.get_timesteps_like method."""

    def test_get_timesteps_like(self):
        """Test getting timesteps like a given timestep."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))
        
        # Create a sample month timestep
        sample = Month(2023, 5)
        
        timesteps = range_obj.get_timesteps_like(sample)
        assert len(timesteps) == 12
        assert all(isinstance(ts, Month) for ts in timesteps)

    def test_get_timesteps_like_with_agg(self):
        """Test get_timesteps_like preserves aggregation window."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 3, 31))
        
        sample = Month(2023, 5)
        sample.agg_window = TimeWindow(7, 'd')
        
        timesteps = range_obj.get_timesteps_like(sample)
        for ts in timesteps:
            assert ts.agg_window is not None


class TestTimeRangeGenTimesteps:
    """Test suite for TimeRange generator methods."""

    def test_gen_timesteps_from_tsnumber(self):
        """Test generator for timesteps by number."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 3, 31))
        
        count = 0
        for ts in range_obj.gen_timesteps_from_tsnumber(12):
            count += 1
            assert isinstance(ts, Month)
        
        assert count == 3  # 3 months

    def test_gen_timesteps_from_DOY(self):
        """Test generator for timesteps from DOY list."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31))
        
        # Every 8 days starting from DOY 1
        doy_list = range(1, 366, 8)
        count = 0
        
        for ts in range_obj.gen_timesteps_from_DOY(doy_list):
            count += 1
        
        assert count > 0

    def test_gen_timesteps_from_issue_hour(self):
        """Test generator for timesteps from issue hours."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2))
        
        issue_hours = [0, 6, 12, 18]
        timesteps = list(range_obj.gen_timesteps_from_issue_hour(issue_hours))
        
        # Should have 4 timesteps per day
        assert len(timesteps) >= 4


class TestTimeRangeExtend:
    """Test suite for TimeRange.extend method."""

    def test_extend_returns_timerange(self):
        """Test that extend returns TimeRange, not TimePeriod."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 1, 20))
        window = TimeWindow(5, 'd')
        
        extended = range_obj.extend(window)
        assert isinstance(extended, TimeRange)
        assert extended.start == range_obj.start

    def test_extend_before_returns_timerange(self):
        """Test extend before returns TimeRange."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 1, 20))
        window = TimeWindow(5, 'd')
        
        extended = range_obj.extend(window, before=True)
        assert isinstance(extended, TimeRange)
        assert extended.end == range_obj.end


class TestTimeRangeEdgeCases:
    """Test suite for TimeRange edge cases."""

    def test_single_day_range(self):
        """Test range containing single day."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 1))
        
        days = range_obj.days
        assert len(days) == 1

    def test_leap_year_range(self):
        """Test range in leap year."""
        range_obj = TimeRange(datetime.datetime(2024, 2, 1), datetime.datetime(2024, 2, 29))
        
        days = range_obj.days
        assert len(days) == 29  # 29 days in Feb 2024

    def test_year_crossing_range(self):
        """Test range that crosses year boundary."""
        range_obj = TimeRange(datetime.datetime(2023, 12, 1), datetime.datetime(2024, 1, 31))
        
        months = range_obj.months
        assert len(months) == 2
        assert months[0].year == 2023
        assert months[1].year == 2024

    def test_partial_month_range(self):
        """Test range that doesn't cover full months."""
        range_obj = TimeRange(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 2, 15))
        
        months = range_obj.months
        # Should include Jan and Feb
        assert len(months) >= 1

    def test_very_short_range(self):
        """Test range of just a few hours."""
        range_obj = TimeRange(
            datetime.datetime(2024, 1, 1, 10),
            datetime.datetime(2024, 1, 1, 15)
        )
        
        hours = range_obj.hours
        assert len(hours) >= 5


class TestTimeRangeMultiYear:
    """Test suite for multi-year ranges."""

    def test_multi_year_months(self):
        """Test getting months across multiple years."""
        range_obj = TimeRange(datetime.datetime(2022, 1, 1), datetime.datetime(2024, 12, 31))
        
        months = range_obj.months
        assert len(months) == 36  # 3 years * 12 months

    def test_multi_year_years(self):
        """Test getting years across range."""
        range_obj = TimeRange(datetime.datetime(2020, 1, 1), datetime.datetime(2024, 12, 31))
        
        years = range_obj.years
        assert len(years) == 5

    def test_multi_year_dekads(self):
        """Test getting dekads across multiple years."""
        range_obj = TimeRange(datetime.datetime(2023, 1, 1), datetime.datetime(2024, 12, 31))
        
        dekads = range_obj.dekads
        assert len(dekads) == 72  # 2 years * 36 dekads
