"""Comprehensive tests for FixedLenTimeStep classes (Day, Hour)."""

import datetime
import pytest
from d3tools.timestepping import Day, Hour


class TestDayInit:
    """Test suite for Day initialization."""

    def test_day_init(self):
        """Test basic Day initialization."""
        day = Day(2024, 1)
        assert day.year == 2024
        assert day.step == 1
        assert day.day_of_year == 1

    def test_day_various_doy(self):
        """Test days at various day-of-year positions."""
        # First day
        d1 = Day(2024, 1)
        assert d1.day_of_year == 1
        
        # Middle of year
        d180 = Day(2024, 180)
        assert d180.day_of_year == 180
        
        # Last day of leap year
        d366 = Day(2024, 366)
        assert d366.day_of_year == 366


class TestDayFromDate:
    """Test suite for Day.from_date method."""

    def test_from_date_jan_1(self):
        """Test creating day from January 1st."""
        day = Day.from_date(datetime.datetime(2024, 1, 1))
        assert day.year == 2024
        assert day.day_of_year == 1

    def test_from_date_various_dates(self):
        """Test from_date with various dates."""
        # February 29 (leap year DOY 60)
        day = Day.from_date(datetime.datetime(2024, 2, 29))
        assert day.day_of_year == 60
        
        # July 4
        day = Day.from_date(datetime.datetime(2024, 7, 4))
        assert day.day_of_year == 186  # mid-year

    def test_from_date_dec_31(self):
        """Test creating day from December 31st."""
        # Leap year
        day = Day.from_date(datetime.datetime(2024, 12, 31))
        assert day.day_of_year == 366
        
        # Non-leap year
        day = Day.from_date(datetime.datetime(2023, 12, 31))
        assert day.day_of_year == 365


class TestDayDates:
    """Test suite for Day start and end dates."""

    def test_get_start_first_day(self):
        """Test start datetime of first day."""
        day = Day(2024, 1)
        assert day.start == datetime.datetime(2024, 1, 1, 0, 0, 0)

    def test_get_start_middle_day(self):
        """Test start datetime of a middle day."""
        day = Day(2024, 100)
        expected = datetime.datetime(2024, 1, 1) + datetime.timedelta(days=99)
        assert day.start == expected

    def test_get_end_includes_full_day(self):
        """Test that end datetime includes the full day."""
        day = Day(2024, 1)
        # End should be 23:59:59
        assert day.end.hour == 23
        assert day.end.minute == 59
        assert day.end.second == 59

    def test_get_end_day_100(self):
        """Test end datetime of day 100."""
        day = Day(2024, 100)
        # Should end at 23:59:59 of that day
        assert day.end.hour == 23
        assert day.end.minute == 59


class TestDayProperties:
    """Test suite for Day properties."""

    def test_month_property(self):
        """Test month property."""
        # January
        d1 = Day(2024, 1)
        assert d1.month == 1
        
        # February
        d32 = Day(2024, 32)  # Feb 1
        assert d32.month == 2
        
        # December
        d365 = Day(2024, 365)
        assert d365.month == 12

    def test_day_of_month_property(self):
        """Test day_of_month property."""
        # January 1
        d1 = Day(2024, 1)
        assert d1.day_of_month == 1
        
        # January 31
        d31 = Day(2024, 31)
        assert d31.day_of_month == 31
        
        # February 1
        d32 = Day(2024, 32)
        assert d32.day_of_month == 1


class TestDayArithmetic:
    """Test suite for Day arithmetic operations."""

    def test_add_one(self):
        """Test adding one day."""
        d1 = Day(2024, 100)
        d2 = d1 + 1
        
        assert d2.day_of_year == 101
        assert d2.year == 2024

    def test_add_multiple(self):
        """Test adding multiple days."""
        d1 = Day(2024, 1)
        d2 = d1 + 30
        
        assert d2.day_of_year == 31

    def test_add_crossing_year(self):
        """Test addition crossing year boundary."""
        d1 = Day(2024, 365)
        d2 = d1 + 5
        
        assert d2.year == 2025
        assert d2.day_of_year == 4

    def test_subtract(self):
        """Test subtracting days."""
        d1 = Day(2024, 100)
        d2 = d1 - 10
        
        assert d2.day_of_year == 90
        assert d2.year == 2024

    def test_subtract_crossing_year(self):
        """Test subtraction crossing year boundary."""
        d1 = Day(2024, 5)
        d2 = d1 - 10
        
        assert d2.year == 2023


class TestDayLength:
    """Test suite for Day length."""

    def test_length_property(self):
        """Test that Day has length of 1."""
        day = Day(2024, 100)
        assert day.length == 1
        assert Day.length == 1


class TestHourInit:
    """Test suite for Hour initialization."""

    def test_hour_init(self):
        """Test basic Hour initialization."""
        hour = Hour(2024, 1)
        assert hour.year == 2024
        assert hour.step == 1
        assert hour.hour_of_year == 1

    def test_hour_various_positions(self):
        """Test hours at various positions."""
        # First hour
        h1 = Hour(2024, 1)
        assert h1.hour_of_year == 1
        
        # 100th hour
        h100 = Hour(2024, 100)
        assert h100.hour_of_year == 100


class TestHourFromDate:
    """Test suite for Hour.from_date method."""

    def test_from_date_midnight(self):
        """Test creating hour from midnight."""
        hour = Hour.from_date(datetime.datetime(2024, 1, 1, 0))
        assert hour.year == 2024
        assert hour.hour_of_year == 1

    def test_from_date_various_times(self):
        """Test from_date with various times."""
        # Jan 1, 13:00
        hour = Hour.from_date(datetime.datetime(2024, 1, 1, 13))
        assert hour.hour_of_year == 14  # 0-12 is 13 hours, so 14th hour
        
        # Jan 2, 00:00
        hour = Hour.from_date(datetime.datetime(2024, 1, 2, 0))
        assert hour.hour_of_year == 25  # Day 2, hour 0 = 25th hour


class TestHourDates:
    """Test suite for Hour start and end dates."""

    def test_get_start_first_hour(self):
        """Test start datetime of first hour."""
        hour = Hour(2024, 1)
        assert hour.start == datetime.datetime(2024, 1, 1, 0, 0, 0)

    def test_get_start_various_hours(self):
        """Test start datetime of various hours."""
        # Hour 25 (2nd day, 1st hour)
        hour = Hour(2024, 25)
        assert hour.start == datetime.datetime(2024, 1, 2, 0, 0, 0)

    def test_get_end_includes_full_hour(self):
        """Test that end datetime includes the full hour."""
        hour = Hour(2024, 1)
        # End should be 00:59:59
        assert hour.end.minute == 59
        assert hour.end.second == 59


class TestHourProperties:
    """Test suite for Hour properties."""

    def test_day_of_year_property(self):
        """Test day_of_year property."""
        # First day, first hour
        h1 = Hour(2024, 1)
        assert h1.day_of_year == 1
        
        # Second day, first hour (hour 25)
        h25 = Hour(2024, 25)
        assert h25.day_of_year == 2


class TestHourArithmetic:
    """Test suite for Hour arithmetic operations."""

    def test_add_one(self):
        """Test adding one hour."""
        h1 = Hour(2024, 100)
        h2 = h1 + 1
        
        assert h2.hour_of_year == 101
        assert h2.year == 2024

    def test_add_crossing_day(self):
        """Test addition crossing day boundary."""
        h1 = Hour(2024, 23)  # 23rd hour (day 1, hour 22)
        h2 = h1 + 2
        
        assert h2.hour_of_year == 25
        assert h2.day_of_year == 2

    def test_add_crossing_year(self):
        """Test addition crossing year boundary."""
        # Last hour of leap year (8784th hour)
        h1 = Hour(2024, 8784)
        h2 = h1 + 1
        
        assert h2.year == 2025
        assert h2.hour_of_year == 1

    def test_subtract(self):
        """Test subtracting hours."""
        h1 = Hour(2024, 100)
        h2 = h1 - 10
        
        assert h2.hour_of_year == 90
        assert h2.year == 2024


class TestHourLength:
    """Test suite for Hour length."""

    def test_length_property(self):
        """Test that Hour has length of 1/24."""
        hour = Hour(2024, 1)
        assert hour.length == 1/24
        assert Hour.length == 1/24


class TestFixedLenTimeStepCommon:
    """Test suite for common FixedLenTimeStep functionality."""

    def test_timestep_has_unit(self):
        """Test that each timestep class has a unit."""
        assert Day.unit == 'd'
        assert Hour.unit == 'h'

    def test_from_step_class_method(self):
        """Test from_step class method."""
        day = Day.from_step(2024, 100)
        assert day.year == 2024
        assert day.step == 100
        
        hour = Hour.from_step(2024, 500)
        assert hour.year == 2024
        assert hour.step == 500


class TestDayRepr:
    """Test suite for Day string representation."""

    def test_repr(self):
        """Test __repr__ method."""
        day = Day(2024, 50)
        repr_str = repr(day)
        
        assert 'Day' in repr_str
        assert '2024' in repr_str


class TestHourRepr:
    """Test suite for Hour string representation."""

    def test_repr(self):
        """Test __repr__ method."""
        hour = Hour(2024, 100)
        repr_str = repr(hour)
        
        assert 'Hour' in repr_str
        assert '2024' in repr_str


class TestFixedLenEdgeCases:
    """Test suite for edge cases."""

    def test_leap_year_last_day(self):
        """Test last day of leap year."""
        day = Day(2024, 366)
        assert day.start.month == 12
        assert day.start.day == 31

    def test_non_leap_year_last_day(self):
        """Test last day of non-leap year."""
        day = Day(2023, 365)
        assert day.start.month == 12
        assert day.start.day == 31

    def test_day_and_hour_consistency(self):
        """Test that Day and Hour align correctly."""
        # First hour of day 2
        hour_25 = Hour(2024, 25)
        day_2 = Day(2024, 2)
        
        assert hour_25.start == day_2.start

    def test_hour_microsecond_precision(self):
        """Test that hour boundaries are precise."""
        h1 = Hour(2024, 1)
        h2 = Hour(2024, 2)
        
        # End of h1 should be 1 second before start of h2
        time_diff = (h2.start - h1.end).total_seconds()
        assert time_diff == 1


class TestDayMonthConversion:
    """Test suite for day to month conversion."""

    def test_day_month_jan(self):
        """Test that days in January have correct month."""
        for doy in range(1, 32):
            day = Day(2024, doy)
            assert day.month == 1

    def test_day_month_feb_leap(self):
        """Test that days in February (leap year) have correct month."""
        # Feb 1 is DOY 32, Feb 29 is DOY 60
        for doy in range(32, 61):
            day = Day(2024, doy)
            assert day.month == 2

    def test_day_month_dec(self):
        """Test that days in December have correct month."""
        # Dec starts at DOY 336 in leap year
        for doy in range(336, 367):
            day = Day(2024, doy)
            assert day.month == 12
