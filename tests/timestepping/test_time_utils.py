"""Comprehensive tests for time_utils module."""

import datetime
import pytest
from d3tools.timestepping.time_utils import (
    get_window, get_md_dates, unit_is_multiple, find_unit_of_time, UNIT_CONVERSIONS
)
from d3tools.timestepping import TimeRange


class TestFindUnitOfTime:
    """Test suite for find_unit_of_time function."""

    def test_find_unit_from_string_single_char(self):
        """Test single character unit codes."""
        assert find_unit_of_time('d') == 'd'
        assert find_unit_of_time('m') == 'm'
        assert find_unit_of_time('y') == 'y'
        assert find_unit_of_time('t') == 't'
        assert find_unit_of_time('h') == 'h'
        assert find_unit_of_time('w') == 'w'
        assert find_unit_of_time('v') == 'v'

    def test_find_unit_from_string_full_words(self):
        """Test full word unit names."""
        assert find_unit_of_time('daily') == 'd'
        assert find_unit_of_time('day') == 'd'
        assert find_unit_of_time('days') == 'd'
        
        assert find_unit_of_time('monthly') == 'm'
        assert find_unit_of_time('month') == 'm'
        assert find_unit_of_time('months') == 'm'
        
        assert find_unit_of_time('yearly') == 'y'
        assert find_unit_of_time('year') == 'y'
        assert find_unit_of_time('years') == 'y'
        assert find_unit_of_time('annual') == 'y'
        assert find_unit_of_time('annually') == 'y'
        
        assert find_unit_of_time('dekads') == 't'
        assert find_unit_of_time('dekad') == 't'
        assert find_unit_of_time('dekadly') == 't'
        
        assert find_unit_of_time('hours') == 'h'
        assert find_unit_of_time('hour') == 'h'
        assert find_unit_of_time('hourly') == 'h'
        
        assert find_unit_of_time('weeks') == 'w'
        assert find_unit_of_time('week') == 'w'
        assert find_unit_of_time('weekly') == 'w'
        
        assert find_unit_of_time('viirs') == 'v'
        assert find_unit_of_time('modis') == 'v'

    def test_find_unit_from_composite(self):
        """Test composite forms like '8d' for VIIRS, '10d' for dekads."""
        assert find_unit_of_time('8d') == 'v'
        assert find_unit_of_time('8day') == 'v'
        assert find_unit_of_time('10d') == 't'
        assert find_unit_of_time('10day') == 't'

    def test_find_unit_from_timesteps_per_year(self):
        """Test inferring unit from timesteps per year."""
        assert find_unit_of_time(timesteps_per_year=365) == 'd'
        assert find_unit_of_time(timesteps_per_year=36) == 't'
        assert find_unit_of_time(timesteps_per_year=12) == 'm'
        assert find_unit_of_time(timesteps_per_year=1) == 'y'

    def test_find_unit_case_insensitive(self):
        """Test that parsing is case-insensitive."""
        assert find_unit_of_time('DAILY') == 'd'
        assert find_unit_of_time('Monthly') == 'm'
        assert find_unit_of_time('YEARLY') == 'y'

    def test_find_unit_ignores_non_alphanumeric(self):
        """Test that non-alphanumeric characters are ignored."""
        assert find_unit_of_time('dai-ly') == 'd'
        assert find_unit_of_time('month_ly') == 'm'

    def test_find_unit_invalid(self):
        """Test that invalid units raise ValueError."""
        with pytest.raises(ValueError):
            find_unit_of_time('invalid')
        
        with pytest.raises(ValueError):
            find_unit_of_time('xyz')

    def test_find_unit_missing_parameters(self):
        """Test that missing both parameters raises ValueError."""
        with pytest.raises(ValueError):
            find_unit_of_time()


class TestUnitIsMultiple:
    """Test suite for unit_is_multiple function."""

    def test_unit_is_multiple_same_unit(self):
        """A unit should be considered a multiple of itself."""
        assert unit_is_multiple('d', 'd')
        assert unit_is_multiple('m', 'm')
        assert unit_is_multiple('y', 'y')
        assert unit_is_multiple('t', 't')

    def test_unit_is_multiple_true_cases(self):
        """Test cases where unit1 is a multiple of unit2."""
        assert unit_is_multiple('m', 'd')  # months are multiples of days
        assert unit_is_multiple('y', 'm')  # years are multiples of months
        assert unit_is_multiple('y', 'd')  # years are multiples of days
        assert unit_is_multiple('w', 'd')  # weeks are multiples of days (7)
        assert unit_is_multiple('m', 'h')  # months are multiples of hours
        assert unit_is_multiple('y', 'h')  # years are multiples of hours
        assert unit_is_multiple('y', 't')  # years are multiples of dekads (36)
        assert unit_is_multiple('m', 't')  # months contain ~3 dekads

    def test_unit_is_multiple_false_cases(self):
        """Test cases where unit1 is not a multiple of unit2."""
        assert not unit_is_multiple('d', 'm')  # days are not multiples of months
        assert not unit_is_multiple('d', 'y')  # days are not multiples of years
        assert not unit_is_multiple('m', 'y')  # months are not multiples of years
        assert not unit_is_multiple('v', 'w')  # VIIRS not multiple of weeks
        assert not unit_is_multiple('m', 'v')  # months not multiple of VIIRS
        assert not unit_is_multiple('d', 'w')  # days not multiple of weeks
        assert not unit_is_multiple('t', 'y')  # dekads not multiple of years

    def test_unit_is_multiple_with_string_variants(self):
        """Test that string parsing works correctly."""
        assert unit_is_multiple('monthly', 'daily')
        assert unit_is_multiple('yearly', 'monthly')
        assert not unit_is_multiple('daily', 'monthly')

    def test_unit_is_multiple_invalid(self):
        """Test that invalid units raise ValueError."""
        with pytest.raises(ValueError):
            unit_is_multiple('invalid', 'd')
        with pytest.raises(ValueError):
            unit_is_multiple('d', 'invalid')


class TestGetWindow:
    """Test suite for get_window function."""

    def test_get_window_ending(self):
        """Test creating windows that end at the given time."""
        dt = datetime.datetime(2024, 2, 20)
        
        # 2-month window ending at dt
        win = get_window(dt, 2, 'm', start=False)
        assert isinstance(win, TimeRange)
        assert win.end == dt
        assert win.start.month == 12  # Should start in December (2 months back)
        
        # 7-day window ending at dt
        win = get_window(dt, 7, 'd', start=False)
        assert win.end == dt
        assert (win.end - win.start).days == 6  # 7 days inclusive

    def test_get_window_starting(self):
        """Test creating windows that start at the given time."""
        dt = datetime.datetime(2024, 2, 1)
        
        # 2-month window starting at dt
        win = get_window(dt, 2, 'm', start=True)
        assert isinstance(win, TimeRange)
        assert win.start == dt
        
        # 7-day window starting at dt
        win = get_window(dt, 7, 'd', start=True)
        assert win.start == dt
        assert (win.end - win.start).days >= 6

    def test_get_window_dekad(self):
        """Test dekad windows."""
        dt = datetime.datetime(2024, 2, 20)  # End of 2nd dekad of Feb
        
        win = get_window(dt, 2, 't', start=False)
        assert isinstance(win, TimeRange)
        
        win = get_window(dt, 2, 't', start=True)
        assert isinstance(win, TimeRange)

    def test_get_window_various_units(self):
        """Test windows with various time units."""
        dt = datetime.datetime(2024, 6, 15)
        
        # Days
        win = get_window(dt, 10, 'd')
        assert isinstance(win, TimeRange)
        
        # Weeks
        win = get_window(dt, 2, 'w')
        assert isinstance(win, TimeRange)
        
        # Months
        win = get_window(dt, 3, 'm')
        assert isinstance(win, TimeRange)
        
        # Years
        win = get_window(dt, 1, 'y')
        assert isinstance(win, TimeRange)

    def test_get_window_invalid_unit(self):
        """Test that invalid units raise ValueError."""
        dt = datetime.datetime(2024, 2, 20)
        with pytest.raises(ValueError):
            get_window(dt, 1, 'invalid')

    def test_get_window_float_size(self):
        """Test that float sizes raise ValueError."""
        dt = datetime.datetime(2024, 2, 20)
        with pytest.raises(ValueError, match='fractional sizes are not supported'):
            win = get_window(dt, 1.5, 'd')

    def test_get_window_edge_cases(self):
        """Test edge cases like year boundaries."""
        # Window crossing year boundary
        dt = datetime.datetime(2024, 1, 15)
        win = get_window(dt, 2, 'm', start=False)
        assert win.start.year == 2023
        assert win.end.year == 2024


class TestGetMDDates:
    """Test suite for get_md_dates function."""

    def test_get_md_dates_leap_year_feb29(self):
        """Test Feb 29 handling for leap and non-leap years."""
        years = [2020, 2021, 2024]  # 2020 and 2024 are leap years
        dates = get_md_dates(years, 2, 29)
        assert len(dates) == 3
        assert dates[0] == datetime.datetime(2020, 2, 29)
        assert dates[1] == datetime.datetime(2021, 2, 28)  # Non-leap -> Feb 28
        assert dates[2] == datetime.datetime(2024, 2, 29)

    def test_get_md_dates_leap_year_feb28(self):
        """Test Feb 28 handling for leap and non-leap years."""
        years = [2020, 2021, 2024]
        dates = get_md_dates(years, 2, 28)
        assert len(dates) == 3
        # Feb 28 request uses last day of Feb: Feb 29 for leap years, Feb 28 for non-leap
        assert dates[0].day == 29  # 2020 is leap
        assert dates[1].day == 28  # 2021 is not leap
        assert dates[2].day == 29  # 2024 is leap

    def test_get_md_dates_regular_date(self):
        """Test regular dates (not Feb 28/29)."""
        years = range(2020, 2025)
        dates = get_md_dates(years, 3, 15)
        assert len(dates) == 5
        assert all(d.month == 3 and d.day == 15 for d in dates)
        assert dates[0] == datetime.datetime(2020, 3, 15)
        assert dates[-1] == datetime.datetime(2024, 3, 15)

    def test_get_md_dates_list_years(self):
        """Test with a list of specific years."""
        years = [2020, 2022, 2024]
        dates = get_md_dates(years, 7, 4)
        assert len(dates) == 3
        assert dates[0] == datetime.datetime(2020, 7, 4)
        assert dates[1] == datetime.datetime(2022, 7, 4)
        assert dates[2] == datetime.datetime(2024, 7, 4)

    def test_get_md_dates_single_year(self):
        """Test with a single year."""
        dates = get_md_dates([2024], 12, 25)
        assert len(dates) == 1
        assert dates[0] == datetime.datetime(2024, 12, 25)

    def test_get_md_dates_sorted(self):
        """Test that output is always sorted."""
        years = [2024, 2020, 2022]  # Unsorted input
        dates = get_md_dates(years, 1, 1)
        assert dates == sorted(dates)
        assert dates[0].year == 2020
        assert dates[-1].year == 2024

    def test_get_md_dates_edge_cases(self):
        """Test edge cases."""
        # First day of year
        dates = get_md_dates([2024], 1, 1)
        assert dates[0] == datetime.datetime(2024, 1, 1)
        
        # Last day of year
        dates = get_md_dates([2024], 12, 31)
        assert dates[0] == datetime.datetime(2024, 12, 31)


class TestUnitConversions:
    """Test suite for UNIT_CONVERSIONS constant."""

    def test_unit_conversions_structure(self):
        """Test that UNIT_CONVERSIONS has expected structure."""
        assert 'h' in UNIT_CONVERSIONS
        assert 'd' in UNIT_CONVERSIONS
        assert 'w' in UNIT_CONVERSIONS
        assert 'm' in UNIT_CONVERSIONS
        assert 'y' in UNIT_CONVERSIONS
        assert 't' in UNIT_CONVERSIONS

    def test_unit_conversions_values(self):
        """Test specific conversion values."""
        # Hours to days
        assert UNIT_CONVERSIONS['h']['d'] == 24
        
        # Days to weeks
        assert UNIT_CONVERSIONS['d']['w'] == 7
        
        # Dekads to months
        assert UNIT_CONVERSIONS['t']['m'] == 3
        
        # Months to years
        assert UNIT_CONVERSIONS['m']['y'] == 12

