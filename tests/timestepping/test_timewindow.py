"""Comprehensive tests for TimeWindow class."""

import datetime
import pytest
from d3tools.timestepping import TimeWindow, TimeRange


class TestTimeWindowInit:
    """Test suite for TimeWindow initialization."""

    def test_init_basic(self):
        """Test basic initialization."""
        window = TimeWindow(3, 'd')
        assert window.size == 3
        assert window.unit == 'd'

    def test_init_various_units(self):
        """Test initialization with various units."""
        units = ['d', 'm', 'y', 't', 'h', 'w', 'v']
        for unit in units:
            window = TimeWindow(5, unit)
            assert window.unit == unit
            assert window.size == 5

    def test_init_with_string_units(self):
        """Test initialization with full string unit names."""
        window = TimeWindow(3, 'days')
        assert window.unit == 'd'
        
        window = TimeWindow(2, 'months')
        assert window.unit == 'm'
        
        window = TimeWindow(1, 'year')
        assert window.unit == 'y'

    def test_init_size_conversion_to_int(self):
        """Test that size is converted to int."""
        window = TimeWindow(3.7, 'd')
        assert window.size == 3
        assert isinstance(window.size, int)


class TestTimeWindowFromStr:
    """Test suite for TimeWindow.from_str method."""

    def test_from_str_no_separator(self):
        """Test parsing strings without separators."""
        window = TimeWindow.from_str('3days')
        assert window.size == 3
        assert window.unit == 'd'
        
        window = TimeWindow.from_str('2months')
        assert window.size == 2
        assert window.unit == 'm'
        
        window = TimeWindow.from_str('1year')
        assert window.size == 1
        assert window.unit == 'y'

    def test_from_str_with_space(self):
        """Test parsing strings with space separator."""
        window = TimeWindow.from_str('3 days')
        assert window.size == 3
        assert window.unit == 'd'
        
        window = TimeWindow.from_str('10 days')
        assert window.size == 10
        assert window.unit == 'd'

    def test_from_str_with_dash(self):
        """Test parsing strings with dash separator."""
        window = TimeWindow.from_str('3-days')
        assert window.size == 3
        assert window.unit == 'd'

    def test_from_str_with_dot(self):
        """Test parsing strings with dot separator."""
        window = TimeWindow.from_str('3.days')
        assert window.size == 3
        assert window.unit == 'd'

    def test_from_str_single_letter_unit(self):
        """Test parsing with single letter units."""
        window = TimeWindow.from_str('3d')
        assert window.size == 3
        assert window.unit == 'd'
        
        window = TimeWindow.from_str('2m')
        assert window.size == 2
        assert window.unit == 'm'

    def test_from_str_ambiguous_without_separator(self):
        """Test that ambiguous strings without separator raise ValueError."""
        # '10day' could be 10 days or 1 dekad
        with pytest.raises(ValueError, match='Cannot figure out window size'):
            TimeWindow.from_str('10day')
        
        # '8day' could be 8 days or VIIRS period
        with pytest.raises(ValueError, match='Cannot figure out window size'):
            TimeWindow.from_str('8day')

    def test_from_str_disambiguated_with_separator(self):
        """Test that ambiguous cases work with separator."""
        window = TimeWindow.from_str('10 days')
        assert window.size == 10
        assert window.unit == 'd'
        
        window = TimeWindow.from_str('8 days')
        assert window.size == 8
        assert window.unit == 'd'


class TestTimeWindowApply:
    """Test suite for TimeWindow.apply method."""

    def test_apply_ending(self):
        """Test applying window that ends at given time."""
        window = TimeWindow(7, 'd')
        time = datetime.datetime(2024, 2, 20)
        
        range_obj = window.apply(time, start=False)
        assert isinstance(range_obj, TimeRange)
        assert range_obj.end == time
        assert (range_obj.end - range_obj.start).days >= 6

    def test_apply_starting(self):
        """Test applying window that starts at given time."""
        window = TimeWindow(7, 'd')
        time = datetime.datetime(2024, 2, 1)
        
        range_obj = window.apply(time, start=True)
        assert isinstance(range_obj, TimeRange)
        assert range_obj.start == time

    def test_apply_months(self):
        """Test applying month windows."""
        window = TimeWindow(3, 'm')
        time = datetime.datetime(2024, 6, 15)
        
        # Ending at time
        range_obj = window.apply(time, start=False)
        assert isinstance(range_obj, TimeRange)
        assert range_obj.end == time
        
        # Starting at time
        range_obj = window.apply(time, start=True)
        assert range_obj.start == time

    def test_apply_years(self):
        """Test applying year windows."""
        window = TimeWindow(2, 'y')
        time = datetime.datetime(2024, 6, 15)
        
        range_obj = window.apply(time, start=False)
        assert isinstance(range_obj, TimeRange)
        assert range_obj.end == time

    def test_apply_hours(self):
        """Test applying hour windows."""
        window = TimeWindow(24, 'h')
        time = datetime.datetime(2024, 2, 20, 12, 0)
        
        # Ending at time
        range_obj = window.apply(time, start=False)
        assert isinstance(range_obj, TimeRange)
        assert range_obj.end == time
        
        # Starting at time
        range_obj = window.apply(time, start=True)
        assert range_obj.start == time


class TestTimeWindowToHours:
    """Test suite for TimeWindow.to_hours method."""

    def test_to_hours_days(self):
        """Test converting days to hours."""
        window = TimeWindow(3, 'd')
        assert window.to_hours() == 72  # 3 * 24

    def test_to_hours_hours(self):
        """Test converting hours to hours."""
        window = TimeWindow(48, 'h')
        assert window.to_hours() == 48

    def test_to_hours_weeks(self):
        """Test converting weeks to hours."""
        window = TimeWindow(1, 'w')
        assert window.to_hours() == 168  # 7 * 24

    def test_to_hours_viirs(self):
        """Test converting VIIRS periods to hours."""
        window = TimeWindow(1, 'v')
        assert window.to_hours() == 192  # 8 * 24

    def test_to_hours_months_max(self):
        """Test converting months to hours with max limit."""
        window = TimeWindow(1, 'm')
        assert window.to_hours(limit='max') == 744  # 31 * 24

    def test_to_hours_months_min(self):
        """Test converting months to hours with min limit."""
        window = TimeWindow(1, 'm')
        assert window.to_hours(limit='min') == 672  # 28 * 24

    def test_to_hours_years_max(self):
        """Test converting years to hours with max limit."""
        window = TimeWindow(1, 'y')
        assert window.to_hours(limit='max') == 8784  # 366 * 24

    def test_to_hours_years_min(self):
        """Test converting years to hours with min limit."""
        window = TimeWindow(1, 'y')
        assert window.to_hours(limit='min') == 8760  # 365 * 24

    def test_to_hours_dekads_max(self):
        """Test converting dekads to hours with max limit."""
        window = TimeWindow(1, 't')
        assert window.to_hours(limit='max') == 264  # 11 * 24

    def test_to_hours_dekads_min(self):
        """Test converting dekads to hours with min limit."""
        window = TimeWindow(1, 't')
        assert window.to_hours(limit='min') == 192  # 8 * 24

    def test_to_hours_requires_limit_for_variable_units(self):
        """Test that variable-length units require limit parameter."""
        window = TimeWindow(1, 'm')
        with pytest.raises(ValueError):
            window.to_hours()  # No limit specified


class TestTimeWindowComparisons:
    """Test suite for TimeWindow comparison operators."""

    def test_equality(self):
        """Test equality comparison."""
        w1 = TimeWindow(3, 'd')
        w2 = TimeWindow(3, 'd')
        w3 = TimeWindow(4, 'd')
        w4 = TimeWindow(3, 'm')
        
        assert w1 == w2
        assert not (w1 == w3)
        assert not (w1 == w4)

    def test_less_than(self):
        """Test less than comparison."""
        w1 = TimeWindow(1, 'd')
        w2 = TimeWindow(2, 'd')
        w3 = TimeWindow(1, 'w')
        
        assert w1 < w2
        assert w1 < w3  # 1 day < 1 week
        assert not (w2 < w1)

    def test_greater_than(self):
        """Test greater than comparison."""
        w1 = TimeWindow(2, 'd')
        w2 = TimeWindow(1, 'd')
        w3 = TimeWindow(1, 'w')
        
        assert w1 > w2
        assert w3 > w1  # 1 week > 2 days
        assert not (w2 > w1)

    def test_less_than_or_equal(self):
        """Test less than or equal comparison."""
        w1 = TimeWindow(3, 'd')
        w2 = TimeWindow(3, 'd')
        w3 = TimeWindow(4, 'd')
        
        assert w1 <= w2
        assert w1 <= w3
        assert not (w3 <= w1)

    def test_greater_than_or_equal(self):
        """Test greater than or equal comparison."""
        w1 = TimeWindow(3, 'd')
        w2 = TimeWindow(3, 'd')
        w3 = TimeWindow(2, 'd')
        
        assert w1 >= w2
        assert w1 >= w3
        assert not (w3 >= w1)


class TestTimeWindowArithmetic:
    """Test suite for TimeWindow arithmetic operations."""

    def test_add_same_unit(self):
        """Test adding windows with same unit."""
        w1 = TimeWindow(3, 'd')
        w2 = TimeWindow(4, 'd')
        result = w1 + w2
        
        assert result.size == 7
        assert result.unit == 'd'

    def test_add_compatible_units(self):
        """Test adding windows with compatible units."""
        w1 = TimeWindow(1, 'w')
        w2 = TimeWindow(7, 'd')
        result = w1 + w2
        
        # Result converts to days (smaller unit)
        assert result.size == 14
        assert result.unit == 'd'

    def test_subtract_compatible_units(self):
        """Test subtracting windows with compatible units."""
        w1 = TimeWindow(2, 'w')
        w2 = TimeWindow(7, 'd')
        result = w1 - w2
        
        # Result converts to days (smaller unit)
        assert result.size == 7
        assert result.unit == 'd'


class TestTimeWindowIsMultiple:
    """Test suite for TimeWindow.is_multiple method."""

    def test_is_multiple_same_unit(self):
        """Test multiples with same unit."""
        w1 = TimeWindow(6, 'd')
        w2 = TimeWindow(2, 'd')
        w3 = TimeWindow(4, 'd')
        
        assert w1.is_multiple(w2)  # 6 is multiple of 2
        assert not w1.is_multiple(w3)  # 6 is not multiple of 4

    def test_is_multiple_compatible_units(self):
        """Test multiples with compatible units."""
        w1 = TimeWindow(2, 'w')
        w2 = TimeWindow(7, 'd')
        
        assert w1.is_multiple(w2)  # 2 weeks = 14 days, multiple of 7

    def test_is_multiple_not_multiple(self):
        """Test cases where not a multiple."""
        w1 = TimeWindow(3, 'd')
        w2 = TimeWindow(2, 'd')
        
        assert not w1.is_multiple(w2)


class TestTimeWindowRepr:
    """Test suite for TimeWindow string representations."""

    def test_repr(self):
        """Test __repr__ method."""
        window = TimeWindow(3, 'd')
        assert repr(window) == 'TimeWindow(3, d)'

    def test_str(self):
        """Test __str__ method."""
        window = TimeWindow(3, 'd')
        assert str(window) == '3d'
        
        window = TimeWindow(2, 'm')
        assert str(window) == '2m'


class TestTimeWindowEdgeCases:
    """Test suite for edge cases and special scenarios."""

    def test_zero_size(self):
        """Test window with zero size."""
        window = TimeWindow(0, 'd')
        assert window.size == 0

    def test_negative_size(self):
        """Test window with negative size."""
        window = TimeWindow(-3, 'd')
        assert window.size == -3

    def test_large_size(self):
        """Test window with large size."""
        window = TimeWindow(1000, 'd')
        assert window.size == 1000
        assert window.to_hours() == 24000

    def test_multiple_windows_operations(self):
        """Test chaining operations."""
        w1 = TimeWindow(10, 'd')
        w2 = TimeWindow(3, 'd')
        w3 = TimeWindow(2, 'd')
        
        result = w1 - w2 - w3
        assert result.size == 5
        assert result.unit == 'd'
