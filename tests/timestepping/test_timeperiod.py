"""Comprehensive tests for TimePeriod class."""

import datetime
import pytest
from d3tools.timestepping.timeperiods.timeperiod import TimePeriod
from d3tools.timestepping import TimeWindow


class TestTimePeriodInit:
    """Test suite for TimePeriod initialization."""

    def test_init_with_datetime(self):
        """Test initialization with datetime objects."""
        start = datetime.datetime(2024, 1, 1)
        end = datetime.datetime(2024, 1, 31)
        period = TimePeriod(start, end)
        
        assert period.start == start
        assert period.end == end

    def test_init_with_strings(self):
        """Test initialization with date strings."""
        period = TimePeriod('2024-01-01', '2024-01-31')
        
        assert period.start == datetime.datetime(2024, 1, 1)
        assert period.end == datetime.datetime(2024, 1, 31, 23, 59, 59)

    def test_init_mixed_types(self):
        """Test initialization with mixed datetime and string."""
        start = datetime.datetime(2024, 1, 1)
        period = TimePeriod(start, '2024-01-31')
        
        assert period.start == start
        assert period.end == datetime.datetime(2024, 1, 31, 23, 59, 59)


class TestTimePeriodFromAny:
    """Test suite for TimePeriod.from_any class method."""

    def test_from_any_with_timeperiod(self):
        """Test from_any with an existing TimePeriod."""
        original = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        new = TimePeriod.from_any(original)
        
        assert new.start == original.start
        assert new.end == original.end

    def test_from_any_with_sequence(self):
        """Test from_any with a sequence of datetimes."""
        dates = [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31)]
        period = TimePeriod.from_any(dates)
        
        assert period.start == dates[0]
        assert period.end == dates[1]

    def test_from_any_with_string_sequence(self):
        """Test from_any with a sequence of date strings."""
        dates = ['2024-01-01', '2024-01-31']
        period = TimePeriod.from_any(dates)
        
        assert period.start == datetime.datetime(2024, 1, 1)
        assert period.end == datetime.datetime(2024, 1, 31, 23, 59, 59)

    def test_from_any_with_none(self):
        """Test from_any with None."""
        result = TimePeriod.from_any(None)
        assert result is None

    def test_from_any_with_object_having_start_end(self):
        """Test from_any with any object having start/end attributes."""
        class MockPeriod:
            def __init__(self):
                self.start = datetime.datetime(2024, 1, 1)
                self.end = datetime.datetime(2024, 1, 31)
        
        mock = MockPeriod()
        period = TimePeriod.from_any(mock)
        
        assert period.start == mock.start
        assert period.end == mock.end

    def test_from_any_invalid_raises_error(self):
        """Test that invalid input raises ValueError."""
        with pytest.raises(ValueError, match='Expecting a TimePeriod'):
            TimePeriod.from_any("invalid")
        
        with pytest.raises(ValueError):
            TimePeriod.from_any([datetime.datetime(2024, 1, 1)])  # Only 1 element

    def test_from_any_with_name_parameter(self):
        """Test error message includes name when provided."""
        with pytest.raises(ValueError, match='MyPeriod must be a TimePeriod'):
            TimePeriod.from_any("invalid", name="MyPeriod")


class TestTimePeriodGetLength:
    """Test suite for TimePeriod.get_length method."""

    def test_get_length_days(self):
        """Test length in days."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        assert period.get_length('days') == 31
        
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 1))
        assert period.get_length('days') == 1

    def test_get_length_hours(self):
        """Test length in hours."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2, 23, 59))
        # If period has 24+ hours
        length_hours = period.get_length('hours')
        assert length_hours >= 24

    def test_get_length_default_unit(self):
        """Test that default unit is days."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 10))
        assert period.get_length() == 10

    def test_get_length_invalid_unit_raises_error(self):
        """Test that invalid unit raises ValueError."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        with pytest.raises(ValueError, match='Unknown unit'):
            period.get_length('invalid')

    def test_get_length_backwards_period(self):
        """Test length when start > end."""
        period = TimePeriod(datetime.datetime(2024, 1, 31), datetime.datetime(2024, 1, 1))
        assert period.get_length() == 0


class TestTimePeriodContains:
    """Test suite for TimePeriod.contains method."""

    def test_contains_datetime(self):
        """Test contains with datetime objects."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        
        assert period.contains(datetime.datetime(2024, 1, 15))
        assert period.contains(datetime.datetime(2024, 1, 1))  # Start
        assert period.contains(datetime.datetime(2024, 1, 31))  # End
        assert not period.contains(datetime.datetime(2024, 2, 1))
        assert not period.contains(datetime.datetime(2023, 12, 31))

    def test_contains_string(self):
        """Test contains with date strings."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        
        assert period.contains('2024-01-15')
        assert period.contains('2024-01-01')
        assert not period.contains('2024-02-01')


class TestTimePeriodExtend:
    """Test suite for TimePeriod.extend method."""

    def test_extend_after(self):
        """Test extending after the end."""
        period = TimePeriod(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 1, 20))
        window = TimeWindow(5, 'd')
        
        extended = period.extend(window, before=False)
        assert extended.start == period.start
        assert extended.end > period.end

    def test_extend_before(self):
        """Test extending before the start."""
        period = TimePeriod(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 1, 20))
        window = TimeWindow(5, 'd')
        
        extended = period.extend(window, before=True)
        assert extended.start < period.start
        assert extended.end == period.end

    def test_extend_with_hours_after(self):
        """Test extending with hour window after."""
        period = TimePeriod(datetime.datetime(2024, 1, 15, 12), datetime.datetime(2024, 1, 15, 18))
        window = TimeWindow(6, 'h')
        
        extended = period.extend(window, before=False)
        assert isinstance(extended, TimePeriod)

    def test_extend_with_hours_before(self):
        """Test extending with hour window before."""
        period = TimePeriod(datetime.datetime(2024, 1, 15, 12), datetime.datetime(2024, 1, 15, 18))
        window = TimeWindow(6, 'h')
        
        extended = period.extend(window, before=True)
        assert isinstance(extended, TimePeriod)


class TestTimePeriodComparisons:
    """Test suite for TimePeriod comparison operators."""

    def test_equality(self):
        """Test equality comparison."""
        p1 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        p2 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        p3 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 2, 1))
        
        assert p1 == p2
        assert not (p1 == p3)

    def test_less_than(self):
        """Test less than (earlier period)."""
        p1 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 15))
        p2 = TimePeriod(datetime.datetime(2024, 1, 20), datetime.datetime(2024, 1, 31))
        
        assert p1 < p2
        assert not (p2 < p1)

    def test_greater_than(self):
        """Test greater than (later period)."""
        p1 = TimePeriod(datetime.datetime(2024, 1, 20), datetime.datetime(2024, 1, 31))
        p2 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 15))
        
        assert p1 > p2
        assert not (p2 > p1)

    def test_overlapping_periods(self):
        """Test that overlapping periods are not < or >."""
        p1 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 20))
        p2 = TimePeriod(datetime.datetime(2024, 1, 15), datetime.datetime(2024, 1, 31))
        
        assert not (p1 < p2)
        assert not (p1 > p2)

    def test_hash(self):
        """Test that TimePeriods can be hashed."""
        p1 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        p2 = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        
        # Same periods should have same hash
        assert hash(p1) == hash(p2)
        
        # Can be used in sets
        period_set = {p1, p2}
        assert len(period_set) == 1


class TestTimePeriodRepr:
    """Test suite for TimePeriod string representation."""

    def test_repr(self):
        """Test __repr__ method."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        repr_str = repr(period)
        
        assert 'TimePeriod' in repr_str
        assert '20240101' in repr_str
        assert '20240131' in repr_str

    def test_repr_with_agg_window(self):
        """Test repr when agg_window is set."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
        period.agg_window = TimeWindow(7, 'd')
        
        repr_str = repr(period)
        assert 'agg' in repr_str


class TestTimePeriodEdgeCases:
    """Test suite for edge cases."""

    def test_single_day_period(self):
        """Test period of a single day."""
        period = TimePeriod(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 1))
        assert period.get_length() == 1

    def test_leap_year_february(self):
        """Test period in February of leap year."""
        period = TimePeriod(datetime.datetime(2024, 2, 1), datetime.datetime(2024, 2, 29))
        assert period.get_length() == 29

    def test_year_boundary(self):
        """Test period crossing year boundary."""
        period = TimePeriod(datetime.datetime(2023, 12, 25), datetime.datetime(2024, 1, 5))
        assert period.contains(datetime.datetime(2024, 1, 1))
        assert period.get_length() == 12

    def test_same_start_and_end(self):
        """Test period where start equals end."""
        time = datetime.datetime(2024, 1, 15, 12, 0, 0)
        period = TimePeriod(time, time)
        assert period.start == period.end
        assert period.contains(time)
