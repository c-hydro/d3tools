"""Comprehensive tests for FixedNTimeStep classes (Month, Year, Dekad)."""

import datetime
import pytest
from d3tools.timestepping import Month, Year, Dekad, TimeWindow


class TestDekadInit:
    """Test suite for Dekad initialization."""

    def test_dekad_init(self):
        """Test basic Dekad initialization."""
        dekad = Dekad(2024, 1)
        assert dekad.year == 2024
        assert dekad.step == 1
        assert dekad.dekad_of_year == 1
        assert dekad.month == 1
        assert dekad.dekad_of_month == 1

    def test_dekad_various_positions(self):
        """Test dekads at various positions in year."""
        # First dekad of year
        d1 = Dekad(2024, 1)
        assert d1.month == 1
        assert d1.dekad_of_month == 1
        
        # Second dekad of January
        d2 = Dekad(2024, 2)
        assert d2.month == 1
        assert d2.dekad_of_month == 2
        
        # Third dekad of January
        d3 = Dekad(2024, 3)
        assert d3.month == 1
        assert d3.dekad_of_month == 3
        
        # First dekad of February (4th of year)
        d4 = Dekad(2024, 4)
        assert d4.month == 2
        assert d4.dekad_of_month == 1
        
        # Last dekad of year (36th)
        d36 = Dekad(2024, 36)
        assert d36.month == 12
        assert d36.dekad_of_month == 3


class TestDekadFromDate:
    """Test suite for Dekad.from_date method."""

    def test_from_date_first_dekad(self):
        """Test creating dekad from date in first dekad."""
        dekad = Dekad.from_date(datetime.datetime(2024, 1, 5))
        assert dekad.year == 2024
        assert dekad.dekad_of_year == 1

    def test_from_date_second_dekad(self):
        """Test creating dekad from date in second dekad."""
        dekad = Dekad.from_date(datetime.datetime(2024, 1, 15))
        assert dekad.dekad_of_year == 2

    def test_from_date_third_dekad(self):
        """Test creating dekad from date in third dekad."""
        dekad = Dekad.from_date(datetime.datetime(2024, 1, 25))
        assert dekad.dekad_of_year == 3

    def test_from_date_various_months(self):
        """Test from_date across different months."""
        # June 5th - first dekad of June (16th of year)
        d = Dekad.from_date(datetime.datetime(2024, 6, 5))
        assert d.month == 6
        assert d.dekad_of_month == 1
        assert d.dekad_of_year == 16

    def test_from_date_boundaries(self):
        """Test from_date at dekad boundaries."""
        # Day 1 - first dekad
        d = Dekad.from_date(datetime.datetime(2024, 1, 1))
        assert d.dekad_of_month == 1
        
        # Day 10 - first dekad
        d = Dekad.from_date(datetime.datetime(2024, 1, 10))
        assert d.dekad_of_month == 1
        
        # Day 11 - second dekad
        d = Dekad.from_date(datetime.datetime(2024, 1, 11))
        assert d.dekad_of_month == 2
        
        # Day 20 - second dekad
        d = Dekad.from_date(datetime.datetime(2024, 1, 20))
        assert d.dekad_of_month == 2
        
        # Day 21 - third dekad
        d = Dekad.from_date(datetime.datetime(2024, 1, 21))
        assert d.dekad_of_month == 3


class TestDekadDates:
    """Test suite for Dekad start and end dates."""

    def test_get_start_first_dekad(self):
        """Test start date of first dekad."""
        dekad = Dekad(2024, 1)
        assert dekad.start == datetime.datetime(2024, 1, 1)

    def test_get_start_second_dekad(self):
        """Test start date of second dekad."""
        dekad = Dekad(2024, 2)
        assert dekad.start == datetime.datetime(2024, 1, 11)

    def test_get_start_third_dekad(self):
        """Test start date of third dekad."""
        dekad = Dekad(2024, 3)
        assert dekad.start == datetime.datetime(2024, 1, 21)

    def test_get_end_first_dekad(self):
        """Test end date of first dekad."""
        dekad = Dekad(2024, 1)
        assert dekad.end == datetime.datetime(2024, 1, 10)

    def test_get_end_second_dekad(self):
        """Test end date of second dekad."""
        dekad = Dekad(2024, 2)
        assert dekad.end == datetime.datetime(2024, 1, 20)

    def test_get_end_third_dekad_short_month(self):
        """Test end date of third dekad in 28-day month."""
        # February 2023 (non-leap year) - 3rd dekad should end on Feb 28
        dekad = Dekad(2023, 6)  # 3rd dekad of Feb
        assert dekad.end == datetime.datetime(2023, 2, 28)

    def test_get_end_third_dekad_leap_year(self):
        """Test end date of third dekad in leap year February."""
        # February 2024 (leap year) - 3rd dekad should end on Feb 29
        dekad = Dekad(2024, 6)  # 3rd dekad of Feb
        assert dekad.end == datetime.datetime(2024, 2, 29)

    def test_get_end_third_dekad_31_day_month(self):
        """Test end date of third dekad in 31-day month."""
        # January - 3rd dekad should end on Jan 31
        dekad = Dekad(2024, 3)
        assert dekad.end == datetime.datetime(2024, 1, 31)


class TestDekadArithmetic:
    """Test suite for Dekad arithmetic operations."""

    def test_add_positive(self):
        """Test adding to a dekad."""
        d1 = Dekad(2024, 1)
        d2 = d1 + 1
        
        assert d2.dekad_of_year == 2
        assert d2.year == 2024

    def test_add_multiple(self):
        """Test adding multiple dekads."""
        d1 = Dekad(2024, 1)
        d2 = d1 + 5
        
        assert d2.dekad_of_year == 6
        assert d2.year == 2024

    def test_add_crossing_year(self):
        """Test addition that crosses year boundary."""
        d1 = Dekad(2024, 35)  # Near end of year
        d2 = d1 + 5
        
        assert d2.year == 2025
        assert d2.dekad_of_year == 4

    def test_subtract_positive(self):
        """Test subtracting from a dekad."""
        d1 = Dekad(2024, 10)
        d2 = d1 - 3
        
        assert d2.dekad_of_year == 7
        assert d2.year == 2024

    def test_subtract_crossing_year(self):
        """Test subtraction that crosses year boundary."""
        d1 = Dekad(2024, 2)
        d2 = d1 - 5
        
        assert d2.year == 2023
        assert d2.dekad_of_year == 33  # 36 - 5 + 2


class TestMonthInit:
    """Test suite for Month initialization."""

    def test_month_init(self):
        """Test basic Month initialization."""
        month = Month(2024, 1)
        assert month.year == 2024
        assert month.month == 1
        assert month.month_of_year == 1

    def test_month_various_months(self):
        """Test months at various positions."""
        for m in range(1, 13):
            month = Month(2024, m)
            assert month.month == m
            assert month.month_of_year == m


class TestMonthFromDate:
    """Test suite for Month.from_date method."""

    def test_from_date_january(self):
        """Test creating month from date in January."""
        month = Month.from_date(datetime.datetime(2024, 1, 15))
        assert month.year == 2024
        assert month.month == 1

    def test_from_date_december(self):
        """Test creating month from date in December."""
        month = Month.from_date(datetime.datetime(2024, 12, 25))
        assert month.year == 2024
        assert month.month == 12

    def test_from_date_boundary(self):
        """Test from_date at month boundaries."""
        # First day of month
        m = Month.from_date(datetime.datetime(2024, 6, 1))
        assert m.month == 6
        
        # Last day of month
        m = Month.from_date(datetime.datetime(2024, 6, 30))
        assert m.month == 6


class TestMonthDates:
    """Test suite for Month start and end dates."""

    def test_get_start(self):
        """Test start date of month."""
        month = Month(2024, 6)
        assert month.start == datetime.datetime(2024, 6, 1)

    def test_get_end_30_day_month(self):
        """Test end date of 30-day month."""
        month = Month(2024, 6)  # June has 30 days
        assert month.end == datetime.datetime(2024, 6, 30)

    def test_get_end_31_day_month(self):
        """Test end date of 31-day month."""
        month = Month(2024, 1)  # January has 31 days
        assert month.end == datetime.datetime(2024, 1, 31)

    def test_get_end_february_leap_year(self):
        """Test end date of February in leap year."""
        month = Month(2024, 2)
        assert month.end == datetime.datetime(2024, 2, 29)

    def test_get_end_february_non_leap_year(self):
        """Test end date of February in non-leap year."""
        month = Month(2023, 2)
        assert month.end == datetime.datetime(2023, 2, 28)


class TestMonthArithmetic:
    """Test suite for Month arithmetic operations."""

    def test_add_positive(self):
        """Test adding months."""
        m1 = Month(2024, 1)
        m2 = m1 + 1
        
        assert m2.month == 2
        assert m2.year == 2024

    def test_add_crossing_year(self):
        """Test addition crossing year boundary."""
        m1 = Month(2024, 11)
        m2 = m1 + 3
        
        assert m2.year == 2025
        assert m2.month == 2

    def test_subtract(self):
        """Test subtracting months."""
        m1 = Month(2024, 5)
        m2 = m1 - 2
        
        assert m2.month == 3
        assert m2.year == 2024

    def test_subtract_crossing_year(self):
        """Test subtraction crossing year boundary."""
        m1 = Month(2024, 2)
        m2 = m1 - 3
        
        assert m2.year == 2023
        assert m2.month == 11


class TestYearInit:
    """Test suite for Year initialization."""

    def test_year_init(self):
        """Test basic Year initialization."""
        year = Year(2024)
        assert year.year == 2024

    def test_year_init_with_dummy(self):
        """Test Year initialization with dummy parameter."""
        year = Year(2024, 1)
        assert year.year == 2024


class TestYearFromDate:
    """Test suite for Year.from_date method."""

    def test_from_date(self):
        """Test creating year from date."""
        year = Year.from_date(datetime.datetime(2024, 6, 15))
        assert year.year == 2024


class TestYearDates:
    """Test suite for Year start and end dates."""

    def test_get_start(self):
        """Test start date of year."""
        year = Year(2024)
        assert year.start == datetime.datetime(2024, 1, 1)

    def test_get_end(self):
        """Test end date of year."""
        year = Year(2024)
        assert year.end == datetime.datetime(2024, 12, 31)


class TestYearLeap:
    """Test suite for Year.is_leap method."""

    def test_is_leap_true(self):
        """Test leap years."""
        assert Year(2024).is_leap()
        assert Year(2020).is_leap()
        assert Year(2000).is_leap()

    def test_is_leap_false(self):
        """Test non-leap years."""
        assert not Year(2023).is_leap()
        assert not Year(2021).is_leap()
        assert not Year(1900).is_leap()  # Divisible by 100 but not 400


class TestYearArithmetic:
    """Test suite for Year arithmetic operations."""

    def test_add(self):
        """Test adding years."""
        y1 = Year(2024)
        y2 = y1 + 1
        
        assert y2.year == 2025

    def test_subtract(self):
        """Test subtracting years."""
        y1 = Year(2024)
        y2 = y1 - 3
        
        assert y2.year == 2021


class TestFixedNTimeStepCommon:
    """Test suite for common FixedNTimeStep functionality."""

    def test_timestep_has_unit(self):
        """Test that each timestep class has a unit."""
        assert Dekad.unit == 't'
        assert Month.unit == 'm'
        assert Year.unit == 'y'

    def test_timestep_has_n_steps(self):
        """Test that each timestep class has n_steps."""
        assert Dekad.n_steps == 36
        assert Month.n_steps == 12
        assert Year.n_steps == 1

    def test_from_step_class_method(self):
        """Test from_step class method."""
        dekad = Dekad.from_step(2024, 5)
        assert dekad.year == 2024
        assert dekad.step == 5
        
        month = Month.from_step(2024, 7)
        assert month.year == 2024
        assert month.step == 7

    def test_set_year(self):
        """Test set_year method."""
        m1 = Month(2024, 5)
        m2 = m1.set_year(2023)
        
        assert m2.year == 2023
        assert m2.month == 5
        assert m1.year == 2024  # Original unchanged

    def test_with_agg_window(self):
        """Test setting aggregation window."""
        month = Month(2024, 5)
        month.agg_window = TimeWindow(7, 'd')
        
        assert month.agg_window.size == 7
        assert month.agg_window.unit == 'd'

    def test_agg_range(self):
        """Test agg_range property."""
        month = Month(2024, 5)
        month.agg_window = TimeWindow(30, 'd')
        
        agg_range = month.agg_range
        assert agg_range.end == month.end


class TestFixedNTimeStepEdgeCases:
    """Test suite for edge cases."""

    def test_year_wraparound(self):
        """Test that year wraparound works correctly for all types."""
        # Dekad
        d = Dekad(2024, 36)
        d_next = d + 1
        assert d_next.year == 2025
        assert d_next.dekad_of_year == 1
        
        # Month
        m = Month(2024, 12)
        m_next = m + 1
        assert m_next.year == 2025
        assert m_next.month == 1

    def test_leap_year_february_dekads(self):
        """Test February dekads in leap vs non-leap years."""
        # Leap year - 3rd dekad of Feb
        d_leap = Dekad(2024, 6)
        assert d_leap.end.day == 29
        
        # Non-leap year - 3rd dekad of Feb
        d_nonleap = Dekad(2023, 6)
        assert d_nonleap.end.day == 28

    def test_timestep_length(self):
        """Test length of timesteps."""
        # Month lengths vary
        jan = Month(2024, 1)
        assert jan.get_length() == 31
        
        feb_leap = Month(2024, 2)
        assert feb_leap.get_length() == 29
        
        feb_nonleap = Month(2023, 2)
        assert feb_nonleap.get_length() == 28
