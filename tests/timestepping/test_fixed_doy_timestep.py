"""Comprehensive tests for FixedDOYTimeStep classes (ViirsModisTimeStep)."""

import datetime
import pytest
from d3tools.timestepping.timeperiods.fixed_doy_timestep import FixedDOYTimeStep, ViirsModisTimeStep


class TestViirsModisTimeStepInit:
    """Test suite for ViirsModisTimeStep initialization."""

    def test_init_first_period(self):
        """Test initialization of first VIIRS period."""
        viirs = ViirsModisTimeStep(2024, 1)
        assert viirs.year == 2024
        assert viirs.step == 1
        assert viirs.step_of_year == 1

    def test_init_various_periods(self):
        """Test various VIIRS periods."""
        for step in [1, 10, 20, 30, 40]:
            viirs = ViirsModisTimeStep(2024, step)
            assert viirs.step == step


class TestViirsModisTimeStepStartDoys:
    """Test suite for VIIRS/MODIS start DOY values."""

    def test_start_doys_constant(self):
        """Test that start_doys is correctly defined."""
        expected = tuple(range(1, 366, 8))
        assert ViirsModisTimeStep.start_doys == expected

    def test_start_doys_count(self):
        """Test that there are 46 VIIRS periods per year."""
        assert len(ViirsModisTimeStep.start_doys) == 46


class TestViirsModisTimeStepFromDate:
    """Test suite for ViirsModisTimeStep.from_date method."""

    def test_from_date_first_day(self):
        """Test from_date with Jan 1 (DOY 1)."""
        viirs = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, 1))
        assert viirs.step == 1

    def test_from_date_within_first_period(self):
        """Test from_date within first 8-day period."""
        for day in range(1, 9):
            viirs = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, day))
            assert viirs.step == 1

    def test_from_date_second_period(self):
        """Test from_date in second period (DOY 9-16)."""
        viirs = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, 9))
        assert viirs.step == 2
        
        viirs = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, 16))
        assert viirs.step == 2

    def test_from_date_mid_year(self):
        """Test from_date in middle of year."""
        # DOY 180 should be in period 23 (DOY 177-184)
        viirs = ViirsModisTimeStep.from_date(datetime.datetime(2024, 6, 28))
        assert viirs.step >= 22 and viirs.step <= 24

    def test_from_date_end_of_year(self):
        """Test from_date at end of year."""
        viirs = ViirsModisTimeStep.from_date(datetime.datetime(2024, 12, 31))
        assert viirs.step == 46  # Last period


class TestViirsModisTimeStepDates:
    """Test suite for VIIRS/MODIS start and end dates."""

    def test_get_start_first_period(self):
        """Test start date of first period."""
        viirs = ViirsModisTimeStep(2024, 1)
        assert viirs.start == datetime.datetime(2024, 1, 1)

    def test_get_start_second_period(self):
        """Test start date of second period."""
        viirs = ViirsModisTimeStep(2024, 2)
        assert viirs.start == datetime.datetime(2024, 1, 9)

    def test_get_start_third_period(self):
        """Test start date of third period."""
        viirs = ViirsModisTimeStep(2024, 3)
        assert viirs.start == datetime.datetime(2024, 1, 17)

    def test_get_end_first_period(self):
        """Test end date of first period (DOY 1-8)."""
        viirs = ViirsModisTimeStep(2024, 1)
        assert viirs.end == datetime.datetime(2024, 1, 8)

    def test_get_end_second_period(self):
        """Test end date of second period."""
        viirs = ViirsModisTimeStep(2024, 2)
        assert viirs.end == datetime.datetime(2024, 1, 16)

    def test_get_end_last_period(self):
        """Test end date of last period (shorter period)."""
        viirs = ViirsModisTimeStep(2024, 46)
        # Last period ends on Dec 31
        assert viirs.end == datetime.datetime(2024, 12, 31)
        # Last period is shorter than 8 days
        length = (viirs.end - viirs.start).days + 1
        assert length < 8


class TestViirsModisTimeStepArithmetic:
    """Test suite for VIIRS/MODIS arithmetic operations."""

    def test_add_one(self):
        """Test adding one period."""
        v1 = ViirsModisTimeStep(2024, 1)
        v2 = v1 + 1
        
        assert v2.step == 2
        assert v2.year == 2024

    def test_add_multiple(self):
        """Test adding multiple periods."""
        v1 = ViirsModisTimeStep(2024, 1)
        v2 = v1 + 10
        
        assert v2.step == 11
        assert v2.year == 2024

    def test_add_crossing_year(self):
        """Test addition crossing year boundary."""
        v1 = ViirsModisTimeStep(2024, 45)
        v2 = v1 + 5
        
        assert v2.year == 2025
        assert v2.step == 4  # 45 + 5 - 46 = 4

    def test_subtract_one(self):
        """Test subtracting one period."""
        v1 = ViirsModisTimeStep(2024, 10)
        v2 = v1 - 1
        
        assert v2.step == 9
        assert v2.year == 2024

    def test_subtract_crossing_year(self):
        """Test subtraction crossing year boundary."""
        v1 = ViirsModisTimeStep(2024, 2)
        v2 = v1 - 5
        
        assert v2.year == 2023
        assert v2.step == 43  # 46 - 5 + 2 = 43


class TestViirsModisTimeStepUnit:
    """Test suite for VIIRS/MODIS unit."""

    def test_unit(self):
        """Test that VIIRS has correct unit code."""
        assert ViirsModisTimeStep.unit == 'v'


class TestFixedDOYTimeStepCustom:
    """Test suite for custom FixedDOYTimeStep instances."""

    def test_custom_doy_list(self):
        """Test creating timestep with custom DOY list."""
        # Custom list: DOYs 1, 15, 30, 45, etc.
        custom_doys = [1, 15, 30, 45, 60, 75]
        ts = FixedDOYTimeStep.from_step(2024, 1, custom_doys)
        
        assert ts.year == 2024
        assert ts.step == 1
        assert ts.start_doys == tuple(custom_doys)

    def test_custom_from_date(self):
        """Test from_date with custom DOY list."""
        custom_doys = [1, 15, 30, 45]
        
        # Date in first period (DOY 1-14)
        ts = FixedDOYTimeStep.from_date(datetime.datetime(2024, 1, 10), custom_doys)
        assert ts.step == 1
        
        # Date in second period (DOY 15-29)
        ts = FixedDOYTimeStep.from_date(datetime.datetime(2024, 1, 20), custom_doys)
        assert ts.step == 2

    def test_custom_get_start(self):
        """Test get_start with custom DOY list."""
        custom_doys = [1, 10, 20, 30]
        ts = FixedDOYTimeStep.from_step(2024, 2, custom_doys)
        
        assert ts.start == datetime.datetime(2024, 1, 10)

    def test_custom_get_end(self):
        """Test get_end with custom DOY list."""
        custom_doys = [1, 10, 20, 30]
        
        # Second period (DOY 10-19)
        ts = FixedDOYTimeStep.from_step(2024, 2, custom_doys)
        assert ts.end == datetime.datetime(2024, 1, 19)
        
        # Last period (DOY 30 - end of year)
        ts = FixedDOYTimeStep.from_step(2024, 4, custom_doys)
        assert ts.end == datetime.datetime(2024, 12, 31)

    def test_custom_arithmetic(self):
        """Test arithmetic with custom DOY list."""
        custom_doys = [1, 10, 20, 30]
        
        ts1 = FixedDOYTimeStep.from_step(2024, 1, custom_doys)
        ts2 = ts1 + 1
        
        assert ts2.step == 2
        assert ts2.start_doys == tuple(custom_doys)


class TestFixedDOYEdgeCases:
    """Test suite for edge cases."""

    def test_viirs_period_lengths(self):
        """Test that most VIIRS periods are 8 days."""
        # Check first 45 periods (all should be 8 days except possibly last)
        for step in range(1, 45):
            viirs = ViirsModisTimeStep(2024, step)
            length = (viirs.end - viirs.start).days + 1
            assert length == 8

    def test_viirs_last_period_leap_year(self):
        """Test last VIIRS period in leap year."""
        viirs = ViirsModisTimeStep(2024, 46)
        # Should end on Dec 31
        assert viirs.end.month == 12
        assert viirs.end.day == 31

    def test_viirs_last_period_non_leap_year(self):
        """Test last VIIRS period in non-leap year."""
        viirs = ViirsModisTimeStep(2023, 46)
        # Should end on Dec 31
        assert viirs.end.month == 12
        assert viirs.end.day == 31

    def test_viirs_coverage_full_year(self):
        """Test that VIIRS periods cover the full year."""
        # First period starts on DOY 1
        v1 = ViirsModisTimeStep(2024, 1)
        assert v1.start == datetime.datetime(2024, 1, 1)
        
        # Last period ends on Dec 31
        v46 = ViirsModisTimeStep(2024, 46)
        assert v46.end == datetime.datetime(2024, 12, 31)

    def test_viirs_no_gaps(self):
        """Test that there are no gaps between VIIRS periods."""
        for step in range(1, 46):
            v1 = ViirsModisTimeStep(2024, step)
            v2 = ViirsModisTimeStep(2024, step + 1)
            
            # Next period should start the day after current period ends
            expected_next_start = v1.end + datetime.timedelta(days=1)
            assert v2.start == expected_next_start

    def test_from_date_boundary_dates(self):
        """Test from_date on exact period boundaries."""
        # DOY 1 - start of period 1
        v = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, 1))
        assert v.step == 1
        
        # DOY 9 - start of period 2
        v = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, 9))
        assert v.step == 2
        
        # DOY 8 - end of period 1
        v = ViirsModisTimeStep.from_date(datetime.datetime(2024, 1, 8))
        assert v.step == 1


class TestFixedDOYNSteps:
    """Test suite for n_steps property."""

    def test_viirs_n_steps(self):
        """Test that VIIRS has 46 steps."""
        viirs = ViirsModisTimeStep(2024, 1)
        assert viirs.n_steps == 46

    def test_custom_n_steps(self):
        """Test n_steps with custom DOY list."""
        custom_doys = [1, 10, 20, 30, 40]
        ts = FixedDOYTimeStep.from_step(2024, 1, custom_doys)
        assert ts.n_steps == 5
