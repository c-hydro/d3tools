"""Comprehensive tests for timestep estimation functionality."""

import datetime
import pytest
from d3tools.timestepping import estimate_timestep, Day, Hour, Month, Year, Dekad
from d3tools.timestepping.timeperiods.fixed_doy_timestep import ViirsModisTimeStep


class TestEstimateTimestep:
    """Test suite for estimate_timestep function."""

    def test_estimate_daily(self):
        """Test estimating daily timesteps."""
        # Daily timesteps
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
            datetime.datetime(2024, 1, 5),
        ]
        
        result = estimate_timestep(dates)
        assert result == Day

    def test_estimate_hourly(self):
        """Test estimating hourly timesteps."""
        # Hourly timesteps
        dates = [
            datetime.datetime(2024, 1, 1, 0),
            datetime.datetime(2024, 1, 1, 1),
            datetime.datetime(2024, 1, 1, 2),
            datetime.datetime(2024, 1, 1, 3),
            datetime.datetime(2024, 1, 1, 4),
        ]
        
        result = estimate_timestep(dates)
        assert result == Hour

    def test_estimate_monthly(self):
        """Test estimating monthly timesteps."""
        # Monthly timesteps
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 2, 1),
            datetime.datetime(2024, 3, 1),
            datetime.datetime(2024, 4, 1),
            datetime.datetime(2024, 5, 1),
        ]
        
        result = estimate_timestep(dates)
        assert result == Month

    def test_estimate_yearly(self):
        """Test estimating yearly timesteps."""
        # Yearly timesteps
        dates = [
            datetime.datetime(2020, 1, 1),
            datetime.datetime(2021, 1, 1),
            datetime.datetime(2022, 1, 1),
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2024, 1, 1),
        ]
        
        result = estimate_timestep(dates)
        assert result == Year

    def test_estimate_dekadly(self):
        """Test estimating dekadly timesteps."""
        # Dekadly timesteps (10-day periods)
        dates = [
            datetime.datetime(2024, 1, 1),   # 1st dekad of Jan
            datetime.datetime(2024, 1, 11),  # 2nd dekad of Jan
            datetime.datetime(2024, 1, 21),  # 3rd dekad of Jan
            datetime.datetime(2024, 2, 1),   # 1st dekad of Feb
            datetime.datetime(2024, 2, 11),  # 2nd dekad of Feb
        ]
        
        result = estimate_timestep(dates)
        assert result == Dekad

    def test_estimate_viirs(self):
        """Test estimating VIIRS/MODIS timesteps."""
        # 8-day VIIRS periods
        dates = [
            datetime.datetime(2024, 1, 1),   # DOY 1
            datetime.datetime(2024, 1, 9),   # DOY 9
            datetime.datetime(2024, 1, 17),  # DOY 17
            datetime.datetime(2024, 1, 25),  # DOY 25
            datetime.datetime(2024, 2, 2),   # DOY 33
        ]
        
        result = estimate_timestep(dates)
        assert result == ViirsModisTimeStep

    def test_estimate_with_few_samples(self):
        """Test that estimation works with minimal sample size."""
        # Just 2 dates
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 2),
        ]
        
        result = estimate_timestep(dates)
        assert result == Day

    def test_estimate_with_single_sample(self):
        """Test that single sample returns None."""
        dates = [datetime.datetime(2024, 1, 1)]
        
        result = estimate_timestep(dates)
        assert result is None

    def test_estimate_with_empty_list(self):
        """Test that empty list returns None."""
        dates = []
        
        result = estimate_timestep(dates)
        assert result is None

    def test_estimate_unsorted_dates(self):
        """Test that unsorted dates are handled."""
        # Unsorted daily dates
        dates = [
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 4),
            datetime.datetime(2024, 1, 2),
        ]
        
        result = estimate_timestep(dates)
        assert result == Day

    def test_estimate_with_irregular_spacing(self):
        """Test estimation with some irregular spacing."""
        # Mostly monthly but with one different
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 2, 1),
            datetime.datetime(2024, 3, 1),
            datetime.datetime(2024, 4, 1),
            datetime.datetime(2024, 4, 15),  # Irregular
            datetime.datetime(2024, 5, 1),
            datetime.datetime(2024, 6, 1),
        ]
        
        # Should still identify as monthly (mode = ~30 days)
        result = estimate_timestep(dates)
        assert result == Month

    def test_estimate_monthly_end_of_month(self):
        """Test monthly estimation with end-of-month dates."""
        # Monthly timesteps at month ends
        dates = [
            datetime.datetime(2024, 1, 31),
            datetime.datetime(2024, 2, 29),  # Leap year
            datetime.datetime(2024, 3, 31),
            datetime.datetime(2024, 4, 30),
        ]
        
        result = estimate_timestep(dates)
        # Should recognize as monthly despite varying day counts
        assert result == Month

    def test_estimate_dekad_vs_viirs(self):
        """Test distinguishing between dekads and VIIRS."""
        # Dekads (10-day) - but note Feb 28 would trigger Dekad
        dates_dekad = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 11),
            datetime.datetime(2024, 1, 21),
            datetime.datetime(2024, 2, 1),
        ]
        
        result = estimate_timestep(dates_dekad)
        assert result == Dekad
        
        # VIIRS (8-day)
        dates_viirs = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 9),
            datetime.datetime(2024, 1, 17),
        ]
        
        result = estimate_timestep(dates_viirs)
        assert result == ViirsModisTimeStep

    def test_estimate_leap_year_considerations(self):
        """Test estimation considers leap years."""
        # Yearly with leap year
        dates = [
            datetime.datetime(2020, 1, 1),  # Leap year
            datetime.datetime(2021, 1, 1),  # 366 days later
            datetime.datetime(2022, 1, 1),  # 365 days later
        ]
        
        result = estimate_timestep(dates)
        # Should still recognize as yearly despite different day counts
        assert result == Year

    def test_estimate_unknown_frequency(self):
        """Test that unknown frequencies return None."""
        # 3-day frequency (not recognized)
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 4),
            datetime.datetime(2024, 1, 7),
            datetime.datetime(2024, 1, 10),
        ]
        
        result = estimate_timestep(dates)
        assert result is None

    def test_estimate_very_short_intervals(self):
        """Test with sub-hourly intervals."""
        # 30-minute intervals (not hourly)
        dates = [
            datetime.datetime(2024, 1, 1, 0, 0),
            datetime.datetime(2024, 1, 1, 0, 30),
            datetime.datetime(2024, 1, 1, 1, 0),
            datetime.datetime(2024, 1, 1, 1, 30),
        ]
        
        result = estimate_timestep(dates)
        # Should return None for unsupported frequency
        assert result is None

    def test_estimate_mixed_months_28_31_days(self):
        """Test monthly detection with varying month lengths."""
        # Months with 28, 30, 31 days
        dates = [
            datetime.datetime(2023, 12, 1),  # 31 days
            datetime.datetime(2024, 1, 1),   # 31 days
            datetime.datetime(2024, 2, 1),   # 29 days (leap)
            datetime.datetime(2024, 3, 1),   # 31 days
            datetime.datetime(2024, 4, 1),   # 30 days
        ]
        
        result = estimate_timestep(dates)
        # Most common interval should be ~30 days, identifying as Month
        assert result == Month


class TestEstimateTimestepEdgeCases:
    """Test suite for edge cases in timestep estimation."""

    def test_estimate_with_duplicates(self):
        """Test estimation with duplicate timestamps."""
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 1),  # Duplicate
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
        ]
        
        # Should handle duplicates (diff = 0)
        result = estimate_timestep(dates)
        # Depending on mode calculation, might be Day or None
        assert result is not None

    def test_estimate_all_same_date(self):
        """Test estimation when all dates are the same."""
        dates = [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 1),
        ]
        
        result = estimate_timestep(dates)
        # All differences are 0, can't estimate
        assert result is None

    def test_estimate_large_sample(self):
        """Test estimation with large sample."""
        # 100 daily dates
        base = datetime.datetime(2024, 1, 1)
        dates = [base + datetime.timedelta(days=i) for i in range(100)]
        
        result = estimate_timestep(dates)
        assert result == Day

    def test_estimate_sparse_yearly(self):
        """Test yearly estimation with sparse sampling."""
        dates = [
            datetime.datetime(2020, 6, 15),
            datetime.datetime(2021, 6, 15),
            datetime.datetime(2022, 6, 15),
        ]
        
        result = estimate_timestep(dates)
        assert result == Year
