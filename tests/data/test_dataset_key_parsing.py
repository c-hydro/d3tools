"""
Tests for Dataset key/time parsing behavior.

Focuses on Dataset.get_time_signature() and Dataset.get_key() semantics,
including legacy adjustments that are currently outside KeyParser.
"""
import datetime as dt

import pytest

from d3tools.data import LocalDataset
from d3tools.timestepping import Day, Dekad


class TestDatasetGetTimeSignature:
    """Test Dataset.get_time_signature behavior."""

    def test_get_time_signature_none_returns_none(self):
        """Test None input returns None."""
        dataset = LocalDataset(path='/data', file='output_%Y%m%d.tif')
        assert dataset.get_time_signature(None) is None

    @pytest.mark.parametrize(
        "signature, expected_resolver",
        [
            ('start', lambda ts: ts.start),
            ('end', lambda ts: ts.end),
            ('end+1', lambda ts: (ts + 1).start),
        ],
    )
    def test_get_time_signature_from_timestep_uses_configured_signature(self, signature, expected_resolver):
        """Test signature mapping for TimeStep input."""
        dataset = LocalDataset(
            path='/data',
            file='output_%Y%m%d_%H%M%S.tif',
            time_signature=signature
        )
        timestep = Dekad.from_date(dt.datetime(2024, 2, 20))

        parsed_time = dataset.get_time_signature(timestep)
        assert parsed_time == expected_resolver(timestep)

    @pytest.mark.parametrize(
        "key_pattern, expected",
        [
            ('output_%Y%m%d_%H%M%S.tif', dt.datetime(2024, 7, 19, 13, 45, 27)),
            ('output_%Y%m%d_%H%M.tif', dt.datetime(2024, 7, 19, 13, 45, 0)),
            ('output_%Y%m%d_%H.tif', dt.datetime(2024, 7, 19, 13, 0, 0)),
            ('output_%Y%m%d.tif', dt.datetime(2024, 7, 19, 0, 0, 0)),
            ('output_%Y%m.tif', dt.datetime(2024, 7, 1, 0, 0, 0)),
            ('output_%Y.tif', dt.datetime(2024, 1, 1, 0, 0, 0)),
        ],
    )
    def test_get_time_signature_progressively_removes_unused_datetime_components(self, key_pattern, expected):
        """Test datetime normalization based on placeholders present in key pattern."""
        dataset = LocalDataset(path='/data', file=key_pattern)

        parsed_time = dataset.get_time_signature(dt.datetime(2024, 7, 19, 13, 45, 27))
        assert parsed_time == expected

    def test_get_time_signature_leap_day_is_adjusted_without_year_for_multiday_length(self):
        """Test Feb-29 is converted to Feb-28 for non-year patterns and multiday steps."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif',
            timestep='m'
        )

        parsed_time = dataset.get_time_signature(dt.datetime(2024, 2, 29))
        assert parsed_time == dt.datetime(2024, 2, 28)

    def test_get_time_signature_leap_day_kept_when_year_present(self):
        """Test Feb-29 is preserved when year is part of key pattern."""
        dataset = LocalDataset(
            path='/data',
            file='output_%Y%m%d.tif',
            timestep='m'
        )

        parsed_time = dataset.get_time_signature(dt.datetime(2024, 2, 29))
        assert parsed_time.day == 29
        assert parsed_time.month == 2

    def test_get_time_signature_leap_day_kept_for_daily_length_without_year(self):
        """Test Feb-29 is preserved for daily timesteps even if year is not in pattern."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif',
            timestep='daily'
        )

        parsed_time = dataset.get_time_signature(dt.datetime(2024, 2, 29))
        assert parsed_time.day == 29
        assert parsed_time.month == 2


class TestDatasetGetKey:
    """Test Dataset.get_key integration with time signature and tags."""

    def test_get_key_applies_end_plus_one_for_timestep(self):
        """Test key rendering uses end+1 signature for TimeStep input."""
        dataset = LocalDataset(
            path='/data',
            file='output_%Y%m%d_{tile}.tif',
            time_signature='end+1'
        )
        timestep = Day.from_date(dt.datetime(2024, 1, 1))

        key = dataset.get_key(timestep, tile='h18v04')
        assert key == '/data/output_20240102_h18v04.tif'

    def test_get_key_applies_leap_day_adjustment_from_get_time_signature(self):
        """Test get_key keeps legacy leap-day adjustment behavior."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif',
            timestep='m'
        )

        key = dataset.get_key(dt.datetime(2024, 2, 29))
        assert key == '/data/output_0228.tif'
