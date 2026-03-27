"""
Tests for Dataset key/time parsing behavior.

Focuses on Dataset-specific responsibilities after logic centralization
in KeyParser:
- step-length inference in Dataset.get_time_signature()
- integration wiring in Dataset.get_key()
"""
import datetime as dt

from d3tools.data import LocalDataset
from d3tools.timestepping import Day, Dekad


class TestDatasetGetTimeSignature:
    """Test Dataset.get_time_signature behavior."""

    def test_get_time_signature_none_returns_none(self):
        """Test None input returns None."""
        dataset = LocalDataset(path='/data', file='output_%Y%m%d.tif')
        assert dataset.get_time_signature(None) is None

    def test_get_time_signature_uses_dataset_timestep_to_derive_length(self):
        """Test datetime branch uses dataset.timestep-derived length for normalization."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif',
            timestep='m'
        )

        parsed_time = dataset.get_time_signature(dt.datetime(2024, 2, 29))
        assert parsed_time == dt.datetime(2024, 2, 28)

    def test_get_time_signature_first_datetime_call_without_length_keeps_leap_day(self):
        """Test first datetime call keeps leap-day when no length can be inferred."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif'
        )

        parsed_time = dataset.get_time_signature(dt.datetime(2024, 2, 29))
        assert parsed_time.day == 29
        assert parsed_time.month == 2

    def test_get_time_signature_uses_previous_requested_time_when_timestep_missing(self):
        """Test datetime branch falls back to previous_requested_time length inference."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif'
        )

        # First call seeds previous_requested_time; no length info yet.
        dataset.get_time_signature(dt.datetime(2024, 1, 30))

        # Second call infers a multi-day length from previous_requested_time.
        parsed_time = dataset.get_time_signature(dt.datetime(2024, 2, 29))
        assert parsed_time == dt.datetime(2024, 2, 28)

    def test_get_time_signature_timestep_still_uses_dataset_time_signature(self):
        """Test timestep branch still honors dataset time_signature setting."""
        dataset = LocalDataset(
            path='/data',
            file='output_%Y%m%d.tif',
            time_signature='end+1'
        )
        timestep = Dekad.from_date(dt.datetime(2024, 2, 20))

        parsed_time = dataset.get_time_signature(timestep)
        assert parsed_time == (timestep + 1).start


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

    def test_get_key_uses_inferred_length_from_previous_requested_time(self):
        """Test get_key uses Dataset's previous_requested_time-based length fallback."""
        dataset = LocalDataset(path='/data', file='output_%m%d.tif')

        # Seed previous_requested_time through a first call.
        dataset.get_key(dt.datetime(2024, 1, 30))
        key = dataset.get_key(dt.datetime(2024, 2, 29))

        assert key == '/data/output_0228.tif'

    def test_get_key_applies_leap_day_adjustment_from_dataset_timestep(self):
        """Test get_key applies leap-day adjustment when dataset timestep is set."""
        dataset = LocalDataset(
            path='/data',
            file='output_%m%d.tif',
            timestep='m'
        )

        key = dataset.get_key(dt.datetime(2024, 2, 29))
        assert key == '/data/output_0228.tif'
