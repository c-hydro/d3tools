"""Tests for Dataset fallback behavior in get_data()."""

import datetime as dt

import pytest

from d3tools.data import MemoryDataset


class TestDatasetFallbackBehavior:
    """Test fallback delegation when primary dataset cannot resolve data."""

    def test_primary_data_is_used_when_available(self):
        """Primary dataset should return its own data without using fallback."""
        fallback = MemoryDataset(key_pattern='fallback_{region}.txt')
        primary = MemoryDataset(key_pattern='primary_{region}.txt', fallback=fallback)

        primary.data_dict['primary_eu.txt'] = 'primary-data'
        fallback.data_dict['fallback_eu.txt'] = 'fallback-data'

        data = primary.get_data(region='eu')

        assert data == 'primary-data'
        assert 'fallback_eu.txt' in fallback.data_dict

    def test_fallback_is_used_when_primary_data_is_missing(self):
        """When primary data is missing, fallback dataset should provide it."""
        fallback = MemoryDataset(key_pattern='fallback_{region}.txt')
        primary = MemoryDataset(key_pattern='primary_{region}.txt', fallback=fallback)

        fallback.data_dict['fallback_eu.txt'] = 'fallback-data'

        data = primary.get_data(region='eu')

        assert data == 'fallback-data'

    def test_chained_fallbacks_are_supported(self):
        """Fallbacks should chain naturally (A -> B -> C)."""
        third = MemoryDataset(key_pattern='third_{region}.txt')
        second = MemoryDataset(key_pattern='second_{region}.txt', fallback=third)
        first = MemoryDataset(key_pattern='first_{region}.txt', fallback=second)

        third.data_dict['third_eu.txt'] = 'third-data'

        data = first.get_data(region='eu')

        assert data == 'third-data'

    def test_fallback_receives_same_arguments(self, monkeypatch):
        """Fallback get_data should receive time/as_is/kwargs unchanged."""
        fallback = MemoryDataset(key_pattern='fallback_{region}.txt')
        primary = MemoryDataset(key_pattern='primary_{region}.txt', fallback=fallback)

        called = {}

        def _mock_get_data(time=None, as_is=False, **kwargs):
            called['time'] = time
            called['as_is'] = as_is
            called['kwargs'] = kwargs
            return 'from-fallback'

        monkeypatch.setattr(fallback, 'get_data', _mock_get_data)

        requested_time = dt.datetime(2024, 1, 1)
        data = primary.get_data(time=requested_time, as_is=True, region='eu', source='x')

        assert data == 'from-fallback'
        assert called['time'] == requested_time
        assert called['as_is'] is True
        assert called['kwargs'] == {'region': 'eu', 'source': 'x'}

    def test_update_and_copy_preserve_fallback(self):
        """update/copy should keep fallback and apply update substitutions."""
        fallback = MemoryDataset(key_pattern='fallback_{region}.txt')
        primary = MemoryDataset(key_pattern='primary_{region}.txt', fallback=fallback)

        updated = primary.update(region='eu')
        copied = primary.copy()

        assert hasattr(updated, 'fallback')
        assert updated.fallback.key_pattern == 'fallback_eu.txt'

        assert hasattr(copied, 'fallback')
        assert copied.fallback.key_pattern == 'fallback_{region}.txt'

    def test_raises_when_neither_primary_nor_fallback_has_data(self):
        """Should raise FileNotFoundError when nothing can resolve the data."""
        fallback = MemoryDataset(key_pattern='fallback_{region}.txt')
        primary = MemoryDataset(key_pattern='primary_{region}.txt', fallback=fallback)

        with pytest.raises(FileNotFoundError):
            primary.get_data(region='eu')
