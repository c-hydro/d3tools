"""Tests for Dataset fallback behavior in get_data()."""

import datetime as dt

import pytest

from d3tools.data import MemoryDataset


class TestDatasetFallbackBehavior:
    """Test fallback delegation when primary dataset cannot resolve data."""
    
    def test_cyclic_fallback_detection(self):
        """Should raise RuntimeError or FileNotFoundError on cyclic fallback."""
        a = MemoryDataset(key_pattern='a_{region}.txt')
        b = MemoryDataset(key_pattern='b_{region}.txt', fallback=a)
        a.fallback = b  # create a cycle

        import pytest
        with pytest.raises((RuntimeError, RecursionError, FileNotFoundError)):
            a.get_data(region='eu')

    def test_deep_fallback_chain_with_missing_intermediate(self):
        """Should find data in a deep fallback chain even if intermediate fallback is empty."""
        c = MemoryDataset(key_pattern='c_{region}.txt')
        b = MemoryDataset(key_pattern='b_{region}.txt', fallback=c)
        a = MemoryDataset(key_pattern='a_{region}.txt', fallback=b)

        c.data_dict['c_eu.txt'] = 'c-data'
        # b has no data

        data = a.get_data(region='eu')
        assert data == 'c-data'

    def test_parent_and_fallback_priority(self, monkeypatch):
        """Fallback is used only if parents cannot provide data."""
        fallback = MemoryDataset(key_pattern='fallback_{region}.txt')
        parent = MemoryDataset(key_pattern='parent_{region}.txt')
        child = MemoryDataset(key_pattern='child_{region}.txt', fallback=fallback)
        child.set_parents({'p': parent}, lambda p: p)

        fallback.data_dict['fallback_eu.txt'] = 'fallback-data'
        parent.data_dict['parent_eu.txt'] = 'parent-data'

        # Should use parent, not fallback
        data = child.get_data(region='eu')
        assert data == 'parent-data'

        # Remove parent data, should now use fallback
        parent.data_dict.clear()
        data = child.get_data(region='eu')
        assert data == 'fallback-data'

    def test_get_data_without_tile_on_tiled_dataset(self):
        """Should raise if get_data is called without tile on a tiled dataset."""
        ds = MemoryDataset(key_pattern='data_{tile}.txt')
        ds.data_dict['data_a.txt'] = 'a'
        import pytest
        with pytest.raises(Exception):
            ds.get_data()
    

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
        fallback.data_dict['fallback_eu.txt'] = 'fallback-data'
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
