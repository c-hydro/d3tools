"""Tests for file_version auto-increment in Dataset.write_data()."""

import pytest

from d3tools.data import MemoryDataset


class TestWriteDataVersioning:
    """Test that write_data auto-increments file_version when has_version is True."""

    def _make_ds(self, pattern='output_{file_version}.txt'):
        return MemoryDataset(key_pattern=pattern, keep_after_reading=True)

    def test_first_write_uses_default_version(self):
        """When no prior versions exist, file_version defaults to '01'."""
        ds = self._make_ds()
        ds.write_data('content')
        assert 'output_01.txt' in ds.data_dict

    def test_second_write_increments_version(self):
        """When version '01' exists, the next write uses '02'."""
        ds = self._make_ds()
        ds.data_dict['output_01.txt'] = 'old content'
        ds.write_data('new content')
        assert 'output_02.txt' in ds.data_dict
        assert 'output_01.txt' in ds.data_dict  # original preserved

    def test_explicit_version_is_not_overridden(self):
        """Explicitly passing file_version bypasses auto-increment."""
        ds = self._make_ds()
        ds.data_dict['output_01.txt'] = 'old content'
        ds.write_data('pinned content', file_version='99')
        assert 'output_99.txt' in ds.data_dict
        assert 'output_02.txt' not in ds.data_dict

    def test_version_increments_from_highest(self):
        """Auto-increment always picks the highest existing version."""
        ds = self._make_ds()
        ds.data_dict['output_01.txt'] = 'v1'
        ds.data_dict['output_02.txt'] = 'v2'
        ds.data_dict['output_03.txt'] = 'v3'
        ds.write_data('v4')
        assert 'output_04.txt' in ds.data_dict

    def test_dataset_without_version_tag_is_unaffected(self):
        """Datasets whose key_pattern lacks {file_version} are unchanged."""
        ds = MemoryDataset(key_pattern='output_{region}.txt', keep_after_reading=True)
        ds.write_data('data', region='eu')
        assert 'output_eu.txt' in ds.data_dict

    def test_versioned_write_with_other_tags(self):
        """Version auto-increment works correctly alongside other tag substitutions."""
        ds = MemoryDataset(
            key_pattern='output_{region}_{file_version}.txt',
            keep_after_reading=True,
        )
        ds.data_dict['output_eu_01.txt'] = 'old'
        ds.write_data('new', region='eu')
        assert 'output_eu_02.txt' in ds.data_dict

    def test_versioned_write_isolated_per_tag(self):
        """Versions for different tag values are tracked independently."""
        ds = MemoryDataset(
            key_pattern='output_{region}_{file_version}.txt',
            keep_after_reading=True,
        )
        # 'eu' already has version 03, 'as' has version 01
        ds.data_dict['output_eu_01.txt'] = 'eu-v1'
        ds.data_dict['output_eu_02.txt'] = 'eu-v2'
        ds.data_dict['output_eu_03.txt'] = 'eu-v3'
        ds.data_dict['output_as_01.txt'] = 'as-v1'

        ds.write_data('eu-v4', region='eu')
        ds.write_data('as-v2', region='as')

        assert 'output_eu_04.txt' in ds.data_dict
        assert 'output_as_02.txt' in ds.data_dict


class TestGetDataVersioningUnchanged:
    """Verify that existing get_data version logic is not broken."""

    def test_get_data_reads_latest_version(self):
        """get_data without file_version picks the alphabetically latest version."""
        ds = MemoryDataset(
            key_pattern='output_{file_version}.txt',
            keep_after_reading=True,
        )
        ds.data_dict['output_01.txt'] = 'v1'
        ds.data_dict['output_02.txt'] = 'v2'
        data = ds.get_data()
        assert data == 'v2'

    def test_get_data_explicit_version_is_honoured(self):
        """get_data with an explicit file_version reads exactly that version."""
        ds = MemoryDataset(
            key_pattern='output_{file_version}.txt',
            keep_after_reading=True,
        )
        ds.data_dict['output_01.txt'] = 'v1'
        ds.data_dict['output_02.txt'] = 'v2'
        data = ds.get_data(file_version='01')
        assert data == 'v1'
