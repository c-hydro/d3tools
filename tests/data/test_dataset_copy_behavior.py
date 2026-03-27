"""
Tests for Dataset.copy() behavior across all subclasses.

These tests lock in the copy() behavior to ensure:
1. Creates independent instances with separate tags
2. Shares managers (template_manager, log, thumbnail) correctly
3. Preserves all subclass-specific attributes
4. Works correctly with FormatMixins
5. Handles template parameter correctly
"""

import pytest
from unittest.mock import Mock, patch
import tempfile

from d3tools.data import LocalDataset, MemoryDataset
from d3tools.data.datasets.remote_dataset import S3Dataset, OVHS3Dataset, SFTPDataset


class TestBasicCopyBehavior:
    """Test basic copy() behavior for all Dataset types."""

    def test_copy_creates_new_instance(self):
        """Test that copy() creates a new Dataset instance."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        copied = ds.copy()

        assert copied is not ds
        assert id(copied) != id(ds)

    def test_copy_preserves_key_pattern(self):
        """Test that key_pattern is preserved."""
        ds = LocalDataset(
            path='/data/region',
            filename='output.tif'
        )

        copied = ds.copy()

        assert copied.key_pattern == ds.key_pattern

    def test_copy_preserves_name(self):
        """Test that name is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            name='my_dataset'
        )

        copied = ds.copy()

        assert copied.name == ds.name

    def test_copy_preserves_format(self):
        """Test that format is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.nc',
            format='netcdf'
        )

        copied = ds.copy()

        assert copied.format == ds.format

    def test_copy_preserves_time_signature(self):
        """Test that time_signature is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            time_signature='start'
        )

        copied = ds.copy()

        assert copied.time_signature == ds.time_signature

    def test_copy_preserves_timestep(self):
        """Test that timestep is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            timestep='daily'
        )

        copied = ds.copy()

        assert copied.timestep == ds.timestep

    def test_copy_preserves_aggregation(self):
        """Test that aggregation is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            timestep='daily',
            aggregation='7d'
        )

        copied = ds.copy()

        assert copied.agg == ds.agg

    def test_copy_preserves_nan_value(self):
        """Test that nan_value is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            nan_value=-9999
        )

        copied = ds.copy()

        assert copied.nan_value == ds.nan_value


class TestCopyIndependentTags:
    """Test that copy() creates independent tags."""

    def test_copy_copies_existing_tags(self):
        """Test that copy COPIES existing tags (via update())."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.tags['region'] = 'eu'
        ds.tags['var'] = 'tp'

        copied = ds.copy()

        # Tags are copied
        assert copied.tags == {'region': 'eu', 'var': 'tp'}
        # But they are independent (different dict)
        assert copied.tags is not ds.tags

    def test_copied_tags_are_independent(self):
        """Test that modifying copied dataset's tags doesn't affect original."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        c1 = ds.copy()
        c2 = ds.copy()

        c1.tags['region'] = 'eu'
        c2.tags['region'] = 'us'

        assert c1.tags['region'] == 'eu'
        assert c2.tags['region'] == 'us'
        # Original should not have region since it was added after copy
        assert 'region' not in ds.tags

    def test_multiple_copies_have_independent_tags(self):
        """Test that multiple copies have independent tag dicts."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        copies = [ds.copy() for _ in range(5)]

        for i, copy in enumerate(copies):
            copy.tags['id'] = i

        for i, copy in enumerate(copies):
            assert copy.tags['id'] == i

        assert 'id' not in ds.tags


class TestCopySharedManagers:
    """Test that copy() shares managers correctly."""

    def test_copy_shares_template_manager(self):
        """Test that template_manager is shared between original and copy."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        original_tm = ds.template_manager

        copied = ds.copy()

        assert copied.template_manager is ds.template_manager
        assert copied.template_manager is original_tm

    def test_copy_shares_log_manager(self):
        """Test that log manager is shared."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.log = Mock()  # Mock log manager

        copied = ds.copy()

        assert copied.log is ds.log

    def test_copy_shares_thumbnail_manager(self):
        """Test that thumbnail manager is shared."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.thumbnail = Mock()  # Mock thumbnail manager

        copied = ds.copy()

        assert copied.thumbnail is ds.thumbnail

    def test_multiple_copies_share_same_managers(self):
        """Test that multiple copies all share the same managers."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.template_manager._templates['test'] = {'dummy': 1}
        ds.log = Mock()
        ds.thumbnail = Mock()

        c1 = ds.copy()
        c2 = ds.copy()
        c3 = ds.copy()

        assert c1.template_manager is c2.template_manager is c3.template_manager
        assert c1.log is c2.log is c3.log
        assert c1.thumbnail is c2.thumbnail is c3.thumbnail

    def test_template_changes_visible_to_all_copies(self):
        """Test that changes to shared template_manager are visible to all."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        c1 = ds.copy()
        c2 = ds.copy()

        # Add template via c1
        c1.template_manager._templates['new_template'] = {'bands': ['b1']}

        # Should be visible in original, c1, and c2
        assert 'new_template' in ds.template_manager._templates
        assert 'new_template' in c1.template_manager._templates
        assert 'new_template' in c2.template_manager._templates


class TestCopyTemplateParameter:
    """Test copy(template=True/False) parameter behavior."""

    def test_copy_template_false_shares_template_manager(self):
        """Test that copy(template=False) shares template_manager (default)."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        copied = ds.copy(template=False)

        assert copied.template_manager is ds.template_manager

    def test_copy_template_true_shares_template_manager(self):
        """Test that copy(template=True) also shares template_manager."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        copied = ds.copy(template=True)

        # Based on implementation, template=True still shares (redundant with default behavior)
        assert copied.template_manager is ds.template_manager

    def test_copy_default_shares_template_manager(self):
        """Test that copy() without parameter shares template_manager."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        copied = ds.copy()

        assert copied.template_manager is ds.template_manager


class TestLocalDatasetCopy:
    """Test copy() specific to LocalDataset."""

    def test_copy_preserves_path_and_filename(self):
        """Test that path and filename are preserved."""
        ds = LocalDataset(
            path='/data/files',
            filename='output.tif'
        )

        copied = ds.copy()

        assert copied.dir == ds.dir
        assert copied.file == ds.file
        assert copied.key_pattern == ds.key_pattern

    def test_copy_preserves_tile_names(self):
        """Test that tile_names are preserved."""
        ds = LocalDataset(
            path='/data/{tile}',
            filename='output.tif',
            tile_names=['t1', 't2', 't3']
        )

        copied = ds.copy()

        assert hasattr(copied, '_tile_names')
        assert copied._tile_names == ds._tile_names

    def test_multiple_copies_preserve_directory_structure(self):
        """Test that multiple copies maintain directory structure."""
        ds = LocalDataset(
            path='/data/complex/path/structure',
            filename='file.tif'
        )

        copies = [ds.copy() for _ in range(3)]

        for copy in copies:
            assert copy.dir == '/data/complex/path/structure'
            assert copy.file == 'file.tif'


class TestMemoryDatasetCopy:
    """Test copy() specific to MemoryDataset."""

    def test_copy_preserves_keep_after_reading(self):
        """Test that keep_after_reading flag is preserved."""
        ds = MemoryDataset(
            key_pattern='data_{tile}.txt',
            keep_after_reading=True
        )

        copied = ds.copy()

        assert copied.keep_after_reading is True

    def test_copy_copies_data_dict_for_matching_keys(self):
        """Test that data_dict IS copied for keys matching the pattern."""
        ds = MemoryDataset(
            key_pattern='data.txt'
        )
        ds.write_data('test_content')

        copied = ds.copy()

        # Copy DOES copy data_dict (via update() which filters by matching keys)
        assert len(copied.data_dict) == 1
        assert 'data.txt' in copied.data_dict
        # But it's not the same dict object
        assert copied.data_dict is not ds.data_dict

    def test_memory_dataset_copies_are_independent(self):
        """Test that MemoryDataset copies have independent data storage."""
        ds = MemoryDataset(
            key_pattern='data_{id}.txt'
        )

        c1 = ds.copy()
        c2 = ds.copy()

        c1.write_data('content1', id='1')
        c2.write_data('content2', id='2')

        assert len(c1.data_dict) == 1
        assert len(c2.data_dict) == 1
        assert 'data_1.txt' in c1.data_dict
        assert 'data_2.txt' in c2.data_dict
        assert 'data_1.txt' not in c2.data_dict


class TestRemoteDatasetCopy:
    """Test copy() for remote Dataset subclasses."""

    @patch('boto3.Session')
    def test_s3_dataset_copy_preserves_bucket_config(self, mock_session):
        """Test that S3Dataset copy preserves bucket configuration."""
        mock_session.return_value.client.return_value = Mock()

        ds = S3Dataset(
            key_pattern='data/output.tif',
            bucket_name='my-bucket',
            region_name='us-east-1',
            profile_name='my-profile'
        )

        copied = ds.copy()

        assert copied.bucket_name == ds.bucket_name
        assert copied.region_name == ds.region_name
        assert copied.profile_name == ds.profile_name

    @patch('boto3.Session')
    def test_s3_dataset_copy_preserves_tmp_dir(self, mock_session):
        """Test that S3Dataset copy preserves tmp_dir."""
        mock_session.return_value.client.return_value = Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            ds = S3Dataset(
                key_pattern='data/output.tif',
                bucket_name='my-bucket',
                tmp_dir=tmpdir
            )

            copied = ds.copy()

            assert copied.tmp_dir == ds.tmp_dir

    @patch('boto3.Session')
    def test_ovh_s3_dataset_copy_preserves_endpoint(self, mock_session):
        """Test that OVHS3Dataset copy preserves endpoint_url."""
        mock_client = Mock()
        mock_session.return_value.client.return_value = mock_client
        mock_session.return_value.get_credentials.return_value.get_frozen_credentials.return_value = Mock(
            access_key='key', secret_key='secret'
        )

        ds = OVHS3Dataset(
            key_pattern='data/output.tif',
            bucket_name='my-bucket',
            endpoint_url='https://s3.ovh.example.com'
        )

        copied = ds.copy()

        assert copied.endpoint_url == ds.endpoint_url
        assert copied.bucket_name == ds.bucket_name

    @patch('d3tools.data.datasets.remote_dataset.SFTPDataset._connect')
    def test_sftp_dataset_copy_preserves_connection_params(self, mock_connect):
        """Test that SFTPDataset copy preserves connection parameters."""
        mock_connect.return_value = Mock()

        ds = SFTPDataset(
            key_pattern='data/output.tif',
            host='sftp.example.com',
            username='user',
            password='pass',
            port=2222
        )

        copied = ds.copy()

        assert copied.hostname == ds.hostname
        assert copied.username == ds.username
        assert copied.password == ds.password
        assert copied.port == ds.port


class TestCopyWithFormatMixins:
    """Test that copy() works correctly with different format mixins."""

    def test_copy_with_geotiff_format(self):
        """Test copy with GeoTIFF format."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.template_manager._templates['test'] = {'bands': ['b1']}

        copied = ds.copy()

        assert copied.format == 'geotiff'
        assert hasattr(copied, 'template_manager')
        assert copied.template_manager is ds.template_manager

    def test_copy_with_netcdf_format(self):
        """Test copy with NetCDF format."""
        ds = LocalDataset(
            path='/data',
            filename='output.nc'
        )

        copied = ds.copy()

        assert copied.format == 'netcdf'
        assert hasattr(copied, 'template_manager')

    def test_copy_preserves_format_across_types(self):
        """Test that different formats are preserved correctly."""
        formats = [
            ('output.tif', 'geotiff'),
            ('output.nc', 'netcdf'),
            ('output.txt', 'txt'),
            ('output.csv', 'csv'),
        ]

        for filename, expected_format in formats:
            ds = LocalDataset(path='/data', filename=filename)
            copied = ds.copy()
            assert copied.format == expected_format


class TestCopyEdgeCases:
    """Test edge cases and special scenarios for copy()."""

    def test_copy_of_copy(self):
        """Test that copying a copy works correctly."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        c1 = ds.copy()
        c2 = c1.copy()

        assert c2 is not c1
        assert c2 is not ds
        assert c2.template_manager is c1.template_manager is ds.template_manager

    def test_copy_chain(self):
        """Test a chain of copies."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.log = Mock()

        c1 = ds.copy()
        c2 = c1.copy()
        c3 = c2.copy()

        # All should share managers
        assert c3.template_manager is ds.template_manager
        assert c3.log is ds.log

        # All should have independent tags
        c1.tags['level'] = 1
        c2.tags['level'] = 2
        c3.tags['level'] = 3

        assert c1.tags['level'] == 1
        assert c2.tags['level'] == 2
        assert c3.tags['level'] == 3

    def test_copy_without_optional_managers(self):
        """Test copy when log/thumbnail are not set."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        # Don't set log or thumbnail

        copied = ds.copy()

        assert not hasattr(copied, 'log')
        assert not hasattr(copied, 'thumbnail')
        # But template_manager should still be shared
        assert copied.template_manager is ds.template_manager

    def test_copy_preserves_creation_kwargs(self):
        """Test that copy preserves _creation_kwargs."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            name='test_ds',
            time_signature='start',
            nan_value=-9999
        )

        copied = ds.copy()

        assert copied._creation_kwargs == ds._creation_kwargs
        assert copied._creation_kwargs is not ds._creation_kwargs  # Should be a copy

    def test_copy_with_parents_attribute(self):
        """Test copy when dataset has parents (derived datasets)."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        # Simulate a derived dataset scenario
        ds.parents = {
            'p1': LocalDataset(path='/parent1', filename='p1.tif'),
            'p2': LocalDataset(path='/parent2', filename='p2.tif')
        }
        ds.fn = Mock()

        copied = ds.copy()

        # Parents should be preserved
        assert hasattr(copied, 'parents')
        # Tags should still be independent
        copied.tags['test'] = 'value'
        assert 'test' not in ds.tags


class TestCopyVsUpdate:
    """Test the relationship between copy() and update()."""

    def test_copy_is_equivalent_to_update_with_no_args(self):
        """Test that copy() behaves like update() with shared managers."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )
        ds.log = Mock()
        ds.thumbnail = Mock()

        copied = ds.copy()
        updated = ds.update()

        # Both create new instances
        assert copied is not ds
        assert updated is not ds

        # Copy shares managers, update shares template_manager via update() 
        assert copied.template_manager is ds.template_manager
        assert updated.template_manager is ds.template_manager

        # Copy explicitly shares log/thumbnail
        assert copied.log is ds.log
        assert copied.thumbnail is ds.thumbnail

    def test_copy_after_update(self):
        """Test copying an updated dataset."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )
        ds.log = Mock()

        updated = ds.update(region='eu')
        copied = updated.copy()

        assert copied.key_pattern == updated.key_pattern
        # update() doesn't preserve log, so updated doesn't have it
        assert not hasattr(updated, 'log')
        # But copy() can't share what doesn't exist
        assert not hasattr(copied, 'log')
        # copy copies tags
        assert copied.tags == {'region': 'eu'}

    def test_copy_preserves_log_but_update_does_not(self):
        """Test that copy() preserves log but update() doesn't."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )
        ds.log = Mock()
        ds.thumbnail = Mock()

        # copy() preserves log and thumbnail
        copied = ds.copy()
        assert copied.log is ds.log
        assert copied.thumbnail is ds.thumbnail

        # update() does NOT preserve log and thumbnail
        updated = ds.update(region='eu')
        assert not hasattr(updated, 'log')
        assert not hasattr(updated, 'thumbnail')

    def test_update_after_copy(self):
        """Test updating a copied dataset."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )

        copied = ds.copy()
        updated = copied.update(region='eu')

        assert updated.key_pattern == '/data/eu/output.tif'
        assert updated.template_manager is ds.template_manager
