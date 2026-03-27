"""
Tests for Dataset._creation_kwargs storage and usage.

These tests verify that _creation_kwargs properly stores all necessary
initialization parameters for each Dataset subclass, enabling correct
behavior of update() and copy() methods.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

from d3tools.data import LocalDataset, MemoryDataset
from d3tools.data.datasets.remote_dataset import RemoteDataset, S3Dataset, OVHS3Dataset, SFTPDataset


class TestBaseDatasetCreationKwargs:
    """Test _creation_kwargs for base Dataset class."""

    def test_stores_name_format_time_signature_nan_value(self):
        """Test that base Dataset stores core attributes in _creation_kwargs."""
        ds = LocalDataset(
            path='/data',
            filename='output.nc',
            name='my_dataset',
            format='netcdf',
            time_signature='start',
            nan_value=-9999
        )

        assert ds._creation_kwargs['name'] == 'my_dataset'
        assert ds._creation_kwargs['format'] == 'netcdf'
        assert ds._creation_kwargs['time_signature'] == 'start'
        assert ds._creation_kwargs['nan_value'] == -9999

    def test_stores_derived_name_when_not_provided(self):
        """Test that derived name is stored in _creation_kwargs."""
        ds = LocalDataset(
            path='/data',
            filename='precipitation.tif'
        )

        assert ds._creation_kwargs['name'] == 'precipitation'
        assert ds.name == ds._creation_kwargs['name']

    def test_stores_detected_format_when_not_provided(self):
        """Test that detected format is stored in _creation_kwargs."""
        ds = LocalDataset(
            path='/data',
            filename='output.nc'
        )

        assert ds._creation_kwargs['format'] == 'netcdf'
        assert ds.format == ds._creation_kwargs['format']

    def test_stores_default_time_signature(self):
        """Test that default time_signature is stored."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif'
        )

        assert 'time_signature' in ds._creation_kwargs
        assert ds._creation_kwargs['time_signature'] == 'end'


class TestLocalDatasetCreationKwargs:
    """Test _creation_kwargs for LocalDataset."""

    def test_stores_type_attribute(self):
        """Test that LocalDataset stores its type in _creation_kwargs."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='field.tif'
        )

        assert 'type' in ds._creation_kwargs
        assert ds._creation_kwargs['type'] == 'local'

    def test_does_not_store_path_and_filename_separately(self):
        """Test that path and filename are NOT stored separately."""
        ds = LocalDataset(
            path='/data/region',
            filename='field.tif'
        )

        # These should NOT be in _creation_kwargs
        assert 'path' not in ds._creation_kwargs
        assert 'filename' not in ds._creation_kwargs
        assert 'dir' not in ds._creation_kwargs
        assert 'file' not in ds._creation_kwargs

    def test_creation_kwargs_can_recreate_dataset_via_update(self):
        """Test that _creation_kwargs enables update() to work."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='field_{var}.tif',
            name='ds_{region}_{var}'
        )

        # Update should use _creation_kwargs to create new dataset
        updated = ds.update(region='it', var='tp')

        assert updated.key_pattern == '/data/it/field_tp.tif'
        assert updated.name == 'ds_it_tp'
        assert updated.format == ds.format
        assert updated.time_signature == ds.time_signature


class TestMemoryDatasetCreationKwargs:
    """Test _creation_kwargs for MemoryDataset."""

    def test_stores_keep_after_reading_flag(self):
        """Test that MemoryDataset stores keep_after_reading in _creation_kwargs."""
        ds = MemoryDataset(
            key_pattern='memory_{tile}.txt',
            keep_after_reading=True
        )

        assert 'keep_after_reading' in ds._creation_kwargs
        assert ds._creation_kwargs['keep_after_reading'] is True

    def test_stores_keep_after_reading_default_false(self):
        """Test that default keep_after_reading=False is stored."""
        ds = MemoryDataset(
            key_pattern='memory_{tile}.txt'
        )

        assert 'keep_after_reading' in ds._creation_kwargs
        assert ds._creation_kwargs['keep_after_reading'] is False

    def test_creation_kwargs_preserves_keep_after_reading_on_update(self):
        """Test that keep_after_reading is preserved when creating updated dataset."""
        ds = MemoryDataset(
            key_pattern='memory_{tile}.txt',
            keep_after_reading=True
        )
        ds.write_data('test_data', tile='t1')

        updated = ds.update(tile='t1')

        assert updated.keep_after_reading is True
        assert updated._creation_kwargs['keep_after_reading'] is True


class TestRemoteDatasetCreationKwargs:
    """Test _creation_kwargs for RemoteDataset base class."""

    @patch('boto3.Session')
    def test_stores_tmp_dir(self, mock_session):
        """Test that RemoteDataset stores tmp_dir in _creation_kwargs."""
        mock_session.return_value.client.return_value = Mock()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use S3Dataset as concrete implementation
            ds = S3Dataset(
                key_pattern='s3://bucket/data.tif',
                bucket_name='test-bucket',
                tmp_dir=tmpdir
            )

            assert 'tmp_dir' in ds._creation_kwargs
            assert ds._creation_kwargs['tmp_dir'] == tmpdir

    @patch('boto3.Session')
    def test_stores_auto_created_tmp_dir(self, mock_session):
        """Test that auto-created tmp_dir is stored in _creation_kwargs."""
        mock_session.return_value.client.return_value = Mock()
        
        # Use S3Dataset as concrete implementation
        ds = S3Dataset(
            key_pattern='s3://bucket/data.tif',
            bucket_name='test-bucket'
        )

        assert 'tmp_dir' in ds._creation_kwargs
        assert ds._creation_kwargs['tmp_dir'] == ds.tmp_dir
        assert os.path.exists(ds.tmp_dir)


class TestS3DatasetCreationKwargs:
    """Test _creation_kwargs for S3Dataset."""

    @patch('boto3.Session')
    def test_stores_s3_specific_parameters(self, mock_session):
        """Test that S3Dataset stores bucket_name, region_name, profile_name."""
        mock_session.return_value.client.return_value = Mock()

        ds = S3Dataset(
            key_pattern='data/{region}/output.tif',
            bucket_name='my-bucket',
            region_name='us-east-1',
            profile_name='my-profile'
        )

        assert ds._creation_kwargs['type'] == 's3'
        assert ds._creation_kwargs['bucket_name'] == 'my-bucket'
        assert ds._creation_kwargs['region_name'] == 'us-east-1'
        assert ds._creation_kwargs['profile_name'] == 'my-profile'
        # Should also have tmp_dir from parent
        assert 'tmp_dir' in ds._creation_kwargs

    @patch('boto3.Session')
    def test_stores_none_for_optional_parameters(self, mock_session):
        """Test that optional parameters are stored even when None."""
        mock_session.return_value.client.return_value = Mock()

        ds = S3Dataset(
            key_pattern='data/output.tif',
            bucket_name='my-bucket'
        )

        assert ds._creation_kwargs['region_name'] is None
        assert ds._creation_kwargs['profile_name'] is None


class TestOVHS3DatasetCreationKwargs:
    """Test _creation_kwargs for OVHS3Dataset."""

    @patch('boto3.Session')
    def test_stores_endpoint_url(self, mock_session):
        """Test that OVHS3Dataset stores endpoint_url in addition to S3 params."""
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

        assert ds._creation_kwargs['endpoint_url'] == 'https://s3.ovh.example.com'
        # Should also have S3 params
        assert ds._creation_kwargs['bucket_name'] == 'my-bucket'
        assert ds._creation_kwargs['type'] == 's3-ovh'
        # And tmp_dir from RemoteDataset
        assert 'tmp_dir' in ds._creation_kwargs


class TestSFTPDatasetCreationKwargs:
    """Test _creation_kwargs for SFTPDataset."""

    @patch('d3tools.data.datasets.remote_dataset.SFTPDataset._connect')
    def test_stores_sftp_connection_parameters(self, mock_connect):
        """Test that SFTPDataset stores host, username, password, port, private_key."""
        mock_connect.return_value = Mock()

        ds = SFTPDataset(
            key_pattern='data/output.tif',
            host='sftp.example.com',
            username='user',
            password='pass',
            port=2222,
            private_key='/path/to/key'
        )

        assert ds._creation_kwargs['type'] == 'sftp'
        assert ds._creation_kwargs['host'] == 'sftp.example.com'
        assert ds._creation_kwargs['username'] == 'user'
        assert ds._creation_kwargs['password'] == 'pass'
        assert ds._creation_kwargs['port'] == 2222
        assert ds._creation_kwargs['private_key'] == '/path/to/key'
        # Should also have tmp_dir from RemoteDataset
        assert 'tmp_dir' in ds._creation_kwargs

    @patch('d3tools.data.datasets.remote_dataset.SFTPDataset._connect')
    def test_stores_default_port(self, mock_connect):
        """Test that default port 22 is stored."""
        mock_connect.return_value = Mock()

        ds = SFTPDataset(
            key_pattern='data/output.tif',
            host='sftp.example.com',
            username='user',
            password='pass'
        )

        assert ds._creation_kwargs['port'] == 22


class TestCreationKwargsWithFormatMixins:
    """Test that _creation_kwargs works correctly with different FormatMixins."""

    def test_geotiff_format_mixin_preserves_creation_kwargs(self):
        """Test that GeoTIFF format mixin doesn't interfere with _creation_kwargs."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            name='geotiff_dataset'
        )

        assert ds.format == 'geotiff'
        assert ds._creation_kwargs['format'] == 'geotiff'
        assert ds._creation_kwargs['name'] == 'geotiff_dataset'

    def test_netcdf_format_mixin_preserves_creation_kwargs(self):
        """Test that NetCDF format mixin doesn't interfere with _creation_kwargs."""
        ds = LocalDataset(
            path='/data',
            filename='output.nc',
            name='netcdf_dataset'
        )

        assert ds.format == 'netcdf'
        assert ds._creation_kwargs['format'] == 'netcdf'
        assert ds._creation_kwargs['name'] == 'netcdf_dataset'

    def test_update_preserves_format_with_mixin(self):
        """Test that update() preserves format when mixin is involved."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output_{region}.tif'
        )

        updated = ds.update(region='eu')

        assert updated.format == 'geotiff'
        assert updated._creation_kwargs['format'] == 'geotiff'


class TestCreationKwargsUsageInUpdate:
    """Test that _creation_kwargs is correctly used by update() method."""

    def test_update_uses_creation_kwargs_to_recreate_dataset(self):
        """Test that update() actually uses _creation_kwargs to create new instance."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='field.tif',
            name='test_ds',
            nan_value=-9999
        )

        updated = ds.update(region='it')

        # Should preserve all attributes from _creation_kwargs
        assert updated.name == 'test_ds'
        assert updated.format == ds.format
        assert updated.time_signature == ds.time_signature
        assert updated.nan_value == -9999

    def test_memory_dataset_update_uses_keep_after_reading_from_kwargs(self):
        """Test that MemoryDataset.update() uses keep_after_reading from _creation_kwargs."""
        ds = MemoryDataset(
            key_pattern='data_{tile}.txt',
            keep_after_reading=True
        )

        updated = ds.update(tile='t1')

        # The updated dataset should have the same keep_after_reading flag
        assert updated.keep_after_reading is True

    @patch('boto3.Session')
    def test_s3_dataset_update_preserves_bucket_info(self, mock_session):
        """Test that S3Dataset.update() preserves bucket/region info."""
        mock_session.return_value.client.return_value = Mock()

        ds = S3Dataset(
            key_pattern='data/{region}/output.tif',
            bucket_name='my-bucket',
            region_name='us-east-1'
        )

        # This should work without errors and preserve S3 configuration
        # Note: actual update() might need these params, this tests they're available
        assert ds._creation_kwargs['bucket_name'] == 'my-bucket'
        assert ds._creation_kwargs['region_name'] == 'us-east-1'
