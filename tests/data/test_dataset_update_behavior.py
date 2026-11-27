"""
Tests for Dataset.update() behavior across all subclasses.

These tests lock in the update() behavior to ensure:
1. Correct handling of placeholders in key_pattern and name
2. Preservation of subclass-specific attributes
3. Correct tag management
4. Data filtering for MemoryDataset
5. Interaction with FormatMixins
"""

import pytest
from unittest.mock import Mock, patch
import tempfile

from d3tools.data import LocalDataset, MemoryDataset
from d3tools.data.datasets.remote_dataset import S3Dataset, OVHS3Dataset, SFTPDataset
from d3tools.timestepping import TimeStep, TimeWindow


class TestLocalDatasetUpdate:
    """Test update() behavior for LocalDataset."""

    def test_update_substitutes_placeholders_in_key_pattern(self):
        """Test that placeholders in path and filename are substituted."""
        ds = LocalDataset(
            path='/data/{region}/{year}',
            filename='field_{var}.tif'
        )

        updated = ds.update(region='eu', year='2020', var='tp')

        assert updated.key_pattern == '/data/eu/2020/field_tp.tif'
        assert updated.dir == '/data/eu/2020'
        assert updated.file == 'field_tp.tif'

    def test_update_substitutes_placeholders_in_name(self):
        """Test that placeholders in name are substituted."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='field.tif',
            name='dataset_{region}'
        )

        updated = ds.update(region='it')

        assert updated.name == 'dataset_it'

    def test_update_preserves_format(self):
        """Test that format is preserved through update."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.nc',
            format='netcdf'
        )

        updated = ds.update(region='eu')

        assert updated.format == 'netcdf'
        assert updated._creation_kwargs['format'] == 'netcdf'

    def test_update_preserves_time_signature(self):
        """Test that time_signature is preserved."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            time_signature='start'
        )

        updated = ds.update()

        assert updated.time_signature == 'start'

    def test_update_preserves_timestep(self):
        """Test that timestep is preserved."""
        ds = LocalDataset(
            path='/data/{tile}',
            filename='output.tif',
            timestep='daily'
        )

        updated = ds.update(tile='t1')

        assert updated.timestep == ds.timestep
        assert updated.timestep is not None

    def test_update_preserves_aggregation(self):
        """Test that aggregation window is preserved."""
        ds = LocalDataset(
            path='/data/{tile}',
            filename='output.tif',
            timestep='daily',
            aggregation='7d'
        )

        updated = ds.update(tile='t1')

        assert hasattr(updated, 'agg')
        assert updated.agg == ds.agg

    def test_update_preserves_nan_value(self):
        """Test that nan_value is preserved."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif',
            nan_value=-9999
        )

        updated = ds.update(region='eu')

        assert updated.nan_value == -9999

    def test_update_creates_independent_tags(self):
        """Test that tags are added and isolated per update."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )

        ds1 = ds.update(region='eu')
        ds2 = ds.update(region='it')

        assert ds1.tags['region'] == 'eu'
        assert ds2.tags['region'] == 'it'
        assert 'region' not in ds.tags

    def test_update_adds_to_existing_tags(self):
        """Test that update adds to existing tags without mutating source."""
        ds = LocalDataset(
            path='/data/{region}/{var}',
            filename='output.tif'
        )
        ds1 = ds.update(region='eu')
        ds2 = ds1.update(var='tp')

        assert ds2.tags['region'] == 'eu'
        assert ds2.tags['var'] == 'tp'
        assert 'var' not in ds1.tags

    def test_update_preserves_template_manager(self):
        """Test that template_manager is shared across updates."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )
        # Add a template to verify sharing
        ds.template_manager._templates['test'] = {'dummy': 1}

        updated = ds.update(region='eu')

        assert updated.template_manager is ds.template_manager
        assert 'test' in updated.template_manager._templates

    def test_update_preserves_tile_names(self):
        """Test that tile_names are preserved."""
        ds = LocalDataset(
            path='/data/{tile}',
            filename='output.tif',
            tile_names=['t1', 't2', 't3']
        )

        updated = ds.update(tile='t1')

        assert hasattr(updated, '_tile_names')
        assert updated._tile_names == ds._tile_names

    def test_update_without_placeholders(self):
        """Test update when no placeholders need substitution."""
        ds = LocalDataset(
            path='/data/static',
            filename='output.tif'
        )

        updated = ds.update()

        assert updated.key_pattern == ds.key_pattern
        assert updated is not ds

    def test_sequential_updates(self):
        """Test multiple sequential updates."""
        ds = LocalDataset(
            path='/data/{region}/{year}',
            filename='{var}_{month}.tif'
        )

        ds1 = ds.update(region='eu')
        ds2 = ds1.update(year='2020')
        ds3 = ds2.update(var='tp', month='jan')

        assert ds3.key_pattern == '/data/eu/2020/tp_jan.tif'
        assert ds3.tags == {'region': 'eu', 'year': '2020', 'var': 'tp', 'month': 'jan'}
        assert ds.tags == {}
        assert ds1.tags == {'region': 'eu'}


class TestLocalDatasetUpdateInPlace:
    """Test in_place=True update behavior for LocalDataset."""

    def test_update_in_place_mutates_self(self):
        """Test that in_place=True modifies the original dataset."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )
        original_id = id(ds)

        none = ds.update(in_place=True, region='eu')

        assert none is None
        assert id(ds) == original_id
        assert ds.key_pattern == '/data/eu/output.tif'
        assert ds.tags['region'] == 'eu'

    def test_update_in_place_updates_tags(self):
        """Test that in_place=True adds tags to the dataset."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )

        ds.update(in_place=True, region='eu')

        assert ds.tags['region'] == 'eu'

    def test_update_in_place_sequential(self):
        """Test sequential in-place updates."""
        ds = LocalDataset(
            path='/data/{region}/{var}',
            filename='output.tif'
        )

        ds.update(in_place=True, region='eu')
        ds.update(in_place=True, var='tp')

        assert ds.key_pattern == '/data/eu/tp/output.tif'
        assert ds.tags == {'region': 'eu', 'var': 'tp'}


class TestMemoryDatasetUpdate:
    """Test update() behavior for MemoryDataset."""

    def test_update_preserves_keep_after_reading_true(self):
        """Test that keep_after_reading=True is preserved."""
        ds = MemoryDataset(
            key_pattern='memory_{tile}.txt',
            keep_after_reading=True
        )

        updated = ds.update(tile='t1')

        assert updated.keep_after_reading is True

    def test_update_preserves_keep_after_reading_false(self):
        """Test that keep_after_reading=False is preserved."""
        ds = MemoryDataset(
            key_pattern='memory_{tile}.txt',
            keep_after_reading=False
        )

        updated = ds.update(tile='t1')

        assert updated.keep_after_reading is False

    def test_update_filters_data_dict_to_matching_keys(self):
        """Test that update() filters data_dict to only matching keys."""
        ds = MemoryDataset(
            key_pattern='data_{tile}_{var}.txt'
        )
        ds.write_data('data1', tile='t1', var='a')
        ds.write_data('data2', tile='t1', var='b')
        ds.write_data('data3', tile='t2', var='a')
        ds.write_data('data4', tile='t2', var='b')

        # Update to narrow down to tile='t1'
        updated = ds.update(tile='t1')

        assert updated.key_pattern == 'data_t1_{var}.txt'
        assert len(updated.data_dict) == 2
        assert 'data_t1_a.txt' in updated.data_dict
        assert 'data_t1_b.txt' in updated.data_dict
        assert 'data_t2_a.txt' not in updated.data_dict

    def test_update_data_dict_preserves_data_values(self):
        """Test that data values are preserved when filtering."""
        ds = MemoryDataset(
            key_pattern='data_{tile}.txt'
        )
        ds.write_data('content_t1', tile='t1')
        ds.write_data('content_t2', tile='t2')

        updated = ds.update(tile='t1')

        assert updated.get_data() == 'content_t1'

    def test_update_empty_data_dict(self):
        """Test update on MemoryDataset with no data."""
        ds = MemoryDataset(
            key_pattern='data_{tile}.txt'
        )

        updated = ds.update(tile='t1')

        assert len(updated.data_dict) == 0
        assert updated.key_pattern == 'data_t1.txt'

    def test_update_in_place_updates_data_dict(self):
        """Test that in_place=True updates data_dict correctly."""
        ds = MemoryDataset(
            key_pattern='data_{tile}_{var}.txt'
        )
        ds.write_data('d1', tile='t1', var='a')
        ds.write_data('d2', tile='t1', var='b')
        ds.write_data('d3', tile='t2', var='a')

        ds.update(in_place=True, tile='t1')

        assert len(ds.data_dict) == 2
        assert 'data_t1_a.txt' in ds.data_dict
        assert 'data_t2_a.txt' not in ds.data_dict


class TestRemoteDatasetUpdate:
    """Test update() behavior for remote Dataset subclasses."""

    @patch('boto3.Session')
    def test_s3_dataset_update_preserves_bucket_name(self, mock_session):
        """Test that S3Dataset update preserves bucket configuration."""
        mock_session.return_value.client.return_value = Mock()

        ds = S3Dataset(
            key_pattern='data/{region}/output.tif',
            bucket_name='my-bucket',
            region_name='us-east-1'
        )

        updated = ds.update(region='eu')

        assert updated.bucket_name == 'my-bucket'
        assert updated.region_name == 'us-east-1'

    @patch('boto3.Session')
    def test_s3_dataset_update_preserves_tmp_dir(self, mock_session):
        """Test that S3Dataset update preserves tmp_dir."""
        mock_session.return_value.client.return_value = Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            ds = S3Dataset(
                key_pattern='data/{tile}/output.tif',
                bucket_name='my-bucket',
                tmp_dir=tmpdir
            )

            updated = ds.update(tile='t1')

            assert updated.tmp_dir == tmpdir

    @patch('boto3.Session')
    def test_ovh_s3_dataset_update_preserves_endpoint_url(self, mock_session):
        """Test that OVHS3Dataset update preserves endpoint_url."""
        mock_client = Mock()
        mock_session.return_value.client.return_value = mock_client
        mock_session.return_value.get_credentials.return_value.get_frozen_credentials.return_value = Mock(
            access_key='key', secret_key='secret'
        )

        ds = OVHS3Dataset(
            key_pattern='data/{region}/output.tif',
            bucket_name='my-bucket',
            endpoint_url='https://s3.ovh.example.com'
        )

        updated = ds.update(region='eu')

        assert updated.endpoint_url == 'https://s3.ovh.example.com'
        assert updated.bucket_name == 'my-bucket'

    @patch('d3tools.data.datasets.remote_dataset.SFTPDataset._connect')
    def test_sftp_dataset_update_preserves_connection_params(self, mock_connect):
        """Test that SFTPDataset update preserves connection parameters."""
        mock_connect.return_value = Mock()

        ds = SFTPDataset(
            key_pattern='data/{region}/output.tif',
            host='sftp.example.com',
            username='user',
            password='pass',
            port=2222
        )

        updated = ds.update(region='eu')

        assert updated.hostname == 'sftp.example.com'
        assert updated.username == 'user'
        assert updated.password == 'pass'
        assert updated.port == 2222


class TestUpdateWithFormatMixins:
    """Test that update() works correctly with different format mixins."""

    def test_update_with_geotiff_format(self):
        """Test update with GeoTIFF format."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )

        updated = ds.update(region='eu')

        assert updated.format == 'geotiff'
        # Verify format mixin is present
        assert hasattr(updated, 'template_manager')

    def test_update_with_netcdf_format(self):
        """Test update with NetCDF format."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.nc'
        )

        updated = ds.update(region='eu')

        assert updated.format == 'netcdf'
        # Verify format mixin is present
        assert hasattr(updated, 'template_manager')

    def test_update_preserves_format_specific_attributes(self):
        """Test that format-specific attributes are preserved."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )
        # Add a template (RasterMixin feature)
        ds.template_manager._templates['test_template'] = {'bands': ['b1']}

        updated = ds.update(region='eu')

        assert updated.template_manager is ds.template_manager
        assert 'test_template' in updated.template_manager._templates

    def test_update_with_text_format(self):
        """Test update with text format."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.txt'
        )

        updated = ds.update(region='eu')

        assert updated.format == 'txt'
        assert updated.key_pattern == '/data/eu/output.txt'

    def test_sequential_updates_preserve_format(self):
        """Test that sequential updates maintain format."""
        ds = LocalDataset(
            path='/data/{region}/{year}',
            filename='output.nc'
        )

        ds1 = ds.update(region='eu')
        ds2 = ds1.update(year='2020')

        assert ds2.format == 'netcdf'
        assert ds1.format == 'netcdf'
        assert ds.format == 'netcdf'


class TestUpdateEdgeCases:
    """Test edge cases and special scenarios for update()."""

    def test_update_with_partial_placeholder_substitution(self):
        """Test update when only some placeholders are substituted."""
        ds = LocalDataset(
            path='/data/{region}/{year}',
            filename='{var}_{month}.tif'
        )

        updated = ds.update(region='eu', var='tp')

        assert updated.key_pattern == '/data/eu/{year}/tp_{month}.tif'
        assert updated.tags == {'region': 'eu', 'var': 'tp'}

    def test_update_with_overlapping_placeholder_names(self):
        """Test update with similar placeholder names."""
        ds = LocalDataset(
            path='/data/{var}/{var_type}',
            filename='output.tif'
        )

        updated = ds.update(var='tp', var_type='daily')

        assert updated.key_pattern == '/data/tp/daily/output.tif'

    def test_update_preserves_options_dict(self):
        """Test that options dict is preserved through update."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif',
            custom_option='value'
        )

        updated = ds.update(region='eu')

        assert updated.options.get('custom_option') == 'value'

    def test_update_with_none_values(self):
        """Test update with None values doesn't cause issues."""
        ds = LocalDataset(
            path='/data',
            filename='output.tif',
            nan_value=None
        )

        updated = ds.update()

        assert updated.nan_value is None

    def test_update_creates_independent_instances(self):
        """Test that multiple updates create independent instances."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output.tif'
        )

        ds1 = ds.update(region='eu')
        ds2 = ds.update(region='us')
        ds3 = ds.update(region='asia')

        # All should be different instances
        assert ds1 is not ds2
        assert ds2 is not ds3
        assert ds1 is not ds

        # Each should have independent state
        ds1.tags['extra'] = 'x1'
        ds2.tags['extra'] = 'x2'
        
        assert ds1.tags['extra'] == 'x1'
        assert ds2.tags['extra'] == 'x2'
        assert 'extra' not in ds3.tags

    def test_update_with_datetime_placeholders(self):
        """Test update works with datetime placeholders in pattern."""
        ds = LocalDataset(
            path='/data/{region}',
            filename='output_%Y%m%d_{tile}.tif'
        )

        updated = ds.update(region='eu', tile='t1')

        # Date placeholders should remain
        assert '%Y%m%d' in updated.key_pattern
        assert 'eu' in updated.key_pattern
        assert 't1' in updated.key_pattern
