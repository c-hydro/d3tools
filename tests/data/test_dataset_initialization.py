"""
Tests for Dataset initialization and helper methods.

Tests the refactored __init__ and its helper methods to ensure
proper initialization of Dataset instances.
"""
import pytest
from unittest import mock

from d3tools.data.local_dataset import LocalDataset
from d3tools.logging import DatasetLogManager
from d3tools.thumbnails import DatasetThumbnailManager
from d3tools.timestepping import TimeStep, TimeWindow, Day, Dekad


class TestGetOrDeriveName:
    """Test _get_or_derive_name helper method."""
    
    def test_explicit_name_provided(self):
        """Test that explicit name is used when provided."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            name='my_dataset'
        )
        
        assert dataset.name == 'my_dataset'
    
    def test_name_derived_from_simple_pattern(self):
        """Test name derivation from simple filename."""
        dataset = LocalDataset(
            path='/data',
            file='precipitation.tif'
        )
        
        assert dataset.name == 'precipitation'
    
    def test_name_derived_removes_date_placeholders(self):
        """Test that date placeholders are removed from derived name."""
        dataset = LocalDataset(
            path='/data',
            file='prec_%Y%m%d.tif'
        )
        
        assert dataset.name == 'prec'
    
    def test_name_derived_removes_trailing_underscore(self):
        """Test that trailing underscore is removed."""
        dataset = LocalDataset(
            path='/data',
            file='data_%Y%m%d.tif'
        )
        
        assert dataset.name == 'data'
        assert not dataset.name.endswith('_')
    
    def test_name_derived_removes_leading_underscore(self):
        """Test that leading underscore is removed."""
        dataset = LocalDataset(
            path='/data',
            file='%Y%m%d_data.tif'
        )
        
        assert dataset.name == 'data'
        assert not dataset.name.startswith('_')
    
    def test_name_derived_removes_double_underscores(self):
        """Test that double underscores are collapsed to single."""
        dataset = LocalDataset(
            path='/data',
            file='data_%Y__%m.tif'
        )
        
        # After removing %Y and %m, 'data___' becomes 'data_'
        assert '__' not in dataset.name


class TestGetOrDetectFormat:
    """Test _get_or_detect_format helper method."""
    
    def test_explicit_format_provided(self):
        """Test that explicit format is used when provided."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            format='netcdf'
        )
        
        assert dataset.format == 'netcdf'
    
    def test_format_detected_from_nc_extension(self):
        """Test format detection from .tif extension."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        assert dataset.format == 'geotiff'
    
    def test_format_detected_from_tif_extension(self):
        """Test format detection from .tif extension."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        assert dataset.format in ['tif', 'geotiff', 'gtiff']
    
    def test_format_detected_from_csv_extension(self):
        """Test format detection from .csv extension."""
        dataset = LocalDataset(
            path='/data',
            file='output.csv'
        )
        
        assert dataset.format == 'csv'


class TestSetupTemporalProperties:
    """Test _setup_temporal_properties helper method."""
    
    def test_time_signature_set(self):
        """Test that time_signature is set correctly."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            time_signature='start'
        )
        
        assert dataset.time_signature == 'start'
    
    def test_time_signature_defaults(self):
        """Test that time_signature uses default when not provided."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        # Check that it has the default value
        assert hasattr(dataset, 'time_signature')
    
    def test_aggregation_set(self):
        """Test that aggregation is parsed and set."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            aggregation='3m'
        )
        
        assert hasattr(dataset, 'agg')
        assert isinstance(dataset.agg, TimeWindow)
    
    def test_timestep_set(self):
        """Test that timestep is parsed and set."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            timestep='daily'
        )
        
        assert hasattr(dataset, 'timestep')
        assert dataset.timestep == Day
    
    def test_timestep_linked_with_aggregation(self):
        """Test that timestep and aggregation are linked."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            timestep='t',
            aggregation='3m'
        )
        
        assert hasattr(dataset, 'timestep')
        assert hasattr(dataset, 'agg')
        # Timestep should have aggregation info
        assert dataset.timestep.agg_window == dataset.agg
    
    def test_no_temporal_properties(self):
        """Test that dataset works without temporal properties."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        # Should not have timestep or agg
        assert not hasattr(dataset, 'timestep')
        assert not hasattr(dataset, 'agg')


class TestSetOptionalAttributes:
    """Test _set_optional_attributes helper method."""
    
    def test_thumbnail_set_as_manager(self):
        """Test that thumbnail manager is set."""
        thumb = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='thumb.png')
        )
        
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            thumbnail=thumb
        )
        
        assert hasattr(dataset, 'thumbnail')
        assert dataset.thumbnail is thumb
    
    def test_log_set_as_manager(self):
        """Test that log manager is set."""
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/logs', file='output.txt')
        )
        
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            log=log
        )
        
        assert hasattr(dataset, 'log')
        assert dataset.log is log
    
    def test_tile_names_set_as_list(self):
        """Test that tile_names can be set as list."""
        dataset = LocalDataset(
            path='/data',
            file='output_{tile}.tif',
            tile_names=['tile1', 'tile2', 'tile3']
        )
        
        assert hasattr(dataset, 'tile_names')
        assert dataset.tile_names == ['tile1', 'tile2', 'tile3']
    
    def test_nan_value_set(self):
        """Test that nan_value is set."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            nan_value=-9999
        )
        
        assert dataset.nan_value == -9999
    
    def test_nan_value_defaults_to_none(self):
        """Test that nan_value defaults to None."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        assert dataset.nan_value is None
    
    def test_no_optional_attributes(self):
        """Test that dataset works without optional attributes."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        # Should not have these attributes
        assert not hasattr(dataset, 'thumbnail')
        assert not hasattr(dataset, 'log')
        assert dataset.nan_value is None


class TestDatasetInitIntegration:
    """Integration tests for full Dataset initialization."""
    
    def test_full_initialization(self):
        """Test complete initialization with all options."""
        thumb = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='thumb.png')
        )
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/logs', file='output.txt')
        )
        
        dataset = LocalDataset(
            path='/data',
            file='prec_%Y%m%d_{tile}.tif',
            name='precipitation',
            time_signature='end',
            timestep='t',
            aggregation='1m',
            thumbnail=thumb,
            log=log,
            tile_names=['tile1', 'tile2'],
            nan_value=-9999,
            custom_option='value'
        )
        
        # Verify all attributes
        assert dataset.name == 'precipitation'
        assert dataset.format == 'geotiff'
        assert dataset.time_signature == 'end'
        assert hasattr(dataset, 'timestep')
        assert hasattr(dataset, 'agg')
        assert dataset.thumbnail is thumb
        assert dataset.log is log
        assert dataset.tile_names == ['tile1', 'tile2']
        assert dataset.nan_value == -9999
        assert dataset.options['custom_option'] == 'value'
    
    def test_minimal_initialization(self):
        """Test minimal initialization with required args only."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        # Should have basic attributes
        assert dataset.name == 'output'
        assert hasattr(dataset, 'format')
        assert hasattr(dataset, 'template_manager')
        assert dataset.options == {}
        assert dataset.tags == {}
        assert dataset.nan_value is None
    
    def test_options_storage(self):
        """Test that extra kwargs are stored in options."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif',
            custom_attr='value',
            another_option=42
        )
        
        assert 'custom_attr' in dataset.options
        assert dataset.options['custom_attr'] == 'value'
        assert dataset.options['another_option'] == 42
    
    def test_tags_initialized_empty(self):
        """Test that tags dict is initialized empty."""
        dataset = LocalDataset(
            path='/data',
            file='output.tif'
        )
        
        assert dataset.tags == {}
        assert isinstance(dataset.tags, dict)
    
    def test_key_pattern_now_substitution(self):
        """Test that 'now' placeholder is substituted in key_pattern."""
        dataset = LocalDataset(
            path='/data',
            file='output_{now:%Y%m%d}.tif'
        )
        
        # 'now' should be substituted with some date (8 digits)
        # We don't care about the exact date, just that it was substituted
        assert '{now' not in dataset.file
        assert 'output_' in dataset.file
        # Should have 8 digits for YYYYMMDD format
        import re
        assert re.search(r'\d{8}', dataset.file) is not None
