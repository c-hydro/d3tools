"""
Tests for config/parsers.py

Tests the centralized configuration parsing logic.
"""
import pytest
from unittest import mock
import os

from d3tools.config.parsers import dataset_from_config, _manager_from_config
from d3tools.data import Dataset
from d3tools.data.local_dataset import LocalDataset
from d3tools.thumbnails import DatasetThumbnailManager
from d3tools.logging import DatasetLogManager
from d3tools.timestepping import Day, Dekad


class TestDatasetFromConfig:
    """Test dataset_from_config function."""
    
    def test_basic_config_parsing(self):
        """Test parsing basic dataset config without managers."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif'
        }
        
        dataset = dataset_from_config(config)
        
        assert isinstance(dataset, LocalDataset)
        assert dataset.dir == '/data'
        assert dataset.file == 'output.tif'
    
    def test_merges_with_defaults(self):
        """Test that defaults are properly merged."""
        config = {
            'file': 'output.tif'
        }
        defaults = {
            'type': 'local',
            'path': '/default/path'
        }
        
        dataset = dataset_from_config(config, defaults)
        
        assert dataset.dir == '/default/path'
        assert dataset.file == 'output.tif'
    
    def test_config_overrides_defaults(self):
        """Test that config values override defaults."""
        config = {
            'type': 'local',
            'path': '/custom/path',
            'file': 'output.tif'
        }
        defaults = {
            'type': 'local',
            'path': '/default/path'
        }
        
        dataset = dataset_from_config(config, defaults)
        
        assert dataset.dir == '/custom/path'
    
    def test_parses_thumbnail_dict(self):
        """Test parsing thumbnail config dict into manager."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/path/colors.json',
                'destination': '/path/thumb.png'
            }
        }
        
        dataset = dataset_from_config(config)
        
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
    
    def test_parses_log_string(self):
        """Test parsing log config string into manager."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'log': '/logs/output.txt'
        }
        
        dataset = dataset_from_config(config)
        
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log, DatasetLogManager)
    
    def test_parses_both_managers(self):
        """Test parsing both thumbnail and log managers."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/path/colors.json',
                'destination': '/path/thumb.png'
            },
            'log': '/logs/output.txt'
        }
        
        dataset = dataset_from_config(config)
        
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
        assert isinstance(dataset.log, DatasetLogManager)
    
    def test_accepts_already_parsed_managers(self):
        """Test that already-parsed managers are passed through."""
        thumb = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='thumb.png')
        )
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/logs', file='output.txt')
        )
        
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': thumb,
            'log': log
        }
        
        dataset = dataset_from_config(config)
        
        # Should be the same objects
        assert dataset.thumbnail is thumb
        assert dataset.log is log
    
    def test_handles_none_managers(self):
        """Test that None managers are handled correctly."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': None,
            'log': None
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.thumbnail is None
        assert dataset.log is None
    
    def test_invalid_thumbnail_returns_none(self):
        """Test that invalid thumbnail config returns None."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'invalid': 'config'
            }
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.thumbnail is None
    
    def test_empty_log_returns_none(self):
        """Test that empty log config returns None."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'log': {}
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.log is None
    
    def test_preserves_other_options(self):
        """Test that other dataset options are preserved."""
        
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'timestep': 't'
        }
        
        dataset = dataset_from_config(config)
        
        assert dataset.timestep == Dekad 

class TestParseManagerConfig:
    """Test _parse_manager_config helper function."""
    
    def test_parse_thumbnail_dict(self):
        """Test parsing thumbnail config dict."""
        config = {
            'colors': '/path/colors.json',
            'destination': '/path/thumb.png'
        }
        
        def mock_factory(path):
            return LocalDataset(path=os.path.dirname(path), file=os.path.basename(path))
        
        manager = _manager_from_config(config, 'thumbnail', mock_factory)
        
        assert isinstance(manager, DatasetThumbnailManager)
    
    def test_parse_log_string(self):
        """Test parsing log config string."""
        config = '/logs/output.txt'
        
        def mock_factory(path):
            return LocalDataset(path=os.path.dirname(path), file=os.path.basename(path))
        
        manager = _manager_from_config(config, 'log', mock_factory)
        
        assert isinstance(manager, DatasetLogManager)
    
    def test_returns_already_parsed_manager(self):
        """Test that already-parsed managers are returned as-is."""
        existing_manager = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='thumb.png')
        )
        
        manager = _manager_from_config(existing_manager, 'thumbnail', lambda x: x)
        
        assert manager is existing_manager
    
    def test_returns_none_for_none(self):
        """Test that None returns None."""
        manager = _manager_from_config(None, 'thumbnail', lambda x: x)
        
        assert manager is None
    
    def test_raises_for_invalid_type(self):
        """Test that invalid manager type raises error."""
        with pytest.raises(ValueError, match="Unknown manager type"):
            _manager_from_config({}, 'invalid_type', lambda x: x)


class TestParserIntegration:
    """Test integration with Dataset.from_options()."""
    
    def test_dataset_from_options_uses_parser(self):
        """Test that Dataset.from_options() uses the parser."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/path/colors.json',
                'destination': '/path/thumb.png'
            }
        }
        
        dataset = LocalDataset.from_options(config)
        
        # Should have parsed thumbnail
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
    
    def test_parser_is_reusable(self):
        """Test that parser can be used directly, separate from Dataset."""
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'log': '/logs/output.txt'
        }
        
        # Use parser directly
        dataset = dataset_from_config(config)
        
        # Should return a fully constructed Dataset
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log, DatasetLogManager)


class TestParserForExternalUse:
    """Test that parser is suitable for use in door/dryes/dam."""
    
    def test_parser_can_be_imported_separately(self):
        """Test that parser can be imported and used independently."""
        # This simulates how door/dryes/dam would use it
        from d3tools.config.parsers import dataset_from_config
        
        config = {
            'type': 'local',
            'path': '/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': '/colors.json',
                'destination': '/thumbs/output.png'
            }
        }
        
        dataset = dataset_from_config(config)
        
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
    
    def test_parser_with_custom_defaults(self):
        """Test parser with custom defaults (as door/dryes/dam might use)."""
        # door/dryes/dam might have their own default configurations
        custom_defaults = {
            'type': 's3',
            'bucket_name': 'my-bucket',
            'timestep': 'day'
        }
        
        config = {
            'key_pattern': 'data/output.tif',
            'log': '/logs/output.txt'
        }
        
        dataset = dataset_from_config(config, custom_defaults)
        
        assert isinstance(dataset, Dataset)  # Will be RemoteDataset subclass
        assert dataset.bucket_name == 'my-bucket'
        assert dataset.timestep == Day
        assert dataset.key_pattern == 'data/output.tif'
        assert isinstance(dataset.log, DatasetLogManager)
