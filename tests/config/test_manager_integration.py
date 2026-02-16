"""
Tests for Options.parse() manager creation and Dataset integration.

Tests the integration of manager classes with config parsing and Dataset class.
"""
import pytest
from unittest import mock

from d3tools.data import Dataset
from d3tools.data.local_dataset import LocalDataset
from d3tools.thumbnails import DatasetThumbnailManager
from d3tools.logging import DatasetLogManager


@pytest.fixture
def basic_dataset_config():
    """Basic dataset configuration without managers."""
    return {
        'name': 'test_dataset',
        'type': 'local',
        'path': '/path/to/data',
        'file': 'output_{tile}.tif'
    }


@pytest.fixture
def dataset_config_with_thumbnail():
    """Dataset configuration with thumbnail options."""
    return {
        'name': 'test_dataset',
        'path': '/path/to/data',
        'file': 'output.tif',
        'thumbnail': {
            'colors': '/path/to/colors.json',
            'destination': '/path/to/thumbnails/output.png'
        }
    }


@pytest.fixture
def dataset_config_with_log():
    """Dataset configuration with log options."""
    return {
        'name': 'test_dataset',
        'path': '/path/to/data',
        'file': 'output.tif',
        'log': '/path/to/logs/output.txt'
    }


@pytest.fixture
def dataset_config_with_both():
    """Dataset configuration with both thumbnail and log."""
    return {
        'name': 'test_dataset',
        'path': '/path/to/data',
        'file': 'output.tif',
        'thumbnail': {
            'colors': '/path/to/colors.json',
            'destination': '/path/to/thumbnails/output.png',
            'dpi': 300
        },
        'log': {
            'file': '/path/to/logs/output.txt'
        }
    }

class TestDatasetFromOptionsManagerCreation:
    """Test that Dataset.from_options() correctly creates manager instances."""
    
    def test_from_options_without_managers(self, basic_dataset_config):
        """Test creating dataset without thumbnail or log options."""
        dataset = LocalDataset.from_options(basic_dataset_config)
        
        # Should create dataset without managers
        assert isinstance(dataset, LocalDataset)
        assert not hasattr(dataset, 'thumbnail') or dataset.thumbnail is None
        assert not hasattr(dataset, 'log') or dataset.log is None
    
    def test_from_options_with_thumbnail(self, dataset_config_with_thumbnail):
        """Test creating dataset with thumbnail options creates manager."""
        # Mock the dataset_factory for nested dataset parsing
        with mock.patch('d3tools.data.local_dataset.LocalDataset') as mock_ds:
            mock_ds.from_options.return_value = mock.Mock(spec=LocalDataset)
            
            dataset = LocalDataset.from_options(dataset_config_with_thumbnail)
            
            assert isinstance(dataset, LocalDataset)
            assert hasattr(dataset, 'thumbnail')
            assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
            assert dataset.thumbnail.colors is not None
            assert dataset.thumbnail.destination is not None
    
    def test_from_options_with_log(self, dataset_config_with_log):
        """Test creating dataset with log options creates manager."""
        dataset = LocalDataset.from_options(dataset_config_with_log)
        
        assert isinstance(dataset, LocalDataset)
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log, DatasetLogManager)
        assert dataset.log.output is not None
    
    def test_from_options_with_both_managers(self, dataset_config_with_both):
        """Test creating dataset with both thumbnail and log creates both managers."""
        dataset = LocalDataset.from_options(dataset_config_with_both)
        
        assert isinstance(dataset, LocalDataset)
        
        # Check thumbnail manager
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail, DatasetThumbnailManager)
        
        # Check log manager
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log, DatasetLogManager)
    
    def test_from_options_thumbnail_options_preserved(self, dataset_config_with_both):
        """Test that additional thumbnail options are preserved."""
        dataset = LocalDataset.from_options(dataset_config_with_both)
        
        assert dataset.thumbnail.options['dpi'] == 300


class TestDatasetFromOptionsNestedDatasets:
    """Test Dataset.from_options() with nested dataset parsing."""
    
    def test_from_options_nested_colors_dataset(self):
        """Test when colors is a nested dataset config."""
        config = {
            'name': 'test_dataset',
            'path': '/path/to/data',
            'file': 'output.tif',
            'thumbnail': {
                'colors': {
                    'path': '/path/to',
                    'file': 'colors.txt'
                },
                'destination': '/path/to/thumbnails/output.png'
            }
        }
        
        dataset = LocalDataset.from_options(config)
        
        assert hasattr(dataset, 'thumbnail')
        assert isinstance(dataset.thumbnail.colors, Dataset)
    
    def test_from_options_nested_log_dataset(self):
        """Test when log is a nested dataset config."""
        config = {
            'name': 'test_dataset',
            'path': '/path/to/data',
            'file': 'output.tif',
            'log': {
                'file': {
                    'path': '/path/to/logs',
                    'file': 'output.txt'
                }
            }
        }
        
        dataset = LocalDataset.from_options(config)
        
        assert hasattr(dataset, 'log')
        assert isinstance(dataset.log.output, Dataset)

class TestDatasetManagerIntegration:
    """Test Dataset class integration with managers."""
    
    def test_dataset_init_with_thumbnail_manager(self):
        """Test Dataset.__init__ accepts thumbnail manager."""
        thumbnail = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='output.png')
        )
        
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc',
            thumbnail=thumbnail
        )
        
        assert dataset.thumbnail is thumbnail
    
    def test_dataset_init_with_log_manager(self):
        """Test Dataset.__init__ accepts log manager."""
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/path', file='output.txt')
        )
        
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc',
            log=log
        )
        
        assert dataset.log is log
    
    def test_dataset_init_with_both_managers(self):
        """Test Dataset.__init__ accepts both managers."""
        thumbnail = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='output.png')
        )
        
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/path', file='output.txt')
        )
        
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc',
            thumbnail=thumbnail,
            log=log
        )
        
        assert dataset.thumbnail is thumbnail
        assert dataset.log is log


class TestDatasetHelperMethods:
    """Test Dataset helper methods for delegating to managers."""
    
    def test_make_thumbnail_with_manager(self):
        """Test _make_thumbnail when manager is present."""
        thumbnail = DatasetThumbnailManager(
            colors=LocalDataset(path='/path', file='colors.json'),
            destination=LocalDataset(path='/path', file='output.png')
        )
        
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc',
            thumbnail=thumbnail
        )
        
        # Mock the thumbnail.make_thumbnail method
        with mock.patch.object(thumbnail, 'make_thumbnail') as mock_make:
            mock_thumbnail_obj = mock.Mock()
            mock_thumbnail_obj.thumbnail_file = '/path/output.png'
            mock_make.return_value = mock_thumbnail_obj
            
            import xarray as xr
            import numpy as np
            data = xr.DataArray(np.random.rand(10, 10))
            
            from datetime import datetime
            time = datetime(2024, 1, 1)
            
            result = dataset._make_thumbnail(data, time)
            
            mock_make.assert_called_once()
            assert result == '/path/output.png'
    
    def test_make_thumbnail_without_manager(self):
        """Test _make_thumbnail returns None when no manager."""
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc'
        )
        
        import xarray as xr
        import numpy as np
        data = xr.DataArray(np.random.rand(10, 10))
        
        from datetime import datetime
        time = datetime(2024, 1, 1)
        
        result = dataset._make_thumbnail(data, time)
        
        assert result is None
    
    def test_make_log_with_manager(self):
        """Test _make_log when manager is present."""
        log = DatasetLogManager(
            output_dataset=LocalDataset(path='/path', file='output.txt')
        )
        
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc',
            log=log
        )
        dataset.name = 'test_dataset'
        dataset.get_time_signature = lambda d, **k: '2024-01-01'
        
        # Mock the log methods
        with mock.patch.object(log, 'get_log') as mock_get, \
             mock.patch.object(log, 'write_log') as mock_write:
            
            mock_get.return_value = {'test': 'log'}
            
            import xarray as xr
            import numpy as np
            output = xr.DataArray(np.random.rand(10, 10))
            
            from datetime import datetime
            time = datetime(2024, 1, 1)
            
            dataset._make_log(output, '/output.nc', '/thumb.png', time)
            
            mock_get.assert_called_once()
            mock_write.assert_called_once_with({'test': 'log'}, time)
    
    def test_make_log_without_manager(self):
        """Test _make_log does nothing when no manager."""
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc'
        )
        
        import xarray as xr
        import numpy as np
        output = xr.DataArray(np.random.rand(10, 10))
        
        from datetime import datetime
        time = datetime(2024, 1, 1)
        
        # Should not raise error
        dataset._make_log(output, '/output.nc', None, time)


class TestBackwardCompatibility:
    """Test backward compatibility scenarios."""
    
    def test_dataset_without_managers_still_works(self):
        """Test that Dataset works without managers (backward compatible)."""
        dataset = LocalDataset(
            path='/path/to/data',
            file='output.nc'
        )
        
        # Should initialize without errors
        assert dataset is not None
        assert dataset.dir  == '/path/to/data'
        assert dataset.file == 'output.nc'
    
    def test_from_options_without_manager_keys(self):
        """Test Dataset.from_options() works without thumbnail/log keys."""
        config = {
            'name': 'test_dataset',
            'type': 'local',
            'path': '/path/to/data',
            'file': 'output.nc'
        }
        
        dataset = LocalDataset.from_options(config)
        
        # Should create successfully without managers
        assert isinstance(dataset, LocalDataset)
        assert not hasattr(dataset, 'thumbnail') or dataset.thumbnail is None
        assert not hasattr(dataset, 'log') or dataset.log is None


class TestEdgeCases:
    """Test edge cases and error scenarios."""
    
    def test_thumbnail_config_missing_colors(self):
        """Test with incomplete thumbnail config (missing colors)."""
        config = {
            'name': 'test_dataset',
            'type': 'local',
            'path': '/path/to/data',
            'file': 'output.nc',
            'thumbnail': {
                'destination': '/path/to/thumbnails/output.png'
                # Missing 'colors'
            }
        }
        
        dataset = LocalDataset.from_options(config)
        
        # Should create but thumbnail should be None
        assert not hasattr(dataset, 'thumbnail') or dataset.thumbnail is None
    
    def test_thumbnail_config_missing_destination(self):
        """Test with incomplete thumbnail config (missing destination)."""
        config = {
            'name': 'test_dataset',
            'type': 'local',
            'path': '/path/to/data',
            'file': 'output.nc',
            'thumbnail': {
                'colors': '/path/to/colors.json'
                # Missing 'destination'
            }
        }
        
        dataset = LocalDataset.from_options(config)
        
        # Should create but thumbnail should be None
        assert not hasattr(dataset, 'thumbnail') or dataset.thumbnail is None
    
    def test_empty_log_config(self):
        """Test with empty log config."""
        config = {
            'name': 'test_dataset',
            'type': 'local',
            'path': '/path/to/data',
            'file': 'output.nc',
            'log': {}
        }
        
        dataset = LocalDataset.from_options(config)
        
        # Should create but log should be None
        assert not hasattr(dataset, 'log') or dataset.log is None
