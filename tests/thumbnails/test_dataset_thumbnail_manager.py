"""
Tests for DatasetThumbnailManager.

Tests the thumbnail manager functionality that was extracted from Dataset class.
"""
import pytest
from unittest import mock
import tempfile
import os

from d3tools.thumbnails import DatasetThumbnailManager
from d3tools.data import Dataset, LocalDataset
import xarray as xr
import numpy as np

@pytest.fixture
def sample_colors_dataset():
    """Mock colors dataset."""
    return LocalDataset(path='/path/to', file='colors.json')


@pytest.fixture
def sample_destination_dataset():
    """Mock destination dataset."""
    return LocalDataset(path='/path/to/thumbnails', file='output_{tile}.png')


class TestDatasetThumbnailManagerInit:
    """Test DatasetThumbnailManager initialization."""
    
    def test_init_with_single_colors(self, sample_colors_dataset, sample_destination_dataset):
        """Test initialization with single color definition."""
        manager = DatasetThumbnailManager(
            colors=sample_colors_dataset,
            destination=sample_destination_dataset
        )
        
        assert manager.colors is sample_colors_dataset
        assert manager.destination is sample_destination_dataset
        assert manager.overlay is None
        assert manager.options == {}
    
    def test_init_with_dict_colors(self, sample_destination_dataset):
        """Test initialization with dictionary of colors."""
        colors_dict = {
            'layer1': LocalDataset(path='/path', file='colors1.json'),
            'layer2': LocalDataset(path='/path', file='colors2.json')
        }
        
        manager = DatasetThumbnailManager(
            colors=colors_dict,
            destination=sample_destination_dataset
        )
        
        assert isinstance(manager.colors, dict)
        assert len(manager.colors) == 2
    
    def test_init_with_overlay(self, sample_colors_dataset, sample_destination_dataset):
        """Test initialization with overlay."""
        overlay = LocalDataset(path='/path', file='overlay.shp')
        
        manager = DatasetThumbnailManager(
            colors=sample_colors_dataset,
            destination=sample_destination_dataset,
            overlay=overlay
        )
        
        assert manager.overlay is overlay
    
    def test_init_with_options(self, sample_colors_dataset, sample_destination_dataset):
        """Test initialization with additional options."""
        manager = DatasetThumbnailManager(
            colors=sample_colors_dataset,
            destination=sample_destination_dataset,
            field='value',
            dpi=300
        )
        
        assert manager.options['field'] == 'value'
        assert manager.options['dpi'] == 300


class TestDatasetThumbnailManagerFromDict:
    """Test DatasetThumbnailManager.from_dict factory method."""
    
    def test_from_dict_with_string_paths(self):
        """Test from_dict with string paths for colors and destination."""
        config = {
            'colors': '/path/to/colors.json',
            'destination': '/path/to/thumbnails/output.png'
        }
        
        def mock_factory(path_str):
            return LocalDataset(path=os.path.dirname(path_str), file=os.path.basename(path_str))
        
        manager = DatasetThumbnailManager.from_dict(config, mock_factory)
        
        assert manager is not None
        assert isinstance(manager.colors, LocalDataset)
        assert isinstance(manager.destination, LocalDataset)
    
    def test_from_dict_with_overlay(self):
        """Test from_dict with overlay option."""
        config = {
            'colors': '/path/to/colors.json',
            'destination': '/path/to/thumbnails/output.png',
            'overlay': '/path/to/overlay.shp'
        }
        
        def mock_factory(path_str):
            return LocalDataset(path=os.path.dirname(path_str), file=os.path.basename(path_str))
        
        manager = DatasetThumbnailManager.from_dict(config, mock_factory)
        
        assert manager.overlay is not None
        assert isinstance(manager.overlay, LocalDataset)

    def test_from_dict_with_nested_destination_and_overlay(self):
        """Test from_dict parses nested destination and overlay configs."""
        config = {
            'colors': {'key_pattern': '/path/to/colors.txt', 'format': 'txt'},
            'destination': {'key_pattern': '/path/to/thumb.png', 'format': 'file'},
            'overlay': {'key_pattern': '/path/to/overlay.geojson', 'format': 'geojson'},
        }

        def mock_factory(cfg):
            return LocalDataset(key_pattern=cfg['key_pattern'], format=cfg['format'])

        manager = DatasetThumbnailManager.from_dict(config, mock_factory)

        assert isinstance(manager.colors, LocalDataset)
        assert manager.colors.key_pattern == '/path/to/colors.txt'
        assert isinstance(manager.destination, LocalDataset)
        assert manager.destination.key_pattern == '/path/to/thumb.png'
        assert isinstance(manager.overlay, LocalDataset)
        assert manager.overlay.key_pattern == '/path/to/overlay.geojson'

    def test_from_dict_preserves_parsed_destination_and_overlay(self):
        """Test from_dict leaves parsed destination and overlay datasets unchanged."""
        destination = LocalDataset(path='/path/to', file='thumb.png')
        overlay = LocalDataset(path='/path/to', file='overlay.geojson')
        config = {
            'colors': '/path/to/colors.txt',
            'destination': destination,
            'overlay': overlay,
        }

        def mock_factory(cfg):
            return LocalDataset(key_pattern=cfg if isinstance(cfg, str) else cfg['key_pattern'])

        manager = DatasetThumbnailManager.from_dict(config, mock_factory)

        assert manager.destination is destination
        assert manager.overlay is overlay
    
    def test_from_dict_missing_colors(self):
        """Test from_dict returns None when colors is missing."""
        config = {
            'destination': '/path/to/thumbnails/output.png'
        }
        
        manager = DatasetThumbnailManager.from_dict(config, lambda x: x)
        
        assert manager is None
    
    def test_from_dict_missing_destination(self):
        """Test from_dict returns None when destination is missing."""
        config = {
            'colors': '/path/to/colors.json'
        }
        
        manager = DatasetThumbnailManager.from_dict(config, lambda x: x)
        
        assert manager is None
    
    def test_from_dict_with_additional_options(self):
        """Test from_dict preserves additional options."""
        config = {
            'colors': '/path/to/colors.json',
            'destination': '/path/to/thumbnails/output.png',
            'field': 'value',
            'dpi': 300,
            'annotation': {'text': 'Test'}
        }
        
        def mock_factory(path_str):
            return LocalDataset(path=os.path.dirname(path_str), file=os.path.basename(path_str))
        
        manager = DatasetThumbnailManager.from_dict(config, mock_factory)
        
        assert manager.options['field'] == 'value'
        assert manager.options['dpi'] == 300
        assert manager.options['annotation'] == {'text': 'Test'}
    
    def test_from_dict_with_already_parsed_datasets(self):
        """Test from_dict when colors/destination are already Dataset objects."""
        colors_ds = LocalDataset(path='/path', file='colors.json')
        dest_ds = LocalDataset(path='/path', file='output.png')
        
        config = {
            'colors': colors_ds,
            'destination': dest_ds
        }
        
        manager = DatasetThumbnailManager.from_dict(config, lambda x: x)
        
        assert manager.colors is colors_ds
        assert manager.destination is dest_ds


class TestDatasetThumbnailManagerMakeThumbnail:
    """Test thumbnail generation option wiring."""

    def test_make_thumbnail_passes_configured_overlay(
        self,
        sample_colors_dataset,
        sample_destination_dataset,
    ):
        overlay = LocalDataset(path='/path/to', file='overlay.geojson')
        manager = DatasetThumbnailManager(
            colors=sample_colors_dataset,
            destination=sample_destination_dataset,
            overlay=overlay,
        )
        data = xr.DataArray(np.ones((2, 2)))

        with mock.patch('d3tools.thumbnails.thumbnail.Thumbnail') as thumbnail_cls:
            thumbnail = thumbnail_cls.return_value
            sample_colors_dataset.update = mock.Mock(return_value='/path/to/colors.txt')
            sample_destination_dataset.get_key = mock.Mock(return_value='/path/to/thumb.png')

            manager.make_thumbnail(data, tile='T001')

        thumbnail.save.assert_called_once_with('/path/to/thumb.png', overlay=overlay)
