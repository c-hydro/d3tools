"""
Tests for DatasetLogManager.

Tests the logging manager functionality that was extracted from Dataset class.
"""
import pytest
from unittest import mock
import tempfile
import os
import json
import xarray as xr
import numpy as np
from datetime import datetime

from d3tools.logging import DatasetLogManager
from d3tools.data import Dataset
from d3tools.data.local_dataset import LocalDataset


@pytest.fixture
def sample_log_dataset():
    """Mock log dataset."""
    return LocalDataset(path='/path/to/logs', file='output_{tile}.txt')


@pytest.fixture
def sample_data():
    """Create sample xarray DataArray for testing."""
    data = xr.DataArray(
        np.random.rand(10, 10),
        dims=['x', 'y'],
        coords={'x': range(10), 'y': range(10)}
    )
    return data


class TestDatasetLogManagerInit:
    """Test DatasetLogManager initialization."""
    
    def test_init_with_dataset(self, sample_log_dataset):
        """Test initialization with Dataset object."""
        manager = DatasetLogManager(output_dataset=sample_log_dataset)
        
        assert manager.output is sample_log_dataset
    
    def test_init_none(self):
        """Test initialization with None."""
        manager = DatasetLogManager(output_dataset=None)
        
        assert manager.output is None


class TestDatasetLogManagerFromDict:
    """Test DatasetLogManager.from_dict factory method."""
    
    def test_from_dict_with_string_path(self):
        """Test from_dict with string path."""
        config = '/path/to/logs/output.txt'
        
        def mock_factory(path_str):
            return LocalDataset(path=os.path.dirname(path_str), file=os.path.basename(path_str))
        
        manager = DatasetLogManager.from_dict(config, mock_factory)
        
        assert manager is not None
        assert isinstance(manager.output, LocalDataset)
    
    def test_from_dict_with_dict_containing_file(self):
        """Test from_dict with dict containing 'file' key."""
        config = {
            'file': '/path/to/logs/output.txt'
        }
        
        def mock_factory(path_str):
            return LocalDataset(path=os.path.dirname(path_str), file=os.path.basename(path_str))
        
        manager = DatasetLogManager.from_dict(config, mock_factory)
        
        assert manager is not None
        assert isinstance(manager.output, LocalDataset)
    
    def test_from_dict_with_dict_as_dataset(self):
        """Test from_dict when entire dict is treated as dataset config."""
        config = {
            'path': '/path/to/logs',
            'file': 'output.txt',
            'format': 'local'
        }
        
        def mock_factory(cfg):
            if isinstance(cfg, dict):
                return LocalDataset(**cfg)
            return LocalDataset(path=os.path.dirname(cfg), file=os.path.basename(cfg))
        
        manager = DatasetLogManager.from_dict(config, mock_factory)
        
        assert manager is not None
        assert isinstance(manager.output, LocalDataset)
    
    def test_from_dict_with_already_parsed_dataset(self):
        """Test from_dict when config is already a Dataset object."""
        log_ds = LocalDataset(path='/path/to/logs', file='output.txt')
        
        manager = DatasetLogManager.from_dict(log_ds, lambda x: x)
        
        assert manager.output is log_ds
    
    def test_from_dict_with_none(self):
        """Test from_dict with None returns None manager."""
        manager = DatasetLogManager.from_dict(None, lambda x: x)
        
        assert manager is None
    
    def test_from_dict_with_empty_dict(self):
        """Test from_dict with empty dict returns None."""
        manager = DatasetLogManager.from_dict({}, lambda x: x)
        
        assert manager is None


class TestDatasetLogManagerQCChecks:
    """Test DatasetLogManager.qc_checks static method."""
    
    def test_qc_checks_basic(self, sample_data):
        """Test basic QC checks on data."""
        checks = DatasetLogManager.qc_checks(sample_data)
        
        assert 'max' in checks
        assert 'min' in checks
        assert 'nans' in checks
        assert 'nans_pc' in checks
        assert 'zeros' in checks
        assert 'zeros_pc' in checks
        assert 'sum' in checks
        assert 'sum_abs' in checks
        
        assert checks['nans'] == 0  # no NaNs in random data
        assert isinstance(checks['max'], float)
        assert isinstance(checks['min'], float)
    
    def test_qc_checks_with_nans(self):
        """Test QC checks with NaN values."""
        data = xr.DataArray(
            np.array([1.0, 2.0, np.nan, 4.0, np.nan]),
            dims=['x']
        )
        
        checks = DatasetLogManager.qc_checks(data)
        
        assert checks['nans'] == 2
        assert checks['nans_pc'] == 40.0  # 2 out of 5
    
    def test_qc_checks_with_zeros(self):
        """Test QC checks with zero values."""
        data = xr.DataArray(
            np.array([0.0, 1.0, 0.0, 2.0, 0.0]),
            dims=['x']
        )
        
        checks = DatasetLogManager.qc_checks(data)
        
        assert checks['zeros'] == 3
    
    def test_qc_checks_sum_calculations(self):
        """Test QC checks sum calculations."""
        data = xr.DataArray(
            np.array([1.0, 2.0, np.nan, 4.0]),
            dims=['x']
        )
        
        checks = DatasetLogManager.qc_checks(data)
        
        assert checks['sum'] == 7.0  # sum ignores NaN
        assert checks['sum_abs'] == 7.0  # sum of absolute values


class TestDatasetLogManagerGetLog:
    """Test DatasetLogManager.get_log method."""
    
    def test_get_log_basic(self, sample_log_dataset, sample_data):
        """Test basic get_log functionality."""
        manager = DatasetLogManager(output_dataset=sample_log_dataset)
        
        def mock_time_sig(data, **kwargs):
            return '2024-01-01'
        
        log_dict = manager.get_log(
            dataset_name='test_dataset',
            data=sample_data,
            time_signature_func=mock_time_sig,
            output_file='/output/test.nc',
            thumbnail_file='/output/test.png'
        )
        
        assert 'dataset' in log_dict
        assert log_dict['dataset'] == 'test_dataset'
        assert 'output_file' in log_dict
        assert 'thumbnail_file' in log_dict
        
        # Check data_checks contains QC fields
        assert 'data_checks' in log_dict
        data_checks = log_dict['data_checks']
        assert 'max' in data_checks
        assert 'min' in data_checks
        assert 'nans' in data_checks
        assert 'zeros' in data_checks
    
    def test_get_log_with_kwargs(self, sample_log_dataset, sample_data):
        """Test get_log with additional kwargs."""
        manager = DatasetLogManager(output_dataset=sample_log_dataset)
        
        log_dict = manager.get_log(
            dataset_name='test_dataset',
            data=sample_data,
            time_signature_func=lambda d, **k: '2024',
            tile='tile001',
            custom_field='value'
        )
        
        assert 'tile' in log_dict
        assert log_dict['tile'] == 'tile001'
        assert 'custom_field' in log_dict
        assert log_dict['custom_field'] == 'value'


class TestDatasetLogManagerWriteLog:
    """Test DatasetLogManager.write_log method."""
    
    def test_write_log_txt_format(self, sample_log_dataset):
        """Test writing log in txt format."""
        manager = DatasetLogManager(output_dataset=sample_log_dataset)
        
        log_dict = {
            'dataset': 'test',
            'time': '2024-01-01',
            'output_file': '/output/test.nc',
            'data_checks': {'max': '1.5', 'min': '0.1'}
        }
        
        time = datetime(2024, 1, 1)
        
        # Mock the output.write_data method
        with mock.patch.object(sample_log_dataset, 'write_data') as mock_write:
            manager.write_log(log_dict, time)
            
            # Verify write was called
            mock_write.assert_called_once()
            
            # Check the content format
            call_args = mock_write.call_args
            content = call_args[0][0]  # first positional arg
            
            assert isinstance(content, str)
            assert 'dataset' in content
            assert 'test' in content
    
    def test_write_log_json_format(self, sample_log_dataset):
        """Test writing log in json format."""
        # Create log dataset with .json extension
        json_log = LocalDataset(path='/path/to/logs', file='output.json')
        manager = DatasetLogManager(output_dataset=json_log)
        
        log_dict = {
            'dataset': 'test',
            'time': '2024-01-01',
            'data_checks': {'max': '1.5', 'min': '0.1'}
        }
        
        time = datetime(2024, 1, 1)
        
        with mock.patch.object(json_log, 'write_data') as mock_write:
            manager.write_log(log_dict, time)
            
            mock_write.assert_called_once()
            
            # Check dict format was passed (not JSON string)
            call_args = mock_write.call_args
            content = call_args[0][0]  # first positional arg
            
            # Should be a dict
            assert isinstance(content, dict)
            assert content['dataset'] == 'test'
            assert content['data_checks']['max'] == '1.5'
    
    def test_write_log_with_none_output(self):
        """Test write_log does nothing when output is None."""
        manager = DatasetLogManager(output_dataset=None)
        
        log_dict = {'test': 'data'}
        time = datetime(2024, 1, 1)
        
        # Should not raise error
        manager.write_log(log_dict, time)
    
    def test_write_log_passes_kwargs(self, sample_log_dataset):
        """Test write_log passes kwargs to dataset.write_data."""
        manager = DatasetLogManager(output_dataset=sample_log_dataset)
        
        log_dict = {'dataset': 'test'}
        time = datetime(2024, 1, 1)
        
        with mock.patch.object(sample_log_dataset, 'write_data') as mock_write:
            manager.write_log(log_dict, time, tile='tile001', custom='value')
            
            call_args = mock_write.call_args
            assert call_args[1]['tile'] == 'tile001'
            assert call_args[1]['custom'] == 'value'


class TestDatasetLogManagerIntegration:
    """Integration tests for DatasetLogManager."""
    
    def test_full_workflow(self, sample_log_dataset, sample_data):
        """Test complete workflow: from_dict -> get_log -> write_log."""
        config = '/path/to/logs/output.txt'
        
        def mock_factory(path_str):
            return LocalDataset(path=os.path.dirname(path_str), file=os.path.basename(path_str))
        
        # Create manager
        manager = DatasetLogManager.from_dict(config, mock_factory)
        assert manager is not None
        
        # Get log
        log_dict = manager.get_log(
            dataset_name='integration_test',
            data=sample_data,
            time_signature_func=lambda d, **k: '2024-01-01'
        )
        assert 'dataset' in log_dict
        assert log_dict['dataset'] == 'integration_test'
        
        # Write log
        time = datetime(2024, 1, 1)
        with mock.patch.object(manager.output, 'write_data'):
            manager.write_log(log_dict, time)
