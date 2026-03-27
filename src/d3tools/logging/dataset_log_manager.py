"""
DatasetLogManager - Handles logging of individual dataset operations.

This class encapsulates logging logic that was previously embedded
in the Dataset class, separating concerns and making Dataset simpler.

Note: This is dataset-specific. Future workflow-level logging will use
a separate LogManager class.
"""

import datetime as dt
import numpy as np
import xarray as xr
from typing import Optional, Dict, Any


class DatasetLogManager:
    """
    Manages logging of dataset write operations.
    
    Collects metadata, performs quality checks, and writes log entries
    to a specified output dataset.
    """
    
    def __init__(self, output_dataset, **options):
        """
        Initialize log manager.
        
        Args:
            output_dataset: Dataset where log entries should be written
            **options: Additional logging options
        """
        self.output = output_dataset
        self.options = options
    
    @classmethod
    def from_dict(cls, config, dataset_factory=None):
        """
        Create DatasetLogManager from a configuration dictionary.
        
        Args:
            config: Dictionary or string. If dict, should have 'file' key.
                   If string, used as path to log file.
            dataset_factory: Function to parse string paths into Dataset objects.
                           Should have signature: func(path_str) -> Dataset
        
        Returns:
            OutputLogManager instance
        """
        from ..parse import substitute_string
        

        # Handle different input types
        if config is None:
            return None # do we want this? None means no log manager?
        elif isinstance(config, str):
            # Simple string path
            log_file = substitute_string(config, {'now': dt.datetime.now()})
            if dataset_factory is not None:
                output_ds = dataset_factory(log_file)
            else:
                raise ValueError("dataset_factory required to parse string paths")
            options = {}
        
        elif isinstance(config, dict):
            if len(config) == 0:
                return None # do we want this? empty dict means no log manager?
            elif 'file' not in config:
                raise ValueError("Config dictionary must have a 'file' key")
            # Dictionary with 'file' key and optional other options
            config_copy = config.copy()
            log_file = substitute_string(config_copy.pop('file'), {'now': dt.datetime.now()})
            if dataset_factory is not None:
                output_ds = dataset_factory(log_file)
            else:
                raise ValueError("dataset_factory required to parse string paths")
            options = config_copy  # Keep remaining options
        
        else:
            # Assume it's already a Dataset object
            output_ds = config
            options = {}
        
        return cls(output_ds, **options)
    
    def get_log(self, dataset_name: str, data: xr.DataArray, time_signature_func, **kwargs) -> Dict[str, Any]:
        """
        Extract log information from data and context.
        
        Args:
            dataset_name: Name of the dataset being logged
            data: Data that was written
            time_signature_func: Function to format time signatures
            **kwargs: Additional context (source_key, thumbnail, time, etc.)
        
        Returns:
            Dictionary with log information
        """
        log_dict = {}
        metadata = data.attrs if hasattr(data, 'attrs') else {}
        
        log_dict['dataset'] = dataset_name
        log_dict['source_key'] = metadata.get('source_key', kwargs.get('source_key', None))
        log_dict['thumbnail'] = kwargs.get('thumbnail', None)
        log_dict['time'] = metadata.get('time', kwargs.get('time', None))
        log_dict['time_produced'] = metadata.get('time_produced', 
                                                 dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Add all metadata and kwargs
        kwargs.update(metadata)
        for k, v in kwargs.items():
            if k not in log_dict:
                log_dict[k] = v
        
        # Format values correctly
        final_log_dict = {}
        for key, value in log_dict.items():
            # Handle TimeStep objects
            if hasattr(value, '__class__') and value.__class__.__name__ == 'TimeStep':
                final_log_dict[key] = time_signature_func(value).strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, dt.datetime):
                final_log_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, str) and len(value.strip()) > 0 and not value.startswith('__'):
                final_log_dict[key] = value
            elif isinstance(value, int):
                final_log_dict[key] = str(value)
            elif isinstance(value, float):
                final_log_dict[key] = str(round(value, 4))
        
        # Add quality checks
        final_log_dict['data_checks'] = {k: str(v) for k, v in self.qc_checks(data).items()}
        
        return final_log_dict
    
    def write_log(self, log_dict: Dict[str, Any], time=None, **kwargs):
        """
        Write log entry to output dataset.
        
        Args:
            log_dict: Dictionary with log information
            time: Optional timestamp for the log entry
            **kwargs: Additional context for writing
        """
        # If no output configured, do nothing
        if self.output is None:
            return
        
        # Format based on output format
        if self.output.format == 'txt':
            log_str = '---\n'
            for key, value in log_dict.items():
                log_str += f'{key}: {value}\n'
            log_str += '---'
            log_output = log_str
        elif self.output.format == 'json':
            log_output = log_dict
        else:
            log_output = log_dict
        
        self.output.write_data(log_output, time, append=True, **kwargs)
    
    @staticmethod
    def qc_checks(data: xr.DataArray | xr.Dataset) -> Dict[str, Any]:
        """
        Perform quality checks on the data.
        
        Returns dict with:
        - max and min values
        - percentage and absolute number of NaNs
        - percentage and absolute number of zeros
        - sum of values
        - sum of absolute values
        
        Args:
            data: Data to check
        
        Returns:
            Dictionary with quality check results
        """
        if isinstance(data, xr.Dataset):
            full_dict = {}
            var_list = list(data.data_vars.values())
            for var in var_list:
                full_dict[var.name] = DatasetLogManager.qc_checks(var)
            
            qc_dict = {}
            for k, d in full_dict.items():
                for k_, v_ in d.items():
                    qc_dict[f'{k}_{k_}'] = v_
            
            return qc_dict
        
        data_values = data.values
        qc_dict = {}
        qc_dict['max'] = np.nanmax(data_values)
        qc_dict['min'] = np.nanmin(data_values)
        qc_dict['nans'] = int(np.sum(np.isnan(data_values)))
        qc_dict['nans_pc'] = qc_dict['nans'] / data_values.size * 100
        qc_dict['zeros'] = int(np.sum(data_values == 0))
        if (data_values.size - qc_dict['nans']) != 0:
            qc_dict['zeros_pc'] = qc_dict['zeros'] / (data_values.size - qc_dict['nans']) * 100
        else:
            qc_dict['zeros_pc'] = 0
        qc_dict['sum'] = np.nansum(data_values)
        qc_dict['sum_abs'] = np.nansum(np.abs(data_values))
        
        for key, value in qc_dict.items():
            if not isinstance(value, int):
                qc_dict[key] = round(float(value), 4)
        
        return qc_dict
