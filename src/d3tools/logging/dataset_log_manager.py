"""Collect and write metadata and quality checks for dataset operations."""

import datetime as dt
import numpy as np
import xarray as xr
from typing import Optional, Dict, Any


class DatasetLogManager:
    """Collect dataset metadata and summary checks before writing a log entry.

    The manager holds a destination dataset-like object and records a dictionary
    of metadata, quality metrics, and any additional context provided by callers.
    When an output dataset is configured, each call to :meth:`write_log` writes a
    single entry with the configured destination's ``write_data`` method.

    Args:
        output_dataset: Destination Dataset-like object. If ``None``, calls to
            :meth:`write_log` are no-ops.
        **options: Extra configuration retained on the manager and passed through
            only when the instance is later used as a managed object.
    """

    def __init__(self, output_dataset, **options):
        """Store the configured destination and metadata options.

        Args:
            output_dataset: Dataset-like object used by :meth:`write_log`.
            **options: Additional state values retained on the instance.
        """
        self.output = output_dataset
        self.options = options

    @classmethod
    def from_dict(cls, config, dataset_factory=None):
        """Build a manager from a configuration value or existing destination.

        ``None`` and empty dictionaries produce ``None``. Strings are rendered
        with ``{now}`` substitution and passed to ``dataset_factory``. Mapping
        inputs must include a ``file`` key, which is substituted before being
        passed to the factory; any remaining mapping entries are retained as
        manager options. Other objects are treated as already-created dataset
        destinations.

        Args:
            config: ``None``, a path string, a ``file``-key mapping, or an
                existing output object.
            dataset_factory: Callable used to materialize a dataset destination
                from a substituted log-file path.

        Returns:
            A configured :class:`DatasetLogManager` or ``None`` when the config
            is absent.

        Raises:
            ValueError: If a non-empty dictionary lacks a ``file`` key or if a
                string path is supplied without a ``dataset_factory``.
        """
        from ..parse import substitute_string

        # Handle different input types
        if config is None:
            return None  # do we want this? None means no log manager?
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
                return None  # do we want this? empty dict means no log manager?
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
        """Build a log mapping from dataset metadata and runtime context.

        Values in ``data.attrs`` take precedence over same-named keyword entries.
        The result keeps only values that match the supported log formatting
        types: ``TimeStep``-like objects, datetimes, non-empty strings not
        starting with ``__``, integers, and floats. Numeric values are converted
        to strings, with floats rounded to four decimal places. The final record
        always includes a ``data_checks`` summary generated from the supplied
        xarray object.

        Args:
            dataset_name: Name recorded under the ``dataset`` key.
            data: DataArray or Dataset whose attributes are inspected for log
                metadata.
            time_signature_func: Callable used when a value's class name is
                ``TimeStep``; it must return an object with ``strftime``.
            **kwargs: Additional context fields, such as ``source_key``,
                ``thumbnail``, or ``time``.

        Returns:
            A dictionary of formatted log fields and quality-check results.
        """
        log_dict = {}
        metadata = data.attrs if hasattr(data, 'attrs') else {}

        log_dict['dataset'] = dataset_name
        log_dict['source_key'] = metadata.get('source_key', kwargs.get('source_key', None))
        log_dict['thumbnail'] = kwargs.get('thumbnail', None)
        log_dict['time'] = metadata.get('time', kwargs.get('time', None))
        log_dict['time_produced'] = metadata.get('time_produced', dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

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
        """Write a single log entry to the configured output destination.

        Text-format destinations receive a delimited block string; JSON and other
        structured formats receive the original dictionary unchanged. The
        destination's ``write_data`` method is called with ``append=True``.

        Args:
            log_dict: Mapping of log fields to write.
            time: Optional timestamp passed through to ``write_data``.
            **kwargs: Additional keyword arguments forwarded to ``write_data``.

        Returns:
            ``None``. The method is a no-op when no output destination is set.
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
        """Calculate summary quality checks for an xarray object.

        For a ``DataArray`` the result contains ``max``, ``min``, ``nans``,
        ``nans_pc``, ``zeros``, ``zeros_pc``, ``sum``, and ``sum_abs``. Dataset
        results collapse each variable into keys named
        ``<variable>_<field>``. NaN values are ignored in extrema and sums,
        ``zeros_pc`` uses the count of non-NaN values as its denominator, and
        any non-integer result is rounded to four decimal places.

        Args:
            data: DataArray or Dataset to inspect.

        Returns:
            Mapping of check names to numeric summary values.
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
