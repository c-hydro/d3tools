"""
Configuration parsers for d3tools.

This module contains functions for parsing configuration dictionaries
into d3tools objects (Datasets, managers, etc.).

Separating parsing logic here makes it reusable across d3tools, door, dryes, and dam.
"""

import os
from typing import Optional, Dict, Any, Callable

def dataset_from_config(config: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None):
    """
    Create a Dataset from a configuration dictionary.
    
    This is the centralized parser for dataset configurations. It handles:
    - Merging with defaults
    - Parsing manager configurations (thumbnail, log)
    - Determining the appropriate Dataset subclass
    - Creating and returning the Dataset instance
    
    Args:
        config: Configuration dictionary for a dataset
        defaults: Optional defaults to merge with config
        
    Returns:
        Dataset instance of the appropriate subclass
        
    Example:
        >>> config = {
        ...     'type': 'local',
        ...     'path': '/data',
        ...     'file': 'output.tif',
        ...     'thumbnail': {
        ...         'colors': '/colors.txt',
        ...         'destination': '/thumbs/output.png'
        ...     },
        ...     'log': '/logs/output.txt'
        ... }
        >>> dataset = dataset_from_config(config)
        >>> # dataset.thumbnail is a DatasetThumbnailManager
        >>> # dataset.log is a DatasetLogManager
    """
    # Import here to avoid circular dependencies
    from ..data import Dataset
    
    # Merge with defaults
    defaults = defaults or {}
    parsed_config = defaults.copy()
    parsed_config.update(config)

    # extract thumbnail and log configs before creating the dataset
    thumbnail_config = parsed_config.pop('thumbnail', None)
    log_config       = parsed_config.pop('log', None)

    # create the dataset without managers first, so we can use it in manager parsing if needed
    type_str = parsed_config.pop('type', None)
    type_str = Dataset.get_type(type_str)
    Subclass = Dataset.get_subclass(type_str)
    ds = Subclass(**parsed_config)

    # Create dataset_factory for nested dataset parsing
    # This allows manager configs to reference other datasets
    def dataset_factory(cfg):

        # use the type from parsed_config as default
        defaults = ds._creation_kwargs.copy()

        # if cfg is a string, assume it is the key_pattern
        if isinstance(cfg, str):
            cfg = {'key_pattern': cfg}

        return dataset_from_config(cfg, defaults=defaults)

    # Parse manager configurations if present
    ds.thumbnail = _manager_from_config(thumbnail_config, 'thumbnail', dataset_factory)
    ds.log       = _manager_from_config(log_config, 'log', dataset_factory)
    
    return ds


def _manager_from_config(config: Any, manager_type: str, dataset_factory: Callable) -> Any:
    """
    Parse a manager configuration.
    
    Args:
        config: Manager configuration (dict, string, or already-parsed manager)
        manager_type: Type of manager ('thumbnail' or 'log')
        dataset_factory: Function to parse nested dataset configs
        
    Returns:
        Parsed manager object or None
    """
    if config is None:
        return None
    
    # Import managers here to avoid circular dependencies
    if manager_type == 'thumbnail':
        from ..thumbnails import DatasetThumbnailManager
        manager_class = DatasetThumbnailManager
        check_method = 'make_thumbnail'
    elif manager_type == 'log':
        from ..logging import DatasetLogManager
        manager_class = DatasetLogManager
        check_method = 'write_log'
    else:
        raise ValueError(f"Unknown manager type: {manager_type}")
    
    # Check if already a manager object
    if hasattr(config, check_method):
        return config
    
    # Parse the config into a manager
    return manager_class.from_dict(config, dataset_factory)


def workflow_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a complete workflow configuration.
    
    This is the entry point for parsing Options/workflow definitions.
    Currently delegates to Options.parse() but could be extended.
    
    Args:
        config: Complete workflow configuration dictionary
        
    Returns:
        Parsed configuration with Dataset objects
        
    Note:
        This function is a placeholder for future workflow parsing logic.
        For now, use Options.parse() directly.
    """
    from .options import Options
    options = Options(config)
    return options.parse()

def workflow_section_from_config(engine: str, section_options: Any) -> Any:
    """
    Parse a workflow section payload for a specific engine.
    
    Args:
        engine: Normalized workflow engine keyword ('door', 'dam', 'dryes')
        section_options: Configuration options for the section
        
    Returns:
        Parsed section payload.

    Note:
        This function is currently a scaffold and returns the input payload
        unchanged after engine validation. A future stage can instantiate
        engine-specific runtime objects here.
    """
    if engine not in ['door', 'dam', 'dryes']:
        raise ValueError(f"Unknown workflow section engine: {engine}")
    
    # return section options for now, later we will create DoorDownloader, DamWorkflow, DryesIndex objects here
    # based on the engine and the options provided
    return section_options
