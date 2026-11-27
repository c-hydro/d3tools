"""
Configuration parsers for d3tools.

This module contains functions for parsing configuration dictionaries
into d3tools objects (Datasets, managers, etc.).

Separating parsing logic here makes it reusable across d3tools, door, dryes, and dam.
"""

import os
from typing import Optional, Dict, Any, Callable, Union

from ..errors import WorkflowEngineImportError

# Create dataset_factory for nested dataset parsing
# This allows manager configs to reference other datasets
def dataset_factory(cfg, template_ds):
    """Build a nested dataset from a template dataset and a config fragment.

    Args:
        cfg: Nested dataset options, or a string interpreted as the
            dataset ``key_pattern``.
        template_ds: Template dataset providing default creation arguments.
            The template's ``name`` is not inherited by the nested dataset.

    Returns:
        A new ``Dataset`` instance configured from ``cfg`` and the template
        defaults.
    """

    # use the type from parsed_config as default
    defaults = template_ds._creation_kwargs.copy()
    # remove the "name" key if present, since it should not be inherited by nested datasets
    defaults.pop("name", None)

    # if cfg is a string, assume it is the key_pattern
    if isinstance(cfg, str):
        cfg = {'key_pattern': cfg}

    return dataset_from_config(cfg, defaults=defaults)

def dataset_from_config(config: Union['Dataset', str, Dict[str, Any]], defaults: Optional[Dict[str, Any]] = None):
    """
    Create a ``Dataset`` from a dataset configuration.
    
    A string is treated as a ``key_pattern``. For mappings, configuration
    values override matching defaults. ``thumbnail``, ``log`` and ``fallback``
    are parsed separately and attached to the created dataset. An existing
    ``Dataset`` instance is returned unchanged.
    
    Args:
        config: Existing ``Dataset``, key-pattern string, or dataset options
            mapping.
        defaults: Optional mapping of defaults (default ``None``), overridden
            by ``config``.
        
    Returns:
        The input ``Dataset`` unchanged when ``config`` is already a dataset;
        otherwise, the constructed instance of the configured ``Dataset``
        subclass. Parsed thumbnail, log, and fallback managers are attached
        to newly constructed datasets.
        
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

    if isinstance(config, Dataset):
        return config
    
    # Merge with defaults
    defaults = defaults or {}
    parsed_config = defaults.copy()
    config = config if isinstance(config, dict) else {"key_pattern": config}
    parsed_config.update(config)

    # extract thumbnail and log configs before creating the dataset
    thumbnail_config = parsed_config.pop('thumbnail', None)
    log_config       = parsed_config.pop('log', None)

    # also extract fallback config if present, to pass to the dataset constructor
    fallback_config = parsed_config.pop('fallback', None)

    # create the dataset without managers first, so we can use it in manager parsing if needed
    type_str = parsed_config.pop('type', None)
    type_str = Dataset.get_type(type_str)
    Subclass = Dataset.get_subclass(type_str)
    ds = Subclass(**parsed_config)

    # Parse manager configurations if present
    ds.thumbnail = _manager_from_config(thumbnail_config, 'thumbnail', lambda x : dataset_factory(x, template_ds=ds))
    ds.log       = _manager_from_config(log_config, 'log',  lambda x : dataset_factory(x, template_ds=ds))
    
    # Handle fallback dataset if present
    if fallback_config is not None:
        ds.fallback = dataset_factory(fallback_config, template_ds=ds)

    return ds

def _manager_from_config(config: Any, manager_type: str, dataset_factory: Callable) -> Any:
    """Parse a thumbnail or log manager configuration, or return ``None``."""
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
    """Wrap a workflow configuration and delegate to ``Options.parse``.

    Args:
        config: Complete workflow configuration mapping.

    Returns:
        The value returned by the underlying ``parse`` method.

    Raises:
        AttributeError: If the wrapped options object does not provide a
            ``parse`` method.
    """
    from .options import Options
    options = Options(config)
    return options.parse()

def _build_door_downloader(section_options: Any) -> Any:
    from door import Downloader
    return Downloader.from_options(section_options)

def _build_dam_workflow(section_options: Any) -> Any:
    from dam import DAMWorkflow
    return DAMWorkflow.from_options(section_options)

def _build_dryes_index(section_options: Any) -> Any:
    if not isinstance(section_options, dict):
        raise TypeError("DRYES section options must be a mapping")
    from dryes import DRYESIndex
    return DRYESIndex.from_options(**section_options)

_WORKFLOW_ENGINE_BUILDERS = {
    "door": _build_door_downloader,
    "dam": _build_dam_workflow,
    "dryes": _build_dryes_index,
}

def workflow_section_from_config(
        engine: str,
        section_options: Any,
        build_object: bool = False,
        strict_imports: bool = False,
    ) -> Any:
    """
    Parse a workflow section payload for a specific engine.
    
    Args:
        engine: Engine keyword: ``"door"``, ``"dam"`` or ``"dryes"``.
        section_options: Configuration options for the section.
        build_object: If ``True``, build and return the engine's runtime object.
            Otherwise return ``section_options`` unchanged (default ``False``).
        strict_imports: When building, whether an ``ImportError`` is wrapped
            and raised as ``WorkflowEngineImportError``. When ``False``, the
            original ``section_options`` are returned after an import failure
            (default ``False``).
        
    Returns:
        Parsed section payload or runtime object depending on ``build_object``.

    Raises:
        ValueError: ``engine`` is not one of the supported keywords.
        WorkflowEngineImportError: Building is requested, ``strict_imports``
            is true, and an engine import fails.
    """
    if engine not in _WORKFLOW_ENGINE_BUILDERS:
        raise ValueError(f"Unknown workflow section engine: {engine}")

    if not build_object:
        return section_options

    builder = _WORKFLOW_ENGINE_BUILDERS[engine]
    try:
        return builder(section_options)
    except ImportError as exc:
        if strict_imports:
            raise WorkflowEngineImportError(engine, exc) from exc
        return section_options

def parse_times_from_run_option(option_value: str|dict) -> tuple['Dataset', Optional[str]]:
    """
    Parse a ``times_from_run`` value into a dataset and optional section name.
    
    Args:
        option_value: A string path optionally prefixed by ``section_name@``,
            or a mapping with a truthy ``file`` value and optional ``section``.
            The file value may be a path string, dataset options mapping, or
            existing ``Dataset``.
    
    Returns:
        A tuple containing the parsed ``Dataset`` and section name, which is
        ``None`` when no section is specified.
    
    Raises:
        ValueError: If the input format is invalid or required keys are missing.
    """

    if isinstance(option_value, str):
        if "@" in option_value:
            section, file  = option_value.split("@", 1)
            file = file.strip()
            section = section.strip()
        else:
            file = option_value.strip()
            section = None

    elif isinstance(option_value, dict):
        file = option_value.get("file")
        section = option_value.get("section")
        if not file:
            raise ValueError("times_from_run dict must contain 'file' key")

    else:
        raise ValueError("times_from_run option must be a string or dict")

    return dataset_from_config(file), section
