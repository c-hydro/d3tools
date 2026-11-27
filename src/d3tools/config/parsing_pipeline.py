"""Stages for resolving and normalizing workflow configuration.

The public :func:`parse_options` function applies the environment, current-time,
tag, dataset, dataset-reference, workflow-log, and workflow-section stages in
that order.
"""

from __future__ import annotations

import os
from typing import Any

from ..data import Dataset
from ..config.options import Options
from ..parse import flatten_dict, set_dataset, set_env, substitute_values, normalise_string
from .workflow_section import WorkflowSection
from .workflow_definition import WorkflowDefinition

def resolve_env(options: Any):
    """Resolve environment placeholders recursively in a configuration mapping.

    Args:
        options: Mapping to process. Plain mappings are first wrapped in
            :class:`Options`.

    Returns:
        An ``Options`` instance (or its subclass) with environment references
        resolved in its keys and values.

    Raises:
        ValueError: A referenced environment variable is unset and has no
            placeholder default.
    """
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)
    return options.__class__(set_env(options))

def resolve_tags(options: Any):
    """Resolve tag placeholders using the mapping's ``tags`` section.

    Tag values are resolved against that same section first, then substituted
    recursively throughout the configuration, including mapping keys.

    Args:
        options: Mapping to process. Plain mappings are first wrapped in
            :class:`Options`.

    Returns:
        An ``Options`` instance (or its subclass) with tag references
        substituted.
    """
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    tags = options.get("tags", {}, ignore_case=True)
    tags = substitute_values(tags, tags, rec=True)
    return options.__class__(substitute_values(options, tags, rec=True))

def resolve_now(options: Any):
    """Resolve ``{now}`` placeholders using the current wall-clock time.

    A single ``datetime`` value is used for all ``{now}`` references within the
    supplied configuration in this call.

    Args:
        options: Mapping to process. Plain mappings are first wrapped in
            :class:`Options`.

    Returns:
        An ``Options`` instance (or subclass) with current-time placeholders
        resolved.
    """
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    import datetime as dt
    now = dt.datetime.now()
    return options.__class__(substitute_values(options, {"now": now}, rec=True))

def build_datasets(options: Any):
    """Replace dataset definitions with ``Dataset`` instances.

    The optional ``__defaults__`` entry is removed from the datasets mapping
    and passed as defaults when constructing each configured dataset.

    Args:
        options: Mapping to update. Plain mappings are first wrapped in
            :class:`Options`.

    Returns:
        The same ``Options`` instance when one is supplied, otherwise the new
        ``Options`` wrapper. The dataset entries are ``Dataset`` instances.
    """

    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    dataset_options, _ = options.get("datasets", {}, ignore_case=True, get_key=True)
    defaults = dataset_options.pop("__defaults__", None)

    for dsname, dsopt in dataset_options.items():
        dataset_options[dsname] = Dataset.from_options(dsopt, defaults)

    return options


def resolve_dataset_refs(options: Any):
    """Replace dataset references with their corresponding dataset objects.

    References are resolved recursively from the configured datasets mapping.
    Unmatched placeholders remain strings; references with tag assignments
    apply those tags through the dataset object's ``update`` method.

    Args:
        options: Mapping to process. Plain mappings are first wrapped in
            :class:`Options`.

    Returns:
        An ``Options`` instance (or its subclass) with resolvable references
        replaced.
    """
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    dataset_options, ds_key = options.get("datasets", {}, ignore_case=True, get_key=True)
    flat_dsoptions = flatten_dict({ds_key: dataset_options})
    return set_dataset(options, flat_dsoptions)


def prepare_workflow_log(options: Any):
    """Normalize and validate the ``workflow_log`` option without creating a logger.

    A string is replaced by a ``{"file": string}`` mapping. ``None`` and an
    empty mapping are retained. Mapping keys outside ``file``, ``level``,
    ``console``, ``format``, ``format_file``, ``format_console``,
    ``logger_name`` and ``run_state_file`` produce a warning but are retained.
    This stage does not resolve ``{now}`` placeholders; the full parsing
    pipeline resolves them in its earlier current-time stage.

    Args:
        options: Mapping to update. Plain mappings are first wrapped in
            :class:`Options`.

    Returns:
        The normalized mapping.

    Raises:
        TypeError: ``workflow_log`` is not ``None``, a string, or a mapping.
    """
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    # Use plain dict.get() since options may be a plain dict at this stage
    workflow_log,_ = options.get("workflow_log", {}, ignore_case=True, get_key=True)
    
    # If workflow_log doesn't exist or is None, nothing to do
    if workflow_log is None:
        return options
    
    # Normalize to dict if it's a string
    if isinstance(workflow_log, str):
        options["workflow_log"] = {"file": workflow_log}
    
    # If it's an empty dict, that's valid (disables logging)
    elif isinstance(workflow_log, dict) and len(workflow_log) == 0:
        pass
    
    # If it's a dict with config, validate it has reasonable keys
    elif isinstance(workflow_log, dict):
        # Valid keys for workflow_log config
        valid_keys = {'file', 'level', 'console', 'format', 'format_file', 
                     'format_console', 'logger_name', 'run_state_file'}
        
        # Check for unknown keys (warn but don't fail)
        unknown_keys = set(workflow_log.keys()) - valid_keys
        if unknown_keys:
            import warnings
            warnings.warn(
                f"Unknown keys in workflow_log config: {', '.join(unknown_keys)}. "
                f"Valid keys are: {', '.join(sorted(valid_keys))}"
            )
    
    else:
        raise TypeError(
            f"workflow_log must be None, str, or dict, got {type(workflow_log).__name__}"
        )
    
    return options


def collect_workflow_sections(
        options: Any,
        build_workflow_objects: bool = True,
        strict_workflow_imports: bool = True,
    ):
    """Collect ordered workflow sections recognized by alias mapping.

    This stage scans top-level keys in insertion order, recognizes workflow
    aliases (download/process/publish/calculate...), and stores them under
    ``workflow_sections`` as ``WorkflowSection`` objects.

    Non-workflow top-level keys are ignored by this collector and kept untouched.
    List-valued workflow sections are flattened into multiple entries preserving
    item order.

    Collected workflow section keys are removed from top-level options so
    downstream code uses ``workflow_sections`` as the single access path.

    Args:
        options: An ``Options`` mapping containing workflow definitions. The
            mapping's extended ``get`` method is used for case-insensitive
            access.
        build_workflow_objects: Whether to build runtime objects for sections
            (default ``True``).
        strict_workflow_imports: If ``True`` (default ``True``), propagate
            build/import failures.

    Returns:
        The same mapping with recognized non-reserved top-level entries
        collected as ``WorkflowSection`` objects under ``workflow_sections``.
        Collected entries are removed from the top level. List-valued entries
        produce numbered sections in list order.
    """

    # a default engine and exec_options can be specified at the top-level and will be passed to all sections that don't specify them explicitly
    def_engine       = options.get("engine", None, ignore_case=True)
    def_exec_options = options.get("exec_options", {}, ignore_case=True)

    default_definition = {"engine": def_engine, "exec_options": def_exec_options}

    collected = []
    collected_keys = []
    for key, value in options.items():
        if key == "workflow_sections":
            continue
        # all the keys that are not recognised as reserved top-level keys are considered workflow sections
        if normalise_string(key) in WorkflowDefinition.RESERVED_TOP_LEVEL_KEYS:
            continue
        collected_keys.append(key)

        if isinstance(value, list):
            i=1
            for item in value:
                this_definition = {**default_definition, **item}
                collected.append(
                    WorkflowSection.from_config(
                        name=f'{key}_{i:02d}',
                        definition=this_definition,
                        build_object=build_workflow_objects,
                        strict_imports=strict_workflow_imports,
                    )
                )
                i+=1
        else:
            this_definition = {**default_definition, **value}
            collected.append(
                WorkflowSection.from_config(
                    name=key,
                    definition=this_definition,
                    build_object=build_workflow_objects,
                    strict_imports=strict_workflow_imports,
                )
            )

    options["workflow_sections"] = collected
    for key in collected_keys:
        del options[key]

    return options


def parse_options(
        options: Any,
        build_workflow_objects: bool = False,
        strict_workflow_imports: bool = False,
    ):
    """Run the full parsing pipeline.

    Args:
        options: Workflow options mapping. Plain mappings are wrapped in
            :class:`Options`.
        build_workflow_objects: Whether to build runtime objects for collected
            workflow sections (default ``False``).
        strict_workflow_imports: If ``True`` (default ``False``), propagate
            build/import failures.

    Returns:
        Parsed ``Options`` with resolved values, instantiated datasets, and
        collected workflow sections. Environment, tag and dataset resolution
        creates updated nested structures; workflow sections replace their
        original top-level entries.

    Raises:
        TypeError: The ``env`` option is neither a mapping nor ``None``.

    Side Effects:
        Values in the ``env`` option are converted to strings and assigned to
        ``os.environ`` before placeholder resolution.
    """
    if not isinstance(options, Options):
        options = Options(options)
    
    # Set environment variables from the "env" section before any other processing
    env_keys = options.get("env", None, ignore_case=True)
    if env_keys is None:
        env_keys = {}
    elif not isinstance(env_keys, dict):
        raise TypeError(
            f"env must be a dict or None, got {type(env_keys).__name__}"
        )

    for key, value in env_keys.items():
        os.environ[str(key)] = str(value)

    parsed = resolve_env(options)
    parsed = resolve_now(parsed)
    parsed = resolve_tags(parsed)
    parsed = build_datasets(parsed)
    parsed = resolve_dataset_refs(parsed)
    parsed = prepare_workflow_log(parsed)
    parsed = collect_workflow_sections(
        parsed,
        build_workflow_objects=build_workflow_objects,
        strict_workflow_imports=strict_workflow_imports,
    )
    return parsed

__all__ = [
    "resolve_env",
    "resolve_tags",
    "build_datasets",
    "resolve_dataset_refs",
    "prepare_workflow_log",
    "collect_workflow_sections",
    "parse_options",
]
