"""Configuration parsing pipeline.

Phase-1 scaffolding for explicit parsing stages used by ``Options.parse``.
Each stage preserves existing behavior while making responsibilities clearer.
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
    """Resolve environment-variable placeholders in options."""
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)
    return options.__class__(set_env(options))


def resolve_tags(options: Any):
    """Resolve tag placeholders in options using the tags section itself."""
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    tags = options.get("tags", {}, ignore_case=True)
    tags = substitute_values(tags, tags, rec=True)
    return options.__class__(substitute_values(options, tags, rec=True))


def build_datasets(options: Any):
    """Instantiate datasets defined under the datasets section."""

    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    dataset_options, _ = options.get("datasets", {}, ignore_case=True, get_key=True)
    defaults = dataset_options.pop("__defaults__", None)

    for dsname, dsopt in dataset_options.items():
        dataset_options[dsname] = Dataset.from_options(dsopt, defaults)

    return options


def resolve_dataset_refs(options: Any):
    """Resolve dataset placeholders (e.g. ``{datasets.foo}``) in options."""
    # convert options to Oprions if it's a plain dict to use case-insensitive get() and ignore_case=True
    if not isinstance(options, Options):
        options = Options(options)

    dataset_options, ds_key = options.get("datasets", {}, ignore_case=True, get_key=True)
    flat_dsoptions = flatten_dict({ds_key: dataset_options})
    return set_dataset(options, flat_dsoptions)


def prepare_workflow_log(options: Any):
    """Prepare and normalize workflow_log configuration.
    
    This stage ensures the workflow_log config is properly normalized
    and validated, but doesn't instantiate the logger (that happens
    at run-time in WorkflowDefinition.run()).
    
    The workflow_log config supports:
        - None or empty dict: Disables logging
        - String: Treated as log file path with defaults
        - Dict: Full configuration with file, level, console, format, etc.
    
    Note: {now:...} placeholders in 'file' paths are intentionally NOT
    resolved here - they're resolved at run-time to get accurate timestamps.
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
        options: Parsed options mapping.
        build_workflow_objects: Whether to build runtime objects for sections.
        strict_workflow_imports: If ``True``, propagate build/import failures.
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
        options: Workflow options mapping.
        build_workflow_objects: Whether to build runtime objects for collected
            workflow sections.
        strict_workflow_imports: If ``True``, propagate build/import failures.
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
