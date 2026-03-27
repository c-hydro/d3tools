"""Configuration parsing pipeline.

Phase-1 scaffolding for explicit parsing stages used by ``Options.parse``.
Each stage preserves existing behavior while making responsibilities clearer.
"""

from __future__ import annotations

from typing import Any

from ..data import Dataset
from ..parse import flatten_dict, set_dataset, set_env, substitute_values
from .workflow_section import WorkflowSection, resolve_workflow_section_alias


def resolve_env(options: Any):
    """Resolve environment-variable placeholders in options."""
    return options.__class__(set_env(options))


def resolve_tags(options: Any):
    """Resolve tag placeholders in options using the tags section itself."""
    tags = options.get("tags", {}, ignore_case=True)
    tags = substitute_values(tags, tags, rec=True)
    return options.__class__(substitute_values(options, tags, rec=True))


def build_datasets(options: Any):
    """Instantiate datasets defined under the datasets section."""
    dataset_options, _ = options.get("datasets", {}, ignore_case=True, get_key=True)
    defaults = dataset_options.pop("__defaults__", None)

    for dsname, dsopt in dataset_options.items():
        dataset_options[dsname] = Dataset.from_options(dsopt, defaults)

    return options


def resolve_dataset_refs(options: Any):
    """Resolve dataset placeholders (e.g. ``{datasets.foo}``) in options."""
    dataset_options, ds_key = options.get("datasets", {}, ignore_case=True, get_key=True)
    flat_dsoptions = flatten_dict({ds_key: dataset_options})
    return set_dataset(options, flat_dsoptions)

def collect_workflow_sections(options: Any):
    """Collect ordered workflow sections recognized by alias mapping.

    This stage scans top-level keys in insertion order, recognizes workflow
    aliases (download/process/publish/calculate...), and stores them under
    ``workflow_sections`` as ``WorkflowSection`` objects.

    Non-workflow top-level keys are ignored by this collector and kept untouched.
    List-valued workflow sections are flattened into multiple entries preserving
    item order.

    Collected workflow section keys are removed from top-level options so
    downstream code uses ``workflow_sections`` as the single access path.
    """
    collected = []
    collected_keys = []
    for key, value in options.items():
        if key == "workflow_sections":
            continue
        if resolve_workflow_section_alias(key) is None:
            continue
        collected_keys.append(key)

        if isinstance(value, list):
            for item in value:
                collected.append(WorkflowSection.from_config(name=key, definition=item))
        else:
            collected.append(WorkflowSection.from_config(name=key, definition=value))

    options["workflow_sections"] = collected
    for key in collected_keys:
        del options[key]

    return options


def parse_options(options: Any):
    """Run the full phase-1 parsing pipeline."""
    parsed = resolve_env(options)
    parsed = resolve_tags(parsed)
    parsed = build_datasets(parsed)
    parsed = resolve_dataset_refs(parsed)
    parsed = collect_workflow_sections(parsed)
    return parsed

__all__ = [
    "resolve_env",
    "resolve_tags",
    "build_datasets",
    "resolve_dataset_refs",
    "collect_workflow_sections",
    "parse_options",
]
