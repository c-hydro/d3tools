"""Configuration parsing pipeline.

Phase-1 scaffolding for explicit parsing stages used by ``Options.parse``.
Each stage preserves existing behavior while making responsibilities clearer.
"""

from __future__ import annotations

from typing import Any

from ..data import Dataset
from ..parse import flatten_dict, set_dataset, set_env, substitute_values


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


def parse_options(options: Any):
    """Run the full phase-1 parsing pipeline."""
    parsed = resolve_env(options)
    parsed = resolve_tags(parsed)
    parsed = build_datasets(parsed)
    parsed = resolve_dataset_refs(parsed)
    return parsed


__all__ = [
    "resolve_env",
    "resolve_tags",
    "build_datasets",
    "resolve_dataset_refs",
    "parse_options",
]
