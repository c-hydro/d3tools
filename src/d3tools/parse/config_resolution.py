"""Configuration placeholder resolution utilities.

This module contains the canonical implementation for replacing environment
and dataset placeholders in workflow option structures.
"""

from __future__ import annotations

import os
import re
from typing import Any

def set_env(structure: Any):
    """Resolve ``{ENV.VAR}`` placeholders recursively.

    Supported syntax:
    - ``{ENV.VAR_NAME}``
    - ``{ENV.VAR_NAME, default = 'fallback'}``

    Resolution is applied to strings across nested dict/list structures.

    Args:
        structure: Input structure (dict/list/string/scalar).

    Returns:
        Structure of the same shape with environment placeholders resolved.

    Raises:
        ValueError: If a referenced variable is missing and no default is
            provided.
    """
    if isinstance(structure, dict):
        return {set_env(key): set_env(value) for key, value in structure.items()}
    if isinstance(structure, list):
        return [set_env(value) for value in structure]
    if isinstance(structure, str):
        pattern = r"(\{ENV\.([\w#\-\.]+)(?:\s*,\s*(?:default\s*=\s*)?'(.*?)'\s*)?\})"
        matches = re.findall(pattern, structure)
        if matches:
            for match in matches:
                var = match[1]
                default = match[2] if len(match[2]) > 0 else None
                value = os.getenv(var, default)

                if value is None:
                    raise ValueError(
                        f"Environment variable {var} is not set and no default value provided"
                    )

                structure = structure.replace(match[0], value)
    return structure


def set_dataset(structure: Any, obj_dict: dict[str, Any]):
    """Resolve dataset placeholders recursively.

    Supported syntax:
    - ``{dataset_key}``
    - ``{dataset_key, tag = 'value'}`` (applies ``.update(**tags)`` on object)

    Placeholder matching is performed only when the full string matches the
    dataset-reference pattern. Resolution is applied across nested dict/list
    structures.

    Args:
        structure: Input structure (dict/list/string/scalar).
        obj_dict: Mapping from dataset placeholder keys to dataset-like objects.

    Returns:
        Structure of the same shape with dataset placeholders resolved where
        possible.
    """
    if isinstance(structure, dict):
        return structure.__class__({set_dataset(key, obj_dict): set_dataset(value, obj_dict) for key, value in structure.items()})
    if isinstance(structure, list):
        return structure.__class__([set_dataset(value, obj_dict) for value in structure])
    if isinstance(structure, str):
        pattern = r"{([\w#-\.]+)(?:\s*,\s*([\w#-\.]+\s*=\s*\'.*?\')+)?}"
        match = re.match(pattern, structure)
        if match:
            key = match.group(1)
            structure = obj_dict.get(key, structure)

            if len(match.groups()) > 1:
                tag_values = match.group(2)
                if tag_values:
                    tags = {}
                    tag_values_pattern = r"([\w#-\.]+)\s*=\s*'(.*?)'"
                    for tag_values_match in re.finditer(tag_values_pattern, tag_values):
                        tags[tag_values_match.group(1)] = tag_values_match.group(2)
                    structure = structure.update(**tags)
    return structure


__all__ = ["set_env", "set_dataset"]
