"""String/template placeholder rendering utilities.

This module contains the canonical implementation for substituting tag
placeholders in strings and nested Python structures.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from ..timestepping.time_utils import get_date_from_str

def normalise_string(value: str) -> str:
    """Normalize an alias string for stable lookup.

    Leading and trailing whitespace is removed, the string is lowercased,
    hyphens and spaces are converted to underscores, and repeated underscores are
    collapsed to a single underscore. The function is intended for user-facing
    aliases and tag names that should be matched case-insensitively.

    Args:
        value: Alias string to normalize.

    Returns:
        Normalized lookup key.
    """
    normalised = value.strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in normalised:
        normalised = normalised.replace("__", "_")
    return normalised

def substitute_string(string: Any, tag_dict: dict[str, Any], rec: bool = False):
    """Render placeholders in a template string.

    Supported placeholder forms are:
    - ``{tag}`` for plain substitution
    - ``{tag:format}`` for format-aware substitution

    If a tag value is a datetime-like string, it is converted through
    :func:`d3tools.timestepping.time_utils.get_date_from_str` before formatting.
    When the value is a datetime and a format is supplied, ``strftime`` is used.
    Otherwise the value is passed to ``format(value, format_spec)``. Unresolved
    tag names remain unchanged in the output.

    List-valued placeholders are expanded recursively to generate every
    combination of rendered values while preserving the original template shape.

    Args:
        string: Template string or any value. Non-string inputs are returned as-is.
        tag_dict: Mapping of placeholder names to values.
        rec: Compatibility flag kept for API stability. List expansion is always
            recursive for backward-compatible behavior.

    Returns:
        A rendered string, or a nested list of rendered strings when list-valued
        tags are expanded.
    """
    if not isinstance(string, str):
        return string

    pattern = r"{([\w#-\.]+)(?::(.*?))?}"

    def replace_match(match, local_tags):
        key = match.group(1)
        fmt = match.group(2)
        value = local_tags.get(key)

        if value is None:
            return match.group(0)

        raw_value = value
        if isinstance(value, str):
            try:
                value = get_date_from_str(value)
            except ValueError:
                pass

        if isinstance(value, dt.datetime) and fmt:
            return value.strftime(fmt)
        if fmt:
            return format(value, fmt)
        if isinstance(raw_value, str):
            return raw_value
        return str(value)

    def generate_strings(value: str, local_tags: dict[str, Any]):
        matches = re.findall(pattern, value)
        if not matches:
            return value

        key = matches[0][0]
        tag_value = local_tags.get(key)

        if isinstance(tag_value, list):
            out = []
            for item in tag_value:
                next_tags = local_tags.copy()
                next_tags[key] = item
                replaced = re.sub(pattern, lambda m: replace_match(m, next_tags), value, count=1)
                out.append(generate_strings(replaced, next_tags))
            return out

        return re.sub(pattern, lambda m: replace_match(m, local_tags), value)

    return generate_strings(string, tag_dict)


def substitute_values(structure: Any, tag_dict: dict[str, Any], **kwargs):
    """Recursively render placeholders in nested structures.

    This helper traverses dictionaries and lists and applies
    :func:`substitute_string` to all string leaves (and string keys in dicts).

    Args:
        structure: Input value to render (dict, list, string, or scalar).
        tag_dict: Mapping of placeholder names to values.
        **kwargs: Forwarded to :func:`substitute_string`.

    Returns:
        A structure of the same shape with placeholders substituted.
    """
    if isinstance(structure, dict):
        return {
            substitute_values(key, tag_dict, **kwargs): substitute_values(value, tag_dict, **kwargs)
            for key, value in structure.items()
        }
    if isinstance(structure, list):
        return [substitute_values(value, tag_dict, **kwargs) for value in structure]
    if isinstance(structure, str):
        return substitute_string(structure, tag_dict, **kwargs)
    return structure


def increment_version(version: str) -> str:
    """Increment the last integer found in a version string.

    The right-most integer token is incremented by 1 and zero-padded to match
    the width of the original token. If the incremented value grows beyond the
    original width, the wider value is kept, for example ``"v99"`` becomes
    ``"v100"``.

    Args:
        version: Version string containing at least one integer (e.g. ``"v01"``).

    Returns:
        Version string with the last integer incremented.

    Raises:
        ValueError: If no integer is found in *version*.
    """
    match = None
    for match in re.finditer(r'(\d+)', version):
        pass  # keep iterating to find the last match

    if match is None:
        raise ValueError(f"No integer found in version string: {version!r}")

    original = match.group(1)
    incremented = str(int(original) + 1).zfill(len(original))
    return version[:match.start()] + incremented + version[match.end():]


__all__ = ["substitute_string", "substitute_values", "increment_version"]
