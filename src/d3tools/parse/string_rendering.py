"""String rendering utilities for parser placeholders.

This module is a phase-1 scaffolding layer that groups placeholder rendering
functions under a clear parsing concern. Behavior is intentionally delegated
to legacy implementations to keep this change non-breaking.
"""

from __future__ import annotations

from typing import Any

from .parse_utils import (
    substitute_string as _legacy_substitute_string,
    substitute_values as _legacy_substitute_values,
)


def substitute_string(string: Any, tag_dict: dict[str, Any], rec: bool = False):
    """Render placeholders in a string.

    This is a compatibility wrapper over the legacy implementation.
    """
    return _legacy_substitute_string(string, tag_dict, rec=rec)


def substitute_values(structure: Any, tag_dict: dict[str, Any], **kwargs):
    """Recursively render placeholders in nested structures.

    This is a compatibility wrapper over the legacy implementation.
    """
    return _legacy_substitute_values(structure, tag_dict, **kwargs)


__all__ = ["substitute_string", "substitute_values"]
