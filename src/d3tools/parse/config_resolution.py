"""Configuration resolution utilities.

This module is a phase-1 scaffolding layer that groups configuration-specific
placeholder resolution functions under a clear parsing concern. Behavior is
intentionally delegated to legacy implementations to keep this change
non-breaking.
"""

from __future__ import annotations

from typing import Any

from .special_substitutions import (
    set_dataset as _legacy_set_dataset,
    set_env as _legacy_set_env,
)


def set_env(structure: Any):
    """Resolve environment-variable placeholders in a structure.

    This is a compatibility wrapper over the legacy implementation.
    """
    return _legacy_set_env(structure)


def set_dataset(structure: Any, obj_dict: dict[str, Any]):
    """Resolve dataset placeholders in a structure.

    This is a compatibility wrapper over the legacy implementation.
    """
    return _legacy_set_dataset(structure, obj_dict)


__all__ = ["set_env", "set_dataset"]
