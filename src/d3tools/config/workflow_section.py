"""Workflow section recognition and metadata container."""

from dataclasses import dataclass

from typing import Any

from .parsers import workflow_section_from_config

WORKFLOW_SECTION_ALIASES = {
    "door_downloader": "door",
    "downloader": "door",
    "download": "door",
    "dam_workflow": "dam",
    "process": "dam",
    "publish": "dam",
    "dryes_index": "dryes",
    "index": "dryes",
    "calculate": "dryes",
}

def normalize_section_alias(value: str) -> str:
    """Normalize alias tokens to a stable lookup key."""
    return value.strip().lower().replace("-", "_").replace(" ", "_")

def resolve_workflow_section_alias(section: str) -> str | None:
    """Resolve a workflow-section alias to an engine keyword."""
    return WORKFLOW_SECTION_ALIASES.get(normalize_section_alias(section), None)

@dataclass
class WorkflowSection:
    """Normalized workflow section entry collected from top-level config keys.

    This object stores only the minimal information required to build the
    package-specific runtime object in a later stage.

    - ``name``:    original top-level key name
    - ``engine``:  engine keyword (``door``, ``dam``, ``dryes``)
    - ``definition``: section payload for this section item
    - ``value``:   parsed section payload (currently passthrough scaffold)
    """
    name: str
    engine: str
    definition: Any
    value: Any

    @classmethod
    def from_config(
            cls,
            name: str,
            definition: Any,
            build_object: bool = False,
            strict_imports: bool = False,
        ) -> "WorkflowSection":
        """Build a workflow section from raw top-level key/value.

        Args:
            name: Original top-level workflow key.
            definition: Parsed section payload.
            build_object: If ``True``, try constructing runtime objects.
            strict_imports: If ``True``, propagate build/import errors.
        """
        engine = resolve_workflow_section_alias(name)
        if engine is None:
            raise ValueError(f"Key '{name}' is not a recognized workflow section")
        value = workflow_section_from_config(
            engine,
            definition,
            build_object=build_object,
            strict_imports=strict_imports,
        )
        return cls(name=name, engine=engine, definition=definition, value=value)
