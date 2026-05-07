"""Workflow section recognition and metadata container."""

from dataclasses import dataclass

from typing import Any
import os

from .parsers import workflow_section_from_config
from ..parse.string_rendering import normalise_string
from ..timestepping import TimeRange, TimeWindow

WORKFLOW_SECTION_ALIASES = {
    "door_downloader": "door",
    "downloader": "door",
    "download": "door",
    "ingest": "door",
    "dam_workflow": "dam",
    "process": "dam",
    "publish": "dam",
    "dryes_index": "dryes",
    "index": "dryes",
    "calculate": "dryes",
}

def resolve_workflow_section_alias(section: str) -> str | None:
    """Resolve a workflow-section alias to an engine keyword."""
    return WORKFLOW_SECTION_ALIASES.get(normalise_string(section), None)

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
    exec_options: dict[str, Any] = None

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
        # attempt to get the `engine` keyword from the definition key
        engine = definition.get("engine", None)
        if engine is None or engine not in ['door', 'dam', 'dryes']:
            # if not found, try to resolve from the section name
            engine = resolve_workflow_section_alias(name)
        if engine is None:
            raise ValueError(f"Key '{name}' is not a recognized workflow section")
        
        exec_options = definition.get("exec_options", {})
        value = workflow_section_from_config(
            engine,
            definition,
            build_object=build_object,
            strict_imports=strict_imports,
        )
        return cls(name=name, engine=engine, definition=definition, value=value, exec_options=exec_options)
    
    def get_run_timerange(self) -> TimeRange:
        """Determine the execution range for this workflow section.

        The section value is expected to provide ``get_last_ts()``, returning a
        pair ``(last_available, last_done)``. The resulting range starts at the
        first timestep that still needs processing and ends at the latest
        available timestep.

        Returns:
            TimeRange spanning the timesteps that still need to be processed.
            None if there are no timesteps to process (i.e. all available timesteps have already been processed).

        Raises:
            ValueError: If the section has no available data.
        """
        process = self.value
        last_available, last_done = process.get_last_ts()
        
        repeat_window = (self.exec_options or {}).get("repeat_window", os.getenv("REPEAT_WINDOW", None))
        if repeat_window is not None:
            repeat_window = TimeWindow.from_str(repeat_window)

        if last_available is None or last_done is None:
            raise ValueError(f"Workflow section '{self.name}' has not enough available data to determine time range for execution")
        elif last_available <= last_done and repeat_window is None:
            return None

        next_ts = last_done + 1
        last_ts = next_ts
        while last_ts.end <= last_available.end:
            last_ts = last_ts + 1
        
        start = next_ts.start
        end = (last_ts - 1).end
        time_range = TimeRange(start, end)

        if repeat_window is not None:
            time_range = time_range.extend(repeat_window, before = True)
        
        return time_range
