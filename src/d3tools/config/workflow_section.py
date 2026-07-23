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


@dataclass
class WorkflowSectionRunResult:
    """Outcome of a WorkflowSection run attempt.

    Fields are intentionally minimal and logging-oriented.
    """

    section_name: str
    engine: str
    time_range: TimeRange | None
    executed: bool
    reason: str | None = None

def resolve_workflow_section_alias(section: str) -> str | None:
    """Resolve a workflow-section alias to an engine keyword."""

    for alias, engine in WORKFLOW_SECTION_ALIASES.items():
        if normalise_string(alias) in normalise_string(section):
            return engine
    return None

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
            build_object: bool = True,
            strict_imports: bool = True,
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
    
    def get_exec_option(self, option_name: str, default: Any = None, asbool=False) -> Any:
        """Helper to get an execution option for this section."""

        option_env_name = f"{option_name.upper()}"
        value = os.getenv(option_env_name, (self.exec_options or {}).get(option_name, default))
        if asbool:
            value = str(value).lower() in ("true", "1", "yes", "on")
        return value

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
        
        repeat_window = self.get_exec_option("repeat_window")
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

    def run(self, time_range: TimeRange | None = None) -> WorkflowSectionRunResult:
        """Execute a single workflow section using its engine-specific interface.

        Args:
            time_range: TimeRange for execution

        Returns:
            WorkflowSectionRunResult with execution/skipping details.

        Raises:
            TypeError: If the section doesn't have a recognized engine
        """

        # Get the actual workflow object
        process = self.value
        engine = self.engine
        section_name = self.name if self.name else "Unnamed Section"
        
        # figure out the time range for this section, if not provided
        if time_range is None:
            time_range = self.get_run_timerange()

        # If no time range is available, section is skipped.
        if time_range is None:
            return WorkflowSectionRunResult(
                section_name=section_name,
                engine=engine,
                time_range=None,
                executed=False,
                reason="nothing to do",
            )
        else:
            output = WorkflowSectionRunResult(
                section_name=section_name,
                engine=engine,
                time_range=time_range,
                executed=True,
                reason=None,
            )           

        split = self.get_exec_option("split", "false", asbool=True)
        if split and time_range.length() > 31:
            time_ranges = time_range.months
            time_ranges[0].start = time_range.start
            time_ranges[-1].end = time_range.end
            results = []
            for tr in time_ranges:
                result = self._execute_section(process, engine, section_name, tr)
                results.append(result)
            return output._replace(reason=f"split into {len(time_ranges)} sub-ranges")
            
        # Execute based on engine type
        match engine:
            case 'door':
                process.get_data(time_range)
            case 'dam':
                process.run(time_range)
            case 'dryes':
                process.compute(time_range)
            case _:
                raise TypeError(
                    f"Workflow section '{section_name}' has unrecognized engine '{engine}'. "
                    f"Expected one of: 'door', 'dam', 'dryes'"
                )

        return output