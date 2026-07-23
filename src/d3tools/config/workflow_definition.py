import os
import datetime as dt
from typing import Optional

from ..timestepping import TimeRange, TimeWindow
from ..logging import WorkflowLogManager

from .utils import load_jsons
from .options import Options

class WorkflowDefinition:
    """
    Workflow definition and execution coordinator.

    ``WorkflowDefinition`` is the primary interface for defining, parsing, and executing
    workflows. It automatically parses configuration through the d3tools pipeline and
    provides structured access to workflow components.

    Attributes:
        options (Options): Full parsed configuration as an Options instance
        workflow_sections (list): Ordered list of WorkflowSection objects to execute
        workflow_name (str): Name identifier for the workflow
        tags (dict): Resolved configuration tags/variables
        datasets (dict): Parsed dataset definitions
        logger (WorkflowLogManager): Workflow logging manager (may be None)

    Usage:
        Create from dict::

            wf = WorkflowDefinition(config_dict, build_workflow_objects=True)
            wf.run(start="2024-01-01", end="2024-12-31")

        Load from JSON::

            wf = WorkflowDefinition.load("workflow.json", build_workflow_objects=True)
            wf.run(time_range=TimeRange("2024-01-01", "2024-12-31"))
    """

    RESERVED_TOP_LEVEL_KEYS = {"workflow_name", "tags", "datasets", "env", "workflow_log", "workflow_sections", "exec_options", "engine", "other_options"}

    def __init__(
        self, 
        config: dict | Options,
        build_workflow_objects: bool = True,
        strict_workflow_imports: bool = True
    ):
        """Initialize and parse workflow definition.
        
        Args:
            config: Workflow configuration dict or Options instance
            build_workflow_objects: Whether to build runtime workflow objects
                (door/dam/dryes) in collected sections
            strict_workflow_imports: If True, raise errors on workflow engine
                import failures instead of falling back to raw section data
                
        Raises:
            TypeError: If config is not a dict-like structure
        """
        # Validate input
        if not isinstance(config, (dict, Options)):
            raise TypeError(
                f"WorkflowDefinition config must be a dict or Options instance, "
                f"got {type(config).__name__}"
            )
        
        # Parse configuration through the pipeline
        from .parsing_pipeline import parse_options
        parsed = parse_options(
            config,
            build_workflow_objects=build_workflow_objects,
            strict_workflow_imports=strict_workflow_imports,
        )

        # Store full parsed config as Options for flexible access
        self.options = Options(parsed)

        # Extract workflow components as proper attributes
        self.workflow_sections: list = self.options.get("workflow_sections", [], ignore_case=True)
        self.workflow_name: str = self.options.get("workflow_name", "workflow", ignore_case=True)
        self.tags: dict = self.options.get("tags", {}, ignore_case=True)
        self.datasets: dict = self.options.get("datasets", {}, ignore_case=True)
        
        # Save other_options for potential future use (not currently used in WorkflowDefinition)
        self.other_options = self.options.get("other_options", {}, ignore_case=True)

        # Initialize logger if configured
        workflow_log_config = self.options.get("workflow_log", {}, ignore_case=True)
        self.logger: Optional[WorkflowLogManager] = WorkflowLogManager.from_dict(workflow_log_config)

    @classmethod
    def load(
        cls,
        *paths: str,
        build_workflow_objects: bool = True,
        strict_workflow_imports: bool = True,
    ) -> "WorkflowDefinition":
        """Load and parse workflow configuration from JSON file(s).

        Args:
            *paths: One or more JSON file paths. If multiple paths provided,
                configurations are merged using :func:`d3tools.config.utils.load_jsons`
            build_workflow_objects: Whether to build runtime workflow objects
                (door/dam/dryes) in collected sections
            strict_workflow_imports: If True, raise errors on workflow engine
                import failures instead of falling back to raw section data

        Returns:
            Parsed and initialized WorkflowDefinition instance

        Example::

            wf = WorkflowDefinition.load(
                "base_config.json",
                "overrides.json",
                build_workflow_objects=True
            )
        """
        config = load_jsons(*paths)
        return cls(
            config,
            build_workflow_objects=build_workflow_objects,
            strict_workflow_imports=strict_workflow_imports,
        )

    def run(
        self,
        time_range: TimeRange | None = None,
        *,
        start: dt.datetime | str | None = None,
        end: dt.datetime | str | None = None,
    ):
        """Execute workflow sections sequentially in order.

        Args:
            time_range: Explicit workflow execution range.
            start: Start datetime/date string for workflow execution when not
                passing ``time_range``.
            end: End datetime/date string for workflow execution. Defaults to
                current time if not provided when ``start`` is set.

        If all arguments are ``None``, the workflow attempts to determine the
        execution time range from ``START_DATE`` and ``END_DATE`` environment
        variables, or from each section's data availability if those are not
        set.

        The method executes each workflow section using its engine-specific interface:

        - **door** engine: calls ``get_data(time_range)``
        - **dam** engine: calls ``run(time_range)``
        - **dryes** engine: calls ``compute(time_range)``

        Raises:
            TypeError: If a section doesn't have a recognized engine
                or runnable workflow object

        Example::

            wf = WorkflowDefinition.load("workflow.json")
            wf.run(start="2024-01-01", end="2024-12-31")
        """
        time_range = self._get_run_timerange(
            time_range=time_range,
            start=start,
            end=end,
        )

        # Run workflow with logging context if logger exists
        if self.logger:
            with self.logger.workflow_execution(self.workflow_name):
                section_results = self._run_sections(time_range)
                # Persist run state if logger is configured for it
                self._write_workflow_state(section_results, time_range)
        else:
            self._run_sections(time_range)

    def _run_sections(self, time_range: TimeRange | None) -> list:
        """Execute workflow sections with optional logging.

        Args:
            time_range: TimeRange for workflow execution. If ``None``, each
                section determines its own range from input/output data
                availability.
        
        Returns:
            List of WorkflowSectionRunResult objects from section execution
        """
        section_results = []
        
        for section in self.workflow_sections:
            # Keep section-level context around actual section execution.
            if self.logger:
                with self.logger.section_execution(section.name, engine=section.engine):
                    result = section.run(time_range)
            else:
                result = section.run(time_range)

            # Backward-compatible: legacy run() may return None.
            if result is None:
                continue

            # Collect result for state persistence
            section_results.append(result)

            # New run contract: log explicit skips.
            if not result.executed and self.logger:
                self.logger.get_logger().info(
                    f"Skipped workflow section '{result.section_name}' with engine '{result.engine}': "
                    f"{result.reason or 'nothing to do'}"
                )
        
        return section_results

    def _write_workflow_state(self, section_results: list, time_range: TimeRange | None):
        """Build and persist workflow run state.
        
        Args:
            section_results: List of WorkflowSectionRunResult from execution
            time_range: Original execution time range
        """
        if not self.logger:
            return
        
        # Determine workflow status from section results
        workflow_status = "success" if all(r.executed for r in section_results) else "partial"
        
        # Calculate workflow start/end from section results
        workflow_start = None
        workflow_end = None
        
        for result in section_results:
            if result.time_range:
                if workflow_start is None or result.time_range.start < workflow_start:
                    workflow_start = result.time_range.start
                if workflow_end is None or result.time_range.end > workflow_end:
                    workflow_end = result.time_range.end
        
        # Build run state structure
        run_state = {
            "version": 1,
            "run_id": dt.datetime.now().isoformat(),
            "name": self.workflow_name,
            "status": workflow_status,
            "start": workflow_start.isoformat() if workflow_start else None,
            "end": workflow_end.isoformat() if workflow_end else None,
            "sections": [
                {
                    "name": result.section_name,
                    "engine": result.engine,
                    "executed": result.executed,
                    "start": result.time_range.start.isoformat() if result.time_range else None,
                    "end": result.time_range.end.isoformat() if result.time_range else None,
                    "reason": result.reason,
                }
                for result in section_results
            ]
        }
        
        # Write to file if configured
        self.logger.write_run_state(run_state)

    @staticmethod
    def _get_run_timerange(
        time_range: TimeRange | None = None,
        *,
        start: dt.datetime | str | None = None,
        end: dt.datetime | str | None = None,
    ) -> TimeRange | None:
        """Determine the workflow execution range.

        Args:
            time_range: Explicit workflow execution range.
            start: Optional start datetime/date string.
            end: Optional end datetime/date string.

        Returns:
            A resolved TimeRange, or ``None`` if execution should be resolved
            separately for each workflow section.

        Raises:
            TypeError: If ``time_range`` is not a TimeRange.
            ValueError: If conflicting or incomplete time arguments are
                provided.
        """

        if time_range is not None:
            if not isinstance(time_range, TimeRange):
                raise TypeError("time_range must be a TimeRange or None")
            if start is not None or end is not None:
                raise ValueError(
                    "Provide either time_range or start/end, not both"
                )
            return time_range

        if start is None:
            start = os.getenv("START_DATE", None)
        if end is None:
            end = os.getenv("END_DATE", None)

        if start is None and end is None:
            return None

        if start is None:
            raise ValueError("end was provided but start is missing")

        if end is None:
            end = dt.datetime.now()

        return TimeRange.from_any([start, end])