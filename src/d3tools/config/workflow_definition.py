import os
import datetime as dt
from typing import Optional

from ..timestepping import TimeRange
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

    RESERVED_TOP_LEVEL_KEYS = {"workflow_name", "tags", "datasets", "env", "workflow_log", "workflow_sections"}

    def __init__(
        self, 
        config: dict | Options,
        build_workflow_objects: bool = False,
        strict_workflow_imports: bool = False
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
        
        # Initialize logger if configured
        workflow_log_config = self.options.get("workflow_log", {}, ignore_case=True)
        self.logger: Optional[WorkflowLogManager] = WorkflowLogManager.from_dict(workflow_log_config)

    @classmethod
    def load(
        cls,
        *paths: str,
        build_workflow_objects: bool = False,
        strict_workflow_imports: bool = False,
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
                self._run_sections(time_range)
        else:
            self._run_sections(time_range)

    def _run_sections(self, time_range: TimeRange | None):
        """Execute workflow sections with optional logging.

        Args:
            time_range: TimeRange for workflow execution. If ``None``, each
                section determines its own range from input/output data
                availability.
        """
        for section in self.workflow_sections:
            section_name = getattr(section, "name", "<unknown>")
            engine = getattr(section, "engine", None)
            section_time_range = time_range
            if section_time_range is None:
                section_time_range = section.get_run_timerange()

            # if section_time_range is None, skip execution
            if section_time_range is None:
                if self.logger:
                    self.logger.get_logger().info(
                        f"Skipping workflow section '{section_name}' with engine '{engine}': "
                        f"nothing to do!"
                    )
                continue

            # Execute section with logging context if logger exists
            if self.logger:
                with self.logger.section_execution(section_name, engine=engine):
                    self._execute_section(section, section_time_range)
            else:
                self._execute_section(section, section_time_range)
    
    def _execute_section(self, section, time_range):
        """Execute a single workflow section using its engine-specific interface.

        Args:
            section: WorkflowSection object containing the workflow process
            time_range: TimeRange for execution

        Raises:
            TypeError: If the section doesn't have a recognized engine
        """
        # Get the actual workflow object from the section
        process = getattr(section, "value", section)
        engine = getattr(section, "engine", None)
        section_name = getattr(section, "name", "<unknown>")
        
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