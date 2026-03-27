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
            wf.run(start="2024-01-01", end="2024-12-31")
    """

    RESERVED_TOP_LEVEL_KEYS = {"workflow_name", "tags", "datasets", "workflow_log", "workflow_sections"}

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
        start: dt.datetime | str,
        end: dt.datetime | str = None
    ):
        """Execute workflow sections sequentially in order.

        Args:
            start: Start datetime/date string for workflow execution
            end: End datetime/date string for workflow execution. 
                Defaults to current time if not provided.

        The method converts start/end to a TimeRange and executes each
        workflow section using its engine-specific interface:

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
        if end is None:
            end = dt.datetime.now()
            
        time_range = TimeRange.from_any([start, end])
        
        # Run workflow with logging context if logger exists
        if self.logger:
            with self.logger.workflow_execution(self.workflow_name):
                self._run_sections(time_range)
        else:
            self._run_sections(time_range)

    def _run_sections(self, time_range):
        """Execute workflow sections with optional logging.
        
        Args:
            time_range: TimeRange for workflow execution
        """
        for section in self.workflow_sections:
            section_name = getattr(section, "name", "<unknown>")
            engine = getattr(section, "engine", None)
            
            # Execute section with logging context if logger exists
            if self.logger:
                with self.logger.section_execution(section_name, engine=engine):
                    self._execute_section(section, time_range)
            else:
                self._execute_section(section, time_range)
    
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