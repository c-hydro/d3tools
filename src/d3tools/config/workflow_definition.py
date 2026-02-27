import datetime as dt

from ..timestepping import TimeRange
from ..logging import WorkflowLogManager

from .utils import load_jsons
from .options import Options

class WorkflowDefinition(Options):
    """
    Canonical workflow configuration container.

    ``WorkflowDefinition`` is the user-facing mapping for workflow options and parsed workflow state.
    It keeps dict semantics for backward compatibility, while adding:
    - Enhanced mapping helpers inherited from ``Options``
    - Convenience parsing/loading helpers that delegate to the explicit configuration parsing pipeline
    - Basic ordered workflow execution via :meth:`run`

    Notes:
            - Nested mappings/lists are wrapped as ``Options`` instances and are not runnable workflow objects
            - Parsing does not mutate the receiver in-place and always returns a ``WorkflowDefinition`` instance
            - Use this class for operational workflow execution and configuration parsing
    """

    @classmethod
    def load(
            cls,
            *paths: str,
            build_workflow_objects: bool = False,
            strict_workflow_imports: bool = False,
            **kwargs,
        ) -> dict:
        """Load JSON configuration files and return a parsed workflow object.

        Args:
            *paths: One or more JSON file paths loaded and merged by
                :func:`d3tools.config.utils.load_jsons`.
            build_workflow_objects: Whether collected workflow sections should
                be converted into runtime objects (door/dam/dryes builders).
            strict_workflow_imports: Whether missing workflow-engine imports
                should raise instead of falling back to raw section payloads.
            **kwargs: Reserved for forward compatibility.

        Returns:
            A parsed ``WorkflowDefinition`` containing resolved tags, datasets,
            and ``workflow_sections``.
        """
        
        config = load_jsons(*paths)

        config_options = cls(config)
        parsed_options = config_options.parse(
            build_workflow_objects=build_workflow_objects,
            strict_workflow_imports=strict_workflow_imports,
            **kwargs,
        )

        return cls(parsed_options)

    def parse(
            self,
            build_workflow_objects: bool = False,
            strict_workflow_imports: bool = False,
            **kwargs,
        ):
        """Parse this workflow definition through the d3tools pipeline.

        Args:
            build_workflow_objects: Whether to try building runtime workflow
                objects in collected workflow sections.
            strict_workflow_imports: If ``True``, propagate build/import errors
                from workflow-section object construction.
            **kwargs: Reserved for forward compatibility.

        Returns:
            A new instance of ``WorkflowDefinition`` containing the parsed
            configuration.
        """
        from .parsing_pipeline import parse_options
        parsed_options = parse_options(
            self,
            build_workflow_objects=build_workflow_objects,
            strict_workflow_imports=strict_workflow_imports,
        )
        return WorkflowDefinition(parsed_options)
    
    def run(
            self,
            start: dt.datetime|str,
            end: dt.datetime|str = dt.datetime.now()
        ):
        """Run parsed workflow sections sequentially in collection order.

        Args:
            start: Start datetime/date string for workflow execution.
            end: End datetime/date string for workflow execution.

        The method converts ``start``/``end`` to a :class:`TimeRange` and, for
        each entry in ``workflow_sections``, executes the first supported
        callable among:

        - ``get_data(time_range)``  (downloader-like)
        - ``run(time_range)``       (workflow-like)
        - ``compute(time_range)``   (index-like)

        Raises:
            TypeError: If a section does not expose a runnable object interface.
        """
        workflow_sections = self.get("workflow_sections", [])
        time_range = TimeRange.from_any([start, end])
        
        # Setup workflow logging if configured
        workflow_log_config = self.get("workflow_log")
        log_manager = WorkflowLogManager.from_dict(workflow_log_config)
        
        # Get workflow name from config or use default
        workflow_name = self.get("workflow_name", "workflow")
        
        # Run workflow with logging context if log_manager exists
        if log_manager:
            with log_manager.workflow_execution(workflow_name):
                self._run_sections(workflow_sections, time_range, log_manager)
        else:
            self._run_sections(workflow_sections, time_range, None)
    
    def _run_sections(self, workflow_sections, time_range, log_manager=None):
        """Execute workflow sections with optional logging.
        
        Args:
            workflow_sections: List of WorkflowSection objects to execute
            time_range: TimeRange for the workflow execution
            log_manager: Optional WorkflowLogManager for logging
        """
        for section in workflow_sections:
            process = getattr(section, "value", section)
            section_name = getattr(section, "name", "<unknown>")
            engine = getattr(section, "engine", None)
            
            # Run section with logging context if log_manager exists
            if log_manager:
                with log_manager.section_execution(section_name, engine=engine):
                    self._execute_section(section, process, time_range)
            else:
                self._execute_section(section, process, time_range)
    
    def _execute_section(self, section, process, time_range):
        """Execute a single workflow section.
        
        Args:
            section: WorkflowSection object
            process: The runnable process (downloader/workflow/index)
            time_range: TimeRange for execution
        """
        match section.engine:
            case 'door':
                process.get_data(time_range)
            case 'dam':
                process.run(time_range)
            case 'dryes':
                process.compute(time_range)
            case _:
                raise TypeError(
                    f"Workflow section '{getattr(section, 'name', '<unknown>')}' "
                    "does not contain a runnable workflow object."
                )
