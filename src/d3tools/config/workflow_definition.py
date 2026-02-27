import datetime as dt

from ..timestepping import TimeRange
from ..parse import get_unique_values
from ..logging import WorkflowLogManager

from .utils import load_jsons

class Options(dict):
    """
    Enhanced mapping with compatibility helpers for config access.

    ``Options`` is a non-runnable configuration container that extends plain dict behavior with:
    - Recursive wrapping of nested mappings/lists into ``Options`` instances
    - Attribute-style access for uniquely resolvable nested keys
    - Compatibility helpers in :meth:`get` (case-insensitive lookup, fallback key lists, optional key return)

    Backward compatibility:
        - Previously, ``Options`` was the runnable workflow container. Now, ``WorkflowDefinition`` is the canonical runnable container.
        - ``Options.load()`` is retained for legacy code and emits a deprecation warning. Use ``WorkflowDefinition.load()`` instead.

    Notes:
        - This class intentionally does not provide workflow parsing/execution behavior.
        - Use ``WorkflowDefinition`` for operational workflow execution.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = Options(**v)
            elif isinstance(v, list):
                self[k] = [Options(**i) if isinstance(i, dict) else i for i in v]

    def __getattr__(self, item):
        key_paths = self.find_keys(item, get_all = True)
        values = []
        for key_path in key_paths:
            current = self
            for key in key_path:
                current = current[key]
            values.append(current)

        values = get_unique_values(values)
        if len(values) == 1:
            return values.pop()
        elif len(values) > 1:
            raise ValueError(f"Multiple values found: {values}")
        else:
            raise AttributeError(f"'Options' object has no attribute '{item}'")

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, item):
        try:
            del self[item]
        except KeyError:
            raise AttributeError(f"'Options' object has no attribute '{item}'")

    def find_keys(self, key: str, get_all = False) -> list[str]:
        """Find nested key paths ending with ``key``.

        Args:
            key: Target key name to search recursively.
            get_all: If ``True``, return all matching paths. If ``False``,
                require a single match and return only that path.

        Returns:
            Either:
            - a list of key-paths when ``get_all=True``;
            - one key-path (list of keys) when ``get_all=False`` and unique;
            - an empty list when no match is found.

        Raises:
            ValueError: If ``get_all=False`` and more than one path matches.
        """
        def search(d, key, path=[]):
            paths_found = []
            if isinstance(d, dict):
                for k, v in d.items():
                    if k == key:
                        paths_found.append(path + [k])
                    else:
                        paths_found.extend(search(v, key, path + [k]))
            return paths_found

        paths = search(self, key)
        if get_all:
            return paths
        elif len(paths) == 1:
            return paths[0]
        elif len(paths) > 1:
            raise ValueError(f"Multiple keys found: {paths}")
        else:
            return []
        
    def get(self, key: list[str]|str, default = None, ignore_case = False, get_key = False):
        """Get a top-level option by key with optional compatibility helpers.

        Args:
            key: Either a single key name or a list of fallback key names.
                For a list, keys are tried in order and the first match wins.
            default: Value returned when no key matches.
            ignore_case: Whether key matching should be case-insensitive.
            get_key: If ``True``, return ``(value, matched_key)``.

        Returns:
            The matched value, or ``default`` if no key matches. If
            ``get_key=True``, returns ``(value, matched_key_or_None)``.

        Notes:
            This method checks only the current mapping level; recursive lookup
            is handled by :meth:`find_keys` and attribute access.
        """
        if isinstance(key, str):
            if ignore_case:
                key = key.lower()

            for k, v in self.items():
                _key = k.lower() if ignore_case else k
                if _key == key:
                    outvalue, outkey = v, k
                    break
            else:
                outvalue, outkey = default, None
        
        elif isinstance(key, list):
            v = None
            while not v and key:
                v, k = self.get(key.pop(0), None, ignore_case, get_key = True)
            
            if not v:
                outvalue, outkey = default, None
            else:
                outvalue, outkey = v, k

        if get_key:
            return outvalue, outkey
        else:
            return outvalue

    @classmethod
    def load(cls, *args, **kwargs):
        """
        Deprecated compatibility loader for workflow JSON files.

        This method is kept only for legacy code that still calls ``Options.load(...)``.
        Emits a deprecation warning and returns a ``WorkflowDefinition`` instance.

        Returns:
            WorkflowDefinition: Parsed workflow definition object.

        Deprecated:
            Use ``WorkflowDefinition.load`` instead. This method will be removed in a future release.
        """
        import warnings
        warnings.warn(
            "Options.load is deprecated and will be removed in a future release. "
            "Please use WorkflowDefinition.load instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return WorkflowDefinition.load(*args, **kwargs)

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
