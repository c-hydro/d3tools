import datetime as dt

from ..timestepping import TimeRange
from ..parse import get_unique_values
from .parsing_pipeline import parse_options
from .utils import load_jsons


class WorkflowDefinition(dict):
    """Canonical workflow configuration container.

    This class wraps nested mapping/list structures so sections can be accessed
    both as dictionary keys and attributes, while preserving backward-compatible
    parsing behavior via ``parse()`` and ``load()``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = self.__class__(v)
            elif isinstance(v, list):
                self[k] = [self.__class__(i) if isinstance(i, dict) else i for i in v]

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

    @classmethod
    def load(
            cls,
            *paths: str,
            build_workflow_objects: bool = False,
            strict_workflow_imports: bool = False,
            **kwargs,
        ) -> dict:
        """
        Load and parse workflow configuration from one or more JSON files.
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
        """
        Parse workflow options through the d3tools parsing pipeline.

        Args:
            build_workflow_objects: Whether to try building runtime workflow
                objects in collected workflow sections.
            strict_workflow_imports: If ``True``, propagate build/import errors
                from workflow-section object construction.
        """
        parsed_options = parse_options(
            self,
            build_workflow_objects=build_workflow_objects,
            strict_workflow_imports=strict_workflow_imports,
        )
        return self.__class__(parsed_options)
    
    def find_keys(self, key: str, get_all = False) -> list[str]:
        """
        Returns the tree of keys that contain the specified key.
        It returns a list of keys that end with the specified key.
        e.g. {'a': {'b': 1, 'c': 2}}, 'c' -> ['a','c']
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
        """
        Get the value of the specified key.
        If the key is a list, it will return the value of the first key that is found.
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

    def run(
            self,
            start: dt.datetime|str,
            end: dt.datetime|str = dt.datetime.now()
        ):
        """Run parsed workflow sections in their collected order.

        This method expects sections to be already parsed and, when execution is
        desired, built as runtime objects (``section.value``).
        """
        workflow_sections = self.get("workflow_sections", [])
        time_range = TimeRange.from_any([start, end])

        for section in workflow_sections:
            process = getattr(section, "value", section)

            if hasattr(process, "get_data"):
                process.get_data(time_range)
            elif hasattr(process, "run"):
                process.run(time_range)
            elif hasattr(process, "compute"):
                process.compute(time_range)
            else:
                raise TypeError(
                    f"Workflow section '{getattr(section, 'name', '<unknown>')}' "
                    "does not contain a runnable workflow object."
                )

class Options(WorkflowDefinition):
    """Backward-compatible alias for ``WorkflowDefinition``."""
    pass
