from ..parse import get_unique_values

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

        from .workflow_definition import WorkflowDefinition
        return WorkflowDefinition.load(*args, **kwargs)
