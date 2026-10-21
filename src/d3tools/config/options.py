from ..parse import get_unique_values, normalise_string

class Options(dict):
    """Case-insensitive dictionary wrapper for workflow configuration.

    Nested dictionaries and dict entries inside lists are converted into
    ``Options`` instances recursively, so configuration can be accessed either
    by key lookup or through attribute access. Attribute lookup searches the
    nested mapping for a name and returns the unique matching value. If more
    than one branch contains the same key with different values, a
    ``ValueError`` is raised.

    ``get`` adds case-insensitive lookup and fallback-key compatibility for
    top-level dictionary access. For backward compatibility, ``Options.load``
    forwards to ``WorkflowDefinition.load`` and emits a deprecation warning.

    Example::
        options = Options({"dataset": {"path": "/tmp"}})
        options.dataset.path
        options.get(["dataset", "source"], default="missing")
    """
    def __init__(self, *args, **kwargs):
        """Initialize the mapping and wrap nested dictionaries as ``Options``.

        Args:
            *args: Positional arguments accepted by ``dict``.
            **kwargs: Keyword items accepted by ``dict``.
        """
        super().__init__(*args, **kwargs)

        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = Options(**v)
            elif isinstance(v, list):
                self[k] = [Options(**i) if isinstance(i, dict) else i for i in v]

    def __getattr__(self, item):
        """Resolve a nested configuration attribute.

        Args:
            item: Attribute name to look up recursively.

        Returns:
            The unique matching value for the requested attribute name.

        Raises:
            AttributeError: If no matching nested key is found.
            ValueError: If multiple branches resolve to different values.
        """
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
        """Store an attribute as a top-level mapping item."""
        self[key] = value

    def __delattr__(self, item):
        """Delete a top-level mapping item through attribute syntax."""
        try:
            del self[item]
        except KeyError:
            raise AttributeError(f"'Options' object has no attribute '{item}'")

    def find_keys(self, key: str, get_all = False) -> list[str]:
        """Find nested key paths ending with ``key``.

        Args:
            key: Target key name to search recursively.
            get_all: If ``True`` (the default is ``False``), return all matching
                paths. Otherwise require a single match and return that path.

        Returns:
            A list of key paths when ``get_all=True``; otherwise one key path
            (a list of keys) when unique. Returns an empty list when no key
            matches.

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
        """Read a top-level option with case-insensitive and fallback-key support.

        Args:
            key: A string key or an ordered list of fallback key names to try.
            default: Value returned when no key matches (default ``None``).
            ignore_case: If ``True`` (default ``False``), compare keys without
                regard to case.
            get_key: If ``True`` (default ``False``), return the matched key
                alongside the value.

        Returns:
            The matched value, or ``default`` if no key matches. When
            ``get_key=True``, returns ``(value, matched_key_or_None)``.

        Notes:
            This method only checks the current mapping level; recursive lookup
            is handled by :meth:`find_keys` and attribute access. When ``key``
            is a list, the list is consumed in order while trying each candidate.
            Falsey matches such as ``0``, ``False``, ``''`` and ``None`` are
            treated as misses in the fallback path.
        """
        if isinstance(key, str):
            if ignore_case:
                key = normalise_string(key)

            for k, v in self.items():
                _key = normalise_string(k) if ignore_case else k
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
        """Load a workflow definition through the legacy ``Options`` entry point.

        This compatibility wrapper is retained for older code that still calls
        ``Options.load``. It forwards to ``WorkflowDefinition.load`` and emits a
        ``DeprecationWarning``.

        Args:
            *args: Positional arguments passed to ``WorkflowDefinition.load``.
            **kwargs: Keyword arguments passed to ``WorkflowDefinition.load``.

        Returns:
            The parsed ``WorkflowDefinition`` object.
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
