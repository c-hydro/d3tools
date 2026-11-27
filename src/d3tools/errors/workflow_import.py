"""Exceptions raised when workflow-engine dependencies cannot be imported.

These errors are used when a workflow section depends on a package such as
``door``, ``dam``, or ``dryes`` but that package cannot be imported in the
current environment.
"""


class WorkflowEngineImportError(ImportError):
    """Report a workflow engine dependency import failure.

    The instance stores the engine name and the original :class:`ImportError`
    in ``engine`` and ``original_error`` so callers can inspect what failed and
    which engine triggered the issue.

    Example::
        raise WorkflowEngineImportError("door", exc)
    """

    def __init__(self, engine: str, original_error: ImportError):
        """Initialize an engine-specific import error.

        Args:
            engine: Engine whose required dependencies could not be imported.
            original_error: Import error raised while loading that engine's
                dependencies. It is retained on the instance for inspection.
        """
        message = (
            f"Could not import dependencies required to build workflow section "
            f"for engine '{engine}'. Install the corresponding package(s) "
            f"(door, dam, or dryes) and their dependencies, or parse with "
            f"`build_workflow_objects=False`."
        )
        super().__init__(message)
        self.engine = engine
        self.original_error = original_error
