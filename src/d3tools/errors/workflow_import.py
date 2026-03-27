class WorkflowEngineImportError(ImportError):
    """Raised when workflow-engine dependencies cannot be imported."""

    def __init__(self, engine: str, original_error: ImportError):
        message = (
            f"Could not import dependencies required to build workflow section "
            f"for engine '{engine}'. Install the corresponding package(s) "
            f"(door, dam, or dryes) and their dependencies, or parse with "
            f"`build_workflow_objects=False`."
        )
        super().__init__(message)
        self.engine = engine
        self.original_error = original_error
