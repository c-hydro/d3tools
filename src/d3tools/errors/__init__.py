"""Public exception classes exported by the d3tools errors package.

These custom errors are raised when optional dependencies are missing,
workflow engine imports fail, or template datasets are invalid, missing,
misconfigured, or incompatible.
"""

from .gdal_import import GDAL_ImportError
from .workflow_import import WorkflowEngineImportError
from .template_errors import (
    TemplateError,
    TemplateValidationError,
    TemplateNotFoundError,
    TemplateMemoryError,
    TemplateCompatibilityError
)
