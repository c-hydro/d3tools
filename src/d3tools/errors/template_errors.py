"""Exception classes used to report template creation and validation failures.

These errors are raised when a dataset template is missing, invalid,
insufficiently compatible with another template, or likely to exceed memory
limits in the current environment.
"""


class TemplateError(Exception):
    """Base class for errors raised while working with dataset templates.

    The original message is copied to the instance attribute ``message`` so
    callers can inspect either the exception text or the attribute directly.
    """

    def __init__(self, message: str):
        """Initialize the exception with its message.

        Args:
            message: Error description passed to :class:`Exception` and stored on
                the instance under ``message``.
        """
        self.message = message
        super().__init__(self.message)


class TemplateValidationError(TemplateError):
    """Report missing required keys or invalid template values.

    The generated message summarizes any missing entries, invalid values, and
    an optional spatial key. Empty or ``None`` detail arguments are omitted.

    Example::
        raise TemplateValidationError(missing_keys=["crs"], spatial_key="tile_01")
    """

    def __init__(self, missing_keys: list[str] = None, invalid_keys: dict = None, spatial_key: str = None):
        """Build a validation error message from the supplied details.

        Args:
            missing_keys: Missing required keys. Defaults to ``None``; empty
                lists are omitted from the generated message.
            invalid_keys: Mapping of keys to their validation problems. Empty
                mappings are omitted from the message.
            spatial_key: Optional spatial identifier included in the message when
                it is truthy.
        """
        parts = []

        if spatial_key:
            parts.append(f"Template validation failed for spatial key '{spatial_key}'")
        else:
            parts.append("Template validation failed")

        if missing_keys:
            parts.append(f"Missing required keys: {', '.join(missing_keys)}")

        if invalid_keys:
            details = [f"{k}: {v}" for k, v in invalid_keys.items()]
            parts.append(f"Invalid values: {'; '.join(details)}")

        message = ". ".join(parts) + "."
        super().__init__(message)


class TemplateNotFoundError(TemplateError):
    """Report that a template was not found or could not be created.

    The message includes a default explanation when ``reason`` is omitted and
    always appends a suggestion to ensure input data exists or disable template
    creation.
    """

    def __init__(self, spatial_key: str = None, dataset_name: str = None, reason: str = None):
        """Build a template-not-found message.

        Args:
            spatial_key: Optional spatial identifier. The special value
                ``'__tile__'`` is omitted from the message.
            dataset_name: Optional dataset name used to qualify the message.
            reason: Optional explanation. If omitted or empty, the message says
                that no data is available to create the template.
        """
        parts = []

        if dataset_name:
            parts.append(f"Template not found for dataset '{dataset_name}'")
        else:
            parts.append("Template not found")

        if spatial_key and spatial_key != '__tile__':
            parts.append(f"(spatial key: '{spatial_key}')")

        if reason:
            parts.append(f"Reason: {reason}")
        else:
            parts.append("No data available to create template from")

        message = " ".join(parts) + "."

        message += "\n\nSuggestion: Ensure input data exists or set make_template=False to skip template creation."

        super().__init__(message)


class TemplateMemoryError(TemplateError):
    """Report that a template operation would exceed available memory.

    The exception does not perform a memory check itself; it formats a message
    describing the operation, the estimated size, and a mitigation suggestion.
    """

    def __init__(self, operation: str, size_mb: float = None, suggestion: str = None):
        """Build a message describing a memory-heavy template operation.

        Args:
            operation: Name of the operation that is expected to exceed memory.
            size_mb: Optional estimated size in megabytes. A truthy value is
                included in the message and formatted to one decimal place.
            suggestion: Optional follow-up text. If omitted or empty, the
                message recommends chunking, smaller tiles, or disk caching.
        """
        parts = [f"Template operation '{operation}' would exceed available memory"]

        if size_mb:
            parts.append(f"(estimated size: {size_mb:.1f} MB)")

        message = " ".join(parts) + "."

        if suggestion:
            message += f"\n\nSuggestion: {suggestion}"
        else:
            message += "\n\nSuggestion: Consider using dask chunking, reducing tile size, or enabling template disk caching."

        super().__init__(message)


class TemplateCompatibilityError(TemplateError):
    """Report differences that make one dataset template incompatible with another.

    The message lists each differing property and ends with a general
    recommendation to align CRS, resolution, and dimensions.
    """

    def __init__(self, source: str, target: str, differences: dict):
        """Build a compatibility error message from template differences.

        Args:
            source: Name identifying the source template.
            target: Name identifying the target dataset or template.
            differences: Mapping of property names to ``(source_value,
                target_value)`` pairs. A truthy mapping is listed in the
                message; an empty mapping produces only the top-level error.
        """
        message = f"Template from '{source}' is incompatible with '{target}'."

        if differences:
            diff_details = []
            for key, (src_val, tgt_val) in differences.items():
                diff_details.append(f"  {key}: {src_val} vs {tgt_val}")
            message += f"\n\nDifferences:\n" + "\n".join(diff_details)

        message += "\n\nSuggestion: Ensure datasets have matching CRS, resolution, and dimensions."

        super().__init__(message)
