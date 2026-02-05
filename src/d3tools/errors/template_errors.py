"""
Template-related error classes for d3tools.
"""


class TemplateError(Exception):
    """Base class for template-related errors."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class TemplateValidationError(TemplateError):
    """Raised when template data is invalid or missing required fields."""
    
    def __init__(self, missing_keys: list[str] = None, invalid_keys: dict = None, spatial_key: str = None):
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
    """Raised when a required template cannot be found or created."""
    
    def __init__(self, spatial_key: str = None, dataset_name: str = None, reason: str = None):
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
        
        # Add helpful suggestion
        message += "\n\nSuggestion: Ensure input data exists or set make_template=False to skip template creation."
        
        super().__init__(message)


class TemplateMemoryError(TemplateError):
    """Raised when template operations would exceed available memory."""
    
    def __init__(self, operation: str, size_mb: float = None, suggestion: str = None):
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
    """Raised when templates are incompatible between datasets."""
    
    def __init__(self, source: str, target: str, differences: dict):
        message = f"Template from '{source}' is incompatible with '{target}'."
        
        if differences:
            diff_details = []
            for key, (src_val, tgt_val) in differences.items():
                diff_details.append(f"  {key}: {src_val} vs {tgt_val}")
            message += f"\n\nDifferences:\n" + "\n".join(diff_details)
        
        message += "\n\nSuggestion: Ensure datasets have matching CRS, resolution, and dimensions."
        
        super().__init__(message)
