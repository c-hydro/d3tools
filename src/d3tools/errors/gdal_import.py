"""Exceptions raised when code paths require the optional GDAL dependency.

This module is used when a d3tools feature needs raster handling capabilities
that are not available in the active Python environment.
"""


class GDAL_ImportError(ImportError):
    """Report that GDAL is unavailable for a feature that requires it.

    The exception includes installation guidance and identifies the specific
    callable or script activity that triggered the missing dependency.

    Example::
        raise GDAL_ImportError("build_template")
    """

    message = """
    The package 'gdal' is not installed. Please install 'gdal' to use {function}.
    To install gdal:
    1. ensure gdal and libgdal-dev are installed on your system (version 3.6.0 or higher)
    2. find the version of gdal that is running your system: `gdalinfo --version`
    3. install the corresponding version of gdal: `pip install gdal==<version>`
    """

    def __init__(self, function = None):
        """Initialize the exception with the calling function name.

        Args:
            function: Name of the function or feature that needs GDAL. When
                omitted, the message falls back to a generic description.
        """
        if function is None:
            function = 'a function in your script (sorry, I cannot figure out which one)'

        message = self.message.format(function=function)
        super().__init__(message)