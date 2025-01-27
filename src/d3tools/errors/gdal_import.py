class GDAL_ImportError(ImportError):

    message = """
    The package 'gdal' is not installed. Please install 'gdal' to use {function}.
    To install gdal:
    1. ensure gdal and libgdal-dev are installed on your system (version 3.6.0 or higher)
    2. find the version of gdal that is running your system: `gdalinfo --version`
    3. install the corresponding version of gdal: `pip install gdal==<version>`
    """

    def __init__(self, function = None):
        if function is None:
            function = 'a function in your script (sorry, I cannot figure out which one)'
        
        message = self.message.format(function=function)
        super().__init__(message)