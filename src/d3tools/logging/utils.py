"""Shared helpers used by the dataset and workflow logging managers.

The functions in this module centralize the default log formats and create the
file and console handlers that are attached to named loggers across d3tools.
"""

import logging
import sys
from typing import Optional, Union

from ..data.io_utils import ensure_directory_exists


# Standard log format templates
LOG_FORMATS = {
    'detailed': '%(asctime)s - %(name)s, %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    'simple': '%(asctime)s - %(levelname)s: %(message)s',
    'minimal': '%(message)s',
}

# Date format for timestamps
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def create_file_handler(
    log_file: str,
    level: Union[int, str] = logging.INFO,
    format_style: str = 'detailed',
    mode: str = 'a'
) -> logging.FileHandler:
    """Create a configured file handler for a logger.

    The target directory is created if necessary, the file is opened in the
    requested mode, and the handler is assigned a formatter from the known
    templates in :data:`LOG_FORMATS`.

    Args:
        log_file: Path to the destination log file.
        level: Logging level for the handler, either as an integer or a string.
        format_style: Name of the formatter template to use.
        mode: File streaming mode, typically ``'a'`` for append or ``'w'`` for
            overwrite.

    Returns:
        A configured :class:`logging.FileHandler` instance.
    """

    ensure_directory_exists(log_file)

    handler = logging.FileHandler(log_file, mode=mode)
    handler.setLevel(level)

    formatter = logging.Formatter(
        LOG_FORMATS.get(format_style, LOG_FORMATS['detailed']),
        datefmt=DATE_FORMAT
    )
    handler.setFormatter(formatter)

    return handler


def create_console_handler(
    level: Union[int, str] = logging.INFO,
    format_style: str = 'simple',
    stream=None
) -> logging.StreamHandler:
    """Create a configured console handler.

    Args:
        level: Logging level for the handler, either as an integer or a string.
        format_style: Name of the formatter template to use.
        stream: Output stream to write to. Defaults to ``sys.stdout``.

    Returns:
        A configured :class:`logging.StreamHandler` instance.
    """
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        LOG_FORMATS.get(format_style, LOG_FORMATS['simple']),
        datefmt=DATE_FORMAT
    )
    handler.setFormatter(formatter)

    return handler


def configure_logger(
    logger_name: str,
    level: Union[int, str] = logging.INFO,
    file_path: Optional[str] = None,
    console: bool = True,
    format_file: str = 'detailed',
    format_console: str = 'simple',
    propagate: bool = True,
    clear_existing: bool = False
) -> logging.Logger:
    """Configure a named logger with optional file and console handlers.

    The logger level is set before any handlers are attached. If
    ``clear_existing`` is true, the logger's current handlers are removed before
    appending new ones. File logging is added first when ``file_path`` is set,
    followed by a console handler when ``console`` is true.

    Args:
        logger_name: Logger name to configure.
        level: Base logging level, as an integer or a standard ``logging`` level
            name.
        file_path: Optional path for a file-based log sink.
        console: If true, attach a stream handler to stdout.
        format_file: Formatter name to use for the file handler.
        format_console: Formatter name to use for the console handler.
        propagate: Whether the logger should propagate records to parent loggers.
        clear_existing: If true, remove existing handlers before creating new
            ones.

    Returns:
        The configured Python :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = propagate

    # Optionally clear existing handlers
    if clear_existing:
        logger.handlers.clear()

    # Add file handler if requested
    if file_path:
        file_handler = create_file_handler(file_path, level, format_file)
        logger.addHandler(file_handler)

    # Add console handler if requested
    if console:
        console_handler = create_console_handler(level, format_console)
        logger.addHandler(console_handler)

    return logger
