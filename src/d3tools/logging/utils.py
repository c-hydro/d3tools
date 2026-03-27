"""
Shared utilities for d3tools logging infrastructure.

Provides common formatters, handlers, and helper functions used
by both DatasetLogManager and WorkflowLogManager.
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
    """
    Create a file handler for logging.
    
    Args:
        log_file: Path to the log file
        level: Logging level (e.g., logging.INFO or "INFO")
        format_style: Format template name from LOG_FORMATS
        mode: File open mode ('a' for append, 'w' for overwrite)
    
    Returns:
        Configured FileHandler instance
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
    """
    Create a console handler for logging.
    
    Args:
        level: Logging level (e.g., logging.INFO or "INFO")
        format_style: Format template name from LOG_FORMATS
        stream: Output stream (default: sys.stdout)
    
    Returns:
        Configured StreamHandler instance
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
    """
    Configure a logger with file and/or console handlers.
    
    This is a convenience function for setting up loggers with
    standard configurations.
    
    Args:
        logger_name: Name of the logger (e.g., 'd3tools.workflow')
        level: Base logging level (int or string like "INFO")
        file_path: If provided, add a FileHandler to this path
        console: If True, add a StreamHandler to stdout
        format_file: Format style for file handler
        format_console: Format style for console handler
        propagate: Whether to propagate to parent loggers
        clear_existing: If True, remove all existing handlers first
    
    Returns:
        Configured Logger instance
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
