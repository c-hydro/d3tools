"""
WorkflowLogManager - Handles workflow-level logging across d3tools, door, dam, and dryes.

This class configures Python's logging infrastructure to enable unified logging
across all modules in a workflow execution. It sets up handlers, formatters, and
provides context managers for tracking workflow execution.

Key features:
- Configures hierarchical loggers (d3tools, door, dam, dryes)
- File + console output
- Workflow execution tracking (start, end, sections)
- Timing/performance metrics
- Thread-safe for concurrent workflows
"""

import datetime as dt
import logging
import time
from contextlib import contextmanager
from typing import Optional, Dict, Any, Union

from .utils import configure_logger, LOG_FORMATS, DATE_FORMAT
from ..parse import substitute_string
from ..exit.exit_handler import run_at_exit


class WorkflowLogManager:
    """
    Manages workflow-level logging configuration and execution tracking.
    
    This class sets up Python's logging infrastructure for a workflow run,
    enabling all modules (d3tools, door, dam, dryes) to use the same
    configured logger via `logging.getLogger(__name__)`.
    
    Usage:
        # From config
        log_mgr = WorkflowLogManager.from_dict(config['workflow_log'])
        
        # Manual setup
        log_mgr = WorkflowLogManager(
            log_file='logs/workflow.log',
            level='INFO',
            console=True
        )
        
        # Use as context manager
        with log_mgr.workflow_execution('my_workflow'):
            # All logging calls within this context are tracked
            log = logging.getLogger(__name__)
            log.info("This will be logged!")
    """
    
    def __init__(
        self,
        log_file: Optional[str] = None,
        level: Union[int, str] = logging.INFO,
        console: bool = True,
        format_file: str = 'detailed',
        format_console: str = 'simple',
        logger_name: str = 'd3tools',
        **options
    ):
        """
        Initialize workflow log manager.
        
        Args:
            log_file: Path to log file (None for console-only logging).
                     Currently supports local file paths only.
                     Future: Will support Dataset objects for remote logging
                     (S3, SFTP, etc.) consistent with other d3tools patterns.
            level: Logging level (e.g., 'INFO', 'DEBUG', logging.INFO)
            console: Whether to log to console
            format_file: Format style for file output (from LOG_FORMATS)
            format_console: Format style for console output
            logger_name: Root logger name (default: 'd3tools')
            **options: Additional options for future extension
            
        Note:
            File logging currently only works with local file paths.
            Remote logging via Dataset objects is planned for future versions.
        """
        self.log_file = log_file
        self.level = level
        self.console = console
        self.format_file = format_file
        self.format_console = format_console
        self.logger_name = logger_name
        self.options = options
        self._managed_logger_names = tuple(dict.fromkeys((self.logger_name, 'door', 'dam', 'dryes')))
        self._managed_loggers = []
        
        # Track workflow execution state
        self._workflow_start_time = None
        self._section_times = {}
        self._current_section = None
        
        # Configure the logger
        self.logger = self._configure_logger()

        # set logger to close at exit to ensure all logs are flushed
        run_at_exit(self.close)
    
    @classmethod
    def from_dict(cls, config: Optional[Union[Dict[str, Any], str]] = None):
        """
        Create WorkflowLogManager from configuration.
        
        Args:
            config: Configuration dictionary, string path, or None.
                   If None or empty dict, returns a console-only logger.
                   If string, treated as log file path with defaults.
                   If dict, expects keys:
                       - file: Log file path (supports {now:...} formatting)
                       - level: Logging level (default: 'INFO')
                       - console: Enable console logging (default: True)
                       - format: Format style or separate format_file/format_console
                       - logger_name: Root logger name (default: 'd3tools')
        
        Returns:
            WorkflowLogManager instance
            
        Examples:
            # Use default console-only logging
            log_mgr = WorkflowLogManager.from_dict(None)
            
            # Simple file path
            log_mgr = WorkflowLogManager.from_dict("logs/workflow.log")
            
            # Full configuration
            log_mgr = WorkflowLogManager.from_dict({
                'file': 'logs/workflow_{now:%Y%m%d_%H%M%S}.log',
                'level': 'DEBUG',
                'console': True,
                'format': 'detailed'
            })
        """
        # Handle None or empty config with default console-only logging
        if config is None or (isinstance(config, dict) and len(config) == 0):
            return cls()
        
        # Handle string path
        if isinstance(config, str):
            log_file = substitute_string(config, {'now': dt.datetime.now()})
            return cls(log_file=log_file)
        
        # Handle dict configuration
        if not isinstance(config, dict):
            raise TypeError(f"Config must be dict, str, or None, got {type(config)}")
        
        # Extract and process file path
        log_file = config.get('file')
        if log_file:
            log_file = substitute_string(log_file, {'now': dt.datetime.now()})
        
        # Extract other settings
        level = config.get('level', 'INFO')
        console = config.get('console', True)
        logger_name = config.get('logger_name', 'd3tools')
        
        # Handle format settings
        format_setting = config.get('format', 'simple')
        format_file = config.get('format_file', format_setting)
        format_console = config.get('format_console', format_setting)
        
        # Pass through any additional options
        extra_options = {k: v for k, v in config.items() 
                        if k not in ['file', 'level', 'console', 'format', 
                                     'format_file', 'format_console', 'logger_name']}
        
        return cls(
            log_file=log_file,
            level=level,
            console=console,
            format_file=format_file,
            format_console=format_console,
            logger_name=logger_name,
            **extra_options
        )
    
    def _configure_logger(self) -> logging.Logger:
        """
        Configure the root logger for the workflow.
        
        This sets up handlers and formatters that will be inherited by
        all child loggers (door, dam, dryes, etc.).
        
        Returns:
            Configured Logger instance
        """
        logger = configure_logger(
            logger_name=self.logger_name,
            level=self.level,
            file_path=self.log_file,
            console=self.console,
            format_file=self.format_file,
            format_console=self.format_console,
            propagate=False,  # Root logger shouldn't propagate
            clear_existing=True  # Clear any existing handlers
        )

        self._managed_loggers = [logger]
        # Attach the workflow handlers to sibling package roots so their child
        # loggers write into the same workflow outputs.
        for package in self._managed_logger_names:
            if package == logger.name:
                continue

            pkg_logger = logging.getLogger(package)
            pkg_logger.setLevel(self.level)
            pkg_logger.propagate = False

            for handler in logger.handlers:
                if handler not in pkg_logger.handlers:
                    pkg_logger.addHandler(handler)

            self._managed_loggers.append(pkg_logger)

        return logger
    
    @contextmanager
    def workflow_execution(self, workflow_name: str = 'workflow'):
        """
        Context manager for tracking workflow execution.
        
        Logs workflow start/end events and tracks total execution time.
        Use this to wrap the entire workflow execution.
        
        Args:
            workflow_name: Name of the workflow for logging
            
        Yields:
            self (for convenience in accessing logger)
            
        Example:
            with log_mgr.workflow_execution('drought_monitoring'):
                workflow.run(start, end)
        """
        self._workflow_start_time = time.time()
        
        self.logger.info(f"{'='*70}")
        self.logger.info(f"Workflow '{workflow_name}' starting")
        self.logger.info(f"Start time: {dt.datetime.now().strftime(DATE_FORMAT)}")
        self.logger.info(f"{'='*70}")
        
        try:
            yield self
            
            # Successful completion
            elapsed = time.time() - self._workflow_start_time
            self.logger.info(f"{'='*70}")
            self.logger.info(f"Workflow '{workflow_name}' completed successfully")
            self.logger.info(f"End time: {dt.datetime.now().strftime(DATE_FORMAT)}")
            self.logger.info(f"Total execution time: {self._format_duration(elapsed)}")
            self.logger.info(f"{'='*70}")
            
        except Exception as e:
            # Log failure
            elapsed = time.time() - self._workflow_start_time
            self.logger.error(f"{'='*70}")
            self.logger.error(f"Workflow '{workflow_name}' failed")
            self.logger.error(f"End time: {dt.datetime.now().strftime(DATE_FORMAT)}")
            self.logger.error(f"Execution time: {self._format_duration(elapsed)}")
            self.logger.error(f"Error: {type(e).__name__}: {e}")
            self.logger.error(f"{'='*70}")
            raise
        
        finally:
            # Log section summary if sections were tracked
            if self._section_times:
                self._log_section_summary()
    
    @contextmanager
    def section_execution(self, section_name: str, engine: str = None):
        """
        Context manager for tracking individual workflow section execution.
        
        Logs section start/end and tracks execution time for each section.
        
        Args:
            section_name: Name of the section
            engine: Optional engine name (door, dam, dryes)
            
        Yields:
            self
            
        Example:
            with log_mgr.section_execution('ERA5_download', engine='door'):
                downloader.get_data(time_range)
        """
        self._current_section = section_name
        section_start = time.time()
        
        engine_label = f" [{engine}]" if engine else ""
        self.logger.info(f"Section '{section_name}'{engine_label} starting...")
        
        try:
            yield self
            
            # Successful completion
            section_elapsed = time.time() - section_start
            self._section_times[section_name] = section_elapsed
            self.logger.info(
                f"Section '{section_name}' completed in {self._format_duration(section_elapsed)}"
            )
            
        except Exception as e:
            # Log section failure
            section_elapsed = time.time() - section_start
            self._section_times[section_name] = section_elapsed
            self.logger.error(
                f"Section '{section_name}' failed after {self._format_duration(section_elapsed)}"
            )
            self.logger.error(f"Error: {type(e).__name__}: {e}")
            raise
        
        finally:
            self._current_section = None
    
    def _log_section_summary(self):
        """Log a summary of all section execution times."""
        self.logger.info(f"{'-'*70}")
        self.logger.info("Workflow Section Summary:")
        
        for section_name, duration in self._section_times.items():
            self.logger.info(f"  {section_name}: {self._format_duration(duration)}")
        
        total_section_time = sum(self._section_times.values())
        self.logger.info(f"  Total section time: {self._format_duration(total_section_time)}")
        self.logger.info(f"{'-'*70}")
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """
        Format duration in human-readable format.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted string (e.g., "2h 34m 12s" or "45.3s")
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        
        minutes, seconds = divmod(int(seconds), 60)
        if minutes < 60:
            return f"{minutes}m {seconds}s"
        
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes}m {seconds}s"
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        Get a logger instance for a specific module.
        
        Args:
            name: Logger name (uses __name__ convention if not provided)
            
        Returns:
            Logger instance configured with workflow settings
            
        Example:
            log = log_mgr.get_logger(__name__)
            log.info("Message from module")
        """
        if name is None:
            return self.logger
        return logging.getLogger(name)
    
    def set_level(self, level: Union[int, str]):
        """
        Dynamically change the logging level.
        
        Args:
            level: New logging level (e.g., 'DEBUG', logging.DEBUG)
        """
        self.level = level
        for managed_logger in self._managed_loggers:
            managed_logger.setLevel(level)

        for handler in self.logger.handlers:
            handler.setLevel(level)
    
    def close(self):
        """
        Close all handlers and clean up.
        
        Call this when workflow execution is complete to ensure
        all log messages are flushed and files are closed.
        """
        if not hasattr(self, '_closed'):
            self._closed = False
        
        if self._closed:
            return  # Already closed

        handlers_to_close = []
        seen_handlers = set()

        for managed_logger in self._managed_loggers:
            for handler in managed_logger.handlers[:]:
                managed_logger.removeHandler(handler)
                handler_id = id(handler)
                if handler_id not in seen_handlers:
                    seen_handlers.add(handler_id)
                    handlers_to_close.append(handler)

        for handler in handlers_to_close:
            handler.close()
        
        self._closed = True
