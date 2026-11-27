"""Configure workflow logging and track workflow and section execution."""

import datetime as dt
import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Optional, Dict, Any, Union

from .utils import configure_logger, DATE_FORMAT
from ..config.parsers import dataset_from_config
from ..exit.exit_handler import run_at_exit
from ..data import Dataset

DatasetLike = Union[Dataset, str, Dict[str, Any]]


class WorkflowLogManager:
    """Configure shared workflow loggers and track execution timings.

    Construction sets up the primary logger and mirrors the same handlers onto
    the ``door``, ``dam``, and ``dryes`` package loggers so workflow messages are
    captured consistently by all related modules. The manager also tracks the
    time spent in workflow and section contexts and registers a process-exit
    cleanup hook.

    Args:
        log_file: Optional log destination accepted by ``dataset_from_config``.
            ``None`` disables file output. Remote destinations use a temporary
            local file that is uploaded by :meth:`close`.
        run_state_file: Optional destination for persisted run-state data.
        level: Logging level as an integer or standard logging level name.
        console: Whether to attach a console handler.
        format_file: File format name; supported values are ``detailed``,
            ``simple``, and ``minimal``. Unknown names fall back to ``detailed``.
        format_console: Console format name; unknown values fall back to
            ``simple``.
        logger_name: Name of the primary logger to configure.
        **options: Extra options retained on the instance and otherwise unused.
    """

    def __init__(
        self,
        log_file: Optional[DatasetLike] = None,
        run_state_file: Optional[DatasetLike] = None,
        level: Union[int, str] = logging.INFO,
        console: bool = True,
        format_file: str = 'detailed',
        format_console: str = 'simple',
        logger_name: str = 'd3tools',
        **options
    ):
        """Resolve destinations, configure managed loggers, and register cleanup.

        Args:
            log_file: Optional destination for the workflow log file.
            run_state_file: Optional destination for persisted run-state data.
            level: Logging threshold applied to managed loggers and handlers.
            console: Whether to attach a console sink to the logger.
            format_file: Formatter name for file output.
            format_console: Formatter name for console output.
            logger_name: Name of the root logger to configure.
            **options: Extra state retained on the instance.
        """
        self.log_file = log_file
        self.run_state_file = run_state_file
        if self.log_file:
            self.log_file = dataset_from_config(self.log_file)
        if self.run_state_file:
            self.run_state_file = dataset_from_config(self.run_state_file)

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
        """Create a manager from a path or configuration mapping.

        Missing or empty configuration yields a console-only manager. Strings are
        treated as a ``log_file`` value using constructor defaults. Mapping
        entries are normalized by resolving ``{now}`` placeholders recursively,
        then the ``file`` and ``run_state_file`` keys are passed to the
        constructor. ``format`` supplies both output formats unless
        ``format_file`` or ``format_console`` override it; any other keys are
        retained as extra instance options.

        Args:
            config: ``None``, a log-file path string, or a configuration mapping.
                Supported keys are ``file``, ``run_state_file``, ``level``,
                ``console``, ``format``, ``format_file``, ``format_console``,
                and ``logger_name``.

        Returns:
            A configured :class:`WorkflowLogManager`.

        Raises:
            TypeError: If ``config`` is neither ``None``, a string, nor a dict.
            Exception: If placeholder resolution, destination parsing, or manager
                initialization fails.
        """

        # Handle None or empty config with default console-only logging
        if config is None or (isinstance(config, dict) and len(config) == 0):
            return cls()

        # Handle string path
        if isinstance(config, str):
            return cls(log_file=config)

        # Handle dict configuration
        if not isinstance(config, dict):
            raise TypeError(f"Config must be dict, str, or None, got {type(config)}")

        # resolve {now} placeholders in paths before creating Dataset objects
        from ..config.parsing_pipeline import resolve_now
        config = resolve_now(config)

        # Extract and process file path
        log_file = config.get('file')

        # Extract and process optional run state path
        run_state_file = config.get('run_state_file')

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
                                     'format_file', 'format_console', 'logger_name',
                                     'run_state_file']}

        return cls(
            log_file=log_file,
            run_state_file=run_state_file,
            level=level,
            console=console,
            format_file=format_file,
            format_console=format_console,
            logger_name=logger_name,
            **extra_options
        )

    def _configure_logger(self) -> logging.Logger:
        """Configure the workflow logger and mirror its handlers to package loggers.

        Local file destinations are written directly to the configured path; remote
        destinations are first written to a named temporary file and later
        uploaded by :meth:`close`.

        Returns:
            The configured root :class:`logging.Logger` instance.
        """

        file_path = None
        if self.log_file:
            if self.log_file.type == 'local':
                file_path = self.log_file.get_key()
            else:
                from tempfile import NamedTemporaryFile
                temp_file = NamedTemporaryFile(delete=False)
                file_path = temp_file.name
                temp_file.close()
                self._temp_log_file = file_path  # Store for cleanup and upload after execution

        logger = configure_logger(
            logger_name=self.logger_name,
            level=self.level,
            file_path=file_path,
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
        """Log workflow start, completion, and failures during a managed run.

        The context manager emits the start banner, yields the manager itself,
        logs elapsed time on success, and logs the failure message and exception
        information if an exception escapes the block. A section summary is written
        in the ``finally`` branch when timing data has been recorded.

        Args:
            workflow_name: Workflow label used in the generated log messages.

        Yields:
            This manager so callers can access the configured logger.
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
        """Log and time a named workflow section.

        The section name is used as the timing key, so repeated sections with the
        same name overwrite earlier elapsed times. Exceptions are logged and then
        re-raised after recording the elapsed time.

        Args:
            section_name: Label used in start, completion, and failure messages.
            engine: Optional engine name appended to the start log message.

        Yields:
            This manager while the section body runs.
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
        """Log a summary of section execution times."""
        self.logger.info(f"{'-'*70}")
        self.logger.info("Workflow Section Summary:")

        for section_name, duration in self._section_times.items():
            self.logger.info(f"  {section_name}: {self._format_duration(duration)}")

        total_section_time = sum(self._section_times.values())
        self.logger.info(f"  Total section time: {self._format_duration(total_section_time)}")
        self.logger.info(f"{'-'*70}")

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Represent elapsed seconds as a compact human-readable duration.

        Args:
            seconds: Number of seconds elapsed.

        Returns:
            A formatted string such as ``"45.3s"`` or ``"2h 34m 12s"``.
        """
        if seconds < 60:
            return f"{seconds:.1f}s"

        minutes, seconds = divmod(int(seconds), 60)
        if minutes < 60:
            return f"{minutes}m {seconds}s"

        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes}m {seconds}s"

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Return the configured logger or retrieve a named logger.

        Args:
            name: Optional logger name. When omitted, return the primary managed
                logger.

        Returns:
            The primary logger when ``name`` is ``None``; otherwise the result of
            ``logging.getLogger(name)``.
        """
        if name is None:
            return self.logger
        return logging.getLogger(name)

    def set_level(self, level: Union[int, str]):
        """Update the level on every managed logger and attached handler.

        Args:
            level: New logging threshold as an integer or standard level name.
        """
        self.level = level
        for managed_logger in self._managed_loggers:
            managed_logger.setLevel(level)

        for handler in self.logger.handlers:
            handler.setLevel(level)

    def write_run_state(self, run_state: Dict[str, Any]):
        """Persist structured run-state data to the configured dataset destination.

        The mapping is written with ``as_is=True`` and is not validated or
        serialized by this method. The call is ignored when no run-state
        destination has been configured.

        Args:
            run_state: Mapping to persist as workflow state.

        Returns:
            ``None``.
        """
        if not self.run_state_file:
            return  # No-op if not configured

        self.run_state_file.write_data(run_state, as_is=True)

    def close(self):
        """Detach handlers, flush temporary log files, and mark the manager closed.

        For non-local log destinations, the temporary file is uploaded to the
        final destination before the temporary file is removed. Repeated close
        calls are ignored after the first successful shutdown. If temporary-file
        removal fails, a warning is printed instead of raising an exception.

        Returns:
            ``None``.
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

        if hasattr(self, '_temp_log_file'):

            # copy temp log file to final destination if using remote logging,
            # then remove temp file
            with open(self._temp_log_file, 'r') as f:
                self.log_file.write_data(f, as_is=True)

            try:
                os.remove(self._temp_log_file)
            except Exception as e:
                print(f"Warning: Failed to remove temporary log file {self._temp_log_file}: {e}")

        self._closed = True
