"""
Tests for WorkflowLogManager.

Tests cover:
- Initialization with various configurations
- from_dict() factory method
- Logger configuration and hierarchy
- Context managers for workflow and section execution
- Timing and duration tracking
- Level changes
- File and console output
- Error handling and cleanup
"""

import datetime as dt
import logging
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from d3tools.logging import WorkflowLogManager
from d3tools.config.parsers import dataset_from_config


class TestWorkflowLogManagerInit:
    """Test WorkflowLogManager initialization."""
    
    def test_init_console_only(self):
        """Test initialization with console-only logging."""
        log_mgr = WorkflowLogManager(console=True, log_file=None)
        
        assert log_mgr.log_file is None
        assert log_mgr.run_state_file is None
        assert log_mgr.console is True
        assert log_mgr.level == logging.INFO
        assert log_mgr.logger_name == 'd3tools'
        assert log_mgr.logger is not None
        
        # Should have console handler but no file handler
        handlers = log_mgr.logger.handlers
        assert len(handlers) == 1
        assert isinstance(handlers[0], logging.StreamHandler)
        assert not isinstance(handlers[0], logging.FileHandler)
    
    def test_init_file_only(self):
        """Test initialization with file-only logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            assert log_mgr.log_file.get_key() == log_file
            assert log_mgr.run_state_file is None
            assert log_mgr.console is False
            
            # Should have file handler but no console handler
            handlers = log_mgr.logger.handlers
            assert len(handlers) == 1
            assert isinstance(handlers[0], logging.FileHandler)
            
            log_mgr.close()

    def test_init_with_run_state_file(self):
        """Test initialization stores run_state_file option."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_state_file = os.path.join(tmpdir, 'workflow_state.json')
            log_mgr = WorkflowLogManager(run_state_file=run_state_file, console=False)

            assert log_mgr.run_state_file.get_key() == run_state_file

            log_mgr.close()
    
    def test_init_file_and_console(self):
        """Test initialization with both file and console logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=True)
            
            # Should have both handlers
            handlers = log_mgr.logger.handlers
            assert len(handlers) == 2
            
            has_console = any(isinstance(h, logging.StreamHandler) and 
                            not isinstance(h, logging.FileHandler) 
                            for h in handlers)
            has_file = any(isinstance(h, logging.FileHandler) for h in handlers)
            
            assert has_console
            assert has_file
            
            log_mgr.close()
    
    def test_init_custom_level(self):
        """Test initialization with custom log level."""
        # Test with string level
        log_mgr = WorkflowLogManager(level='DEBUG')
        assert log_mgr.logger.level == logging.DEBUG
        log_mgr.close()
        
        # Test with int level
        log_mgr = WorkflowLogManager(level=logging.WARNING)
        assert log_mgr.logger.level == logging.WARNING
        log_mgr.close()
    
    def test_init_custom_formats(self):
        """Test initialization with custom format styles."""
        log_mgr = WorkflowLogManager(
            format_file='minimal',
            format_console='detailed',
            console=True
        )
        
        assert log_mgr.format_file == 'minimal'
        assert log_mgr.format_console == 'detailed'
        log_mgr.close()
    
    def test_init_custom_logger_name(self):
        """Test initialization with custom logger name."""
        log_mgr = WorkflowLogManager(logger_name='my_workflow')
        assert log_mgr.logger_name == 'my_workflow'
        assert log_mgr.logger.name == 'my_workflow'
        log_mgr.close()
    
    def test_init_creates_log_directory(self):
        """Test that initialization creates log directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'logs', 'subdir', 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            # Directory should be created
            assert os.path.exists(os.path.dirname(log_file))
            
            log_mgr.close()

    def test_init_log_file_as_dataset_dict(self):
        """Test initialization with log_file given as a Dataset config dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(
                log_file={'key_pattern': log_file},
                console=False,
            )

            assert hasattr(log_mgr.log_file, 'get_key')
            assert log_mgr.log_file.get_key() == log_file

            log_mgr.close()

    def test_init_accepts_dataset_instances_directly(self, monkeypatch):
        """Test initialization uses Dataset instances as-is without re-parsing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_ds = dataset_from_config(os.path.join(tmpdir, 'test.log'))
            state_ds = dataset_from_config(os.path.join(tmpdir, 'state.json'))

            # If __init__ tries to re-parse Dataset instances, this test fails.
            monkeypatch.setattr(
                'd3tools.logging.workflow_log_manager.dataset_from_config',
                lambda _cfg: (_ for _ in ()).throw(AssertionError('should not re-parse Dataset'))
            )

            log_mgr = WorkflowLogManager(
                log_file=log_ds,
                run_state_file=state_ds,
                console=False,
            )

            assert log_mgr.log_file is log_ds
            assert log_mgr.run_state_file is state_ds

            log_mgr.close()


class TestRemoteLogging:
    """Test remote Dataset logging with temp file handling."""

    def test_configure_logger_creates_temp_file_for_remote_dataset(self, monkeypatch):
        """Test that remote Datasets trigger temp file creation."""
        # Create a mock Dataset with type='remote'
        mock_dataset = Mock()
        mock_dataset.type = 'remote'
        mock_dataset.get_key = Mock(return_value='/remote/path.log')

        # Monkeypatch dataset_from_config to return our mock
        def mock_dataset_from_config(config):
            return mock_dataset if config else None

        monkeypatch.setattr(
            'd3tools.logging.workflow_log_manager.dataset_from_config',
            mock_dataset_from_config
        )

        log_mgr = WorkflowLogManager(log_file='/remote/path.log', console=False)

        assert hasattr(log_mgr, '_temp_log_file')
        assert os.path.exists(log_mgr._temp_log_file)

        log_mgr.close()

    def test_close_uploads_temp_log_file_to_remote_dataset(self, monkeypatch):
        """Test that close() uploads temp file to remote Dataset and cleans up."""
        # Create a mock Dataset that captures write_data calls
        mock_dataset = Mock()
        mock_dataset.type = 'remote'
        mock_dataset.write_data = Mock()

        def mock_dataset_from_config(config):
            return mock_dataset if config else None

        monkeypatch.setattr(
            'd3tools.logging.workflow_log_manager.dataset_from_config',
            mock_dataset_from_config
        )

        log_mgr = WorkflowLogManager(log_file='/remote/path.log', console=False)
        temp_file = log_mgr._temp_log_file

        log_mgr.logger.info("Test message")
        log_mgr.close()

        # Verify write_data was called
        assert mock_dataset.write_data.called

        # Verify temp file was deleted
        assert not os.path.exists(temp_file)

    def test_close_handles_temp_file_cleanup_error(self, monkeypatch):
        """Test that a cleanup error is logged but doesn't crash close()."""
        mock_dataset = Mock()
        mock_dataset.type = 'remote'
        mock_dataset.write_data = Mock()

        def mock_dataset_from_config(config):
            return mock_dataset if config else None

        monkeypatch.setattr(
            'd3tools.logging.workflow_log_manager.dataset_from_config',
            mock_dataset_from_config
        )

        monkeypatch.setattr('os.remove', Mock(side_effect=OSError("Permission denied")))

        log_mgr = WorkflowLogManager(log_file='/remote/path.log', console=False)

        # Should not raise, just log a warning
        log_mgr.close()
        assert log_mgr._closed


class TestWorkflowLogManagerFromDict:
    """Test WorkflowLogManager.from_dict() factory method."""

    @pytest.fixture(autouse=True)
    def _stub_logger_setup(self, monkeypatch):
        """Isolate config parsing from runtime handler compatibility.

        WorkflowLogManager currently normalizes file targets to Dataset objects,
        while logger handlers still expect local string paths. Stub logger setup
        so from_dict tests validate creation behavior only.
        """
        def _fake_configure_logger(self):
            logger = logging.getLogger(f"test.{id(self)}")
            logger.handlers.clear()
            logger.propagate = False
            self._managed_loggers = [logger]
            return logger

        monkeypatch.setattr(WorkflowLogManager, "_configure_logger", _fake_configure_logger)
    
    def test_from_dict_none(self):
        """Test from_dict with None returns a default console-only logger."""
        log_mgr = WorkflowLogManager.from_dict(None)
        assert log_mgr is not None
        assert log_mgr.log_file is None
        assert log_mgr.run_state_file is None
        assert log_mgr.console is True

        log_mgr.close()
    
    def test_from_dict_empty_dict(self):
        """Test from_dict with empty dict returns a default console-only logger."""
        log_mgr = WorkflowLogManager.from_dict({})
        assert log_mgr is not None
        assert log_mgr.log_file is None
        assert log_mgr.run_state_file is None
        assert log_mgr.console is True

        log_mgr.close()
    
    def test_from_dict_string_path(self):
        """Test from_dict with simple string path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager.from_dict(log_file)
            
            assert log_mgr is not None
            assert hasattr(log_mgr.log_file, 'get_key')
            assert log_mgr.level == logging.INFO  # Default
            assert log_mgr.console is True  # Default
            
            log_mgr.close()
    
    def test_from_dict_with_now_placeholder(self):
        """Test from_dict with {now:...} placeholder in path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'log_{now:%Y%m%d}.log')
            }
            log_mgr = WorkflowLogManager.from_dict(config)
            
            # Should substitute {now} with current date
            expected_date = dt.datetime.now().strftime('%Y%m%d')
            assert expected_date in log_mgr.log_file.get_key()
            
            log_mgr.close()
    
    def test_from_dict_full_config(self):
        """Test from_dict with full configuration dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'test.log'),
                'run_state_file': os.path.join(tmpdir, 'workflow_state.json'),
                'level': 'DEBUG',
                'console': False,
                'format': 'minimal',
                'logger_name': 'test_workflow'
            }
            log_mgr = WorkflowLogManager.from_dict(config)
            
            assert hasattr(log_mgr.log_file, 'get_key')
            assert hasattr(log_mgr.run_state_file, 'get_key')
            assert log_mgr.level == 'DEBUG'
            assert log_mgr.console is False
            assert log_mgr.format_file == 'minimal'
            assert log_mgr.logger_name == 'test_workflow'
            
            log_mgr.close()
    
    def test_from_dict_separate_formats(self):
        """Test from_dict with separate format_file and format_console."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'test.log'),
                'format_file': 'detailed',
                'format_console': 'simple'
            }
            log_mgr = WorkflowLogManager.from_dict(config)
            
            assert log_mgr.format_file == 'detailed'
            assert log_mgr.format_console == 'simple'
            
            log_mgr.close()

    def test_from_dict_with_run_state_now_placeholder(self):
        """Test from_dict substitutes {now:...} in run_state_file path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'run_state_file': os.path.join(tmpdir, 'state_{now:%Y%m%d}.json')
            }

            log_mgr = WorkflowLogManager.from_dict(config)

            expected_date = dt.datetime.now().strftime('%Y%m%d')
            assert expected_date in log_mgr.run_state_file.get_key()

            log_mgr.close()

    def test_from_dict_resolves_now_once_for_both_targets(self):
        """Direct from_dict should resolve now placeholders for all targets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'log_{now:%Y%m%d_%H%M%S}.log'),
                'run_state_file': os.path.join(tmpdir, 'state_{now:%Y%m%d_%H%M%S}.json'),
            }

            log_mgr = WorkflowLogManager.from_dict(config)
            log_name = os.path.basename(log_mgr.log_file.get_key())
            state_name = os.path.basename(log_mgr.run_state_file.get_key())

            log_stamp = log_name.replace('log_', '').replace('.log', '')
            state_stamp = state_name.replace('state_', '').replace('.json', '')
            assert log_stamp == state_stamp

            log_mgr.close()
    
    def test_from_dict_format_key_applies_to_both(self):
        """Test that 'format' key sets both format_file and format_console."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'test.log'),
                'format': 'detailed',
            }
            log_mgr = WorkflowLogManager.from_dict(config)

            assert log_mgr.format_file == 'detailed'
            assert log_mgr.format_console == 'detailed'

            log_mgr.close()

    def test_from_dict_explicit_format_file_overrides_format(self):
        """Test that explicit format_file/format_console override the 'format' key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'test.log'),
                'format': 'minimal',
                'format_file': 'detailed',
                'format_console': 'simple',
            }
            log_mgr = WorkflowLogManager.from_dict(config)

            assert log_mgr.format_file == 'detailed'
            assert log_mgr.format_console == 'simple'

            log_mgr.close()

    def test_from_dict_file_as_dataset_dict(self):
        """Test from_dict with 'file' value given as a Dataset config dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            config = {'file': {'key_pattern': log_file}}
            log_mgr = WorkflowLogManager.from_dict(config)

            assert hasattr(log_mgr.log_file, 'get_key')
            assert log_mgr.log_file.get_key() == log_file

            log_mgr.close()

    def test_from_dict_invalid_type(self):
        """Test from_dict with invalid type raises TypeError."""
        with pytest.raises(TypeError, match="Config must be dict, str, or None"):
            WorkflowLogManager.from_dict(123)


class TestWorkflowLogManagerLogging:
    """Test actual logging behavior."""
    
    def test_logging_to_file(self):
        """Test that log messages are written to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            # Log some messages
            log_mgr.logger.info("Test message 1")
            log_mgr.logger.warning("Test warning")
            log_mgr.logger.debug("Test debug (should not appear)")
            
            log_mgr.close()
            
            # Read log file and verify content
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert "Test message 1" in content
            assert "Test warning" in content
            assert "Test debug" not in content  # DEBUG < INFO
    
    def test_logging_with_debug_level(self):
        """Test logging with DEBUG level captures all messages."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, level='DEBUG', console=False)
            
            log_mgr.logger.debug("Debug message")
            log_mgr.logger.info("Info message")
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert "Debug message" in content
            assert "Info message" in content
    
    def test_hierarchical_logging(self):
        """Test that child loggers inherit configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(
                log_file=log_file, 
                console=False,
                logger_name='d3tools'
            )
            
            # Get child loggers (simulating door, dam, dryes)
            door_log = logging.getLogger('door')
            dam_log = logging.getLogger('dam')
            
            door_log.info("Message from door")
            dam_log.info("Message from dam")
            
            # Child logger messages should appear in the log
            # d3tools children definitely should
            d3tools_child = logging.getLogger('d3tools.submodule')
            d3tools_child.info("Message from d3tools child")
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # All module messages should appear
            assert "Message from door" in content
            assert "Message from dam" in content
            assert "Message from d3tools child" in content


class TestWorkflowExecutionContext:
    """Test workflow_execution() context manager."""
    
    def test_workflow_execution_success(self):
        """Test workflow_execution logs start and end for successful execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with log_mgr.workflow_execution('test_workflow'):
                log_mgr.logger.info("Inside workflow")
                time.sleep(0.1)  # Small delay to measure duration
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Check for expected log messages
            assert "Workflow 'test_workflow' starting" in content
            assert "Inside workflow" in content
            assert "Workflow 'test_workflow' completed successfully" in content
            assert "Total execution time:" in content
    
    def test_workflow_execution_failure(self):
        """Test workflow_execution logs failure when exception occurs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with pytest.raises(ValueError):
                with log_mgr.workflow_execution('failing_workflow'):
                    log_mgr.logger.info("About to fail")
                    raise ValueError("Test error")
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Check for failure logging
            assert "Workflow 'failing_workflow' starting" in content
            assert "About to fail" in content
            assert "Workflow 'failing_workflow' failed" in content
            assert "ValueError: Test error" in content
            assert "Execution time:" in content
    


class TestSectionExecutionContext:
    """Test section_execution() context manager."""
    
    def test_section_execution_success(self):
        """Test section_execution logs section start and end."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with log_mgr.section_execution('download_section', engine='door'):
                log_mgr.logger.info("Downloading data")
                time.sleep(0.1)
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert "Section 'download_section' [door] starting" in content
            assert "Downloading data" in content
            assert "Section 'download_section' completed" in content
    
    def test_section_execution_failure(self):
        """Test section_execution logs failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with pytest.raises(RuntimeError):
                with log_mgr.section_execution('failing_section'):
                    raise RuntimeError("Section failed")
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert "Section 'failing_section' starting" in content
            assert "Section 'failing_section' failed" in content
            assert "RuntimeError: Section failed" in content
    
    def test_section_execution_tracks_time(self):
        """Test that section_execution tracks section times."""
        log_mgr = WorkflowLogManager(console=False)
        
        with log_mgr.section_execution('section1'):
            time.sleep(0.05)
        
        with log_mgr.section_execution('section2'):
            time.sleep(0.05)
        
        # Both sections should be tracked
        assert 'section1' in log_mgr._section_times
        assert 'section2' in log_mgr._section_times
        assert log_mgr._section_times['section1'] > 0
        assert log_mgr._section_times['section2'] > 0
        
        log_mgr.close()
    
    def test_section_execution_without_engine(self):
        """Test section_execution without engine parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with log_mgr.section_execution('generic_section'):
                pass
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Should not have [engine] tag
            assert "Section 'generic_section' starting" in content
            assert "[door]" not in content

    def test_section_execution_failure_still_populates_section_times(self):
        """Test that a failed section still records its elapsed time."""
        log_mgr = WorkflowLogManager(console=False)

        with pytest.raises(RuntimeError):
            with log_mgr.section_execution('bad_section'):
                raise RuntimeError("boom")

        assert 'bad_section' in log_mgr._section_times
        assert log_mgr._section_times['bad_section'] > 0

        log_mgr.close()

    def test_section_execution_clears_current_section(self):
        """Test that _current_section is None after the context exits (success and failure)."""
        log_mgr = WorkflowLogManager(console=False)

        with log_mgr.section_execution('ok_section'):
            assert log_mgr._current_section == 'ok_section'
        assert log_mgr._current_section is None

        with pytest.raises(ValueError):
            with log_mgr.section_execution('fail_section'):
                assert log_mgr._current_section == 'fail_section'
                raise ValueError("fail")
        assert log_mgr._current_section is None

        log_mgr.close()


class TestWorkflowLogManagerMethods:
    """Test WorkflowLogManager utility methods."""
    
    def test_get_logger_default(self):
        """Test get_logger() without name returns the workflow logger."""
        log_mgr = WorkflowLogManager(console=False)
        logger = log_mgr.get_logger()
        
        assert logger is log_mgr.logger
        log_mgr.close()
    
    def test_get_logger_with_name(self):
        """Test get_logger() with name returns named logger."""
        log_mgr = WorkflowLogManager(console=False, logger_name='d3tools')
        logger = log_mgr.get_logger('d3tools.submodule')
        
        assert logger.name == 'd3tools.submodule'
        log_mgr.close()
    
    def test_set_level(self):
        """Test set_level() changes logging level."""
        log_mgr = WorkflowLogManager(level='INFO', console=False)
        
        assert log_mgr.logger.level == logging.INFO
        
        log_mgr.set_level('DEBUG')
        assert log_mgr.logger.level == logging.DEBUG
        assert logging.getLogger('door').level == logging.DEBUG
        
        # Handlers should also be updated
        for handler in log_mgr.logger.handlers:
            assert handler.level == logging.DEBUG
        
        log_mgr.close()

    def test_set_level_with_int(self):
        """Test set_level() accepts an integer constant."""
        log_mgr = WorkflowLogManager(level='INFO', console=False)

        log_mgr.set_level(logging.WARNING)
        assert log_mgr.logger.level == logging.WARNING
        for handler in log_mgr.logger.handlers:
            assert handler.level == logging.WARNING

        log_mgr.close()
    
    def test_close_idempotent(self):
        """Test that close() is idempotent (safe to call multiple times)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager(log_file=log_file)
            file_handler = next(
                handler for handler in log_mgr.logger.handlers
                if isinstance(handler, logging.FileHandler)
            )
            assert file_handler in logging.getLogger('door').handlers
            
            # Close multiple times - should not raise
            log_mgr.close()
            log_mgr.close()
            log_mgr.close()
            
            # Handlers should be cleared after first close
            assert len(log_mgr.logger.handlers) == 0
            assert file_handler not in logging.getLogger('door').handlers
            assert log_mgr._closed is True
    
    def test_format_duration(self):
        """Test _format_duration() method."""
        # Test various durations
        assert WorkflowLogManager._format_duration(5.5) == "5.5s"
        assert WorkflowLogManager._format_duration(65) == "1m 5s"
        assert WorkflowLogManager._format_duration(3665) == "1h 1m 5s"
        assert WorkflowLogManager._format_duration(0.1) == "0.1s"


class TestWorkflowLogManagerIntegration:
    """Integration tests combining multiple features."""
    
    def test_full_workflow_with_sections(self):
        """Test complete workflow with multiple sections."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with log_mgr.workflow_execution('full_workflow'):
                with log_mgr.section_execution('section1', engine='door'):
                    log_mgr.logger.info("Section 1 work")
                    time.sleep(0.05)
                
                with log_mgr.section_execution('section2', engine='dam'):
                    log_mgr.logger.info("Section 2 work")
                    time.sleep(0.05)
                
                with log_mgr.section_execution('section3', engine='dryes'):
                    log_mgr.logger.info("Section 3 work")
                    time.sleep(0.05)
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Check workflow start/end
            assert "Workflow 'full_workflow' starting" in content
            assert "Workflow 'full_workflow' completed successfully" in content
            
            # Check all sections
            assert "Section 'section1' [door]" in content
            assert "Section 'section2' [dam]" in content
            assert "Section 'section3' [dryes]" in content
            
            # Check section summary
            assert "Workflow Section Summary" in content
            assert "section1:" in content
            assert "section2:" in content
            assert "section3:" in content
            assert "Total section time:" in content
    
    def test_workflow_with_partial_failure(self):
        """Test workflow where one section fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)
            
            with pytest.raises(ValueError):
                with log_mgr.workflow_execution('partial_fail'):
                    with log_mgr.section_execution('section1'):
                        log_mgr.logger.info("Section 1 OK")
                    
                    with log_mgr.section_execution('section2'):
                        raise ValueError("Section 2 failed")
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Section 1 should complete
            assert "Section 'section1' completed" in content
            
            # Section 2 should fail
            assert "Section 'section2' failed" in content
            
            # Workflow should fail
            assert "Workflow 'partial_fail' failed" in content

    def test_workflow_with_partial_failure_shows_section_summary(self):
        """Test that the section summary is logged even when the workflow fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            log_mgr = WorkflowLogManager(log_file=log_file, console=False)

            with pytest.raises(ValueError):
                with log_mgr.workflow_execution('summary_fail'):
                    with log_mgr.section_execution('s1'):
                        pass
                    with log_mgr.section_execution('s2'):
                        raise ValueError("fail")

            log_mgr.close()

            with open(log_file, 'r') as f:
                content = f.read()

            assert "Workflow Section Summary" in content
            assert "s1:" in content
            assert "s2:" in content
            assert "Total section time:" in content
    
    def test_nested_logging_with_child_modules(self):
        """Test that modules using logging.getLogger(__name__) work correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            
            # Create log manager with d3tools as root
            log_mgr = WorkflowLogManager(
                log_file=log_file,
                console=False,
                logger_name='d3tools'
            )
            
            # Simulate different modules logging
            data_log = logging.getLogger('d3tools.data')
            config_log = logging.getLogger('d3tools.config')
            parse_log = logging.getLogger('d3tools.parse')
            
            with log_mgr.workflow_execution('module_test'):
                data_log.info("Data module message")
                config_log.info("Config module message")
                parse_log.warning("Parse module warning")
            
            log_mgr.close()
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # All module messages should appear
            assert "Data module message" in content
            assert "Config module message" in content
            assert "Parse module warning" in content


class TestWorkflowRunStateWriting:
    """Test write_run_state() method for persisting workflow run state."""
    
    def test_write_run_state_creates_file(self):
        """Test that write_run_state creates a JSON file with run state."""
        import json
        
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, 'run_state.json')
            log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)
            
            run_state = {
                "version": 1,
                "run_id": "2024-05-07T14:30:00",
                "workflow": {
                    "name": "test_workflow",
                    "status": "success",
                    "start": "2024-05-07T00:00:00",
                    "end": "2024-05-07T23:59:59"
                },
                "sections": [
                    {
                        "name": "download",
                        "engine": "door",
                        "executed": True,
                        "start": "2024-05-07T00:00:00",
                        "end": "2024-05-07T06:00:00",
                        "reason": None
                    }
                ]
            }
            
            log_mgr.write_run_state(run_state)
            log_mgr.close()
            
            # Verify file was created
            assert os.path.exists(state_file)
            
            # Verify JSON structure
            with open(state_file, 'r') as f:
                loaded_state = json.load(f)
            
            assert loaded_state["version"] == 1
            assert loaded_state["workflow"]["name"] == "test_workflow"
            assert len(loaded_state["sections"]) == 1
            assert loaded_state["sections"][0]["name"] == "download"
    
    def test_write_run_state_no_op_if_not_configured(self):
        """Test that write_run_state is a no-op if run_state_file is not configured."""
        log_mgr = WorkflowLogManager(console=False, run_state_file=None)
        
        run_state = {
            "version": 1,
            "run_id": "test",
            "name": "test",
            "sections": []
        }
        
        # Should not raise, just silently skip
        log_mgr.write_run_state(run_state)
        log_mgr.close()
    
    def test_write_run_state_creates_parent_directories(self):
        """Test that write_run_state creates missing parent directories."""
        import json
        
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, 'subdir', 'nested', 'run_state.json')
            log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)
            
            run_state = {
                "version": 1,
                "run_id": "test_id",
                "name": "test",
                "sections": []
            }
            
            log_mgr.write_run_state(run_state)
            log_mgr.close()
            
            # Parent directories should have been created
            assert os.path.exists(os.path.dirname(state_file))
            assert os.path.exists(state_file)
    
    def test_write_run_state_with_multiple_sections(self):
        """Test write_run_state with multiple sections."""
        import json
        
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, 'run_state.json')
            log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)
            
            run_state = {
                "version": 1,
                "run_id": "2024-05-07T14:30:00",
                "name": "multi_section",
                "status": "success",
                "sections": [
                    {"name": "download", "engine": "door", "executed": True},
                    {"name": "process", "engine": "dam", "executed": True},
                    {"name": "calculate", "engine": "dryes", "executed": True}
                ]
            }
            
            log_mgr.write_run_state(run_state)
            log_mgr.close()
            
            with open(state_file, 'r') as f:
                loaded_state = json.load(f)
            
            assert len(loaded_state["sections"]) == 3
            assert [s["name"] for s in loaded_state["sections"]] == ["download", "process", "calculate"]
    
    def test_write_run_state_with_skipped_sections(self):
        """Test write_run_state with skipped sections."""
        import json
        
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, 'run_state.json')
            log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)
            
            run_state = {
                "version": 1,
                "run_id": "2024-05-07T14:30:00",
                "name": "partial_workflow",
                "status": "partial",
                "sections": [
                    {"name": "download", "engine": "door", "executed": True, "reason": None},
                    {"name": "process", "engine": "dam", "executed": False, "reason": "No data available"}
                ]
            }
            
            log_mgr.write_run_state(run_state)
            log_mgr.close()
            
            with open(state_file, 'r') as f:
                loaded_state = json.load(f)
            
            assert loaded_state["status"] == "partial"
            assert loaded_state["sections"][0]["executed"] is True
            assert loaded_state["sections"][1]["executed"] is False
            assert loaded_state["sections"][1]["reason"] == "No data available"
    
    def test_write_run_state_error_handling(self):
        """Test write_run_state raises error when file write fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a read-only directory to cause write failure
            state_file = os.path.join(tmpdir, 'readonly_dir', 'run_state.json')
            os.makedirs(os.path.dirname(state_file))
            os.chmod(os.path.dirname(state_file), 0o444)  # Read-only
            
            try:
                log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)
                
                run_state = {
                    "version": 1,
                    "run_id": "test",
                    "name": "test",
                    "sections": []
                }
                
                # Should raise an error
                with pytest.raises(Exception):  # PermissionError or OSError
                    log_mgr.write_run_state(run_state)
                
                log_mgr.close()
            finally:
                # Restore permissions for cleanup
                os.chmod(os.path.dirname(state_file), 0o755)
    
    def test_write_run_state_formats_json_nicely(self):
        """Test that write_run_state formats JSON with indentation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, 'run_state.json')
            log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)
            
            run_state = {
                "version": 1,
                "run_id": "test",
                "workflow": {"name": "test", "nested": {"key": "value"}},
                "sections": []
            }
            
            log_mgr.write_run_state(run_state)
            log_mgr.close()
            
            # Read and verify formatting (indentation = 2)
            with open(state_file, 'r') as f:
                content = f.read()
            
            # Should have indented formatting
            assert '  ' in content  # 2-space indentation
            assert '\n' in content  # Multiple lines

    def test_write_run_state_overwrites_on_second_call(self):
        """Test that write_run_state replaces the file on subsequent calls."""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, 'run_state.json')
            log_mgr = WorkflowLogManager(run_state_file=state_file, console=False)

            log_mgr.write_run_state({"version": 1, "run_id": "first", "sections": []})
            log_mgr.write_run_state({"version": 1, "run_id": "second", "sections": []})
            log_mgr.close()

            with open(state_file, 'r') as f:
                loaded = json.load(f)

            assert loaded["run_id"] == "second"
