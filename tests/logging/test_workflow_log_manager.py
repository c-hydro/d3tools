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

import pytest

from d3tools.logging import WorkflowLogManager


class TestWorkflowLogManagerInit:
    """Test WorkflowLogManager initialization."""
    
    def test_init_console_only(self):
        """Test initialization with console-only logging."""
        log_mgr = WorkflowLogManager(console=True, log_file=None)
        
        assert log_mgr.log_file is None
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
            
            assert log_mgr.log_file == log_file
            assert log_mgr.console is False
            
            # Should have file handler but no console handler
            handlers = log_mgr.logger.handlers
            assert len(handlers) == 1
            assert isinstance(handlers[0], logging.FileHandler)
            
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


class TestWorkflowLogManagerFromDict:
    """Test WorkflowLogManager.from_dict() factory method."""
    
    def test_from_dict_none(self):
        """Test from_dict with None returns a default console-only logger."""
        log_mgr = WorkflowLogManager.from_dict(None)
        assert log_mgr is not None
        assert log_mgr.log_file is None
        assert log_mgr.console is True

        log_mgr.close()
    
    def test_from_dict_empty_dict(self):
        """Test from_dict with empty dict returns a default console-only logger."""
        log_mgr = WorkflowLogManager.from_dict({})
        assert log_mgr is not None
        assert log_mgr.log_file is None
        assert log_mgr.console is True

        log_mgr.close()
    
    def test_from_dict_string_path(self):
        """Test from_dict with simple string path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            log_mgr = WorkflowLogManager.from_dict(log_file)
            
            assert log_mgr is not None
            assert log_mgr.log_file == log_file
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
            assert expected_date in log_mgr.log_file
            
            log_mgr.close()
    
    def test_from_dict_full_config(self):
        """Test from_dict with full configuration dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'file': os.path.join(tmpdir, 'test.log'),
                'level': 'DEBUG',
                'console': False,
                'format': 'minimal',
                'logger_name': 'test_workflow'
            }
            log_mgr = WorkflowLogManager.from_dict(config)
            
            assert log_mgr.log_file == config['file']
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
    
    def test_workflow_execution_tracks_time(self):
        """Test that workflow_execution tracks execution time."""
        log_mgr = WorkflowLogManager(console=False)
        
        with log_mgr.workflow_execution('timed_workflow'):
            time.sleep(0.1)
        
        # _workflow_start_time should have been set
        assert log_mgr._workflow_start_time is not None
        
        log_mgr.close()


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
