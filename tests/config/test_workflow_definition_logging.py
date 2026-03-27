"""
Integration tests for WorkflowDefinition.run() with logging.

Tests cover:
- WorkflowDefinition execution with logging enabled
- Logging disabled (None config)
- Log file creation and content
- Integration with workflow sections
- Error handling with logging
"""

import os
import pytest
import tempfile
from datetime import datetime

from d3tools.config.workflow_definition import WorkflowDefinition
from d3tools.config.section_definition import SectionDefinition
from d3tools.timestepping import FixedLenTimestep


class TestWorkflowDefinitionWithLogging:
    """Test WorkflowDefinition.run() with logging enabled."""
    
    def test_run_with_logging_disabled(self):
        """Test workflow execution with logging disabled."""
        workflow = WorkflowDefinition(
            name='test_workflow',
            time=FixedLenTimestep(num_steps=1),
            workflow_sections=[],
            workflow_log=None
        )
        
        # Should run without creating log files
        workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
    
    def test_run_with_logging_enabled(self):
        """Test workflow execution with logging enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            # Run workflow
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # Log file should be created
            assert os.path.exists(log_file)
            
            # Log should contain workflow execution messages
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert 'Starting workflow' in content or 'test_workflow' in content
    
    def test_run_with_sections_and_logging(self):
        """Test workflow execution with sections and logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow_sections.log')
            
            # Create mock sections
            section1 = SectionDefinition(
                name='section1',
                parallel=False,
                processes=[]
            )
            section2 = SectionDefinition(
                name='section2',
                parallel=False,
                processes=[]
            )
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[section1, section2],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            # Run workflow
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # Log should contain section execution messages
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Check for section mentions
            # (exact format depends on implementation)
            assert 'section1' in content or 'section2' in content
    
    def test_run_creates_log_directory(self):
        """Test that run() creates log directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'logs', 'subdir', 'workflow.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            # Directory should not exist yet
            assert not os.path.exists(os.path.dirname(log_file))
            
            # Run workflow
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # Directory and file should be created
            assert os.path.exists(log_file)
    
    def test_run_with_placeholder_substitution(self):
        """Test that {now:...} placeholders are substituted in run()."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_pattern = os.path.join(tmpdir, 'workflow_{now:%Y%m%d_%H%M%S}.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_pattern,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            # Run workflow
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # A log file with timestamp should be created
            log_files = [f for f in os.listdir(tmpdir) if f.startswith('workflow_')]
            assert len(log_files) > 0
            
            # The placeholder should be replaced with actual timestamp
            log_file = log_files[0]
            assert '{now:' not in log_file
            assert 'workflow_' in log_file
            assert '.log' in log_file
    
    def test_multiple_runs_with_same_workflow(self):
        """Test multiple runs with same workflow object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            # First run
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # Read log content after first run
            with open(log_file, 'r') as f:
                content1 = f.read()
            
            # Second run
            workflow.run(start=datetime(2024, 2, 1), end=datetime(2024, 2, 2))
            
            # Read log content after second run
            with open(log_file, 'r') as f:
                content2 = f.read()
            
            # Both runs should be logged (append mode)
            assert len(content2) > len(content1)
    
    def test_run_with_custom_log_level(self):
        """Test workflow execution with custom log level."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow_debug.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'DEBUG',
                    'console': False
                }
            )
            
            # Run workflow
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # Log file should exist
            assert os.path.exists(log_file)
    
    def test_run_with_console_logging(self, capsys):
        """Test workflow execution with console logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': True
                }
            )
            
            # Run workflow
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            # Check console output
            captured = capsys.readouterr()
            
            # Should have some output (either stdout or stderr)
            assert captured.out or captured.err


class TestWorkflowDefinitionLoggingErrorHandling:
    """Test error handling in WorkflowDefinition with logging."""
    
    def test_run_with_invalid_log_config(self):
        """Test that invalid log config is handled gracefully."""
        # Invalid config (missing required keys, etc.)
        workflow = WorkflowDefinition(
            name='test_workflow',
            time=FixedLenTimestep(num_steps=1),
            workflow_sections=[],
            workflow_log={}  # Empty dict - no file specified
        )
        
        # Should handle gracefully (either skip logging or use defaults)
        # Exact behavior depends on implementation
        try:
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
        except Exception as e:
            # If it raises, should be a clear error about config
            assert 'log' in str(e).lower() or 'config' in str(e).lower()
    
    def test_run_with_unwritable_log_path(self):
        """Test handling of unwritable log paths."""
        # Try to write to root (should fail without permissions)
        workflow = WorkflowDefinition(
            name='test_workflow',
            time=FixedLenTimestep(num_steps=1),
            workflow_sections=[],
            workflow_log={
                'file': '/root/test.log',
                'level': 'INFO',
                'console': False
            }
        )
        
        # Should raise PermissionError or handle gracefully
        with pytest.raises((PermissionError, OSError)):
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))


class TestWorkflowDefinitionLoggingContent:
    """Test the content of log messages."""
    
    def test_log_contains_workflow_name(self):
        """Test that log contains workflow name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            
            workflow = WorkflowDefinition(
                name='my_custom_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert 'my_custom_workflow' in content
    
    def test_log_contains_timestamps(self):
        """Test that log contains timestamp information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'workflow.log')
            
            workflow = WorkflowDefinition(
                name='test_workflow',
                time=FixedLenTimestep(num_steps=1),
                workflow_sections=[],
                workflow_log={
                    'file': log_file,
                    'level': 'INFO',
                    'console': False
                }
            )
            
            workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
            
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Should contain timestamp patterns
            # (format depends on log format configuration)
            assert any(char.isdigit() for char in content)
