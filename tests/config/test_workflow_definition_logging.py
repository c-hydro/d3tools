"""
Integration tests for WorkflowDefinition logging.

These tests verify that WorkflowDefinition properly integrates with
WorkflowLogManager during workflow execution.

NOTE: Detailed logging functionality (file handlers, directory creation,
log content formatting, etc.) is tested in tests/logging/.
These tests focus ONLY on the WorkflowDefinition integration layer.
"""
import pytest

from d3tools import WorkflowDefinition
from d3tools.config.workflow_section import WorkflowSection
from d3tools.logging import WorkflowLogManager


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def config_with_logging(tmp_path):
    """Configuration with logging enabled."""
    log_file = tmp_path / "workflow.log"
    return {
        "TAGS": {},
        "DATASETS": {},
        "workflow_name": "test_workflow",
        "workflow_log": {
            "file": str(log_file),
            "level": "INFO",
            "console": False
        }
    }, log_file


@pytest.fixture
def config_with_sections_and_logging(tmp_path, monkeypatch):
    """Configuration with workflow sections and logging."""
    from d3tools.config import parsing_pipeline
    
    log_file = tmp_path / "workflow.log"
    
    class MockDoorProcess:
        def get_data(self, time_range):
            pass
    
    class MockDamProcess:
        def run(self, time_range):
            pass
    
    parsed_config = {
        "TAGS": {},
        "DATASETS": {},
        "workflow_name": "test_workflow",
        "workflow_log": {
            "file": str(log_file),
            "level": "INFO",
            "console": False
        },
        "workflow_sections": [
            WorkflowSection("Download", "door", {}, MockDoorProcess()),
            WorkflowSection("Process", "dam", {}, MockDamProcess()),
        ]
    }
    
    monkeypatch.setattr(
        parsing_pipeline,
        "parse_options",
        lambda config, **kwargs: parsed_config
    )
    
    return {}, log_file


# ============================================================================
# Integration Tests
# ============================================================================

class TestWorkflowDefinitionLoggingIntegration:
    """Test WorkflowDefinition integration with WorkflowLogManager."""

    def test_logger_initialized_from_config(self, config_with_logging):
        """WorkflowDefinition should initialize logger from workflow_log config."""
        config, log_file = config_with_logging
        wf = WorkflowDefinition(config)
        
        assert wf.logger is not None
        assert isinstance(wf.logger, WorkflowLogManager)
        assert hasattr(wf.logger.log_file, 'get_key')
        assert wf.logger.log_file.get_key() == str(log_file)

    def test_run_uses_logger_for_workflow_execution(self, config_with_logging):
        """run() should use logger to log workflow execution."""
        config, log_file = config_with_logging
        config["workflow_name"] = "my_test_workflow"
        
        wf = WorkflowDefinition(config)
        wf.run(start="2024-01-01", end="2024-01-02")
        
        # Verify log file was created and contains workflow execution logs
        assert log_file.exists()
        content = log_file.read_text()
        assert "'my_test_workflow' starting" in content

    def test_run_uses_logger_for_section_execution(self, config_with_sections_and_logging):
        """run() should use logger to log section execution."""
        config, log_file = config_with_sections_and_logging
        
        wf = WorkflowDefinition(config)
        wf.run(start="2024-01-01", end="2024-01-02")
        
        # Verify sections are logged
        content = log_file.read_text()
        assert "Download" in content
        assert "Process" in content

    def test_run_without_logger_works(self):
        """run() should work when logger is None."""
        config = {
            "TAGS": {},
            "DATASETS": {},
            "workflow_log": None
        }
        wf = WorkflowDefinition(config)
        
        # Should not raise
        wf.run(start="2024-01-01", end="2024-01-02")
