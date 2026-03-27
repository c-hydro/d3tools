"""
Tests for prepare_workflow_log() stage in parsing pipeline.

Tests cover:
- Validation of workflow_log config types
- Normalization of string paths to dict format
- Preservation of placeholders
- Warning on unknown keys
- Integration with parsing pipeline
"""

import os
import pytest
import tempfile

from d3tools.config.parsing_pipeline import prepare_workflow_log


class TestPrepareWorkflowLogValidation:
    """Test validation of workflow_log config."""
    
    def test_none_config_unchanged(self):
        """Test that None config is left unchanged."""
        options = {'workflow_log': None}
        result = prepare_workflow_log(options)
        
        assert result['workflow_log'] is None
    
    def test_missing_key_adds_none(self):
        """Test that missing workflow_log key is handled."""
        options = {}
        result = prepare_workflow_log(options)
        
        # Should add the key with None value or leave it missing
        # (actual behavior depends on implementation)
        assert 'workflow_log' not in result or result['workflow_log'] is None
    
    def test_empty_dict_config_unchanged(self):
        """Test that empty dict config is left unchanged."""
        options = {'workflow_log': {}}
        result = prepare_workflow_log(options)
        
        assert result['workflow_log'] == {}
    
    def test_string_path_normalized_to_dict(self):
        """Test that string path is normalized to dict format."""
        options = {'workflow_log': '/path/to/file.log'}
        result = prepare_workflow_log(options)
        
        assert isinstance(result['workflow_log'], dict)
        assert result['workflow_log']['file'] == '/path/to/file.log'
    
    def test_dict_config_unchanged(self):
        """Test that valid dict config is preserved."""
        config = {
            'file': '/path/to/file.log',
            'level': 'DEBUG',
            'console': True,
            'format_file': 'detailed',
            'format_console': 'simple'
        }
        options = {'workflow_log': config.copy()}
        result = prepare_workflow_log(options)
        
        assert result['workflow_log'] == config
    
    def test_invalid_type_raises_error(self):
        """Test that invalid config types raise TypeError."""
        with pytest.raises(TypeError, match="workflow_log must be"):
            prepare_workflow_log({'workflow_log': 123})
        
        with pytest.raises(TypeError, match="workflow_log must be"):
            prepare_workflow_log({'workflow_log': ['list', 'of', 'things']})
    
    def test_dict_with_file_key(self):
        """Test dict config with file key is preserved."""
        config = {'file': '/path/to/file.log'}
        options = {'workflow_log': config.copy()}
        result = prepare_workflow_log(options)
        
        assert result['workflow_log'] == config


class TestPrepareWorkflowLogPlaceholders:
    """Test preservation of placeholders in workflow_log config."""
    
    def test_preserves_now_placeholder_in_string(self):
        """Test that {now:...} placeholders are preserved in string paths."""
        path_with_placeholder = '/path/to/log_{now:%Y%m%d_%H%M%S}.log'
        options = {'workflow_log': path_with_placeholder}
        result = prepare_workflow_log(options)
        
        # Should normalize to dict but preserve placeholder
        assert '{now:' in result['workflow_log']['file']
        assert result['workflow_log']['file'] == path_with_placeholder
    
    def test_preserves_now_placeholder_in_dict(self):
        """Test that {now:...} placeholders are preserved in dict configs."""
        config = {
            'file': '/path/to/log_{now:%Y%m%d_%H%M%S}.log',
            'level': 'INFO'
        }
        options = {'workflow_log': config.copy()}
        result = prepare_workflow_log(options)
        
        # Should preserve placeholder
        assert '{now:' in result['workflow_log']['file']
        assert result['workflow_log']['file'] == config['file']


class TestPrepareWorkflowLogValidKeys:
    """Test validation of config keys."""
    
    def test_all_valid_keys_accepted(self):
        """Test that all valid keys are accepted without warnings."""
        config = {
            'file': '/path/to/file.log',
            'level': 'DEBUG',
            'console': True,
            'format_file': 'detailed',
            'format_console': 'simple',
            'logger_name': 'custom_logger'
        }
        options = {'workflow_log': config.copy()}
        
        # Should not raise any errors
        result = prepare_workflow_log(options)
        assert result['workflow_log'] == config
    
    def test_warns_on_unknown_keys(self):
        """Test that unknown keys trigger warnings."""
        config = {
            'file': '/path/to/file.log',
            'unknown_key': 'some_value',
            'another_unknown': 123
        }
        options = {'workflow_log': config}
        
        # Should warn but not fail
        with pytest.warns(UserWarning, match='Unknown keys'):
            result = prepare_workflow_log(options)


class TestPrepareWorkflowLogIntegration:
    """Test integration with broader parsing pipeline."""
    
    def test_works_with_plain_dict(self):
        """Test that function accepts plain dict input and converts to Options."""
        # Function should handle plain dict input and convert it to Options internally
        options = {'workflow_log': '/path/to/file.log'}
        result = prepare_workflow_log(options)
        
        # Result should be an Options instance (or dict subclass)
        assert isinstance(result, dict)
        assert 'workflow_log' in result
        assert isinstance(result['workflow_log'], dict)
    
    def test_preserves_other_options(self):
        """Test that other options are not affected."""
        options = {
            'workflow_log': '/path/to/file.log',
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'other_config': {'key': 'value'}
        }
        result = prepare_workflow_log(options)
        
        # Other keys should be unchanged
        assert result['start_date'] == '2024-01-01'
        assert result['end_date'] == '2024-12-31'
        assert result['other_config'] == {'key': 'value'}
        
        # Only workflow_log should be modified
        assert isinstance(result['workflow_log'], dict)
    
    def test_case_insensitive_key_access(self):
        """Test that function handles case-insensitive key access."""
        # Test with various casings of workflow_log key
        for key_name in ['workflow_log', 'Workflow_Log', 'WORKFLOW_LOG']:
            options = {key_name: '/path/to/file.log'}
            result = prepare_workflow_log(options)
            
            # Should find and normalize the config regardless of case
            # The result should contain the normalized workflow_log
            log_config = result.get('workflow_log') or result.get(key_name)
            assert log_config is not None
            assert isinstance(log_config, dict)
            assert log_config['file'] == '/path/to/file.log'


class TestPrepareWorkflowLogEdgeCases:
    """Test edge cases for prepare_workflow_log()."""
    
    def test_empty_string_path(self):
        """Test handling of empty string path."""
        options = {'workflow_log': ''}
        
        # Empty string is normalized to dict (may fail at runtime when trying to create file)
        result = prepare_workflow_log(options)
        assert isinstance(result['workflow_log'], dict)
        assert result['workflow_log']['file'] == ''
    
    def test_relative_path(self):
        """Test handling of relative paths."""
        options = {'workflow_log': 'logs/workflow.log'}
        result = prepare_workflow_log(options)
        
        # Should normalize to dict
        assert isinstance(result['workflow_log'], dict)
        assert result['workflow_log']['file'] == 'logs/workflow.log'
    
    def test_path_with_spaces(self):
        """Test handling of paths with spaces."""
        path = '/path/with spaces/file.log'
        options = {'workflow_log': path}
        result = prepare_workflow_log(options)
        
        # Should preserve spaces
        assert result['workflow_log']['file'] == path
    
    def test_dict_without_file_key(self):
        """Test dict config without file key (console only?)."""
        config = {
            'level': 'INFO',
            'console': True,
            'format_console': 'simple'
        }
        options = {'workflow_log': config.copy()}
        result = prepare_workflow_log(options)
        
        # Should be preserved as-is (will be validated at runtime)
        assert result['workflow_log'] == config
