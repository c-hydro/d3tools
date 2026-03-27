"""
Tests for logging utility functions.

Tests cover:
- create_file_handler()
- create_console_handler()
- configure_logger()
- LOG_FORMATS and DATE_FORMAT constants
"""

import logging
import os
import tempfile

import pytest

from d3tools.logging.utils import (
    create_file_handler,
    create_console_handler,
    configure_logger,
    LOG_FORMATS,
    DATE_FORMAT
)


class TestCreateFileHandler:
    """Test create_file_handler() function."""
    
    def test_creates_file_handler(self):
        """Test that create_file_handler creates a FileHandler."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            handler = create_file_handler(log_file)
            
            assert isinstance(handler, logging.FileHandler)
            assert handler.level == logging.INFO  # Default level
            
            handler.close()
    
    def test_creates_directory_if_not_exists(self):
        """Test that handler creation creates log directory if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'logs', 'subdir', 'test.log')
            handler = create_file_handler(log_file)
            
            # Directory should be created
            assert os.path.exists(os.path.dirname(log_file))
            
            handler.close()
    
    def test_accepts_string_level(self):
        """Test that handler accepts string log level."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            handler = create_file_handler(log_file, level='DEBUG')
            
            assert handler.level == logging.DEBUG
            
            handler.close()
    
    def test_accepts_int_level(self):
        """Test that handler accepts integer log level."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            handler = create_file_handler(log_file, level=logging.WARNING)
            
            assert handler.level == logging.WARNING
            
            handler.close()
    
    def test_format_styles(self):
        """Test different format styles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            for style in ['detailed', 'simple', 'minimal']:
                log_file = os.path.join(tmpdir, f'{style}.log')
                handler = create_file_handler(log_file, format_style=style)
                
                # Handler should have a formatter
                assert handler.formatter is not None
                assert handler.formatter.datefmt == DATE_FORMAT
                
                handler.close()
    
    def test_mode_append(self):
        """Test file handler in append mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            
            # Write first message
            handler1 = create_file_handler(log_file, mode='a')
            logger = logging.getLogger('test_append')
            logger.addHandler(handler1)
            logger.setLevel(logging.INFO)
            logger.info("First message")
            handler1.close()
            logger.removeHandler(handler1)
            
            # Append second message
            handler2 = create_file_handler(log_file, mode='a')
            logger.addHandler(handler2)
            logger.info("Second message")
            handler2.close()
            logger.removeHandler(handler2)
            
            # Both messages should be in file
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert "First message" in content
            assert "Second message" in content
    
    def test_mode_overwrite(self):
        """Test file handler in overwrite mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            
            # Write first message
            handler1 = create_file_handler(log_file, mode='w')
            logger = logging.getLogger('test_overwrite')
            logger.addHandler(handler1)
            logger.setLevel(logging.INFO)
            logger.info("First message")
            handler1.close()
            logger.removeHandler(handler1)
            
            # Overwrite with second message
            handler2 = create_file_handler(log_file, mode='w')
            logger.addHandler(handler2)
            logger.info("Second message")
            handler2.close()
            logger.removeHandler(handler2)
            
            # Only second message should be in file
            with open(log_file, 'r') as f:
                content = f.read()
            
            assert "First message" not in content
            assert "Second message" in content


class TestCreateConsoleHandler:
    """Test create_console_handler() function."""
    
    def test_creates_stream_handler(self):
        """Test that create_console_handler creates a StreamHandler."""
        handler = create_console_handler()
        
        assert isinstance(handler, logging.StreamHandler)
        assert not isinstance(handler, logging.FileHandler)
        assert handler.level == logging.INFO  # Default level
        
        handler.close()
    
    def test_accepts_string_level(self):
        """Test that handler accepts string log level."""
        handler = create_console_handler(level='ERROR')
        
        assert handler.level == logging.ERROR
        
        handler.close()
    
    def test_accepts_int_level(self):
        """Test that handler accepts integer log level."""
        handler = create_console_handler(level=logging.CRITICAL)
        
        assert handler.level == logging.CRITICAL
        
        handler.close()
    
    def test_format_styles(self):
        """Test different format styles."""
        for style in ['detailed', 'simple', 'minimal']:
            handler = create_console_handler(format_style=style)
            
            # Handler should have a formatter
            assert handler.formatter is not None
            assert handler.formatter.datefmt == DATE_FORMAT
            
            handler.close()
    
    def test_custom_stream(self):
        """Test handler with custom stream."""
        import io
        stream = io.StringIO()
        handler = create_console_handler(stream=stream)
        
        # Log a message
        logger = logging.getLogger('test_stream')
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("Test message")
        
        # Message should be in the stream
        assert "Test message" in stream.getvalue()
        
        handler.close()
        logger.removeHandler(handler)


class TestConfigureLogger:
    """Test configure_logger() function."""
    
    def test_console_only(self):
        """Test configuring logger with console only."""
        logger = configure_logger(
            'test_console',
            console=True,
            file_path=None
        )
        
        assert logger.name == 'test_console'
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)
        
        # Clean up
        logger.handlers.clear()
    
    def test_file_only(self):
        """Test configuring logger with file only."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            logger = configure_logger(
                'test_file',
                file_path=log_file,
                console=False
            )
            
            assert logger.name == 'test_file'
            assert len(logger.handlers) == 1
            assert isinstance(logger.handlers[0], logging.FileHandler)
            
            # Clean up
            for handler in logger.handlers:
                handler.close()
            logger.handlers.clear()
    
    def test_file_and_console(self):
        """Test configuring logger with both file and console."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            logger = configure_logger(
                'test_both',
                file_path=log_file,
                console=True
            )
            
            assert len(logger.handlers) == 2
            
            has_console = any(isinstance(h, logging.StreamHandler) and 
                            not isinstance(h, logging.FileHandler) 
                            for h in logger.handlers)
            has_file = any(isinstance(h, logging.FileHandler) for h in logger.handlers)
            
            assert has_console
            assert has_file
            
            # Clean up
            for handler in logger.handlers:
                handler.close()
            logger.handlers.clear()
    
    def test_custom_level(self):
        """Test configuring logger with custom level."""
        logger = configure_logger('test_level', level='DEBUG', console=True)
        
        assert logger.level == logging.DEBUG
        
        # Clean up
        logger.handlers.clear()
    
    def test_custom_formats(self):
        """Test configuring logger with custom formats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, 'test.log')
            logger = configure_logger(
                'test_formats',
                file_path=log_file,
                console=True,
                format_file='minimal',
                format_console='detailed'
            )
            
            # Both handlers should have formatters
            for handler in logger.handlers:
                assert handler.formatter is not None
            
            # Clean up
            for handler in logger.handlers:
                handler.close()
            logger.handlers.clear()
    
    def test_propagate_setting(self):
        """Test propagate parameter."""
        logger1 = configure_logger('test_prop1', propagate=True, console=True)
        assert logger1.propagate is True
        logger1.handlers.clear()
        
        logger2 = configure_logger('test_prop2', propagate=False, console=True)
        assert logger2.propagate is False
        logger2.handlers.clear()
    
    def test_clear_existing_handlers(self):
        """Test clear_existing parameter removes old handlers."""
        logger = logging.getLogger('test_clear')
        
        # Add an initial handler
        handler1 = logging.StreamHandler()
        logger.addHandler(handler1)
        assert len(logger.handlers) == 1
        
        # Configure with clear_existing=True
        configure_logger('test_clear', clear_existing=True, console=True)
        
        # Old handler should be gone, new one added
        assert len(logger.handlers) == 1
        assert logger.handlers[0] is not handler1
        
        # Clean up
        logger.handlers.clear()
    
    def test_keep_existing_handlers(self):
        """Test that existing handlers are kept when clear_existing=False."""
        logger = logging.getLogger('test_keep')
        
        # Add an initial handler
        handler1 = logging.StreamHandler()
        logger.addHandler(handler1)
        assert len(logger.handlers) == 1
        
        # Configure with clear_existing=False (default)
        configure_logger('test_keep', clear_existing=False, console=True)
        
        # Should have both handlers now
        assert len(logger.handlers) == 2
        assert handler1 in logger.handlers
        
        # Clean up
        logger.handlers.clear()


class TestLogFormatsAndConstants:
    """Test LOG_FORMATS and DATE_FORMAT constants."""
    
    def test_log_formats_exist(self):
        """Test that all expected format styles exist."""
        assert 'detailed' in LOG_FORMATS
        assert 'simple' in LOG_FORMATS
        assert 'minimal' in LOG_FORMATS
    
    def test_log_formats_are_strings(self):
        """Test that all formats are strings."""
        for style, format_str in LOG_FORMATS.items():
            assert isinstance(format_str, str)
            assert len(format_str) > 0
    
    def test_date_format_is_string(self):
        """Test that DATE_FORMAT is a valid string."""
        assert isinstance(DATE_FORMAT, str)
        assert len(DATE_FORMAT) > 0
        
        # Should contain typical datetime format specifiers
        assert '%Y' in DATE_FORMAT or '%y' in DATE_FORMAT
