# WorkflowLogManager

`WorkflowLogManager` is the core logging component that manages workflow-level logging, including automatic tracking of workflow and section execution with timing information.

## Overview

WorkflowLogManager provides:
- **Automatic workflow tracking**: Logs workflow start/end with duration
- **Section execution tracking**: Logs each section with timing
- **Unified configuration**: Single configuration point for entire workflow
- **Hierarchical logging**: Configures root logger that all modules inherit from
- **Context managers**: Clean API for tracking execution blocks
- **File and console output**: Flexible output destinations
- **Multiple formats**: Predefined formats for different use cases

## Basic Usage

### In JSON Configuration

```json
{
    "workflow_log": {
        "file": "logs/workflow.log",
        "level": "INFO",
        "console": true
    }
}
```

That's it! d3tools automatically:
1. Parses the configuration
2. Creates WorkflowLogManager in `WorkflowDefinition.run()`
3. Logs workflow and section execution
4. Cleans up resources when done

### Programmatic Usage

```python
from d3tools.logging import WorkflowLogManager

# Create log manager
log_manager = WorkflowLogManager(
    log_file='logs/workflow.log',
    level='INFO',
    console=True,
    format_file='detailed',
    format_console='simple'
)

# Use context manager for workflow tracking
with log_manager.workflow_execution('my_workflow'):
    # Your workflow code here
    
    with log_manager.section_execution('download', engine='door'):
        # Download section code
        pass
    
    with log_manager.section_execution('process', engine='dam'):
        # Process section code
        pass

# Clean up (happens automatically, but can be called explicitly)
log_manager.close()
```

## Configuration Options

### Constructor Parameters

```python
WorkflowLogManager(
    log_file: str,
    level: str | int = 'INFO',
    console: bool = False,
    format_file: str = 'detailed',
    format_console: str = 'simple',
    logger_name: str = 'd3tools'
)
```

**Parameters:**

- **`log_file`** (str, required): Path to log file
  - Can include `{now:...}` placeholders (resolved with `from_dict()`)
  - Directories are created automatically
  - Example: `'logs/workflow_{now:%Y%m%d}.log'`

- **`level`** (str or int, default: `'INFO'`): Logging level
  - String: `'DEBUG'`, `'INFO'`, `'WARNING'`, `'ERROR'`, `'CRITICAL'`
  - Integer: 10 (DEBUG), 20 (INFO), 30 (WARNING), 40 (ERROR), 50 (CRITICAL)

- **`console`** (bool, default: `False`): Enable console output
  - `True`: Log to both file and console
  - `False`: Log to file only

- **`format_file`** (str, default: `'detailed'`): Log format for file
  - `'detailed'`: Full timestamp, logger name, level, message
  - `'simple'`: Level and message only
  - `'minimal'`: Message only

- **`format_console`** (str, default: `'simple'`): Log format for console
  - Same options as `format_file`

- **`logger_name`** (str, default: `'d3tools'`): Root logger name
  - Usually keep as default
  - Advanced use: create separate logger hierarchies

### Factory Method: from_dict()

Create from configuration dictionary (used internally by d3tools):

```python
config = {
    'file': 'logs/workflow_{now:%Y%m%d_%H%M%S}.log',
    'level': 'DEBUG',
    'console': True,
    'format_file': 'detailed',
    'format_console': 'simple'
}

log_manager = WorkflowLogManager.from_dict(config)
```

**Special handling:**
- Resolves `{now:...}` placeholders with current timestamp
- Handles `None` config (returns `None`)
- Handles string config (converts to `{'file': path}`)
- Validates configuration keys

## Context Managers

### workflow_execution()

Track entire workflow execution:

```python
with log_manager.workflow_execution('my_workflow'):
    # Workflow code
    pass
```

**Logs:**

```
2024-01-15 10:00:00 - INFO - Starting workflow: my_workflow
... (workflow execution) ...
2024-01-15 10:15:23 - INFO - Workflow my_workflow completed in 15m 23s
```

**On error:**

```
2024-01-15 10:00:00 - INFO - Starting workflow: my_workflow
... (workflow execution) ...
2024-01-15 10:05:30 - ERROR - Workflow my_workflow failed after 5m 30s
```

### section_execution()

Track individual section execution:

```python
with log_manager.section_execution('download', engine='door'):
    # Section code
    pass
```

**Logs:**

```
2024-01-15 10:00:00 - INFO - Executing section: download (engine: door)
... (section execution) ...
2024-01-15 10:05:23 - INFO - Section download completed in 5m 23s
```

**Without engine:**

```python
with log_manager.section_execution('custom_section'):
    pass
```

**Logs:**

```
2024-01-15 10:00:00 - INFO - Executing section: custom_section
... (section execution) ...
2024-01-15 10:02:15 - INFO - Section custom_section completed in 2m 15s
```

## Methods

### get_logger()

Get the configured logger:

```python
logger = log_manager.get_logger()
logger.info("Custom message")
logger.debug("Debug information")
```

Returns the root logger (default: `d3tools`).

### set_level()

Change log level dynamically:

```python
log_manager.set_level('DEBUG')  # More verbose
log_manager.set_level('WARNING')  # Less verbose
log_manager.set_level(logging.INFO)  # Using constant
```

Useful for temporarily increasing verbosity during troubleshooting.

### close()

Clean up resources:

```python
log_manager.close()
```

**Notes:**
- Called automatically via `atexit` handler
- Idempotent - safe to call multiple times
- Closes file handlers and flushes buffers

## Log Formats

### Detailed Format

```
2024-01-15 10:30:45 - d3tools.logging - INFO - Starting workflow: my_workflow
2024-01-15 10:30:46 - door.ERA5 - DEBUG - Fetching data from API
2024-01-15 10:35:12 - dam.reproject - INFO - Reprojecting to EPSG:4326
2024-01-15 10:35:15 - dryes.SPI - INFO - Calculating SPI-3
2024-01-15 10:40:30 - d3tools.logging - INFO - Workflow my_workflow completed in 9m 45s
```

**Format string:**
```python
'%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

**Best for:**
- Production logging
- Debugging module interactions
- Understanding execution flow

### Simple Format

```
INFO - Starting workflow: my_workflow
DEBUG - Fetching data from API
INFO - Reprojecting to EPSG:4326
INFO - Calculating SPI-3
INFO - Workflow my_workflow completed in 9m 45s
```

**Format string:**
```python
'%(levelname)s - %(message)s'
```

**Best for:**
- Console output
- Quick status updates
- Development

### Minimal Format

```
Starting workflow: my_workflow
Fetching data from API
Reprojecting to EPSG:4326
Calculating SPI-3
Workflow my_workflow completed in 9m 45s
```

**Format string:**
```python
'%(message)s'
```

**Best for:**
- Clean console output
- User-facing messages
- Scripts with minimal logging needs

## Duration Formatting

Execution times are automatically formatted in human-readable form:

| Duration | Formatted |
|----------|-----------|
| 0.5s | `0.5s` |
| 45s | `45.0s` |
| 90s | `1m 30s` |
| 3723s | `1h 2m 3s` |
| 7265s | `2h 1m 5s` |

## Integration with WorkflowDefinition

The WorkflowDefinition.run() method automatically creates and uses WorkflowLogManager:

```python
def run(self, start, end):
    # Create log manager from config
    if self.workflow_log:
        log_manager = WorkflowLogManager.from_dict(self.workflow_log)
    else:
        log_manager = None
    
    # Track workflow execution
    if log_manager:
        with log_manager.workflow_execution(self.name):
            self._run_sections(...)
    else:
        self._run_sections(...)
```

You don't need to interact with WorkflowLogManager directly - it works automatically!

## Advanced Use Cases

### Custom Logger Hierarchy

Create separate logger hierarchy:

```python
log_manager = WorkflowLogManager(
    log_file='logs/custom.log',
    logger_name='my_app'
)

# Gets logger under 'my_app'
logger = logging.getLogger('my_app.module')
logger.info("Message appears in custom.log")
```

### Multiple Log Managers

Different workflows with different configurations:

```python
workflow1_log = WorkflowLogManager(
    log_file='logs/workflow1.log',
    level='DEBUG',
    logger_name='workflow1'
)

workflow2_log = WorkflowLogManager(
    log_file='logs/workflow2.log',
    level='INFO',
    logger_name='workflow2'
)
```

### Conditional Logging

Enable logging only in certain conditions:

```python
if os.getenv('ENABLE_LOGGING') == 'true':
    log_manager = WorkflowLogManager(
        log_file='logs/workflow.log'
    )
else:
    log_manager = None

if log_manager:
    with log_manager.workflow_execution('my_workflow'):
        # ... workflow code ...
        pass
else:
    # ... workflow code without logging ...
    pass
```

### Dynamic Log Level

Adjust based on environment:

```python
log_level = os.getenv('LOG_LEVEL', 'INFO')
log_manager = WorkflowLogManager(
    log_file='logs/workflow.log',
    level=log_level
)
```

## Error Handling

### Automatic Error Logging

Context managers automatically log errors:

```python
with log_manager.workflow_execution('my_workflow'):
    raise ValueError("Something went wrong")
```

**Logs:**

```
2024-01-15 10:00:00 - INFO - Starting workflow: my_workflow
2024-01-15 10:00:05 - ERROR - Workflow my_workflow failed after 5.0s
```

The exception is re-raised after logging.

### Manual Error Logging

```python
logger = log_manager.get_logger()

try:
    risky_operation()
except Exception as e:
    logger.error(f"Operation failed: {e}", exc_info=True)
    raise
```

With `exc_info=True`, full traceback is included.

## Thread Safety

WorkflowLogManager uses Python's logging module, which is thread-safe. However:

- File handlers are thread-safe by default
- Console handlers are thread-safe
- Be careful with shared log managers across threads

## Performance Considerations

### File I/O

- Logging has I/O overhead
- Use `INFO` or `WARNING` in production to reduce writes
- Avoid logging in tight loops at DEBUG level

### String Formatting

Use lazy formatting:

```python
# ✅ Good - formatted only if logged
logger.debug("Processing item %s with %d values", item, len(values))

# ❌ Bad - formatted even if not logged
logger.debug(f"Processing item {item} with {len(values)} values")
```

### Buffering

Python's logging is buffered. Force flush when needed:

```python
for handler in log_manager.get_logger().handlers:
    handler.flush()
```

## Best Practices

### 1. Use Hierarchical Logger Names

```python
# Module structure
logging.getLogger('door')
logging.getLogger('door.ERA5')
logging.getLogger('door.ERA5.temperature')

# All inherit from d3tools configuration
```

### 2. Log at Appropriate Levels

```python
logger.debug("Detailed state information")        # Development
logger.info("Important milestones")                # Production
logger.warning("Unexpected but handled situations") # Always
logger.error("Errors that need attention")         # Always
logger.critical("Critical failures")               # Always
```

### 3. Include Context in Messages

```python
# ✅ Good
logger.error(f"Failed to download {source} data for {date}: {error}")

# ❌ Bad
logger.error("Download failed")
```

### 4. Use Context Managers

```python
# ✅ Good
with log_manager.section_execution('download'):
    download_data()

# ❌ Bad
logger.info("Starting download")
download_data()
logger.info("Download complete")
```

### 5. Clean Up Resources

```python
# Automatic cleanup via atexit
log_manager = WorkflowLogManager(log_file='logs/workflow.log')

# Or explicit cleanup
try:
    with log_manager.workflow_execution('workflow'):
        # ... code ...
        pass
finally:
    log_manager.close()
```

## Troubleshooting

### Log File Not Created

Check:
1. File path is valid
2. Parent directory is writable
3. No permission errors
4. Log manager was instantiated

### No Console Output

Check:
1. `console=True` is set
2. Log level allows messages through
3. Messages are being logged

### Duplicate Messages

Multiple handlers might be attached:

```python
# Check handlers
logger = log_manager.get_logger()
print(f"Handlers: {logger.handlers}")

# Clear existing handlers before creating log manager
logger.handlers.clear()
```

### Placeholder Not Resolved

`{now:...}` placeholders are only resolved in `from_dict()`:

```python
# ✅ Resolves placeholder
config = {'file': 'logs/workflow_{now:%Y%m%d}.log'}
log_manager = WorkflowLogManager.from_dict(config)

# ❌ Doesn't resolve placeholder
log_manager = WorkflowLogManager(
    log_file='logs/workflow_{now:%Y%m%d}.log'
)
```

## Future Enhancements

Planned features (noted in docstrings):

- **Dataset Integration**: Pass Dataset objects for remote logging (S3, SFTP)
- **Structured Logging**: JSON log format for machine parsing
- **Log Aggregation**: Integration with centralised logging systems
- **Metrics**: Performance metrics collection

## See Also

- [Logging Overview](overview.md)
- [Logging Examples](examples.md)
- [Configuration Overview](../configuration/overview.md)
- [Python Logging Docs](https://docs.python.org/3/library/logging.html)
