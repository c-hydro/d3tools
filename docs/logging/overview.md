# Logging Overview

d3tools provides a hierarchical logging system that enables unified logging across the entire workflow ecosystem (d3tools, DOOR, DAM, and DRYES).

## Why Logging?

Logging helps you:
- **Track execution**: See what your workflow is doing
- **Debug issues**: Understand where and why failures occur
- **Monitor performance**: Measure section execution times
- **Audit runs**: Keep records of workflow executions
- **Production monitoring**: Track workflows in production environments

## Hierarchical Logging

d3tools uses Python's built-in logging module with a hierarchical structure:

```
d3tools (root logger)
├── door (inherits from d3tools)
├── dam (inherits from d3tools)
└── dryes (inherits from d3tools)
```

When you configure logging in d3tools, all child loggers (door, dam, dryes) automatically inherit the configuration!

## Quick Start

### Minimal Configuration

Log to a file:

```json
{
    "name": "my_workflow",
    "workflow_log": "logs/workflow.log",
    "time": { ... },
    "door_downloader": { ... }
}
```

### Full Configuration

```json
{
    "name": "my_workflow",
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
    },
    "time": { ... },
    "door_downloader": { ... }
}
```

## Log Levels

Standard Python log levels (from most to least verbose):

| Level | Value | Usage |
|-------|-------|-------|
| DEBUG | 10 | Detailed information for diagnosing problems |
| INFO | 20 | Confirmation that things are working as expected |
| WARNING | 30 | Something unexpected happened, but work continues |
| ERROR | 40 | A serious problem occurred, some functionality failed |
| CRITICAL | 50 | A very serious error, programme may not continue |

**Default**: `INFO`

### Choosing a Log Level

- **Development**: Use `DEBUG` to see everything
- **Production**: Use `INFO` for normal operation
- **Troubleshooting**: Temporarily set to `DEBUG`
- **Critical systems**: Use `WARNING` or `ERROR` to reduce noise

## Log Formats

d3tools provides three predefined formats:

### Detailed (default for files)

```
2024-01-15 10:30:45 - d3tools - INFO - Starting workflow: my_workflow
2024-01-15 10:30:46 - door.ERA5 - DEBUG - Downloading data for 2024-01-15
2024-01-15 10:35:12 - dam.reproject - INFO - Reprojecting to EPSG:4326
```

**Format**: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### Simple (default for console)

```
INFO - Starting workflow: my_workflow
DEBUG - Downloading data for 2024-01-15
INFO - Reprojecting to EPSG:4326
```

**Format**: `%(levelname)s - %(message)s`

### Minimal

```
Starting workflow: my_workflow
Downloading data for 2024-01-15
Reprojecting to EPSG:4326
```

**Format**: `%(message)s`

## What Gets Logged?

### Automatic Workflow Logging

When `workflow_log` is configured, d3tools automatically logs:

```
2024-01-15 10:00:00 - INFO - Starting workflow: drought_monitoring
2024-01-15 10:00:01 - INFO - Executing section: door_downloader (engine: door)
2024-01-15 10:05:23 - INFO - Section door_downloader completed in 5m 22s
2024-01-15 10:05:23 - INFO - Executing section: dam_workflow (engine: dam)
2024-01-15 10:08:15 - INFO - Section dam_workflow completed in 2m 52s
2024-01-15 10:08:15 - INFO - Executing section: dryes_calculator (engine: dryes)
2024-01-15 10:12:40 - INFO - Section dryes_calculator completed in 4m 25s
2024-01-15 10:12:40 - INFO - Workflow drought_monitoring completed in 12m 40s
```

### Module-Specific Logging

Modules (DOOR, DAM, DRYES) can add their own log messages:

```python
import logging

logger = logging.getLogger('door.ERA5')
logger.info("Downloading ERA5 data")
logger.debug(f"Request parameters: {params}")
logger.warning("Rate limit approaching, slowing down")
```

These messages automatically appear in the workflow log!

## Configuration Options

### String Configuration

Simplest form - just specify the log file:

```json
{
    "workflow_log": "logs/workflow.log"
}
```

Equivalent to:
```json
{
    "workflow_log": {
        "file": "logs/workflow.log",
        "level": "INFO",
        "console": false,
        "format_file": "detailed"
    }
}
```

### Dict Configuration

Full control over logging:

```json
{
    "workflow_log": {
        "file": "logs/workflow.log",
        "level": "DEBUG",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple",
        "logger_name": "d3tools"
    }
}
```

**Parameters:**

- **`file`** (string, required): Log file path
  - Absolute or relative path
  - Directories are created automatically
  - Supports `{now:...}` placeholders for timestamps

- **`level`** (string, optional): Log level
  - Valid: `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`, `"CRITICAL"`
  - Default: `"INFO"`

- **`console`** (boolean, optional): Also log to console
  - `true`: Show logs in terminal
  - `false`: File only
  - Default: `false`

- **`format_file`** (string, optional): Format for file output
  - Valid: `"detailed"`, `"simple"`, `"minimal"`
  - Default: `"detailed"`

- **`format_console`** (string, optional): Format for console output
  - Valid: `"detailed"`, `"simple"`, `"minimal"`
  - Default: `"simple"`

- **`logger_name`** (string, optional): Logger name
  - Default: `"d3tools"`
  - Advanced use only

### Disable Logging

Set to `null` or omit the key:

```json
{
    "workflow_log": null
}
```

Or:

```json
{
    "name": "my_workflow"
    // workflow_log not defined
}
```

## Runtime Placeholders

Use `{now:...}` for timestamp-based log files:

```json
{
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log"
    }
}
```

**When run on 2024-01-15 at 10:30:45, creates:**
```
logs/workflow_20240115_103045.log
```

**Common patterns:**

```json
// Daily logs
"file": "logs/workflow_{now:%Y%m%d}.log"

// Monthly logs
"file": "logs/{now:%Y%m}/workflow.log"

// Timestamped logs
"file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log"

// With workflow name
"file": "logs/{name}_{now:%Y%m%d}.log"
```

**Note**: `{now:...}` is resolved at **runtime** in `WorkflowDefinition.run()`, not during configuration parsing. This ensures accurate timestamps.

## Using Logging in Your Code

### In DOOR/DAM/DRYES Modules

```python
import logging

# Get a logger for your module
logger = logging.getLogger('door.MyDownloader')

def download_data():
    logger.info("Starting download")
    logger.debug(f"Parameters: {params}")
    
    try:
        # Download logic
        logger.info("Download successful")
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
```

### Logger Naming Convention

Use hierarchical names:

```python
# DOOR module
logging.getLogger('door')
logging.getLogger('door.ERA5')
logging.getLogger('door.CHIRPS')

# DAM module
logging.getLogger('dam')
logging.getLogger('dam.reproject')
logging.getLogger('dam.aggregate')

# DRYES module
logging.getLogger('dryes')
logging.getLogger('dryes.SPI')
logging.getLogger('dryes.CDI')
```

All inherit from `d3tools` root logger!

### Log Levels in Code

```python
logger.debug("Detailed debugging information")
logger.info("General information")
logger.warning("Something unexpected but not critical")
logger.error("An error occurred")
logger.critical("Critical failure")
```

### Exception Logging

Include tracebacks with `exc_info=True`:

```python
try:
    risky_operation()
except Exception as e:
    logger.error(f"Operation failed: {e}", exc_info=True)
```

## Common Patterns

### Development Workflow

```json
{
    "workflow_log": {
        "file": "logs/dev_{now:%Y%m%d}.log",
        "level": "DEBUG",
        "console": true,
        "format_console": "simple"
    }
}
```

### Production Workflow

```json
{
    "workflow_log": {
        "file": "/var/log/workflows/{name}_{now:%Y%m%d}.log",
        "level": "INFO",
        "console": false,
        "format_file": "detailed"
    }
}
```

### Automated Monitoring

```json
{
    "workflow_log": {
        "file": "/mnt/shared/logs/{name}/{now:%Y/%m/%d}/run_{now:%H%M%S}.log",
        "level": "INFO",
        "console": false
    }
}
```

### Troubleshooting

```json
{
    "workflow_log": {
        "file": "logs/debug_{now:%Y%m%d_%H%M%S}.log",
        "level": "DEBUG",
        "console": true,
        "format_file": "detailed",
        "format_console": "detailed"
    }
}
```

## Log File Management

### Log Rotation

d3tools doesn't automatically rotate logs. Use external tools:

**Linux (logrotate):**

```bash
# /etc/logrotate.d/d3tools
/var/log/workflows/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
}
```

**Python (RotatingFileHandler):**

For programmatic control, you can extend WorkflowLogManager or manage rotation separately.

### Clean Old Logs

```bash
# Delete logs older than 30 days
find /var/log/workflows/ -name "*.log" -mtime +30 -delete

# Compress logs older than 7 days
find /var/log/workflows/ -name "*.log" -mtime +7 -exec gzip {} \;
```

## Performance Considerations

### Log Level Impact

- **DEBUG**: High overhead, lots of I/O
- **INFO**: Moderate overhead, good for production
- **WARNING+**: Minimal overhead

### File vs Console

- **File only**: Faster, logs don't slow down terminal
- **Console**: Convenient for development, slower

### Buffering

Python's logging is buffered by default. Force flush with:

```python
import logging
logging.getLogger().handlers[0].flush()
```

## Troubleshooting

### No Log File Created

1. Check file path is writable
2. Check directory exists (it should be auto-created)
3. Verify `workflow_log` is not `null`
4. Check for permission errors

### Logs Not Appearing

1. Check log level - DEBUG messages won't show at INFO level
2. Verify logger name matches hierarchy
3. Check if logger is being created before WorkflowLogManager

### Duplicate Log Messages

If using custom loggers, ensure you're not adding duplicate handlers:

```python
logger = logging.getLogger('door.MyModule')
if not logger.handlers:  # Only add if no handlers exist
    handler = logging.StreamHandler()
    logger.addHandler(handler)
```

### Console Output But No File

Check the `console` parameter is not the only one set:

```json
{
    "workflow_log": {
        "file": "logs/workflow.log",  // Must specify file!
        "console": true
    }
}
```

## Next Steps

- [WorkflowLogManager Guide](workflow-log-manager.md) - Detailed configuration
- [Logging Examples](examples.md) - Common use cases
- [Configuration Overview](../configuration/overview.md) - Full workflow configuration

## See Also

- [Python Logging Documentation](https://docs.python.org/3/library/logging.html)
- [Python Logging Cookbook](https://docs.python.org/3/howto/logging-cookbook.html)
