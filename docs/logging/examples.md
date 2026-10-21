# Logging Examples

Practical examples of logging configurations and usage patterns.

## Basic Examples

### 1. Simple File Logging

```json
{
    "name": "simple_workflow",
    "workflow_log": "logs/workflow.log",
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 1
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.output}"
    }
}
```

**Creates:** `logs/workflow.log` with INFO level messages

### 2. Console and File Logging

```json
{
    "name": "console_workflow",
    "workflow_log": {
        "file": "logs/workflow.log",
        "console": true
    },
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 1
    },
    "dam_workflow": {
        "input": "{DATASETS.input}",
        "output": "{DATASETS.output}"
    }
}
```

**Result:** Logs appear in both `logs/workflow.log` and terminal

### 3. Debug Logging for Troubleshooting

```json
{
    "name": "debug_workflow",
    "workflow_log": {
        "file": "logs/debug.log",
        "level": "DEBUG",
        "console": true,
        "format_file": "detailed",
        "format_console": "detailed"
    },
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 1
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.output}"
    }
}
```

**Output:**
```
2024-01-15 10:30:45 - d3tools.logging - INFO - Starting workflow: debug_workflow
2024-01-15 10:30:46 - door.ERA5 - DEBUG - Connecting to CDS API
2024-01-15 10:30:47 - door.ERA5 - DEBUG - Request parameters: {'variable': 'temperature', ...}
2024-01-15 10:30:48 - door.ERA5 - DEBUG - Submitting retrieval request
...
```

## Timestamp-Based Log Files

### 4. Daily Log Files

```json
{
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d}.log"
    }
}
```

**Creates:** `logs/workflow_20240115.log` (one file per day)

### 5. Unique Log per Run

```json
{
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log"
    }
}
```

**Creates:** `logs/workflow_20240115_103045.log` (unique file per run)

### 6. Monthly Log Organisation

```json
{
    "workflow_log": {
        "file": "logs/{now:%Y/%m}/workflow_{now:%d}.log"
    }
}
```

**Creates:** `logs/2024/01/workflow_15.log` (organised by year/month)

### 7. Workflow-Specific Logs

```json
{
    "name": "era5_download",
    "workflow_log": {
        "file": "logs/{name}/{now:%Y%m%d}.log"
    }
}
```

**Creates:** `logs/era5_download/20240115.log` (organised by workflow)

## Environment-Specific Configurations

### 8. Development Environment

```json
{
    "workflow_log": {
        "file": "logs/dev/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "DEBUG",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
    }
}
```

**Features:**
- DEBUG level for maximum detail
- Console output for immediate feedback
- Timestamped files to track each run
- Simple console format for readability

### 9. Production Environment

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

**Features:**
- INFO level for essential information only
- No console output (runs in background)
- Standard system log location
- Daily log files
- Detailed format for post-mortem analysis

### 10. Testing Environment

```json
{
    "workflow_log": {
        "file": "logs/test/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "WARNING",
        "console": true,
        "format_console": "minimal"
    }
}
```

**Features:**
- WARNING level to reduce noise during tests
- Console output for test feedback
- Minimal console format

## Use Case Examples

### 11. Long-Running Production Workflow

```json
{
    "name": "daily_processing",
    "tags": {
        "log_root": "{ENV.LOG_ROOT, default='/var/log/workflows'}",
        "year": "{now:%Y}",
        "month": "{now:%m}"
    },
    "workflow_log": {
        "file": "{log_root}/daily_processing/{year}/{month}/run_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": false,
        "format_file": "detailed"
    },
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 365
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.raw_data}"
    },
    "dam_workflow": {
        "input": "{DATASETS.raw_data}",
        "output": "{DATASETS.processed_data}"
    }
}
```

**Log structure:**
```
/var/log/workflows/daily_processing/
├── 2024/
│   ├── 01/
│   │   ├── run_20240115_000000.log
│   │   ├── run_20240116_000000.log
│   │   └── ...
│   ├── 02/
│   │   └── ...
```

### 12. Multi-Region Workflow

```json
{
    "name": "multi_region_download",
    "tags": {
        "regions": ["europe", "africa", "asia"]
    },
    "workflow_log": {
        "file": "logs/multi_region/{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
    },
    "door_downloader": [
        {
            "source": "ERA5",
            "region": "europe",
            "destination": "{DATASETS.europe_data}"
        },
        {
            "source": "ERA5",
            "region": "africa",
            "destination": "{DATASETS.africa_data}"
        },
        {
            "source": "ERA5",
            "region": "asia",
            "destination": "{DATASETS.asia_data}"
        }
    ]
}
```

**Log output:**
```
2024-01-15 10:00:00 - INFO - Starting workflow: multi_region_download
2024-01-15 10:00:01 - INFO - Executing section: door_downloader (engine: door)
2024-01-15 10:00:02 - INFO - Parallel execution of 3 items
2024-01-15 10:05:23 - INFO - Section door_downloader completed in 5m 22s
2024-01-15 10:05:23 - INFO - Workflow multi_region_download completed in 5m 23s
```

### 13. Debugging Failed Workflow

```json
{
    "name": "debugging_workflow",
    "workflow_log": {
        "file": "logs/debug/detailed_{now:%Y%m%d_%H%M%S}.log",
        "level": "DEBUG",
        "console": true,
        "format_file": "detailed",
        "format_console": "detailed"
    },
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 1
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.output}"
    }
}
```

**Detailed output shows exactly what's happening:**
```
2024-01-15 10:00:00 - d3tools.logging - INFO - Starting workflow: debugging_workflow
2024-01-15 10:00:01 - d3tools.config - DEBUG - Loading workflow configuration
2024-01-15 10:00:01 - d3tools.timestepping - DEBUG - Creating FixedLenTimestep: length=1D, num_steps=1
2024-01-15 10:00:02 - d3tools.logging - INFO - Executing section: door_downloader (engine: door)
2024-01-15 10:00:02 - door - DEBUG - Initializing ERA5 downloader
2024-01-15 10:00:03 - door.ERA5 - DEBUG - Connecting to CDS API
2024-01-15 10:00:04 - door.ERA5 - DEBUG - API key: ****** (masked)
2024-01-15 10:00:05 - door.ERA5 - DEBUG - Request parameters: {...}
2024-01-15 10:00:06 - door.ERA5 - ERROR - API connection failed: HTTP 401 Unauthorized
2024-01-15 10:00:06 - door.ERA5 - ERROR - Traceback: ...
2024-01-15 10:00:06 - d3tools.logging - ERROR - Section door_downloader failed after 4.5s
2024-01-15 10:00:06 - d3tools.logging - ERROR - Workflow debugging_workflow failed after 6.0s
```

### 14. Monitoring Workflow Performance

```json
{
    "name": "performance_monitored",
    "workflow_log": {
        "file": "logs/performance/{now:%Y%m%d}.log",
        "level": "INFO",
        "console": false
    },
    "time": {
        "type": "fixed_len",
        "length": "1H",
        "num_steps": 24
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.raw_data}"
    },
    "dam_workflow": {
        "input": "{DATASETS.raw_data}",
        "output": "{DATASETS.processed_data}"
    },
    "dryes_calculator": {
        "input": "{DATASETS.processed_data}",
        "output": "{DATASETS.indices}"
    }
}
```

**Performance tracking in logs:**
```
2024-01-15 10:00:00 - INFO - Starting workflow: performance_monitored
2024-01-15 10:00:01 - INFO - Executing section: door_downloader (engine: door)
2024-01-15 10:05:23 - INFO - Section door_downloader completed in 5m 22s
2024-01-15 10:05:23 - INFO - Executing section: dam_workflow (engine: dam)
2024-01-15 10:08:15 - INFO - Section dam_workflow completed in 2m 52s
2024-01-15 10:08:15 - INFO - Executing section: dryes_calculator (engine: dryes)
2024-01-15 10:12:40 - INFO - Section dryes_calculator completed in 4m 25s
2024-01-15 10:12:40 - INFO - Workflow performance_monitored completed in 12m 40s
```

Analyse performance with:
```bash
grep "completed in" logs/performance/20240115.log
```

## Programmatic Usage

### 15. Custom Logging in Python

```python
from d3tools import WorkflowDefinition
from d3tools.logging import WorkflowLogManager
import logging

# Create workflow
workflow = WorkflowDefinition.load('workflow.json')

# Get logger
logger = logging.getLogger('d3tools.custom')

# Add custom logging
logger.info("Starting custom preprocessing")

try:
    # Custom pre-processing
    logger.debug("Preprocessing step 1")
    # ...
    logger.debug("Preprocessing step 2")
    # ...
    
    # Run workflow
    workflow.run(start=..., end=...)
    
    # Custom post-processing
    logger.info("Starting custom postprocessing")
    # ...
    
    logger.info("Workflow completed successfully")
    
except Exception as e:
    logger.error(f"Workflow failed: {e}", exc_info=True)
    raise
```

### 16. Standalone WorkflowLogManager

```python
from d3tools.logging import WorkflowLogManager
from datetime import datetime
import logging

# Create log manager
log_manager = WorkflowLogManager(
    log_file=f"logs/custom_{datetime.now():%Y%m%d_%H%M%S}.log",
    level='INFO',
    console=True,
    format_file='detailed',
    format_console='simple'
)

# Get logger for your module
logger = logging.getLogger('my_module')

# Track workflow
with log_manager.workflow_execution('my_custom_workflow'):
    logger.info("Starting download phase")
    
    with log_manager.section_execution('download', engine='custom'):
        logger.debug("Downloading file 1")
        # ... download logic ...
        logger.debug("Downloading file 2")
        # ... download logic ...
    
    logger.info("Starting processing phase")
    
    with log_manager.section_execution('process', engine='custom'):
        logger.debug("Processing file 1")
        # ... processing logic ...
        logger.debug("Processing file 2")
        # ... processing logic ...

# Cleanup
log_manager.close()
```

### 17. Dynamic Log Level

```python
import os
import logging
from d3tools import WorkflowDefinition

# Get log level from environment
log_level = os.getenv('LOG_LEVEL', 'INFO')

# Update workflow config
workflow_config = {
    'name': 'dynamic_logging',
    'workflow_log': {
        'file': 'logs/workflow.log',
        'level': log_level,
        'console': True
    },
    'time': {'type': 'fixed_len', 'length': '1D', 'num_steps': 1},
    'door_downloader': {'source': 'ERA5', 'destination': '{DATASETS.output}'}
}

# Save and load
import json
with open('workflow_dynamic.json', 'w') as f:
    json.dump(workflow_config, f)

workflow = WorkflowDefinition.load('workflow_dynamic.json')
workflow.run(...)
```

**Usage:**
```bash
# Normal logging
python run_workflow.py

# Debug logging
LOG_LEVEL=DEBUG python run_workflow.py

# Minimal logging
LOG_LEVEL=WARNING python run_workflow.py
```

## Log Analysis

### 18. Extracting Timing Information

```bash
# Extract all timing information
grep "completed in" logs/workflow_20240115.log

# Output:
# Section door_downloader completed in 5m 22s
# Section dam_workflow completed in 2m 52s
# Section dryes_calculator completed in 4m 25s
# Workflow my_workflow completed in 12m 40s

# Find slowest section
grep "Section.*completed in" logs/workflow_20240115.log | sort -t' ' -k6 -r | head -1
```

### 19. Finding Errors

```bash
# Find all errors
grep "ERROR" logs/workflow_20240115.log

# Find errors with context (5 lines before and after)
grep -B5 -A5 "ERROR" logs/workflow_20240115.log

# Count errors by type
grep "ERROR" logs/workflow_20240115.log | cut -d'-' -f4 | sort | uniq -c
```

### 20. Monitor Active Workflows

```bash
# Watch log file in real-time
tail -f logs/workflow_20240115.log

# Watch only INFO and above
tail -f logs/workflow_20240115.log | grep -E "(INFO|WARNING|ERROR|CRITICAL)"

# Watch only errors
tail -f logs/workflow_20240115.log | grep "ERROR"
```

## Integration with External Tools

### 21. Systemd Service Logging

```ini
# /etc/systemd/system/d3tools-workflow.service
[Unit]
Description=D3Tools Workflow
After=network.target

[Service]
Type=simple
User=workflow
WorkingDirectory=/opt/workflows
ExecStart=/usr/bin/python3 /opt/workflows/run_workflow.py workflow.json
StandardOutput=journal
StandardError=journal
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Workflow config:
```json
{
    "workflow_log": {
        "file": "/var/log/d3tools/workflow.log",
        "level": "INFO",
        "console": false
    }
}
```

View logs:
```bash
journalctl -u d3tools-workflow -f
```

### 22. Docker Container Logging

```yaml
# docker-compose.yml
version: '3'
services:
  workflow:
    image: d3tools:latest
    volumes:
      - ./logs:/app/logs
      - ./config:/app/config
    environment:
      - LOG_LEVEL=INFO
    command: python run_workflow.py config/workflow.json
```

Workflow config:
```json
{
    "workflow_log": {
        "file": "/app/logs/workflow_{now:%Y%m%d}.log",
        "level": "{ENV.LOG_LEVEL, default='INFO'}",
        "console": true
    }
}
```

View logs:
```bash
docker-compose logs -f workflow
```

## Best Practices Summary

### Development

```json
{
    "workflow_log": {
        "file": "logs/dev_{now:%Y%m%d_%H%M%S}.log",
        "level": "DEBUG",
        "console": true,
        "format_console": "simple"
    }
}
```

### Production

```json
{
    "workflow_log": {
        "file": "/var/log/workflows/{name}/{now:%Y%m%d}.log",
        "level": "INFO",
        "console": false,
        "format_file": "detailed"
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

### Performance Monitoring

```json
{
    "workflow_log": {
        "file": "logs/perf/{now:%Y/%m}/workflow_{now:%d}.log",
        "level": "INFO",
        "console": false
    }
}
```

## See Also

- [Logging Overview](overview.md)
- [WorkflowLogManager Guide](workflow-log-manager.md)
- [Configuration Overview](../configuration/overview.md)
