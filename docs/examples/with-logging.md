# Workflow with Logging Example

This example demonstrates how to add comprehensive logging to track workflow execution, section timing, and debug issues.

## Basic Logging Configuration

```json
{
    "name": "logged_workflow",
    
    "workflow_log": {
        "file": "logs/workflow.log",
        "level": "INFO",
        "console": true
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 5
    },
    
    "datasets": {
        "output": {
            "type": "local",
            "path": "/tmp/output/",
            "filename": "data_%Y%m%d.txt"
        }
    }
}
```

## What This Configuration Does

- **Logs to file**: `logs/workflow.log`
- **Log level**: `INFO` (shows important milestones)
- **Console output**: `true` (also displays in terminal)
- **Automatic tracking**: Workflow and section execution times

## Running the Workflow

```python
from datetime import datetime
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load('logged_workflow.json')
workflow.run(start=datetime(2024, 1, 1), end=datetime(2024, 1, 5))
```

## Log Output

```
2024-01-15 10:00:00 - d3tools.logging - INFO - Starting workflow: logged_workflow
2024-01-15 10:00:05 - d3tools.logging - INFO - Workflow logged_workflow completed in 5.0s
```

## With Workflow Sections

Add sections to see detailed tracking:

```json
{
    "name": "logged_pipeline",
    
    "workflow_log": {
        "file": "logs/pipeline_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 3
    },
    
    "tags": {
        "data_root": "/tmp/data"
    },
    
    "datasets": {
        "raw_data": {
            "type": "local",
            "path": "{data_root}/raw/",
            "filename": "raw_%Y%m%d.tif"
        },
        "processed_data": {
            "type": "local",
            "path": "{data_root}/processed/",
            "filename": "processed_%Y%m%d.tif"
        },
        "final_output": {
            "type": "local",
            "path": "{data_root}/output/",
            "filename": "output_%Y%m%d.tif"
        }
    },
    
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.raw_data}"
    },
    
    "dam_workflow": {
        "input": "{DATASETS.raw_data}",
        "output": "{DATASETS.processed_data}"
    },
    
    "publish": {
        "source": "{DATASETS.processed_data}",
        "destination": "{DATASETS.final_output}"
    }
}
```

## Detailed Log Output

### File Log (detailed format)

```
2024-01-15 10:00:00 - d3tools.logging - INFO - Starting workflow: logged_pipeline
2024-01-15 10:00:01 - d3tools.logging - INFO - Executing section: door_downloader (engine: door)
2024-01-15 10:00:02 - door.ERA5 - INFO - Connecting to CDS API
2024-01-15 10:00:05 - door.ERA5 - INFO - Downloading ERA5 data for 2024-01-01
2024-01-15 10:02:30 - door.ERA5 - INFO - Download complete: raw_20240101.tif
2024-01-15 10:02:30 - d3tools.logging - INFO - Section door_downloader completed in 2m 29s
2024-01-15 10:02:31 - d3tools.logging - INFO - Executing section: dam_workflow (engine: dam)
2024-01-15 10:02:32 - dam.reproject - INFO - Reprojecting to EPSG:4326
2024-01-15 10:03:15 - dam.resample - INFO - Resampling to 0.1 degree resolution
2024-01-15 10:04:00 - d3tools.logging - INFO - Section dam_workflow completed in 1m 29s
2024-01-15 10:04:01 - d3tools.logging - INFO - Executing section: publish (engine: publish)
2024-01-15 10:04:05 - d3tools.logging - INFO - Section publish completed in 4.0s
2024-01-15 10:04:05 - d3tools.logging - INFO - Workflow logged_pipeline completed in 4m 5s
```

### Console Output (simple format)

```
INFO - Starting workflow: logged_pipeline
INFO - Executing section: door_downloader (engine: door)
INFO - Connecting to CDS API
INFO - Downloading ERA5 data for 2024-01-01
INFO - Download complete: raw_20240101.tif
INFO - Section door_downloader completed in 2m 29s
INFO - Executing section: dam_workflow (engine: dam)
INFO - Reprojecting to EPSG:4326
INFO - Resampling to 0.1 degree resolution
INFO - Section dam_workflow completed in 1m 29s
INFO - Executing section: publish (engine: publish)
INFO - Section publish completed in 4.0s
INFO - Workflow logged_pipeline completed in 4m 5s
```

## Development Configuration

For active development with maximum detail:

```json
{
    "name": "development_workflow",
    
    "workflow_log": {
        "file": "logs/dev_{now:%Y%m%d_%H%M%S}.log",
        "level": "DEBUG",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
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

**Features:**
- **Unique log file** per run with timestamp
- **DEBUG level** shows all details
- **Console output** for immediate feedback
- **Detailed file format** for subsequent analysis
- **Simple console format** for readability

**Debug output includes:**
```
DEBUG - Initializing ERA5 downloader
DEBUG - API endpoint: https://cds.climate.copernicus.eu/api/v2
DEBUG - Request parameters: {'product_type': 'reanalysis', ...}
DEBUG - Submitting retrieval request
DEBUG - Request ID: 12345
DEBUG - Polling request status...
DEBUG - Request status: queued
DEBUG - Request status: running
DEBUG - Download URL received
DEBUG - Downloading file: 512 MB
DEBUG - Download progress: 10%
DEBUG - Download progress: 20%
...
```

## Production Configuration

For production deployment with essential logging:

```json
{
    "name": "production_workflow",
    
    "workflow_log": {
        "file": "/var/log/workflows/{name}/{now:%Y%m%d}.log",
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
    },
    
    "dryes_calculator": {
        "input": "{DATASETS.processed_data}",
        "output": "{DATASETS.indices}"
    }
}
```

**Features:**
- **Standard log location** in `/var/log/`
- **Daily log files** for organization
- **INFO level** for essential information
- **No console output** (runs in background)
- **Detailed format** for analysis

## Troubleshooting Configuration

When something goes wrong:

```json
{
    "name": "troubleshooting_workflow",
    
    "workflow_log": {
        "file": "logs/debug_{now:%Y%m%d_%H%M%S}.log",
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

**Features:**
- **Unique log file** for each debugging session
- **DEBUG level** everywhere
- **Detailed format** on both file and console
- **Full tracebacks** for errors

## Custom Module Logging

Add your own log messages to modules:

### In DOOR Module

```python
import logging

logger = logging.getLogger('door.CustomDownloader')

def download_data():
    logger.info("Starting custom download")
    logger.debug(f"Download parameters: {params}")
    
    try:
        # Download logic
        logger.info("Download successful")
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
        raise
```

### In DAM Module

```python
import logging

logger = logging.getLogger('dam.CustomProcessor')

def process_data(input_file, output_file):
    logger.info(f"Processing {input_file}")
    logger.debug(f"Output will be: {output_file}")
    
    try:
        # Processing logic
        logger.info("Processing complete")
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        raise
```

These messages automatically appear in the workflow log!

## Log Analysis

### Extract Timing Information

```bash
# Show all section timings
grep "completed in" logs/workflow_20240115.log

# Find slowest section
grep "Section.*completed in" logs/workflow_20240115.log | sort -t' ' -k6 -r | head -1

# Calculate total workflow time
grep "Workflow.*completed in" logs/workflow_20240115.log
```

### Monitor Active Workflow

```bash
# Watch log in real-time
tail -f logs/workflow_20240115.log

# Watch only important messages
tail -f logs/workflow_20240115.log | grep -E "(INFO|WARNING|ERROR)"
```

### Find Errors

```bash
# Show all errors
grep "ERROR" logs/workflow_20240115.log

# Show errors with context
grep -B5 -A5 "ERROR" logs/workflow_20240115.log
```

## Environment-Specific Setup

Use environment variables to control logging:

```json
{
    "name": "adaptive_logging",
    
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d}.log",
        "level": "{ENV.LOG_LEVEL, default='INFO'}",
        "console": "{ENV.LOG_CONSOLE, default='false'}",
        "format_file": "detailed"
    }
}
```

**Usage:**

```bash
# Development
export LOG_LEVEL=DEBUG
export LOG_CONSOLE=true
python run_workflow.py workflow.json

# Production
export LOG_LEVEL=INFO
export LOG_CONSOLE=false
python run_workflow.py workflow.json

# Troubleshooting
export LOG_LEVEL=DEBUG
export LOG_CONSOLE=true
python run_workflow.py workflow.json
```

## Best Practices

### 1. Always Enable Logging in Production

```json
{
    "workflow_log": {
        "file": "/var/log/workflows/{name}_{now:%Y%m%d}.log",
        "level": "INFO"
    }
}
```

### 2. Use DEBUG Level Temporarily

Enable DEBUG only when troubleshooting, then return to INFO:

```bash
# Temporary debug
{"level": "DEBUG"}  # During troubleshooting

# Normal operation
{"level": "INFO"}   # Regular production use
```

### 3. Organise Logs by Date

```json
{
    "file": "logs/{now:%Y/%m}/workflow_{now:%d}.log"
}
```

Creates structure: `logs/2024/01/workflow_15.log`

### 4. Use Timestamps for Unique Runs

```json
{
    "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log"
}
```

Ensures each run has a unique log file.

### 5. Different Formats for File vs Console

```json
{
    "format_file": "detailed",     // Maximum detail in files
    "format_console": "simple"     // Clean console output
}
```

## Complete Example

Full-featured workflow with logging:

```json
{
    "name": "complete_logged_workflow",
    
    "tags": {
        "project": "drought_monitoring",
        "region": "mediterranean",
        "log_root": "{ENV.LOG_ROOT, default='logs'}",
        "data_root": "{ENV.DATA_ROOT, default='/data'}"
    },
    
    "workflow_log": {
        "file": "{log_root}/{project}/{region}/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "{ENV.LOG_LEVEL, default='INFO'}",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 30
    },
    
    "datasets": {
        "raw_data": {
            "type": "local",
            "path": "{data_root}/{project}/{region}/raw/%Y/",
            "filename": "ERA5_raw_%Y%m%d.tif"
        },
        "processed_data": {
            "type": "local",
            "path": "{data_root}/{project}/{region}/processed/%Y/",
            "filename": "ERA5_processed_%Y%m%d.tif"
        },
        "drought_indices": {
            "type": "local",
            "path": "{data_root}/{project}/{region}/indices/%Y/",
            "filename": "CDI_%Y%m%d.tif"
        }
    },
    
    "door_downloader": {
        "source": "ERA5",
        "product": "reanalysis-era5-single-levels",
        "destination": "{DATASETS.raw_data}",
        "bounds": [8, 38.5, 10, 41.5],
        "options": {
            "variables": ["2m_temperature", "total_precipitation"]
        }
    },
    
    "dam_workflow": {
        "input": "{DATASETS.raw_data}",
        "output": "{DATASETS.processed_data}",
        "operations": ["reproject", "resample", "clip"]
    },
    
    "dryes_calculator": {
        "indicator": "CDI",
        "input": "{DATASETS.processed_data}",
        "output": "{DATASETS.drought_indices}"
    }
}
```

## See Also

- [Logging Overview](../logging/overview.md)
- [WorkflowLogManager Guide](../logging/workflow-log-manager.md)
- [Logging Examples](../logging/examples.md)
- [Minimal Workflow](minimal-workflow.md)
