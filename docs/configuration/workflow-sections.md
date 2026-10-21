# Configuration Workflow Sections

Workflow sections define the ordered stages of your workflow. Each section represents a distinct operation (download, process, calculate, publish) that is executed in sequence.

## Section Aliases

Instead of explicitly defining `workflow_sections`, use convenient aliases:

| Alias | Purpose | Module | Execution |
|-------|---------|--------|-----------|
| `door_downloader` | Download data from sources | DOOR | Sequential or Parallel |
| `dam_workflow` | Process/transform data | DAM | Sequential or Parallel |
| `dryes_calculator` | Calculate drought indices | DRYES | Sequential or Parallel |
| `download` | Generic download | Custom | Sequential or Parallel |
| `process` | Generic processing | Custom | Sequential or Parallel |
| `calculate` | Generic calculation | Custom | Sequential or Parallel |
| `publish` | Publish/upload results | Custom | Sequential or Parallel |

## Basic Usage

### Single Section

```json
{
    "name": "download_workflow",
    "time": { "type": "fixed_len", "length": "1D", "num_steps": 30 },
    
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.output}"
    }
}
```

### Multiple Sections

Sections execute in the order they appear

 in the JSON:

```json
{
    "name": "full_pipeline",
    "time": { "type": "fixed_len", "length": "1D", "num_steps": 30 },
    
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
    },
    
    "publish": {
        "source": "{DATASETS.indices}",
        "destination": "{DATASETS.remote_storage}"
    }
}
```

## Parallel vs Sequential

### Sequential (Single Item)

Process one item at a time:

```json
{
    "door_downloader": {
        "source": "ERA5",
        "variable": "temperature",
        "destination": "{DATASETS.output}"
    }
}
```

### Parallel (Multiple Items)

Process multiple items in parallel using an array:

```json
{
    "door_downloader": [
        {
            "source": "ERA5",
            "variable": "temperature",
            "destination": "{DATASETS.temp_data}"
        },
        {
            "source": "ERA5",
            "variable": "precipitation",
            "destination": "{DATASETS.prec_data}"
        },
        {
            "source": "MODIS",
            "product": "MOD13",
            "destination": "{DATASETS.modis_data}"
        }
    ]
}
```

All items in the array are executed in parallel.

## DOOR Downloader Section

Downloads data from external sources (ERA5, CHIRPS, MODIS, etc.).

### Basic Structure

```json
{
    "door_downloader": {
        "source": "ERA5",
        "product": "reanalysis-era5-single-levels",
        "destination": "{DATASETS.output}",
        "bounds": [lon_min, lat_min, lon_max, lat_max],
        "options": {
            "variables": ["2m_temperature"],
            "agg_method": "mean"
        }
    }
}
```

### Common Parameters

- `source`: Data source (ERA5, CHIRPS, MODIS, GLOFAS, etc.)
- `product`: Specific product/dataset
- `destination`: Dataset object for output
- `bounds`: Spatial extent [lon_min, lat_min, lon_max, lat_max]
- `options`: Source-specific options

### Multiple Downloads

```json
{
    "door_downloader": [
        {
            "source": "ERA5",
            "variables": ["temperature", "precipitation"],
            "destination": "{DATASETS.era5_data}"
        },
        {
            "source": "CHIRPS",
            "destination": "{DATASETS.chirps_data}"
        }
    ]
}
```

See [DOOR documentation](../integration/door.md) for source-specific options.

## DAM Workflow Section

Processes and transforms data.

### Basic Structure

```json
{
    "dam_workflow": {
        "input": "{DATASETS.raw_data}",
        "output": "{DATASETS.processed_data}",
        "operations": ["reproject", "resample", "clip"]
    }
}
```

### Common Parameters

- `input`: Input dataset
- `output`: Output dataset
- `operations`: List of processing operations
- `parameters`: Operation-specific parameters

### Multiple Processing Pipelines

```json
{
    "dam_workflow": [
        {
            "name": "process_temperature",
            "input": "{DATASETS.raw_temp}",
            "output": "{DATASETS.processed_temp}",
            "operations": ["reproject", "resample"]
        },
        {
            "name": "process_precipitation",
            "input": "{DATASETS.raw_prec}",
            "output": "{DATASETS.processed_prec}",
            "operations": ["reproject", "resample", "aggregate"]
        }
    ]
}
```

See [DAM documentation](../integration/dam.md) for operation details.

## DRYES Calculator Section

Calculates drought indices and indicators.

### Basic Structure

```json
{
    "dryes_calculator": {
        "indicator": "SPI",
        "input": "{DATASETS.precipitation}",
        "output": "{DATASETS.spi_index}",
        "parameters": {
            "timescale": 3
        }
    }
}
```

### Common Parameters

- `indicator`: Indicator type (SPI, SPEI, CDI, etc.)
- `input`: Input dataset(s)
- `output`: Output dataset
- `parameters`: Indicator-specific parameters

### Multiple Indicators

```json
{
    "dryes_calculator": [
        {
            "indicator": "SPI",
            "input": "{DATASETS.precipitation}",
            "output": "{DATASETS.spi}"
        },
        {
            "indicator": "SPEI",
            "temperature": "{DATASETS.temperature}",
            "precipitation": "{DATASETS.precipitation}",
            "output": "{DATASETS.spei}"
        },
        {
            "indicator": "CDI",
            "inputs": {
                "precipitation": "{DATASETS.precipitation}",
                "temperature": "{DATASETS.temperature}",
                "soil_moisture": "{DATASETS.soil_moisture}"
            },
            "output": "{DATASETS.cdi}"
        }
    ]
}
```

See [DRYES documentation](../integration/dryes.md) for indicator details.

## Custom Sections

Generic section types for custom workflows:

### Download

```json
{
    "download": {
        "url": "https://example.com/data.zip",
        "destination": "/data/downloads/",
        "extract": true
    }
}
```

### Process

```json
{
    "process": {
        "script": "custom_processing.py",
        "input": "{DATASETS.input}",
        "output": "{DATASETS.output}"
    }
}
```

### Calculate

```json
{
    "calculate": {
        "formula": "A + B",
        "inputs": {
            "A": "{DATASETS.dataset_a}",
            "B": "{DATASETS.dataset_b}"
        },
        "output": "{DATASETS.result}"
    }
}
```

### Publish

```json
{
    "publish": {
        "source": "{DATASETS.local_output}",
        "destination": "{DATASETS.s3_storage}",
        "format": "COG"
    }
}
```

## Explicit workflow_sections

For advanced use cases, define sections explicitly:

```json
{
    "workflow_sections": [
        {
            "name": "download",
            "parallel": false,
            "processes": [
                {
                    "engine": "door",
                    "config": { ... }
                }
            ]
        },
        {
            "name": "process",
            "parallel": true,
            "processes": [
                {"engine": "dam", "config": { ... }},
                {"engine": "dam", "config": { ... }}
            ]
        }
    ]
}
```

## Section Execution

### Order of Execution

Sections execute in the order they're defined in the JSON file:

```json
{
    "door_downloader": { ... },    // Executes first
    "dam_workflow": { ... },        // Executes second
    "dryes_calculator": { ... },    // Executes third
    "publish": { ... }              // Executes fourth
}
```

### Time Iteration

Each section iterates over the time range defined by the `time` configuration:

```json
{
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 30
    }
}
```

For 30 daily timesteps:
1. Download data for day 1
2. Process data for day 1
3. Calculate indices for day 1
4. Download data for day 2
5. ...and so on

### Logging

When `workflow_log` is configured, each section execution is automatically logged:

```
2024-01-15 10:00:00 - INFO - Starting workflow: my_workflow
2024-01-15 10:00:01 - INFO - Executing section: door_downloader (engine: door)
2024-01-15 10:05:23 - INFO - Section door_downloader completed in 5m 22s
2024-01-15 10:05:23 - INFO - Executing section: dam_workflow (engine: dam)
2024-01-15 10:08:15 - INFO - Section dam_workflow completed in 2m 52s
2024-01-15 10:08:15 - INFO - Workflow my_workflow completed in 8m 15s
```

## Complete Example

```json
{
    "name": "complete_drought_monitoring_workflow",
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 365
    },
    
    "tags": {
        "region": "mediterranean",
        "year": "2024"
    },
    
    "datasets": {
        "era5_temp": {
            "type": "local",
            "path": "/data/era5/temperature/%Y/",
            "filename": "temp_%Y%m%d.tif"
        },
        "era5_prec": {
            "type": "local",
            "path": "/data/era5/precipitation/%Y/",
            "filename": "prec_%Y%m%d.tif"
        },
        "modis_ndvi": {
            "type": "local",
            "path": "/data/modis/ndvi/%Y/",
            "filename": "ndvi_%Y%m%d.tif"
        },
        "processed_temp": {
            "type": "local",
            "path": "/data/processed/temperature/%Y/",
            "filename": "temp_processed_%Y%m%d.tif"
        },
        "processed_prec": {
            "type": "local",
            "path": "/data/processed/precipitation/%Y/",
            "filename": "prec_processed_%Y%m%d.tif"
        },
        "processed_ndvi": {
            "type": "local",
            "path": "/data/processed/ndvi/%Y/",
            "filename": "ndvi_processed_%Y%m%d.tif"
        },
        "drought_indices": {
            "type": "local",
            "path": "/data/indices/%Y/",
            "filename": "cdi_%Y%m%d.tif"
        },
        "webserver": {
            "type": "sftp",
            "host": "web.example.com",
            "path": "/var/www/drought_monitor/{year}/",
            "username": "{ENV.WEB_USER}",
            "password": "{ENV.WEB_PASS}"
        }
    },
    
    "workflow_log": {
        "file": "logs/workflow_{year}_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": true
    },
    
    "door_downloader": [
        {
            "source": "ERA5",
            "variable": "2m_temperature",
            "destination": "{DATASETS.era5_temp}",
            "bounds": [8, 38.5, 10, 41.5]
        },
        {
            "source": "ERA5",
            "variable": "total_precipitation",
            "destination": "{DATASETS.era5_prec}",
            "bounds": [8, 38.5, 10, 41.5]
        },
        {
            "source": "MODIS",
            "product": "MOD13A1",
            "variable": "NDVI",
            "destination": "{DATASETS.modis_ndvi}",
            "bounds": [8, 38.5, 10, 41.5]
        }
    ],
    
    "dam_workflow": [
        {
            "name": "process_temperature",
            "input": "{DATASETS.era5_temp}",
            "output": "{DATASETS.processed_temp}",
            "operations": ["reproject", "resample"]
        },
        {
            "name": "process_precipitation",
            "input": "{DATASETS.era5_prec}",
            "output": "{DATASETS.processed_prec}",
            "operations": ["reproject", "resample", "unit_conversion"]
        },
        {
            "name": "process_ndvi",
            "input": "{DATASETS.modis_ndvi}",
            "output": "{DATASETS.processed_ndvi}",
            "operations": ["reproject", "resample", "gap_fill"]
        }
    ],
    
    "dryes_calculator": {
        "indicator": "CDI",
        "inputs": {
            "temperature": "{DATASETS.processed_temp}",
            "precipitation": "{DATASETS.processed_prec}",
            "vegetation": "{DATASETS.processed_ndvi}"
        },
        "output": "{DATASETS.drought_indices}",
        "parameters": {
            "method": "combined",
            "weights": {"temp": 0.3, "prec": 0.4, "veg": 0.3}
        }
    },
    
    "publish": {
        "source": "{DATASETS.drought_indices}",
        "destination": "{DATASETS.webserver}",
        "format": "COG",
        "thumbnail": true
    }
}
```

## Best Practices

### 1. Order Sections Logically

```
download → process → calculate → publish
```

### 2. Use Parallel Execution Wisely

Parallelise independent operations:

```json
{
    "door_downloader": [
        {"source": "ERA5", "variable": "temp"},
        {"source": "CHIRPS", "variable": "prec"}  // Independent, can run in parallel
    ]
}
```

### 3. Name Sections Descriptively

When using multiple similar sections:

```json
{
    "dam_workflow": [
        {"name": "calibrate_temperature", ...},
        {"name": "aggregate_precipitation", ...},
        {"name": "interpolate_gaps", ...}
    ]
}
```

### 4. Keep Sections Focused

Each section should have a single responsibility.

❌ Don't mix responsibilities:
```json
{
    "process": {
        "download": true,
        "process": true,
        "calculate": true,
        "publish": true
    }
}
```

✅ Separate concerns:
```json
{
    "download": { ... },
    "process": { ... },
    "calculate": { ... },
    "publish": { ... }
}
```

### 5. Use Logging

Always configure `workflow_log` to track section execution.

## Troubleshooting

### Section Not Recognised

```
ValueError: Unknown workflow section alias: 'my_downloader'
```

**Solution**: Use recognised aliases or define sections explicitly in `workflow_sections`.

### Import Error

```
ModuleNotFoundError: No module named 'door'
```

**Solution**: Install required module or set `strict_workflow_imports=False` during development.

### Section Not Executing

Check:
1. Section is defined in the configuration
2. Time range is set correctly
3. No errors in previous sections (execution stops on error)

### Parallel Execution Not Working

Ensure you're using an array:

❌ Sequential:
```json
{"door_downloader": {"source": "ERA5"}}
```

✅ Parallel:
```json
{"door_downloader": [{"source": "ERA5"}, {"source": "CHIRPS"}]}
```

## See Also

- [Configuration Overview](overview.md)
- [DOOR Integration](../integration/door.md)
- [DAM Integration](../integration/dam.md)
- [DRYES Integration](../integration/dryes.md)
- [Examples](../examples/full-pipeline.md)
