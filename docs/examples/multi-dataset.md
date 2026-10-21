# Multi-Dataset Workflow Example

This example demonstrates how to work with multiple datasets across different storage types (local, S3, SFTP), using defaults, referencing datasets, and organising complex data flows.

## Basic Multi-Dataset Setup

```json
{
    "name": "multi_dataset_workflow",
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 7
    },
    
    "datasets": {
        "raw_temperature": {
            "type": "local",
            "path": "/data/raw/temp/",
            "filename": "temp_%Y%m%d.tif"
        },
        "raw_precipitation": {
            "type": "local",
            "path": "/data/raw/prec/",
            "filename": "prec_%Y%m%d.tif"
        },
        "processed_temp": {
            "type": "local",
            "path": "/data/processed/temp/",
            "filename": "temp_processed_%Y%m%d.tif"
        },
        "processed_prec": {
            "type": "local",
            "path": "/data/processed/prec/",
            "filename": "prec_processed_%Y%m%d.tif"
        },
        "combined_output": {
            "type": "local",
            "path": "/data/output/",
            "filename": "combined_%Y%m%d.tif"
        }
    },
    
    "door_downloader": [
        {
            "source": "ERA5",
            "variable": "2m_temperature",
            "destination": "{DATASETS.raw_temperature}"
        },
        {
            "source": "ERA5",
            "variable": "total_precipitation",
            "destination": "{DATASETS.raw_precipitation}"
        }
    ],
    
    "dam_workflow": [
        {
            "input": "{DATASETS.raw_temperature}",
            "output": "{DATASETS.processed_temp}",
            "operation": "thermal_processing"
        },
        {
            "input": "{DATASETS.raw_precipitation}",
            "output": "{DATASETS.processed_prec}",
            "operation": "precipitation_processing"
        }
    ],
    
    "combine": {
        "temp_input": "{DATASETS.processed_temp}",
        "prec_input": "{DATASETS.processed_prec}",
        "output": "{DATASETS.combined_output}"
    }
}
```

## Using Dataset Defaults

Simplify configurations with `__defaults__`:

```json
{
    "name": "datasets_with_defaults",
    
    "tags": {
        "data_root": "/data",
        "year_folder": "%Y"
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 30
    },
    
    "datasets": {
        "__defaults__": {
            "type": "local",
            "path": "{data_root}/{name}/{year_folder}/",
            "filename": "{name}_%Y%m%d.tif"
        },
        
        "raw_temperature": {},
        "raw_precipitation": {},
        "raw_soilmoisture": {},
        "processed_temperature": {},
        "processed_precipitation": {},
        "processed_soilmoisture": {}
    }
}
```

**Result:**
- `raw_temperature`: `/data/raw_temperature/2024/raw_temperature_20240115.tif`
- `raw_precipitation`: `/data/raw_precipitation/2024/raw_precipitation_20240115.tif`
- etc.

## Hierarchical Storage Organization

Organise datasets by project/region/variable:

```json
{
    "name": "hierarchical_datasets",
    
    "tags": {
        "data_root": "/mnt/storage",
        "project": "drought_monitoring",
        "region": "mediterranean"
    },
    
    "datasets": {
        "__defaults__": {
            "type": "local",
            "path": "{data_root}/{project}/{region}/{category}/{name}/%Y/",
            "filename": "{name}_%Y%m%d.tif"
        },
        
        "raw_era5_temp": {
            "category": "raw"
        },
        "raw_era5_prec": {
            "category": "raw"
        },
        "processed_temp": {
            "category": "processed"
        },
        "processed_prec": {
            "category": "processed"
        },
        "cdi_index": {
            "category": "indices"
        },
        "spi_index": {
            "category": "indices"
        }
    }
}
```

**Generated paths:**
```
/mnt/storage/drought_monitoring/mediterranean/raw/raw_era5_temp/2024/raw_era5_temp_20240115.tif
/mnt/storage/drought_monitoring/mediterranean/processed/processed_temp/2024/processed_temp_20240115.tif
/mnt/storage/drought_monitoring/mediterranean/indices/cdi_index/2024/cdi_index_20240115.tif
```

## Mixed Storage Types

Combine local, S3, and SFTP storage:

```json
{
    "name": "mixed_storage_workflow",
    
    "tags": {
        "local_data": "/data",
        "s3_bucket": "my-climate-data",
        "sftp_host": "data-server.example.com"
    },
    
    "datasets": {
        "raw_from_sftp": {
            "type": "sftp",
            "host": "{sftp_host}",
            "port": 22,
            "username": "{ENV.SFTP_USERNAME}",
            "password": "{ENV.SFTP_PASSWORD}",
            "path": "/data/raw/%Y/",
            "filename": "raw_%Y%m%d.tif"
        },
        
        "local_processing": {
            "type": "local",
            "path": "{local_data}/tmp/",
            "filename": "processing_%Y%m%d.tif"
        },
        
        "archive_to_s3": {
            "type": "s3",
            "bucket": "{s3_bucket}",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "archive/%Y/%m/",
            "filename": "archived_%Y%m%d.tif"
        },
        
        "final_output": {
            "type": "local",
            "path": "{local_data}/output/",
            "filename": "final_%Y%m%d.tif"
        }
    },
    
    "workflow_sections": [
        {
            "name": "download",
            "engine": "door",
            "options": {
                "source": "{DATASETS.raw_from_sftp}",
                "destination": "{DATASETS.local_processing}"
            }
        },
        {
            "name": "process",
            "engine": "dam",
            "options": {
                "input": "{DATASETS.local_processing}",
                "output": "{DATASETS.final_output}"
            }
        },
        {
            "name": "archive",
            "engine": "publish",
            "options": {
                "source": "{DATASETS.final_output}",
                "destination": "{DATASETS.archive_to_s3}"
            }
        }
    ]
}
```

## S3 Multi-Region Setup

Work with different S3 regions and buckets:

```json
{
    "name": "s3_multi_region",
    
    "datasets": {
        "source_eu": {
            "type": "s3",
            "bucket": "climate-data-eu",
            "region": "eu-west-1",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "era5/%Y/",
            "filename": "era5_%Y%m%d.nc"
        },
        
        "source_us": {
            "type": "s3",
            "bucket": "climate-data-us",
            "region": "us-east-1",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "chirps/%Y/",
            "filename": "chirps_%Y%m%d.tif"
        },
        
        "local_merged": {
            "type": "local",
            "path": "/data/merged/",
            "filename": "merged_%Y%m%d.tif"
        },
        
        "archive_global": {
            "type": "s3",
            "bucket": "climate-archive-global",
            "region": "us-west-2",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "merged/%Y/%m/",
            "filename": "merged_%Y%m%d.tif"
        }
    },
    
    "door_downloader": [
        {
            "source": "{DATASETS.source_eu}",
            "destination": "/data/tmp/eu/"
        },
        {
            "source": "{DATASETS.source_us}",
            "destination": "/data/tmp/us/"
        }
    ],
    
    "merge": {
        "inputs": ["/data/tmp/eu/", "/data/tmp/us/"],
        "output": "{DATASETS.local_merged}"
    },
    
    "publish": {
        "source": "{DATASETS.local_merged}",
        "destination": "{DATASETS.archive_global}"
    }
}
```

## Dataset with Different Time Formats

Use different datetime patterns for different datasets:

```json
{
    "name": "mixed_time_formats",
    
    "datasets": {
        "daily_data": {
            "type": "local",
            "path": "/data/daily/",
            "filename": "data_%Y%m%d.tif"
        },
        
        "monthly_data": {
            "type": "local",
            "path": "/data/monthly/",
            "filename": "data_%Y%m.tif"
        },
        
        "yearly_data": {
            "type": "local",
            "path": "/data/yearly/",
            "filename": "data_%Y.tif"
        },
        
        "hourly_data": {
            "type": "local",
            "path": "/data/hourly/%Y/%m/%d/",
            "filename": "data_%Y%m%d_%H%M.tif"
        },
        
        "iso_format_data": {
            "type": "local",
            "path": "/data/iso/",
            "filename": "data_%Y-%m-%d.json"
        }
    }
}
```

## Multi-Variable Climate Data

Handle multiple climate variables efficiently:

```json
{
    "name": "multi_variable_climate",
    
    "tags": {
        "data_root": "/climate_data",
        "variables": ["2m_temperature", "total_precipitation", "soil_moisture", "wind_speed"]
    },
    
    "datasets": {
        "__defaults__": {
            "type": "local",
            "path": "{data_root}/{stage}/{variable}/%Y/",
            "filename": "{variable}_%Y%m%d.nc"
        },
        
        "raw_2m_temperature": {
            "stage": "raw",
            "variable": "2m_temperature"
        },
        "raw_total_precipitation": {
            "stage": "raw",
            "variable": "total_precipitation"
        },
        "raw_soil_moisture": {
            "stage": "raw",
            "variable": "soil_moisture"
        },
        "raw_wind_speed": {
            "stage": "raw",
            "variable": "wind_speed"
        },
        
        "processed_2m_temperature": {
            "stage": "processed",
            "variable": "2m_temperature"
        },
        "processed_total_precipitation": {
            "stage": "processed",
            "variable": "total_precipitation"
        },
        "processed_soil_moisture": {
            "stage": "processed",
            "variable": "soil_moisture"
        },
        "processed_wind_speed": {
            "stage": "processed",
            "variable": "wind_speed"
        }
    },
    
    "door_downloader": [
        {
            "source": "ERA5",
            "variable": "2m_temperature",
            "destination": "{DATASETS.raw_2m_temperature}"
        },
        {
            "source": "ERA5",
            "variable": "total_precipitation",
            "destination": "{DATASETS.raw_total_precipitation}"
        },
        {
            "source": "ERA5",
            "variable": "soil_moisture",
            "destination": "{DATASETS.raw_soil_moisture}"
        },
        {
            "source": "ERA5",
            "variable": "wind_speed",
            "destination": "{DATASETS.raw_wind_speed}"
        }
    ],
    
    "dam_workflow": [
        {
            "input": "{DATASETS.raw_2m_temperature}",
            "output": "{DATASETS.processed_2m_temperature}"
        },
        {
            "input": "{DATASETS.raw_total_precipitation}",
            "output": "{DATASETS.processed_total_precipitation}"
        },
        {
            "input": "{DATASETS.raw_soil_moisture}",
            "output": "{DATASETS.processed_soil_moisture}"
        },
        {
            "input": "{DATASETS.raw_wind_speed}",
            "output": "{DATASETS.processed_wind_speed}"
        }
    ]
}
```

## Backup Strategy with Multiple Datasets

Primary and backup storage locations:

```json
{
    "name": "backup_strategy",
    
    "datasets": {
        "primary_local": {
            "type": "local",
            "path": "/data/primary/",
            "filename": "data_%Y%m%d.tif"
        },
        
        "backup_local": {
            "type": "local",
            "path": "/backup/local/",
            "filename": "data_%Y%m%d.tif"
        },
        
        "backup_s3": {
            "type": "s3",
            "bucket": "data-backup",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "daily_backup/%Y/%m/",
            "filename": "data_%Y%m%d.tif"
        },
        
        "backup_sftp": {
            "type": "sftp",
            "host": "backup-server.example.com",
            "username": "{ENV.BACKUP_USERNAME}",
            "password": "{ENV.BACKUP_PASSWORD}",
            "path": "/backup/%Y/",
            "filename": "data_%Y%m%d.tif"
        }
    },
    
    "workflow_sections": [
        {
            "name": "process",
            "engine": "dam",
            "options": {
                "output": "{DATASETS.primary_local}"
            }
        },
        {
            "name": "backup_local",
            "engine": "publish",
            "options": {
                "source": "{DATASETS.primary_local}",
                "destination": "{DATASETS.backup_local}"
            }
        },
        {
            "name": "backup_s3",
            "engine": "publish",
            "options": {
                "source": "{DATASETS.primary_local}",
                "destination": "{DATASETS.backup_s3}"
            }
        },
        {
            "name": "backup_sftp",
            "engine": "publish",
            "options": {
                "source": "{DATASETS.primary_local}",
                "destination": "{DATASETS.backup_sftp}"
            }
        }
    ]
}
```

## Dynamic Dataset Paths

Use environment variables for flexible deployment:

```json
{
    "name": "dynamic_datasets",
    
    "tags": {
        "environment": "{ENV.ENVIRONMENT, default='development'}",
        "data_root": "{ENV.DATA_ROOT, default='/data'}",
        "s3_bucket": "{ENV.S3_BUCKET, default='dev-bucket'}"
    },
    
    "datasets": {
        "input_local": {
            "type": "local",
            "path": "{data_root}/{environment}/input/",
            "filename": "input_%Y%m%d.tif"
        },
        
        "output_local": {
            "type": "local",
            "path": "{data_root}/{environment}/output/",
            "filename": "output_%Y%m%d.tif"
        },
        
        "archive_s3": {
            "type": "s3",
            "bucket": "{s3_bucket}",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "{environment}/archive/%Y/",
            "filename": "archive_%Y%m%d.tif"
        }
    }
}
```

**Usage:**

```bash
# Development
export ENVIRONMENT=development
export DATA_ROOT=/tmp/dev_data
export S3_BUCKET=dev-climate-bucket
python run_workflow.py workflow.json

# Production
export ENVIRONMENT=production
export DATA_ROOT=/data/production
export S3_BUCKET=prod-climate-bucket
python run_workflow.py workflow.json
```

## Multi-Dataset Logging

Track data flow between datasets:

```json
{
    "name": "logged_multi_dataset",
    
    "workflow_log": {
        "file": "logs/multi_dataset_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": true
    },
    
    "datasets": {
        "raw_data": {
            "type": "local",
            "path": "/data/raw/",
            "filename": "raw_%Y%m%d.tif"
        },
        "processed_data": {
            "type": "local",
            "path": "/data/processed/",
            "filename": "processed_%Y%m%d.tif"
        },
        "qc_data": {
            "type": "local",
            "path": "/data/qc/",
            "filename": "qc_%Y%m%d.tif"
        },
        "final_data": {
            "type": "local",
            "path": "/data/final/",
            "filename": "final_%Y%m%d.tif"
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
    
    "quality_control": {
        "input": "{DATASETS.processed_data}",
        "output": "{DATASETS.qc_data}"
    },
    
    "publish": {
        "source": "{DATASETS.qc_data}",
        "destination": "{DATASETS.final_data}"
    }
}
```

**Log output shows data flow:**

```
INFO - Starting workflow: logged_multi_dataset
INFO - Executing section: door_downloader
INFO - Downloading to: /data/raw/raw_20240115.tif
INFO - Section door_downloader completed in 2m 30s
INFO - Executing section: dam_workflow
INFO - Reading from: /data/raw/raw_20240115.tif
INFO - Writing to: /data/processed/processed_20240115.tif
INFO - Section dam_workflow completed in 45s
INFO - Executing section: quality_control
INFO - Reading from: /data/processed/processed_20240115.tif
INFO - Writing to: /data/qc/qc_20240115.tif
INFO - Section quality_control completed in 10s
INFO - Executing section: publish
INFO - Publishing from: /data/qc/qc_20240115.tif
INFO - Publishing to: /data/final/final_20240115.tif
INFO - Section publish completed in 5s
INFO - Workflow completed in 3m 30s
```

## Complete Multi-Dataset Example

Full production workflow with multiple datasets:

```json
{
    "name": "complete_multi_dataset_workflow",
    
    "tags": {
        "project": "climate_monitoring",
        "region": "europe",
        "data_root": "{ENV.DATA_ROOT, default='/mnt/climate_data'}",
        "s3_bucket": "{ENV.S3_BUCKET}",
        "environment": "{ENV.ENVIRONMENT, default='production'}"
    },
    
    "workflow_log": {
        "file": "{data_root}/logs/{project}/{region}/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": false,
        "format_file": "detailed"
    },
    
    "time": {
        "type": "fixed_len",
        "length": "1D",
        "num_steps": 365
    },
    
    "datasets": {
        "__defaults__": {
            "path": "{data_root}/{project}/{region}/{stage}/{name}/%Y/",
            "filename": "{name}_%Y%m%d.{ext}"
        },
        
        "era5_temperature_raw": {
            "type": "local",
            "stage": "raw",
            "ext": "nc"
        },
        "era5_precipitation_raw": {
            "type": "local",
            "stage": "raw",
            "ext": "nc"
        },
        "modis_fapar_raw": {
            "type": "local",
            "stage": "raw",
            "ext": "hdf"
        },
        
        "temperature_processed": {
            "type": "local",
            "stage": "processed",
            "ext": "tif"
        },
        "precipitation_processed": {
            "type": "local",
            "stage": "processed",
            "ext": "tif"
        },
        "fapar_processed": {
            "type": "local",
            "stage": "processed",
            "ext": "tif"
        },
        
        "spi_index": {
            "type": "local",
            "stage": "indices",
            "ext": "tif"
        },
        "cdi_index": {
            "type": "local",
            "stage": "indices",
            "ext": "tif"
        },
        
        "archive_s3": {
            "type": "s3",
            "bucket": "{s3_bucket}",
            "aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "{ENV.AWS_SECRET_ACCESS_KEY}",
            "path": "{environment}/{project}/{region}/archive/%Y/%m/",
            "filename": "archive_%Y%m%d.tar.gz"
        }
    },
    
    "door_downloader": [
        {
            "source": "ERA5",
            "variable": "2m_temperature",
            "destination": "{DATASETS.era5_temperature_raw}",
            "bounds": [-10, 35, 30, 65]
        },
        {
            "source": "ERA5",
            "variable": "total_precipitation",
            "destination": "{DATASETS.era5_precipitation_raw}",
            "bounds": [-10, 35, 30, 65]
        },
        {
            "source": "MODIS",
            "product": "MOD15A2H",
            "destination": "{DATASETS.modis_fapar_raw}",
            "bounds": [-10, 35, 30, 65]
        }
    ],
    
    "dam_workflow": [
        {
            "name": "process_temperature",
            "input": "{DATASETS.era5_temperature_raw}",
            "output": "{DATASETS.temperature_processed}",
            "operations": ["reproject", "resample", "clip"]
        },
        {
            "name": "process_precipitation",
            "input": "{DATASETS.era5_precipitation_raw}",
            "output": "{DATASETS.precipitation_processed}",
            "operations": ["reproject", "resample", "clip"]
        },
        {
            "name": "process_fapar",
            "input": "{DATASETS.modis_fapar_raw}",
            "output": "{DATASETS.fapar_processed}",
            "operations": ["extract", "reproject", "resample"]
        }
    ],
    
    "dryes_calculator": [
        {
            "indicator": "SPI",
            "input": "{DATASETS.precipitation_processed}",
            "output": "{DATASETS.spi_index}",
            "window": 90
        },
        {
            "indicator": "CDI",
            "inputs": [
                "{DATASETS.precipitation_processed}",
                "{DATASETS.temperature_processed}",
                "{DATASETS.fapar_processed}"
            ],
            "output": "{DATASETS.cdi_index}"
        }
    ],
    
    "archive": {
        "sources": [
            "{DATASETS.temperature_processed}",
            "{DATASETS.precipitation_processed}",
            "{DATASETS.fapar_processed}",
            "{DATASETS.spi_index}",
            "{DATASETS.cdi_index}"
        ],
        "destination": "{DATASETS.archive_s3}",
        "compression": "gzip"
    }
}
```

## Best Practices

### 1. Use Descriptive Dataset Names

```json
// Good
"era5_temperature_raw"
"era5_temperature_processed"
"modis_fapar_raw"

// Avoid
"data1"
"output"
"temp"
```

### 2. Organise by Stage

```json
{
    "raw_data": {...},
    "processed_data": {...},
    "final_output": {...}
}
```

### 3. Use Defaults to Reduce Duplication

```json
{
    "__defaults__": {
        "type": "local",
        "path": "/data/{name}/"
    },
    "dataset1": {},  // Inherits defaults
    "dataset2": {}   // Inherits defaults
}
```

### 4. Secure Credentials

Never hardcode credentials:

```json
// Good
"aws_access_key_id": "{ENV.AWS_ACCESS_KEY_ID}"

// NEVER
"aws_access_key_id": "AKIAIOSFODNN7EXAMPLE"
```

### 5. Use Hierarchical organisation

```json
{
    "path": "{data_root}/{project}/{region}/{stage}/{variable}/%Y/"
}
```

## See Also

- [Configuration: Datasets](../configuration/datasets.md)
- [Configuration: Tags](../configuration/tags.md)
- [Configuration: Environment Variables](../configuration/environment.md)
- [Minimal Workflow](minimal-workflow.md)
- [Workflow with Logging](with-logging.md)
