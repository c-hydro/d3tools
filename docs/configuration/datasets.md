# Configuration Datasets

Datasets define data sources and destinations used throughout your workflow. They provide a unified interface for local files, S3 buckets, and SFTP servers.

## Basic Usage

Define datasets in the `datasets` section:

```json
{
    "datasets": {
        "my_data": {
            "type": "local",
            "path": "/data/raw/",
            "filename": "data_%Y%m%d.tif"
        }
    }
}
```

Reference them using a qualified or unqualified dataset placeholder, such as
`{DATASETS.my_data}`, `{datasets.my_data}`, or `{my_data}`:

```json
{
    "door_downloader": {
        "destination": "{DATASETS.my_data}"
    }
}
```

## Dataset Types

### Local Files

```json
{
    "datasets": {
        "local_data": {
            "type": "local",
            "path": "/data/output/",
            "filename": "result_%Y%m%d.tif"
        }
    }
}
```

**Parameters:**
- `type`: `"local"`
- `path`: Directory path (absolute or relative), used together with `filename`
- `filename`: File name pattern with optional datetime placeholders
- `key_pattern`: Optional complete path and filename pattern; use this instead
  of `path` and `filename` when a single value is more convenient

The two local-dataset forms are equivalent:

```json
{
    "datasets": {
        "local_data": {
            "type": "local",
            "key_pattern": "/data/output/result_%Y%m%d.tif"
        }
    }
}
```

If `type` is not specified, the dataset defaults to type local, so this is also equivalent to the ones above:

```json
{
    "datasets": {
        "local_data": {
            "key_pattern": "/data/output/result_%Y%m%d.tif"
        }
    }
}
```

### S3 Buckets

```json
{
    "datasets": {
        "s3_data": {
            "type": "s3",
            "bucket_name": "my-bucket",
            "key_pattern": "data/output/result_%Y%m%d.tif",
            "region_name": "us-east-1"
        }
    }
}
```

**Parameters:**
- `type`: `"s3"`
- `bucket_name`: S3 bucket name
- `key_pattern`: Object-key pattern, including any directory prefix and filename
- `region_name`: AWS region (optional)

**Authentication**: Uses standard AWS credentials (environment variables, ~/.aws/credentials, IAM role)

### SFTP

```json
{
    "datasets": {
        "sftp_data": {
            "type": "sftp",
            "host": "data.example.com",
            "key_pattern": "/remote/data/result_%Y%m%d.tif",
            "username": "{ENV.SFTP_USER}",
            "password": "{ENV.SFTP_PASS}"
        }
    }
}
```

**Parameters:**
- `type`: `"sftp"`
- `host`: SFTP server hostname
- `key_pattern`: Remote path and filename pattern
- `username`: SFTP username; this is required even when using key authentication
- `password`: Optional SFTP password; use an environment variable when provided
- `private_key`: Optional path to an RSA or Ed25519 private key file
- `port`: SFTP port (optional, default: 22)

If `password` is omitted, d3tools uses key authentication. When `private_key` is
provided, it loads that RSA or Ed25519 key file. Otherwise, it searches `~/.ssh`
for private key filenames containing `rsa` or `ed25519` and tries those keys.

Password authentication:

```json
{
    "type": "sftp",
    "host": "data.example.com",
    "key_pattern": "/remote/data/result_%Y%m%d.tif",
    "username": "{ENV.SFTP_USER}",
    "password": "{ENV.SFTP_PASS}"
}
```

Explicit private-key authentication:

```json
{
    "type": "sftp",
    "host": "data.example.com",
    "key_pattern": "/remote/data/result_%Y%m%d.tif",
    "username": "{ENV.SFTP_USER}",
    "private_key": "/home/user/.ssh/id_ed25519"
}
```

Use environment variables for passwords and usernames where appropriate, and
never hardcode passwords in a shared configuration file. The current
implementation does not read SSH configuration or authenticate through an SSH
agent; provide the username and, when needed, the private-key path explicitly.

## Defaults

Use `__defaults__` to set common parameters for all datasets:

```json
{
    "datasets": {
        "__defaults__": {
            "type": "local",
            "path": "/data/"
        },
        "input_data": {
            "path": "/data/input/",
            "filename": "input_%Y%m%d.tif"
        },
        "output_data": {
            "path": "/data/output/",
            "filename": "output_%Y%m%d.tif"
        }
    }
}
```

Datasets inherit from `__defaults__` and can override specific parameters.

## Datetime Placeholders in Paths

Use strftime format codes in `filename` or `key_pattern`:

```json
{
    "datasets": {
        "daily_data": {
            "type": "local",
            "path": "/data/%Y/",
            "filename": "data_%Y%m%d.tif"
        }
    }
}
```

**Common placeholders:**
- `%Y`: 4-digit year (2024)
- `%m`: 2-digit month (01-12)
- `%d`: 2-digit day (01-31)
- `%j`: Day of year (001-366)
- `%H`: Hour (00-23)
- `%M`: Minute (00-59)
- `%S`: Second (00-59)

See [Python strftime documentation](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) for all format codes.

## Referencing Datasets

### In Workflow Sections

```json
{
    "datasets": {
        "input_data":  {"key_pattern": "/data/input/input_%Y%m%d.tif" },
        "output_data": {"key_pattern": "/data/output/output_%Y%m%d.tif" }
    },
    "dam_workflow": {
        "input": "{DATASETS.input_data}",
        "output": "{DATASETS.output_data}"
    }
}
```

### With Tags

Use tags inside dataset definitions:

```json
{
    "tags": {
        "source": "ERA5",
        "region": "europe"
    },
    "datasets": {
        "source_data": {
            "type": "local",
            "path": "/data/{source}/{region}/",
            "filename": "{source}_%Y%m%d.tif"
        }
    }
}
```

After resolution:
- `path`: `/data/ERA5/europe/`
- `filename`: `ERA5_%Y%m%d.tif`

### With Update Tags

Apply additional tags to datasets at reference time:

```json
{
    "datasets": {
        "base_data": {
            "type": "local",
            "path": "/data/",
            "filename": "{variable}_%Y%m%d.tif"
        }
    },
    "door_downloader": {
        "destination": "{DATASETS.base_data, variable='temperature'}"
    }
}
```

This resolves the dataset reference and calls
`.update(variable='temperature')` on the dataset object. Because the filename
contains `{variable}`, the returned dataset has the key pattern
`/data/temperature_%Y%m%d.tif`. The original dataset is not modified.

This is dataset-reference tagging, which is separate from the top-level
configuration tags described in [Tags](tags.md#dataset-update-tags).

## Thumbnail Definitions

Datasets can include a `thumbnail` object to generate PNG thumbnails when data is written or when the thumbnail manager is called directly.

```json
{
    "datasets": {
        "fapar_anomaly": {
            "type": "local",
            "key_pattern": "/data/fapar_%Y%m%d.tif",
            "format": "geotiff",
            "thumbnail": {
                "colors": "/config/colors/fapar_anomaly.txt",
                "destination": "/output/thumbnails/fapar_%Y%m%d.png",
                "overlay": "/data/boundaries.geojson",
                "annotation": {"text": "FAPAR anomaly"},
                "legend": {"loc": "lower left"}
            }
        }
    }
}
```

See the [Thumbnail Generation](../thumbnails/overview.md) guide for the full thumbnail contract, including raster/vector source rules, colour definitions, overlay behaviour, annotation and legend options.

## Dataset Logging

Datasets can include a `log` option to record metadata and quality checks each
time data is written. The value may be a log-file path or a mapping containing a
`file` path:

```json
{
    "datasets": {
        "output_data": {
            "type": "local",
            "key_pattern": "/data/output_%Y%m%d.tif",
            "log": "/logs/output_data.json"
        }
    }
}
```

Dataset logging is separate from `workflow_log`, which records workflow and
section execution. See [Dataset Logging](../logging/dataset-logging.md) for the
full configuration and the information written by `DatasetLogManager`.

## Dataset API and Concepts

The configuration above creates `Dataset` objects. For their runtime behaviour,
see:

- [Dataset Concepts](../datasets/concepts.md): key patterns, time, tags, tiles, and composition;
- [Dataset Operations](../datasets/operations.md): reading, writing, copying, moving, and deleting data;
- [Dataset Time and Availability](../datasets/time-and-availability.md): catalogues, dates, timesteps, and availability queries;
- [Dataset Composition](../datasets/composition.md): updates, copies, parents, fallbacks, and versions;
- [Dataset Storage Backends](../datasets/storage-backends.md): local, memory, S3, and SFTP behaviour;
- [Dataset Formats and Mixins](../datasets/formats-and-mixins.md): format detection and format-specific processing;
- [Dataset Catalogues and Templates](../datasets/catalogues-and-templates.md): key matching, discovery, and spatial templates;
