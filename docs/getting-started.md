# Getting Started with d3tools

d3tools provides the configuration, dataset, logging, and execution machinery
shared by the DOOR, DAM, and DRYES workflow engines.
These packages provide a full suite of tools for building operational drought
monitoring systems.
This guide takes you from installation to loading and running a workflow configuration.

## Installation

d3tools requires Python 3.12 or newer. From a local checkout, install it in an
active virtual environment:

```bash
git clone https://github.com/c-hydro/d3tools.git
cd d3tools
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
```

Install optional dependencies only when the workflow needs them:

```bash
python -m pip install 'd3tools[more-data]'    # CSV, Parquet, and shapefile support
python -m pip install 'd3tools[thumbnails]'   # thumbnail generation
python -m pip install 'd3tools[remote-data]'  # remote data access
```

The base installation provides the shared d3tools functionality. A workflow
that builds runtime objects also needs the engine packages used by its sections:

- DOOR for `door` sections (data download);
- DAM for `dam` sections (data processing);
- DRYES for `dryes` sections (drought indices calculation).

Install those packages according to the corresponding project instructions.
During configuration development, `strict_workflow_imports=False` can be used
to defer engine import failures, but a runnable workflow still needs its engine
dependencies installed.

## The Configuration Model

A workflow is a JSON object, or an equivalent Python `dict`, loaded into a
`WorkflowDefinition`:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("workflow.json")
workflow.run(start="2024-01-01", end="2024-01-31")
```

The most commonly used top-level keys are:

- `workflow_name`: name used in logging and run-state files;
- `tags`: reusable values substituted throughout the configuration;
- `datasets`: local or remote data definitions;
- `workflow_log`: optional file and console logging configuration;
- `engine`: default engine for sections that do not resolve one otherwise;
- `exec_options`: default execution-range options for sections.

The public Python entry points exported by d3tools are `WorkflowDefinition`,
`Options`, `WorkflowSection`, and `WORKFLOW_SECTION_ALIASES`. Use
`WorkflowDefinition` to load and run workflows; `Options` is the parsed
configuration mapping rather than the runnable workflow object.

Every other non-reserved top-level key is collected as a workflow section. A
section can identify its engine through its own `engine` key, through the
workflow-level default, or through a recognised section-name alias. The detailed
resolution order and alias table are documented in the
[Configuration Overview](configuration/overview.md).

## Your First Workflow

Create `workflow.json` with a section appropriate for the engine package you
have installed. This example uses a DOOR downloader and a local destination:

```json
{
    "workflow_name": "my_first_workflow",
    "datasets": {
        "output_data": {
            "type": "local",
            "path": "/data/output",
            "filename": "data_%Y%m%d.tif"
        }
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.output_data}",
        "product" : "reanalysis-era5-single-levels",
        "bounds" : [8, 38.5, 10, 41.5],
        "options" : {
            "variables" : ["total_precipitation"],
            "agg_method" : "sum",
            "ts_per_year_agg" : 365
        }
    }
}
```

Load and run it from Python:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("workflow.json")
workflow.run(start="2024-01-01", end="2024-01-05")
```

The `start` and `end` values may be ISO date strings or `datetime` objects. If
they are omitted, d3tools checks `START_DATE` and `END_DATE` and can otherwise
let each section resolve its own range from data availability.

This workflow will download the daily precipitation from ERA5 for the bounds specified.
See the specific documentation for the `door` package for details on the downloader definition syntax.

## Running from the Command Line

From the repository root, use the supplied script:

```bash
python scripts/run_workflow.py workflow.json \
    --start 2024-01-01 \
    --end 2024-01-05
```

The positional argument is the workflow JSON path. `--start` and `--end` are
optional; when omitted, the workflow resolves its range using the normal runtime
rules.

## Tags and Environment Variables

Tags make paths and other configuration values reusable. Tags may refer to
earlier-resolved environment and time placeholders, and may refer to other tags:

```json
{
    "tags": {
        "data_root": "{ENV.DATA_ROOT, default='/data'}",
        "product": "ERA5",
        "output_path": "{data_root}/{product}"
    },
    "datasets": {
        "output_data": {
            "type": "local",
            "path": "{output_path}",
            "filename": "data_%Y%m%d.tif"
        }
    }
}
```

An `env` top-level object can set environment variables before placeholders are
resolved:

```json
{
    "env": {
        "DATA_ROOT": "/mnt/data"
    },
    "tags": {
        "data_root": "{ENV.DATA_ROOT}"
    }
}
```

Use real environment variables for secrets such as credentials. See the
[Tags](configuration/tags.md) and
[Environment Variables](configuration/environment.md) guides for placeholder
syntax and resolution order.

## Dataset References

Dataset definitions are built before workflow sections are instantiated. A
dataset can be referenced as `{DATASETS.output_data}`, `{datasets.output_data}`,
or `{output_data}`. The intended usage is for the placeholder to occupy the
entire string value:

```json
{
    "datasets": {
        "input_data": {
            "type": "local",
            "path": "/data/input",
            "filename": "input_%Y%m%d.tif"
        },
        "output_data": {
            "type": "local",
            "path": "/data/output",
            "filename": "output_%Y%m%d.tif"
        }
    },
    "dam_workflow": {
        "input": "{input_data}",
        "output": "{DATASETS.output_data}",
        "options": {}
    }
}
```

Dataset defaults can reduce repetition:

```json
{
    "datasets": {
        "__defaults__": {
            "type": "local",
            "path": "/data"
        },
        "input_data": {
            "filename": "input_%Y%m%d.tif"
        },
        "output_data": {
            "filename": "output_%Y%m%d.tif"
        }
    }
}
```

See the [Datasets](configuration/datasets.md) guide for dataset types, defaults,
remote storage, and tag overrides.

## Logging

Logging is optional. Configure a file, console output, or both with
`workflow_log`:

```json
{
    "workflow_name": "logged_workflow",
    "datasets": {
        "output_data": {
            "type": "local",
            "path": "/data/output",
            "filename": "data_%Y%m%d.tif"
        }
    },
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": true
    },
    "door_downloader": {
        "source": "ERA5",
        "destination": "{DATASETS.output_data}",
        "product": "reanalysis-era5-single-levels",
        "bounds": [8, 38.5, 10, 41.5],
        "options": {}
    }
}
```

The `{now:...}` placeholder in `workflow_log.file` is resolved when the workflow
is loaded and its logging manager is constructed, not when it runs. See the
[Logging](logging/overview.md) guide for levels, formats, and run-state logging.

## Troubleshooting

### Engine import errors

Check that the package for every configured engine is installed. During
development, load the configuration with `strict_workflow_imports=False` to
inspect parsed sections without requiring every engine import to succeed:

```python
workflow = WorkflowDefinition.load(
    "workflow.json",
    build_workflow_objects=False,
    strict_workflow_imports=False,
)
```

This is useful for inspecting configuration, but it does not make the workflow
executable.

### Configuration errors

Check that:

- reserved top-level keys are spelt correctly;
- every section resolves to a recognised engine;
- dataset references use a defined dataset name;
- environment placeholders have a value or a default;
- section-specific options are placed inside the section's `exec_options`.

The [Configuration Overview](configuration/overview.md) describes the parsing
pipeline and the specialised guides cover each configuration feature in detail.

## Next Steps

- [Configuration Overview](configuration/overview.md)
- [Workflow Sections](configuration/workflow-sections.md)
- [Datasets](configuration/datasets.md)
- [Tags](configuration/tags.md)
- [Environment Variables](configuration/environment.md)
- [Logging](logging/overview.md)
- [Examples](examples/minimal-workflow.md)