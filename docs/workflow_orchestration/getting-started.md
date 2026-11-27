# Getting started

## Install D3tools and required extras

D3tools requires Python 3.12 or newer. Install it into an active virtual
environment:

```bash
python -m pip install cima-d3tools
```

Install optional extras only when your workflow needs those capabilities:

```bash
python -m pip install 'cima-d3tools[more-data]'    # Parquet formats
python -m pip install 'cima-d3tools[thumbnails]'   # configured thumbnail generation
python -m pip install 'cima-d3tools[remote-data]'  # remote dataset access
```

The distribution is named `cima-d3tools`; the Python package remains
`d3tools`, so existing imports continue to use `import d3tools`.

Each section also needs its engine package: [DOOR](https://github.com/c-hydro/door/tree/main_new)
for `door`, [DAM](https://github.com/c-hydro/dam) for `dam`, or
[DRYES](https://github.com/c-hydro/dryes) for `dryes`. Follow that project's
installation guidance for its engine and domain-specific dependencies. A
configuration can be inspected without constructing engine objects, but it
cannot execute a section whose engine or dependencies are missing; see
[Troubleshooting](troubleshooting.md#installation-and-engine-imports).

Raster and vector support is part of the base installation through Rasterio,
rioxarray, and GeoPandas. Some deployments also require the system GDAL
libraries used by those packages; GDAL is not a separate D3tools extra. S3
and SFTP support are both supplied by `remote-data`.

## Create, load, and run a minimal workflow

Start with a small JSON file named `workflow.json`:

```json
{
  "workflow_name": "first_workflow",
  "tags": {},
  "datasets": {}
}
```

Load and run it through the supported Python entry points:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("workflow.json")
workflow.run(start="2024-01-01", end="2024-01-05")
```

This checks configuration loading and the run path; with no workflow sections,
it performs no engine work. For a workflow that downloads and processes real
data, continue to the [CHIRPS-to-SPI case study](case-study.md).

## Run from the command line

After installing D3tools, pass the JSON file to `d3run`. Date arguments are
optional:

```bash
d3run workflow.json \
  --start 2024-01-01 \
  --end 2024-01-05
```

The command accepts only these arguments:

| Argument | Required | Meaning |
| --- | --- | --- |
| `workflow_json` | Yes | Path to the workflow JSON file. |
| `-s`, `--start` | No | Start date string, normally `YYYY-MM-DD`. |
| `-e`, `--end` | No | End date string, normally `YYYY-MM-DD`. |
| `-h`, `--help` | No | Print the generated usage message and exit. |

When supplied, `--start` and `--end` set the workflow run range. If omitted,
`START_DATE` and `END_DATE` environment variables may provide the range;
otherwise sections resolve their own ranges from available data. A start with
no end uses `END_DATE` if set, or the current time. An end without a start is
an error. A supplied workflow range is passed to every section. See
[execution ranges and options](workflows.md#set-execution-ranges-and-options)
for the precise rules and exceptions.

The CLI constructs all configured engine objects in strict mode. Before
calling `run()`, it also queries each object with `get_last_ts()` and, when
available, `get_first_ts()`. A missing engine, invalid engine configuration,
or failed availability query therefore stops the command before section
execution, even when an explicit date range was supplied.

### CLI exit codes

The runner does not translate failures into engine-specific status codes:

| Exit code | Meaning |
| --- | --- |
| `0` | Argument parsing, workflow loading, availability checks, and the run all returned normally. `--help` also returns `0`. A `0` does not imply that every section performed work; a section may legitimately have nothing to do. |
| `1` | An ordinary uncaught loading, configuration, engine-import, availability, or execution exception reached Python's top level. The traceback identifies the underlying failure. |
| `2` | `argparse` rejected the command line, for example because `workflow_json` is missing or an option is unknown. It prints usage information before exiting. |

Signal-derived shell statuses, such as an interactive interruption, are
platform and shell behaviour rather than D3tools exit codes.

## How engines are selected and imported

Every top-level key that is not reserved configuration is treated as a
workflow section. A section can set `engine` to `door`, `dam`, or `dryes`;
otherwise D3tools infers the engine from a recognised section-name alias such
as `door_downloader`, `dam_workflow`, or `dryes_index`. A top-level `engine`
provides the default for sections that do not set their own. See
[workflow sections and engines](workflows.md#define-sections-and-choose-an-engine) for
the full alias table and ordering rules.

`WorkflowDefinition.load()` and the CLI build runtime objects by default, so
only the engines used by the configuration are imported and constructed. If
an engine import fails, strict loading raises `WorkflowEngineImportError` and
identifies the selected engine. For configuration inspection without the
engine packages, disable construction explicitly:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load(
    "workflow.json",
    build_workflow_objects=False,
)
print([(section.name, section.engine) for section in workflow.workflow_sections])
```

The retained section values are parsed payloads, not executable engine
objects. Do not call `run()` on an inspection-only definition. Missing engine
packages and optional imports are catalogued in
[Troubleshooting](troubleshooting.md#installation-and-engine-imports).
