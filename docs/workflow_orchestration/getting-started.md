# Getting started

## Install d3tools and required extras

d3tools requires Python 3.12 or newer. Install it into an active virtual
environment:

```bash
python -m pip install d3tools
```

Install optional extras only when your workflow needs those capabilities:

```bash
python -m pip install 'd3tools[more-data]'    # CSV, Parquet, and shapefile formats
python -m pip install 'd3tools[thumbnails]'   # configured thumbnail generation
python -m pip install 'd3tools[remote-data]'  # remote dataset access
```

Each section also needs its engine package: [DOOR](https://github.com/c-hydro/door)
for `door`, [DAM](https://github.com/c-hydro/dam) for `dam`, or
[DRYES](https://github.com/c-hydro/dryes) for `dryes`. Follow that project's
installation guidance for its engine and domain-specific dependencies. A
configuration can be inspected without constructing engine objects, but it
cannot execute a section whose engine or dependencies are missing; see
[Troubleshooting](troubleshooting.md#installation-and-engine-imports).

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

From a d3tools checkout, pass the JSON file to the CLI runner. Date arguments
are optional:

```bash
python scripts/run_workflow.py workflow.json \
  --start 2024-01-01 \
  --end 2024-01-05
```

When supplied, `--start` and `--end` set the workflow run range. If omitted,
`START_DATE` and `END_DATE` environment variables may provide the range;
otherwise sections resolve their own ranges from available data. A start with
no end uses `END_DATE` if set, or the current time. An end without a start is
an error. A supplied workflow range is passed to every section. See
[execution ranges and options](workflows.md#set-execution-ranges-and-options)
for the precise rules and exceptions.
