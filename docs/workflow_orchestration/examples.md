# Examples

## Minimal load-and-run example

For a configuration-load check before adding an engine section, create
`minimal.json`:

```json
{
  "workflow_name": "minimal",
  "tags": {},
  "datasets": {}
}
```

Then load and run it:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("minimal.json")
workflow.run(start="2024-01-01", end="2024-01-05")
```

With no sections this does not perform engine work. Use the
[CHIRPS-to-SPI case study](case-study.md) for a complete engine-backed
example.

## Multi-dataset example

This engine-free example defines a daily input, a static reference dataset,
and a family of daily products. Shared local settings live in
`__defaults__`, while `other_options` shows how complete-string dataset
references are resolved without inventing an engine payload. Save it as
`multi_dataset.json`:

```json
{
  "workflow_name": "multi_dataset",
  "tags": {
    "data_root": "/tmp/d3tools-multi-dataset"
  },
  "datasets": {
    "__defaults__": {
      "type": "local",
      "path": "{data_root}",
      "format": "json"
    },
    "precipitation": {
      "filename": "input/precipitation_%Y%m%d.json",
      "timestep": "daily"
    },
    "climatology": {
      "filename": "reference/climatology.json"
    },
    "products": {
      "filename": "output/{variable}_%Y%m%d.json",
      "timestep": "daily"
    }
  },
  "other_options": {
    "input": "{datasets.precipitation}",
    "reference": "{datasets.climatology}",
    "output": "{datasets.products, variable='anomaly'}"
  }
}
```

Load it, inspect the three resolved datasets, and render their keys:

```python
from datetime import datetime

from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("multi_dataset.json")
run_date = datetime(2024, 1, 5)

print(sorted(workflow.datasets))
print(workflow.other_options["input"].get_key(run_date))
print(workflow.other_options["reference"].get_key())
print(workflow.other_options["output"].get_key(run_date))

workflow.run(start=run_date, end=run_date)
```

The rendered paths end in `input/precipitation_20240105.json`,
`reference/climatology.json`, and `output/anomaly_20240105.json`. The output
reference is an updated copy of `products`; the configured dataset definition
remains reusable for another `{variable}` value. As in the minimal example,
`run()` performs no engine work because the configuration has no workflow
sections. See [Configuration](configuration.md#reference-and-update-datasets)
for reference syntax and [Datasets](datasets.md) for storage and key options.

## Logging example

Add a log path to the same small configuration:

```json
{
  "workflow_name": "logged_minimal",
  "tags": {},
  "datasets": {},
  "workflow_log": {
    "file": "logs/workflow.log",
    "console": true,
    "level": "INFO",
    "format_file": "simple"
  }
}
```

Run it using the same `WorkflowDefinition.load()` / `run()` pattern above.
The example logs workflow execution but has no section or dataset writes.
Check the console and `logs/workflow.log`; see [Logging](logging.md) for
formats, run-state output, and dataset logs.
