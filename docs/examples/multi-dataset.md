# Multi-Dataset Workflow

This example defines several datasets and references them from a workflow
configuration. Dataset schemas and storage behavior are documented in the
[Datasets guide](../configuration/datasets.md); this page focuses on the
integration points used by a workflow.

## Configuration

```json
{
    "workflow_name": "multi_dataset_workflow",
    "tags": {
        "data_root": "/data"
    },
    "datasets": {
        "__defaults__": {
            "type": "local",
            "path": "{data_root}"
        },
        "input_data": {
            "filename": "input_%Y%m%d.tif"
        },
        "output_data": {
            "filename": "output_%Y%m%d.tif"
        }
    },
    "door_downloader": {
        "destination": "{input_data}"
    },
    "dam_workflow": {
        "input": "{input_data}",
        "output": "{DATASETS.output_data}"
    }
}
```

The section payloads are engine-specific. Install and configure the relevant
DOOR or DAM package before running this example; consult those packages for
their section options.

Dataset placeholders can occupy a complete string value. An unqualified
reference such as `{input_data}` resolves by dataset name. A qualified form
must use the exact case of the top-level key, so `{DATASETS.output_data}` is
valid only when the configuration key is literally `DATASETS`.

## Load and Inspect

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load(
    "multi_dataset_workflow.json",
    build_workflow_objects=False,
    strict_workflow_imports=False,
)

print(workflow.workflow_name)
print(workflow.datasets.keys())
```

Use `build_workflow_objects=False` while inspecting configuration without
constructing engine objects. This does not make the workflow executable.

## See Also

- [Getting Started](../getting-started.md)
- [Datasets](../configuration/datasets.md)
- [Workflow Sections](../configuration/workflow-sections.md)
- [Workflow with Logging](with-logging.md)
