# Minimal Workflow Example

This example loads a valid workflow with no engine-specific sections. It is
useful for checking configuration loading and execution-range handling before
adding a DOOR, DAM, or DRYES section.

## Configuration

Save this as `minimal_workflow.json`:

```json
{
    "workflow_name": "minimal_workflow",
    "tags": {},
    "datasets": {}
}
```

`workflow_name`, `tags`, and `datasets` are d3tools configuration keys. There
is no top-level `time` object; the execution range is supplied when the
workflow runs or is resolved from the supported environment variables.

## Run It

From Python:

```python
from datetime import datetime

from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("minimal_workflow.json")
workflow.run(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 5),
)
```

The same workflow can be run from the repository root with:

```bash
python scripts/run_workflow.py minimal_workflow.json \
    --start 2024-01-01 \
    --end 2024-01-05
```

Because the configuration has no sections, the run has no engine work to
perform. Add a non-reserved section key when the workflow is ready to call an
engine; see [Workflow Sections](../configuration/workflow-sections.md) for
engine selection and section structure.

## Tags and Datasets

Tags and datasets are available through the parsed workflow attributes:

```json
{
    "workflow_name": "configured_workflow",
    "tags": {
        "project": "test_project"
    },
    "datasets": {
        "input_data": {
            "type": "local",
            "key_pattern": "/tmp/data/input_%Y%m%d.tif"
        }
    }
}
```

```python
workflow = WorkflowDefinition.load("configured_workflow.json")
print(workflow.workflow_name)
print(workflow.tags["project"])
print(workflow.datasets["input_data"])
```

See [Datasets](../configuration/datasets.md) for storage types and dataset
references.

## See Also

- [Getting Started](../getting-started.md)
- [Workflow with Logging](with-logging.md)
- [Multi-Dataset Workflow](multi-dataset.md)
