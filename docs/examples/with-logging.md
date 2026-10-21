# Workflow with Logging

`workflow_log` configures the handlers used by `WorkflowDefinition.run()`. A
file path should be writable by the account running the workflow.

## Configuration

Save this as `logged_workflow.json`:

```json
{
    "workflow_name": "logged_workflow",
    "datasets": {},
    "workflow_log": {
        "file": "logs/workflow.log",
        "level": "INFO",
        "console": true,
        "format_file": "detailed",
        "format_console": "simple"
    }
}
```

The mapping creates a file handler and a console handler. Parent directories
for a local file destination are created when logging is configured. Use
`console: false` and omit `file` to suppress both configured outputs.

## Run It

```python
from datetime import datetime

from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("logged_workflow.json")
workflow.run(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 5),
)
```

The workflow manager logs workflow and section start/completion or failure
messages. Engine package loggers for DOOR, DAM, and DRYES are also connected
when those engines are used.

## Timestamped Files

Use `{now:...}` in a local destination when each run needs a distinct file:

```json
{
    "workflow_name": "timestamped_workflow",
    "workflow_log": {
        "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log",
        "level": "INFO",
        "console": false
    }
}
```

This placeholder is resolved when the workflow runs. For the complete option
table and available formats, see [Logging Overview](../logging/overview.md).

## See Also

- [Getting Started](../getting-started.md)
- [Minimal Workflow](minimal-workflow.md)
- [Multi-Dataset Workflow](multi-dataset.md)
