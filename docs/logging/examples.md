# Logging Examples

These examples show the `workflow_log` option only. Add it to a workflow configuration as described in [Workflow Configuration](../configuration/overview.md).

## File and Console

Write to a relative file and keep console output enabled:

```json
{
  "workflow_log": {
    "file": "logs/workflow.log",
    "level": "INFO",
    "console": true,
    "format_file": "detailed",
    "format_console": "simple"
  }
}
```

`console` defaults to `true` for mapping configuration. Set it to `false` for file-only output. The parent directory of a local file path is created when the file handler is configured.

## Timestamped Paths

The `{now:...}` placeholder is formatted when the logging manager is created:

```json
{
  "workflow_log": {
    "file": "logs/workflow_{now:%Y%m%d_%H%M%S}.log",
    "console": false
  }
}
```

The timestamp uses Python `datetime` format directives. This example produces a different file path when the manager is created at a different second.

## Run-State Output

`run_state_file` is a separate optional Dataset destination from the text log:

```json
{
  "workflow_log": {
    "file": "logs/workflow.log",
    "run_state_file": "logs/workflow_state.json",
    "console": false
  }
}
```

After section execution, the workflow writes structured state including its name, status, time range, and section results. See the [WorkflowLogManager reference](workflow-log-manager.md).

## Python Context Managers

The manager can also track Python code outside `WorkflowDefinition.run()`:

```python
from d3tools.logging import WorkflowLogManager

manager = WorkflowLogManager(log_file="logs/custom.log", console=False)
try:
    with manager.workflow_execution("custom_task"):
        with manager.section_execution("prepare", engine="custom"):
            print("Preparing data")
finally:
    manager.close()
```

The context managers log start and completion, or log the exception type and message before re-raising an exception. `close()` removes and closes the manager's handlers; it is also registered for process exit.
