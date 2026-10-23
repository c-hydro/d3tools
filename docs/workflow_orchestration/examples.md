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
