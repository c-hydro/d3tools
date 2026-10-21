# WorkflowLogManager

`WorkflowLogManager` configures Python logging handlers and provides context managers for workflow and section execution. It is exported from `d3tools.logging`.

## Configuration Factory

Workflow configuration uses `WorkflowLogManager.from_dict()`. It accepts a mapping, a string destination, or `None`:

```python
from d3tools.logging import WorkflowLogManager

manager = WorkflowLogManager.from_dict({
    "file": "logs/workflow.log",
    "level": "INFO",
    "console": True,
    "format_file": "detailed",
    "format_console": "simple",
})
try:
    logger = manager.get_logger()
    logger.info("Configured workflow logger")
finally:
    manager.close()
```

A string is treated as a file destination. `None` or an empty mapping creates a console-only manager. A mapping may omit `file`, and may use `run_state_file`, `format`, `format_file`, `format_console`, and `logger_name`. The `{now:...}` placeholder in destination strings is resolved when the factory processes the configuration. Unsupported top-level input types raise `TypeError`.

## Constructor

`WorkflowLogManager` accepts these parameters:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `log_file` | Dataset, string, Dataset mapping, or `None` | `None` | Optional log destination. |
| `run_state_file` | Dataset, string, Dataset mapping, or `None` | `None` | Optional run-state Dataset destination. |
| `level` | string or integer | `INFO` | Logging threshold. |
| `console` | boolean | `True` | Whether to add a console handler. |
| `format_file` | string | `detailed` | File handler format. |
| `format_console` | string | `simple` | Console handler format. |
| `logger_name` | string | `d3tools` | Name of the primary logger. |

See [Logging Overview](overview.md#formats) for the available format names. Direct-constructor defaults differ from the defaults applied to a configuration mapping; see [Configuration](overview.md#configuration).

## Execution Contexts

`workflow_execution(workflow_name)` logs workflow start, successful completion or failure, and total duration. `section_execution(section_name, engine=None)` logs section start and completion or failure, and records elapsed time. Failure messages include the exception type and message, and the original exception is re-raised.

```python
from d3tools.logging import WorkflowLogManager

manager = WorkflowLogManager(log_file="logs/custom.log", console=False)
try:
    with manager.workflow_execution("custom_task"):
        with manager.section_execution("prepare", engine="custom"):
            prepare_data()
finally:
    manager.close()
```

`WorkflowDefinition.run()` uses these contexts automatically when its manager is configured. Workflow sections execute according to the behavior documented in [Workflow sections](../configuration/workflow-sections.md).

## Logger Access and Level

`get_logger()` with no name returns the manager's primary logger. With a name, it returns `logging.getLogger(name)`. The manager attaches its handlers directly to the configured logger and the `door`, `dam`, and `dryes` package loggers; those package loggers do not inherit as children of `d3tools`.

`set_level(level)` updates the managed loggers and the primary logger's handlers. `close()` removes and closes managed handlers. It is safe to call more than once, and is registered to run at process exit.

## Run-State

Set `run_state_file` in the workflow logging mapping to persist structured state after workflow sections return:

```json
{
  "workflow_log": {
    "run_state_file": "logs/workflow_state.json",
    "console": false
  }
}
```

The state includes a schema version, run identifier, workflow name and status, overall start/end times, and section results. Section results contain the section name, engine, whether it executed, its time range, and a skip reason when provided. Status is `success` when all returned results executed and `partial` otherwise. The Dataset destination must support writing the supplied state structure.

The `run_state_file` is distinct from the human-readable `file` log destination. See [Logging Examples](examples.md#run-state-output) for a configuration example.
