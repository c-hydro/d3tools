# Workflow lifecycle

## Workflow loading and parsing lifecycle

The normal Python entry path loads JSON into a `WorkflowDefinition`, which
passes the resulting mapping through d3tools' configuration pipeline. The
pipeline creates resolved options; it does not make engine payloads part of
d3tools' configuration contract.

The parsing stages run in this order:

1. Values from top-level `env` are copied into `os.environ` as strings.
2. Environment placeholders are resolved recursively.
3. `{now}` placeholders, including those in workflow-log destinations, are
   resolved together for one parser call.
4. Top-level tags are resolved against one another and substituted recursively.
5. Dataset definitions are constructed as `Dataset` objects, applying the
   optional dataset defaults entry.
6. Whole-value dataset references are replaced by their corresponding objects.
   References with tag assignments use Dataset update behavior.
7. Workflow logging options are normalized.
8. Recognized ordinary top-level section entries are collected as ordered
   `WorkflowSection` objects and removed from the top-level option mapping.

This ordering creates dependencies: environment values can feed placeholders,
time and environment values can feed tags, and resolved tags can feed dataset
definitions. Dataset objects are not present during tag resolution, so tags
cannot depend on them. One `datetime` value is used for all `{now}` references
within a parser call. A list-valued top-level tag can expand the containing
string into a list of rendered strings. Workflow JSON syntax, reserved keys, and placeholder
examples belong to the [configuration guide](../configuration/overview.md).

For applications that already prepare a mapping in Python, see
[Configuration internals](configuration.md). Prefer the supported
`WorkflowDefinition.load()` / `run()` path for normal execution.

## Section construction, dispatch, and engine boundary

The parser scans non-reserved top-level keys in insertion order. A section
engine is determined by a section-level `engine`, then a workflow-level
default, then a recognized section-name alias. Current aliases map download
and ingest names to DOOR, process and publish names to DAM, and index and
calculate names to DRYES. Matching is normalized and an alias can occur in a
longer key such as `Download_01`. If no supported engine can be resolved,
parsing fails rather than treating an arbitrary key as a plugin.

A section value can be one mapping or a list. Lists are flattened into
separate numbered section entries in list order. The resulting
`workflow_sections` field is an internal parsed-workflow representation;
ordinary workflow input should use the section keys described in
[Workflow Sections guide](../configuration/workflow-sections.md). Section-level `engine`
and `exec_options` are extracted from the section definition; remaining
payload content is passed through for the selected engine.

At execution, d3tools calls the selected engine interface in order: DOOR's
data retrieval method, DAM's run method, or DRYES' compute method. The
payload schema, validation, and domain behavior remain owned by the relevant
engine project. Section execution is sequential; a section failure interrupts
the run rather than dispatching later sections. d3tools' current engine list
does not establish a general third-party engine extension contract.

See [Extensions and contributions — Integrate with workflow
engines](extensions.md#integrate-with-workflow-engines) for the change
boundary. For complete engine-specific configuration contracts, consult
[DOOR](https://github.com/c-hydro/door),
[DAM](https://github.com/c-hydro/dam), and
[DRYES](https://github.com/c-hydro/dryes).

## Execution ranges and sequential execution

`WorkflowDefinition.run()` accepts an explicit `TimeRange` or a start/end
pair, but not both. With an explicit `TimeRange`, d3tools passes that range to
each section without consulting the date environment variables. Otherwise,
each omitted boundary is filled independently: `start` from `START_DATE` and
`end` from `END_DATE`. Explicit boundaries take precedence over their
respective environment values. After that substitution, no start and no end
means no workflow-wide range, so each section resolves its own range. An end
without a start raises `ValueError`; a start without an end uses the current
time. Thus an explicit start with no explicit end uses `END_DATE` when set,
not necessarily the current time.

When a section derives its own range, execution options influence the
resolution. A section that omits `exec_options` inherits the workflow-level
mapping. A section that supplies `exec_options` replaces that entire mapping,
not individual keys; an empty section mapping clears all workflow defaults.
For each option lookup, an environment variable named by the upper-case
option takes precedence over the resulting configured mapping.

`all_available` asks for the range from the first to last available timestep
and is currently supported only for DAM sections; using it with another
engine or with no available data raises `ValueError`. This option takes
priority over `times_from_run`. When `times_from_run` is selected, its outcomes
are distinct:

- A restored range is used, with `repeat_window` applied if configured.
- A readable state that returns no range (for example, absent start/end
  values) causes the section to be skipped. It does not fall back to
  availability.
- A missing or unreadable state destination, or an error parsing its content,
  can raise and interrupt execution. These errors are not converted into a
  skip or an availability-based range.

Without either of these options selected, normal resolution uses the section object's
available/completed state to select the first unprocessed timestep through
the last available timestep. If all available timesteps are complete and no
repeat interval is set, the section has no range to execute. `repeat_window`
can extend the selected start backwards, including a range restored from
prior state, so earlier timesteps may be processed again.

Missing availability needed to calculate a range and invalid engine/option
combinations can raise errors. A section with no remaining work can be
skipped and reported as not executed. The option names, accepted values,
monthly splitting, and operator examples are owned by
[Workflow Sections — Execution Options](../configuration/workflow-sections.md#execution-options).
Do not treat the workflow's generic range selection as an engine-independent
interpretation of payload options.

Sections and list entries are processed in configuration order. A failure
stops later section execution; results are internal to d3tools and are not
returned by `run()`. Available section results are used by logging and
run-state output. See the generated
[configuration reference](reference/config.md) for the member-level API.

## Logging, exceptions, and cleanup lifecycle

`WorkflowDefinition` creates a `WorkflowLogManager` from the parsed
`workflow_log` option. The manager configures the d3tools logger and mirrors
its handlers to the DOOR, DAM, and DRYES package loggers. Workflow and
section execution contexts record start, completion or failure, and elapsed
time; an exception escaping a context is logged and re-raised. A failed
section therefore interrupts the ordered run.

`get_logger()` returns the manager's primary logger; with a name it returns
`logging.getLogger(name)`, which does not by itself attach the manager's
handlers to an arbitrary logger. `set_level()` accepts an integer threshold
or standard level name and updates every managed logger and the handlers
attached to the primary logger. Use it when changing the threshold after
construction rather than changing only one engine logger. Member details are
in the [logging reference](reference/logging.md).

Full workflow parsing has already resolved workflow-log `{now}` values in
the shared parser stage described above. In contrast, direct
`WorkflowLogManager.from_dict()` use resolves any `{now}` values still present
in its input before constructing destination Datasets. The log-normalization
stage alone (`prepare_workflow_log`) does not resolve these placeholders.

Structured run-state output is separate from the human-readable log. When a
run-state destination is configured, d3tools constructs a state mapping from
workflow and section outcomes and passes it to the manager for writing. The
run-state destination must support the supplied mapping. State is written
after the section loop returns; this description does not promise state
recovery after an exception.

The workflow owns its logging manager. `WorkflowDefinition.run()` does not
close it; a Python caller that manages a workflow lifetime may call
`workflow.logger.close()`. `close()` detaches and closes handlers and, for a
remote log destination, flushes the temporary file to the destination.
Cleanup is also registered for interpreter exit. Exit callback ordering,
timing, and behavior when a callback raises remain unresolved; do not rely on
them as a workflow contract. The open cleanup/API questions are tracked in
the maintainer's developer open-question register.

Python code can use the same manager contexts around work outside a workflow
run:

```python
from d3tools.logging import WorkflowLogManager

manager = WorkflowLogManager(log_file="logs/custom.log", console=False)
try:
    with manager.workflow_execution("custom_task"):
        with manager.section_execution("prepare"):
            print("Preparing data")
finally:
    manager.close()
```

The contexts log start and completion or failure, and re-raise exceptions.
`close()` removes and closes managed handlers; it is safe to call more than
once. This example uses a Python section label, not an engine-plugin contract.

Dataset write logs are a separate integration and belong to
[Datasets — Dataset logging and thumbnails](datasets.md#dataset-logging-and-thumbnails).
Operator logging options and output formats belong to
[Logging Overview](../logging/overview.md).
