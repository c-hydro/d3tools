# Workflow orchestration

## Start here

This section is for operators who write workflow JSON and run it. Start with
[installation and a minimal run](getting-started.md), then use the task guides
for [configuration](configuration.md), [datasets](datasets.md),
[workflows and execution ranges](workflows.md), and [logging](logging.md).
The [CHIRPS-to-SPI case study](case-study.md) is the first complete
engine-backed journey; [examples](examples.md) and
[troubleshooting](troubleshooting.md) help with common next steps.

D3tools has two related roles. As an orchestrator, it reads workflow JSON,
resolves shared configuration, runs sections in order, and delegates their
domain work to an engine. As a Python package, it also provides standalone
[tools](../tools/index.md) for timestepping, datasets, spatial operations,
cases, thumbnails, cleanup, and notification. The orchestration documentation
uses Python only where needed to load and run a `WorkflowDefinition`; use the
Tools section for direct Python usage and the [API reference](../reference/config.md)
for signatures and members.

## Engine responsibilities

| Engine | Responsibility in a workflow |
| --- | --- |
| [DOOR](https://github.com/c-hydro/door/tree/main_new) | Download source data. |
| [DAM](https://github.com/c-hydro/dam) | Process and aggregate data. |
| [DRYES](https://github.com/c-hydro/dryes) | Calculate indicators. |

D3tools owns the shared workflow structure, configuration resolution, dataset
objects, execution order, and logging around these sections. Each engine owns
the payload schema and domain behaviour inside its section.

## Find the right page

- [Getting started](getting-started.md): install D3tools, load a minimal
  workflow, and use the command-line runner.
- [Configuration](configuration.md): define top-level settings, environment
  values, tags, and dataset references.
- [Datasets](datasets.md): configure dataset keys, formats, storage, time, and
  relationships in workflow JSON.
- [Workflows](workflows.md): choose engines, order sections, and control
  execution ranges and options.
- [Logging](logging.md): configure workflow and dataset logs.
- [Case study](case-study.md): follow the CHIRPS download, aggregation, and SPI
  workflow.
- [Examples](examples.md): start from smaller workflow configurations.
- [Troubleshooting](troubleshooting.md): diagnose installation, configuration,
  execution, and output failures.
- Under the hood: follow parsing, dispatch, cleanup, architecture, and engine
  integration. These pages are being prepared as part of the 2.4.0
  documentation refresh.
