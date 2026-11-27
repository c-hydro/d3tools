# Architecture and engine integration

This page defines the boundary between D3tools and the workflow engines. It
is intended for maintainers and engine integrators. Operators normally need
the [workflow](../workflows.md) and [configuration](../configuration.md)
pages instead.

## Responsibilities and boundaries

D3tools has two related roles: it orchestrates multi-engine workflows, and it
provides reusable Python tools. The workflow layer resolves shared
configuration, constructs datasets, preserves section order, selects
execution ranges, dispatches sections, and records workflow-level logs. It
does not own the domain schema or processing performed inside an engine
section.

| Component | Owned responsibility |
| --- | --- |
| D3tools | Workflow loading and parsing, shared datasets, section ordering, execution ranges, dispatch, workflow logging, and run-state output. |
| [DOOR](https://github.com/c-hydro/door/tree/main_new) | Download configuration and data acquisition. |
| [DAM](https://github.com/c-hydro/dam) | Processing, aggregation, and publication configuration. |
| [DRYES](https://github.com/c-hydro/dryes) | Indicator configuration and computation. |
| Calling application | Chooses when to load and run a workflow and manages the resulting objects' lifetime. |

Engine payloads have already passed through the shared parsing pipeline when
they reach an engine: environment, time, and tag placeholders are resolved;
dataset definitions are constructed; and whole-value dataset references are
replaced by dataset objects. Each engine still owns the meaning and validation
of the remaining payload. See the [workflow lifecycle](lifecycle.md) for the
stage-by-stage data flow.

## Module map

The current package contains both orchestration and standalone-tool modules:

| Area | Modules | Role |
| --- | --- | --- |
| Workflow orchestration | `d3tools.config` | `WorkflowDefinition`, `WorkflowSection`, options, parsers, and the configuration pipeline. |
| Configuration resolution | `d3tools.parse` | Environment, tag, string, dataset-reference, and dataset-key parsing shared by workflows and tools. |
| Workflow logging | `d3tools.logging` | Workflow and section logging, handler setup, and run-state output. |
| Dataset tools | `d3tools.data` | Dataset abstractions, storage backends, formats, and catalogue queries. |
| Time and space tools | `d3tools.timestepping`, `d3tools.spatial` | Execution-range values, calendar operations, bounding boxes, and raster templates. |
| Supporting tools | `d3tools.cases`, `d3tools.thumbnails`, `d3tools.exit`, `d3tools.notification` | Case expansion, previews, cleanup callbacks, and experimental notification support. |

The Tools documentation owns direct Python use of these reusable modules.
The orchestration pages own workflow JSON and engine coordination. Keeping
those responsibilities explicit also leaves room for the two roles to be
separated in a future package layout without treating today's module paths as
a plugin architecture.

## Public and compatibility surfaces

Use documented package exports and the [API reference](../../reference/config.md)
as the supported surface. Importability from an implementation module alone
does not make a helper a stable public contract.

- `d3tools` re-exports `WorkflowDefinition`, `Options`,
  `WORKFLOW_SECTION_ALIASES`, and `WorkflowSection`.
- `d3tools.config` exports the workflow types plus the documented dataset and
  pipeline parsing functions. Lower-level stages such as `resolve_now()` and
  `prepare_workflow_log()` remain implementation helpers even though they can
  be imported from their defining module.
- `d3tools.data` exposes the core dataset classes. It also installs the legacy
  module paths `d3tools.data.dataset`, `local_dataset`, `memory_dataset`, and
  `remote_dataset`; new code should use the current paths documented in the
  [data reference](../../reference/data.md).
- `d3tools.timestepping` retains legacy module aliases including `timestep`,
  `timerange`, `timeperiod`, and the fixed-timestep modules. New code should
  use the imports in the [timestepping guide](../../tools/timestepping.md).
- `d3tools.parse.parse_utils` and its re-exported names are compatibility
  surfaces. New parsing implementation belongs in the focused parsing
  modules documented by the [parsing reference](../../reference/parse.md).
- `Options.load()` is a deprecated compatibility wrapper. New workflow code
  should call `WorkflowDefinition.load()`.

Remote dataset classes are available only when their optional dependencies
can be imported. `EmailNotification` is explicitly experimental and is not a
production workflow integration; see [Exit handling and
notification](../../tools/exit-and-notification.md#experimental-email-notification).

## Engine construction contract

Engine integration is a fixed, built-in mapping rather than a third-party
plugin registry. While collecting sections, D3tools chooses `door`, `dam`, or
`dryes` from a valid section-level engine, the workflow default, or the
recognised section-name aliases. The selected builder receives the resolved
section payload after `engine` and `exec_options` have been removed:

| Engine | Construction call | Execution call |
| --- | --- | --- |
| DOOR | `Downloader.from_options(section_options)` | `get_data(time_range)` |
| DAM | `DAMWorkflow.from_options(section_options)` | `run(time_range)` |
| DRYES | `DRYESIndex.from_options(**section_options)` | `compute(time_range, make_parameters=setup)` |

A DRYES payload must be a mapping. Its `setup` value is true when the `SETUP`
environment variable is `1`, `true`, or `yes`, ignoring case. All three
runtime objects must provide `get_last_ts()` when D3tools derives a range from
availability; DAM must additionally provide `get_first_ts()` when
`all_available` is enabled. The timestep values returned by those methods
must support the operations used to construct a D3tools `TimeRange`.

`WorkflowSection.run()` passes the same explicit workflow range to each
section, or derives a range separately for each section. It calls only the
engine-specific execution method in the table and treats a normally returned
call as success. An escaping exception stops the sequential workflow and is
re-raised after logging. Engines therefore retain responsibility for raising
when their payload or processing fails.

Normal `WorkflowDefinition` construction builds engine objects and treats
engine import failures as errors. Inspection code can disable construction
with `build_workflow_objects=False`; the retained payload is useful for
inspection but does not satisfy the execution contract.

## Changing or adding an engine integration

Adding an engine requires coordinated source changes; configuration alone
cannot register one. A contributor must update section recognition, the
builder mapping, execution dispatch, range behaviour, logging integration, and
the relevant engine-owned documentation. Preserve these boundaries:

1. Keep shared parsing, ordering, range selection, logging, and result
   reporting in D3tools.
2. Keep domain payload semantics and processing in the engine package.
3. Pass constructed datasets and resolved shared values through the existing
   payload instead of introducing a second configuration path.
4. Add tests for construction, missing imports, aliases and defaults,
   explicit and derived ranges, success, skipping, and stop-on-failure order.
5. Update operator examples only after the engine configuration has been
   verified against the corresponding engine version.

Do not describe a new integration as supported until every fixed dispatch
site is updated and cross-package behaviour has been exercised.

## Contributor checks

For architecture or engine-boundary changes:

- run the focused configuration and workflow tests, then the full test suite
  with the repository's supported environment;
- run `scripts/check_docs.py` for changed Markdown and finally for all docs;
- verify every `mkdocs.yml` navigation target and local link exists;
- keep complete JSON examples parseable and Python examples importable;
- update API docstrings when a public signature or member contract changes;
  and
- coordinate engine-owned schema changes with DOOR, DAM, or DRYES rather than
  duplicating their complete option catalogues here.

The documentation checker validates code fences, workflow-shaped JSON, and
local links. It does not prove that an engine payload is operational, so
engine-backed examples still require explicit integration verification before
publication.
