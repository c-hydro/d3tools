# Architecture

## Responsibilities and external boundaries

d3tools supplies shared workflow orchestration and reusable Python
infrastructure. A workflow passes through configuration resolution and
dataset construction, then its ordered sections are handed to an engine. The
engine performs the section's domain work:

| Component | Responsibility |
|---|---|
| d3tools | Parse workflow options; resolve environment values, time placeholders, tags, and dataset references; construct Dataset objects and section entries; determine or pass execution ranges; run sections sequentially; provide shared data, logging, timestepping, spatial, and utility APIs. |
| DOOR | Owns downloader section behavior and its payload schema. |
| DAM | Owns processing and publishing behavior and its payload schema. |
| DRYES | Owns index calculation behavior and its payload schema. |
| Host Python application | Chooses when to load and run a workflow, and can consume supported d3tools objects and helpers. |

The engine names and section aliases identify the current d3tools dispatch
targets; they do not establish a general third-party engine plugin contract.
d3tools forwards the engine's section payload and does not define the meaning
of engine-specific options. See [Workflow lifecycle](workflow-lifecycle.md)
for parsing and dispatch, and [Extensions and contributions](extensions.md)
for the supported integration boundary.

The common Python path is `WorkflowDefinition.load()` followed by
`WorkflowDefinition.run()`. The resulting object exposes parsed `options`,
ordered `workflow_sections`, workflow `tags`, and constructed `datasets`;
these are properties of the workflow object, not a promise that arbitrary
configuration keys become Python attributes. The generated
[configuration reference](reference/config.md) documents the objects and
modules.

## Public, compatibility, helper, and internal surfaces

Prefer documented package imports and the `WorkflowDefinition` entry point.
Submodules expose additional helpers, so a name being importable from an
implementation module does not by itself make it a supported top-level API.
Use the module-specific generated references for member documentation.

| Surface | Guidance |
|---|---|
| Workflow entry points | `WorkflowDefinition` and `Options` are re-exported from `d3tools`; the curated configuration exports are documented under [`d3tools.config`](reference/config.md). |
| Dataset abstractions | `Dataset`, `LocalDataset`, `RemoteDataset`, and `MemoryDataset` are available from `d3tools.data`. `S3Dataset` and `SFTPDataset` are exposed by the remote-data implementation when its optional imports are available. See [Data reference](reference/data.md). |
| Supported legacy module paths | `d3tools.data.dataset`, `d3tools.data.local_dataset`, `d3tools.data.memory_dataset`, and `d3tools.data.remote_dataset` alias the current dataset modules. Timestepping also installs legacy submodule paths such as `d3tools.timestepping.timestep` and `d3tools.timestepping.timerange`. New code should use the current package/module paths documented in the [data](reference/data.md) and [timestepping](reference/timestepping.md) references. |
| Parsing | The package `d3tools.parse` exposes key parsing and rendering utilities; legacy names from `parse_utils` remain available for compatibility. New implementation work belongs in the focused parsing modules. See [Parsing reference](reference/parse.md). |
| Exceptions | `d3tools.errors` imports the package's GDAL, workflow-import, and template exception classes. See [Errors reference](reference/errors.md); do not infer untested error attributes or message formatting. |
| Spatial and timestepping names | Package-level wildcard imports may expose implementation dependencies or helper names. Such incidental names are not promises of a supported API. Use the documented spatial and timestepping classes, and do not depend on leaked imports. |
| Helpers and internals | Pipeline-stage helpers and implementation methods may be useful within d3tools but are not interchangeable with the supported Python workflow entry path. Check the exact reference page and preserve conservative assumptions where no public contract is stated. |

For example, `resolve_now()`, `prepare_workflow_log()`, and
`WorkflowSectionRunResult` appear at helper/submodule level rather than in the
curated `d3tools.config` export list. Their appearance in the
[configuration reference](reference/config.md) is not equivalent to a root
package export.

The `ExitHandler` export decision and exit callback behavior have not been
settled. The package currently exposes selected exit helper functions; do not
assume undocumented callback ordering or timing. See [Errors, exits, and
notification boundary](python-apis.md#errors-exits-and-notification-boundary)
and the maintainer-tracked developer open-question register.

## Experimental and unsupported features

`EmailNotification` is experimental and unsupported for current use. Its
presence in the generated [notification reference](reference/notification.md)
does not imply production readiness, reliability, or an operational contract.
No notification workflow integration is recommended.
