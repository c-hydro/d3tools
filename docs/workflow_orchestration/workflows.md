# Workflows

## Define sections and choose an engine

Each ordinary workflow section is a non-reserved top-level JSON key. You can
name an engine explicitly on the section, use a workflow-level default, or
choose a recognised section-name alias.

| Section-name aliases | Engine |
|---|---|
| `door_downloader`, `downloader`, `download`, `ingest` | DOOR (`door`) |
| `dam_workflow`, `process`, `publish` | DAM (`dam`) |
| `dryes_index`, `index`, `calculate` | DRYES (`dryes`) |

Matching is case-insensitive and ignores dashes and spaces; an alias can occur
within a longer section name. A recognised section-level `engine` takes
precedence. If the section omits it, the workflow-level `engine` is used;
otherwise the section name is matched to an alias. A null or unsupported
section-level `engine` falls back to alias matching, not to the workflow
default. If no supported engine can be selected, loading fails.

Use the spelling shown in the selected engine's docs for section payloads.
D3tools selects the engine and orders sections; the schema and behaviour of the
engine-owned payload are defined by [DOOR](https://github.com/c-hydro/door/tree/main_new),
[DAM](https://github.com/c-hydro/dam), and
[DRYES](https://github.com/c-hydro/dryes). See also the
[section construction and engine dispatch](under-the-hood/lifecycle.md#section-construction-and-engine-dispatch).

## Control section order and lists

Sections run sequentially in their order in the JSON object. A section value
can be one definition or a list of definitions. Each list item is a separate
section, named with a two-digit suffix, and runs in list order. A list is not
parallel execution.

The combined [case-study workflow](case-study.md#run-the-whole-chain-as-one-workflow)
shows three sections in order: download, aggregate, then compute.

## Set execution ranges and options

The CLI accepts `--start` and `--end`. Python callers may use only the
documented `WorkflowDefinition.load()` and `run()` entry points in this guide:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("workflow.json")
workflow.run(start="2024-01-01", end="2024-01-31")
```

Explicit run arguments take precedence over `START_DATE` and `END_DATE` for
their respective boundaries. If a start is supplied without an end,
`END_DATE` is used when set, otherwise the current time; an end without a
start raises an error. If neither boundary is supplied by arguments or
environment, each section resolves a range using its own available and
completed data. A supplied workflow range is passed to every section.
Sections execute sequentially; a section failure stops later sections.

`exec_options` can be set at the workflow level. A section-level
`exec_options` replaces that whole mapping for the section; it does not merge
individual keys. For each option, a same-named upper-case environment
variable (for example `REPEAT_WINDOW`) takes precedence over configuration.

| Option | Operator effect |
|---|---|
| `times_from_run` | Use a range saved in a run-state file. A string can name a file or use `section_name@file`; an object accepts `file` and optional `section`. A readable state without a range skips that section. Missing/unreadable state or malformed state can fail the run. |
| `all_available` | For DAM sections only, use the first-to-last available timestep. Other engines, or no available data, produce an error. |
| `repeat_window` | Extend a selected range backwards by a duration such as `7d`; this can reprocess data already completed. |
| `split` | For a range longer than 31 days, process monthly sub-ranges clipped to the requested start and end. |

When deriving a range from availability, a section normally starts after its
last completed timestep and ends at the last available timestep. No remaining
work can result in a skipped section; missing availability needed to calculate
the range can raise an error. See [Datasets — time and availability](datasets.md#understand-time-and-availability).

## Keep engine responsibilities clear

D3tools resolves sections to DOOR, DAM, or DRYES and runs them in order. It
does not define a universal schema for engine payloads. A key observed in one
workflow is not necessarily required or supported in every workflow. Use the
complete configuration contract published by the selected engine; the
[case study](case-study.md) illustrates D3tools integration without
attempting to document engine options.
