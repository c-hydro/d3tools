# Configuration internals

## Configuration contract and parser stages

Configuration accepts the same declarative shape whether it comes from JSON
or from a Python mapping. The supported workflow entry point is
`WorkflowDefinition.load()` / `run()`; a mapping is not a separate
configuration language. The parser wraps plain mappings in `Options` and
resolves them before creating the workflow object.

The parsing stages and their dependencies are described in
[Workflow lifecycle](workflow-lifecycle.md#workflow-loading-and-parsing-lifecycle).
This page focuses on the Python-facing boundaries:

- reserved workflow controls remain options; other top-level keys are
  interpreted as workflow sections and must resolve to a supported engine;
- `Options` holds parsed controls and supports case-insensitive access used by
  the pipeline;
- dataset definitions are converted to Dataset objects before whole-value
  references to them are substituted;
- section collection stores ordered `WorkflowSection` values in the internal
  `workflow_sections` field; and
- engine construction and payload behavior belong at the engine boundary.

The mapping passes through these parser stages, in order:

1. Apply the top-level `env` mapping to `os.environ`.
2. Resolve environment placeholders throughout the nested options.
3. Resolve `{now}` placeholders using one current datetime for the parse call.
4. Resolve tag values against the tag mapping, then recursively substitute
   them through the options structure.
5. Replace dataset definitions with Dataset objects. The reserved dataset
   `__defaults__` entry is removed and used as construction defaults.
6. Replace resolvable whole-string dataset references with those Dataset
   objects; tag assignments use the Dataset update behavior.
7. Normalize the workflow-log value and retain unknown mapping entries while
   warning about keys outside the recognized set.
8. Collect recognized workflow section keys into the internal ordered section
   list and remove those keys from the top-level options.

The ordering is a dependency contract: environment and current-time values
can feed tags, and resolved tags can feed datasets, but datasets are not
available while tags are being resolved. A tag whose value is a list can
expand its containing string into a list of strings. Dataset references are
resolved after construction; an unmatched reference stays a string, while a
prefixed reference only matches when the prefix exactly names the literal
datasets top-level key.

`WorkflowDefinition` is the execution-oriented wrapper around the pipeline.
Its normal defaults build runtime engine objects and propagate strict engine
import failures. Direct `parse_options()` is useful when a host needs parsed
options without immediately building runtime engine objects; its documented
defaults are less strict. Prefer the workflow wrapper for execution rather
than depending on those lower-level flags.

Environment assignments in the `env` mapping are process side effects: values
are converted to strings and copied into `os.environ` before placeholder
resolution. An unset environment placeholder without a fallback raises
`ValueError`; a top-level `env` value other than a mapping or `None` raises
`TypeError`. For operator syntax, placeholder forms, and safe secret handling,
see the [configuration guide](../configuration/overview.md).

The `d3tools.config` package publishes a curated set of workflow, parser, and
section objects. Its submodules expose additional implementation-level stages.
Do not treat every helper in a submodule as a top-level supported API; see the
[configuration reference](reference/config.md) and
[Architecture — Public, compatibility, helper, and internal
surfaces](architecture.md#public-compatibility-helper-and-internal-surfaces).
The curated package exports include `WORKFLOW_SECTION_ALIASES`,
`WorkflowSection`, `WorkflowDefinition`, `Options`, `dataset_from_config`,
`workflow_from_config`, `resolve_env`, `resolve_tags`, `build_datasets`,
`resolve_dataset_refs`, `collect_workflow_sections`, and `parse_options`.
Lower-level names such as `resolve_now`, `prepare_workflow_log`, and
`WorkflowSectionRunResult` are documented in their submodules/reference but
are not part of this curated export list.

## Standalone parsing and Python integration

Use `WorkflowDefinition.load()` for JSON workflow files and `run()` to execute
the collected sections through d3tools' normal lifecycle:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("workflow.json")
workflow.run(start="2024-01-01", end="2024-12-31")
```

`parse_options()` is useful to a Python host that needs d3tools to normalize
an options mapping without immediately running the workflow. The mapping must
follow the workflow JSON structure, including reserved controls and ordinary
engine section keys. It returns parsed options with constructed datasets and
collected workflow sections; it is not a recipe for inventing a parallel
object model.

```python
from d3tools.config.parsing_pipeline import parse_options

options = {
    "workflow_name": "daily_download",
    "tags": {"source": "ERA5"},
    "datasets": {},
    "door_downloader": {"engine": "door"},
}

parsed_options = parse_options(options)
```

Use `WorkflowDefinition`'s supported attributes such as `workflow.tags` and
`workflow.options` when working with a loaded workflow. Avoid assuming that
arbitrary workflow keys are exposed as attributes. The generated
[configuration reference](reference/config.md) documents the Python objects.

Python applications may set environment variables before loading a workflow.
The top-level configuration `env` control is distinct: it modifies
`os.environ` while the options mapping is parsed.

```python
import os

from d3tools import WorkflowDefinition

os.environ["DATA_ROOT"] = "/srv/example-data"
workflow = WorkflowDefinition.load("workflow.json")
```

## Resolution and compatibility helpers

The parser resolves environment values, `{now}` values, tags, and dataset
references in stages. Standalone parsing helpers serve a narrower role and
should not be confused with a second workflow pipeline.

`KeyParser` is the shared implementation for dataset key patterns. A pattern
can combine `strftime` date directives with named tags such as `{tile}` or
`{variable}`. Rendering resolves those inputs to a concrete storage key;
matching parses a concrete key into a `ParsedKey` containing its original
key, parsed time, and extracted tags. Prefix calculation supports catalogue
discovery. The parser handles logical/storage date conversion and overlap
expansion using the dataset time signature; Dataset and DataCatalogue use
these operations so rendering and discovery agree. See the
[parsing reference](reference/parse.md) and
[Datasets — Availability and catalogue contract](datasets.md#availability-and-catalogue-contract).

`substitute_string()` and `substitute_values()` provide reusable string and
nested-structure rendering. Normalization helpers support alias and tag
matching; `increment_version()` is used for version-like values. Their exact
member behavior belongs to the [Parsing reference](reference/parse.md),
not to a copied signature list here.

The compatibility helpers `set_env()` and `set_dataset()` resolve
placeholders in Python structures:

- `set_env()` recursively rebuilds dictionaries and lists and substitutes
  environment placeholders. A missing variable without a non-empty default
  raises `ValueError`.
- `set_dataset()` resolves a dataset placeholder only when it occupies the
  entire string. Nested dictionaries and lists are rebuilt. A tagged
  reference invokes the mapped object's `update()` method and uses its return
  value as the replacement. Workflow dataset-reference resolution uses this
  same helper, not a different update contract. For a `Dataset`, default
  `update()` behavior returns an updated copy and leaves the original Dataset
  unchanged; retain the returned replacement. Other mapped objects follow
  their own `update()` contract and may mutate themselves or return `None`.
  Dataset copy and preservation details belong to
  [Datasets — Construction, composition, and key rendering](datasets.md#construction-composition-and-key-rendering).

Legacy helper names from `d3tools.parse.parse_utils` remain available for
compatibility. New implementation work belongs in focused modules such as
`key_parser`, `string_rendering`, and `config_resolution`. The package-level
re-exports and implementation modules are listed in the
[Parsing reference](reference/parse.md); examples of configuration placeholders
belong to [Configuration Overview](../configuration/overview.md).
