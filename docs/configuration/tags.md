# Configuration Tags

The word “tag” is used for several related but distinct mechanisms in a d3tools
workflow. This page separates them:

1. **Top-level configuration tags** are the key/value pairs under `tags`. d3tools
   substitutes these placeholders throughout the configuration.
2. **Dataset update tags** are values supplied on a dataset reference, such as
   `{DATASETS.my_data, member='value'}`. They create a configured copy of a
   dataset for that use.
3. **Engine-specific case tags or options** are interpreted by DOOR, DAM, or
   DRYES after d3tools has parsed the section. They allow one section to describe
   multiple variables, parameter sets, or processing cases where that engine
   supports them.

Only the first two mechanisms are defined by d3tools itself. The third depends
on the engine and on the option being configured.

## Top-Level Configuration Tags

Top-level tags are key/value pairs declared under the `tags` object. During
configuration parsing, d3tools resolves them recursively against one another and
then substitutes them throughout the complete configuration tree, including
dataset definitions and workflow sections.

```json
{
    "tags": {
        "source": "ERA5",
        "output_dir": "/data/{source}"
    },
    "datasets": {
        "output_data": {
            "type": "local",
            "path": "{output_dir}/raw/"
        }
    }
}
```

Here `{source}` is resolved inside `output_dir`, and `{output_dir}` is then
resolved inside the dataset definition.

### Recursive Resolution

Tags may refer to other tags:

```json
{
    "tags": {
        "root": "/data",
        "source": "ERA5",
        "source_dir": "{root}/{source}",
        "output_dir": "{source_dir}/output"
    }
}
```

`{output_dir}` resolves to `/data/ERA5/output`. Tag substitution is performed
recursively before the remaining configuration is processed.

### Resolution Order

The parsing pipeline applies configuration substitutions in this order:

1. Values in the top-level `env` object are copied to `os.environ`.
2. `{ENV.VAR}` placeholders are resolved.
3. `{now}` and `{now:format}` placeholders are resolved, except for the
   deliberately deferred `workflow_log.file` case.
4. The top-level `tags` mapping is resolved recursively and substituted
   throughout the configuration.
5. Dataset definitions are built.
6. Dataset references are resolved, including any dataset update tags.
7. Workflow sections are collected and their engine-specific objects may be
   built.

This means top-level tags can use environment and time placeholders, and dataset
definitions can use both of them. Top-level tags cannot refer to datasets because
datasets do not exist until after tag resolution.

```json
{
    "tags": {
        "data_root": "{ENV.DATA_ROOT, default='/data'}",
        "run_stamp": "{now:%Y%m%d}",
        "output_dir": "{data_root}/{run_stamp}"
    }
}
```

See [Environment Variables](environment.md) and the
[Configuration Overview](overview.md) for the complete pipeline.

### Placeholder Syntax

The general top-level tag forms are `{tag}` and `{tag:format}`:

- `{tag}` substitutes the tag value;
- `{tag:format}` applies `strftime` formatting to datetime values and Python
  format specifications to other values;
- an unknown top-level tag is left unchanged.

Environment placeholders use a related but separate syntax:
`{ENV.VAR_NAME}` or `{ENV.VAR_NAME, default='value'}`. They are resolved before
top-level tags, so they can be used to define a tag value, for example
`"data_root": "{ENV.DATA_ROOT, default='/data'}"`. They are not entries in the
top-level `tags` mapping themselves. See [Environment Variables](environment.md)
for the complete syntax and behaviour of environment placeholders.

```json
{
    "tags": {
        "run_date": "2024-06-01",
        "resolution": 0.1
    },
    "some_config": {
        "date_label": "{run_date:%Y-%m}",
        "res_label": "{resolution:.2f}"
    }
}
```

Tag names are case-sensitive: `{Source}` and `{source}` refer to different keys.
Nested tag mappings are substituted as values, but they do not provide dotted
tag lookup. For example, `{paths}` can substitute the whole `paths` mapping,
whereas `{paths.input}` is not a nested lookup into that mapping.

## Dataset Update Tags

Once dataset definitions have been built, a dataset reference may include tag
assignments. The assignments are passed to `Dataset.update()`, which creates an
updated dataset copy with those values applied to its name, key pattern, and
dataset tags.

```json
{
    "datasets": {
        "my_data": {
            "type": "local",
            "path": "/data/{member}",
            "filename": "data_%Y%m%d.tif"
        }
    },
    "door_downloader": {
        "destination": "{DATASETS.my_data, member='control'}"
    }
}
```

The same dataset definition can therefore be reused with different values:

```json
{
    "door_downloader": {
        "destination": "{DATASETS.my_data, member='control'}"
    },
    "dam_workflow": {
        "input": "{DATASETS.my_data, member='experiment'}"
    }
}
```

Dataset references may use the qualified or unqualified forms supported by the
dataset resolver, for example `{DATASETS.my_data}`, `{datasets.my_data}`, or
`{my_data}`. The intended usage is for the complete string value to be the
dataset reference, including its optional assignments.

These assignments are not entries in the top-level `tags` object. They are local
values supplied for that particular dataset reference. See
[Datasets](datasets.md) for dataset definitions and reference syntax.

## Engine-Specific Cases

After d3tools resolves top-level tags and dataset references, it passes each
workflow section to the selected engine. DOOR, DAM, and DRYES may then interpret
their own option structures as multiple cases. The exact syntax belongs to the
engine, not to this document.

Examples of engine-level behaviour include:

- **DOOR** options that accept a list of variables or products, potentially
  causing multiple variables to be downloaded;
- **DAM** process and case structures that use mapping-valued options to create
  combinations of processing settings;
- **DRYES** index options that define multiple parameter or input cases.

Top-level tags can be useful for generating these values, but d3tools only
performs the substitution. Whether the resulting list or mapping creates
parallel cases, sequential work, or an invalid configuration is decided by the
engine's own parser and runtime.

Consult the relevant engine documentation for the supported case structures:

- [Workflow Sections](workflow-sections.md) for how d3tools selects and passes
  sections to DOOR, DAM, and DRYES;
- the DOOR documentation for downloader variables and products;
- the DAM documentation for process and case options;
- the DRYES documentation for index and parameter cases.

## Common Patterns

### Reusable Paths

```json
{
    "tags": {
        "data_root": "/mnt/data",
        "project": "drought_monitoring",
        "source": "ERA5",
        "project_dir": "{data_root}/{project}",
        "source_dir": "{project_dir}/{source}"
    },
    "datasets": {
        "raw_data": {
            "type": "local",
            "path": "{source_dir}/raw",
            "filename": "data_%Y%m%d.tif"
        }
    }
}
```

### Combining Environment and Top-Level Tags

```json
{
    "tags": {
        "data_root": "{ENV.DATA_ROOT, default='/data'}",
        "source": "ERA5",
        "output_dir": "{data_root}/{source}"
    }
}
```

See [Configuration Overview](overview.md) for the full parsing pipeline and
[Environment Variables](environment.md) for `{ENV.VAR}` syntax.
