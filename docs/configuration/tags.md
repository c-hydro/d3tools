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
            "path": "{output_dir}/raw/",
            "filename": "data_%Y%m%d.tif"
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
3. `{now}` and `{now:format}` placeholders are resolved. A `workflow_log.file`
   time placeholder is resolved when the workflow is loaded and its logging
   manager is constructed.
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
- If a tag value is a list, each string containing that placeholder expands
    into a list of rendered strings.

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

Dataset references can carry local update assignments. Their syntax and effects
are documented in [Datasets](datasets.md).

## Engine-Specific Cases

Top-level tags may supply values used by engine options. Section parsing and
engine-specific options are covered in [Workflow Sections](workflow-sections.md).

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
