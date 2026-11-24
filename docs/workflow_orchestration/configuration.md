# Configuration

## Workflow JSON shape

A workflow file is a JSON object. Reserved top-level keys configure d3tools;
every other top-level key is interpreted as a workflow section and must select
a recognized engine.

| Top-level key | Value | Purpose |
|---|---|---|
| `workflow_name` | string | Name used in workflow logging and run-state output; defaults to `workflow`. |
| `tags` | object | Reusable values substituted into configuration strings. |
| `datasets` | object | Named storage and key definitions used by sections. |
| `env` | object or `null` | Sets process environment values before placeholder resolution. |
| `workflow_log` | string, object, or `null` | Workflow log and optional run-state destinations. |
| `engine` | string | Default engine for sections that omit `engine`. |
| `exec_options` | object | Default execution-range options for sections. |
| `workflow_sections` | array | Internal/advanced parsed-section input; ordinary JSON should use named section keys instead. |
| `other_options` | object | Additional values retained as workflow options; not interpreted as sections. |

The usual workflow has `tags`, `datasets`, optional logging and execution
settings, and one or more engine sections. Use a section key such as
`download`, `process`, or `index`; section syntax is covered in
[Workflows](workflows.md). This is the JSON shape, not a second Python
configuration API. See [Workflow loading and
parsing](under-the-hood/lifecycle.md#workflow-loading-and-parsing) for parser
behaviour.

## Environment values, tags, and time placeholders

Environment placeholders use `{ENV.NAME}`. An optional single-quoted default
prevents an unset non-secret value from stopping the load:

```json
{
  "tags": {
    "data_root": "{ENV.DATA_ROOT, default='/tmp/workflow-data'}",
    "run_day": "{now:%Y%m%d}",
    "output_dir": "{data_root}/{run_day}"
  }
}
```

The CLI shell can provide environment values:

```bash
export DATA_ROOT="/srv/my-workflow-data"
python scripts/run_workflow.py workflow.json
```

Alternatively, the workflow's top-level `env` object sets values in the
current process before resolution:

```json
{
  "env": {
    "DATA_ROOT": "/tmp/workflow-data"
  },
  "tags": {
    "data_root": "{ENV.DATA_ROOT}"
  }
}
```

Values from `env` are converted to strings and affect the whole Python process,
not just one workflow. Do not put credentials in committed JSON; use the
process environment or a secret manager. d3tools does not load `.env` files
itself.

The operator-visible resolution order is: values in `env`, `{ENV...}`
placeholders, `{now...}` time placeholders, then recursive top-level tags.
Dataset definitions are created next, and dataset references are resolved
after that. Thus tags can use environment/time values but cannot refer to
datasets. Tag names are case-sensitive; tags can refer to other tags. If a
tag's value is a list, a string containing it expands into a list of rendered
strings. `{tag:format}` uses date formatting for datetime values and Python
format specifications for other values. Unknown tag placeholders are left as
strings. `{now:...}` normally uses the time the workflow is loaded. A
`workflow_log.file` time placeholder is also resolved during workflow loading,
when its logging manager is constructed, not when the workflow runs.

Without a default, an unset `{ENV.NAME}` raises a `ValueError` during loading.
Environment defaults must use the form
`{ENV.NAME, default='fallback'}`; an empty default is treated as missing.
Together with the tag and formatting rules above, this section is the owning
reference for workflow environment and tag syntax.

## Reference and update datasets

Define a dataset once and use its reference as the complete string value in a
section. The unqualified form `{raw_data}` resolves by dataset name. The
qualified form `{datasets.raw_data}` is also accepted when the top-level key
is literally `datasets`; the prefix is case-sensitive.

```json
{
  "datasets": {
    "raw_data": {
      "type": "local",
      "key_pattern": "/tmp/workflow-data/raw_%Y%m%d.tif"
    }
  },
  "process": {
    "input": "{raw_data}"
  }
}
```

The section payload above is illustrative; the selected engine defines
whether `input` and the rest of that section are valid. Do not add surrounding
text to a dataset placeholder. A reference that does not match a dataset may
remain as a literal string; a reference with update assignments to an
unknown dataset can instead raise an error.

Dataset update tags create a use-specific configured dataset, for example
when one key pattern contains `{variable}`:

```json
{
  "datasets": {
    "variable_data": {
      "type": "local",
      "key_pattern": "/tmp/workflow-data/{variable}_%Y%m%d.tif"
    }
  },
  "download": {
    "destination": "{variable_data, variable='precipitation'}"
  }
}
```

Update tags belong to the dataset reference and are distinct from the
workflow-level `tags` object. See [datasets](datasets.md) for definitions and
[Tools — Dataset composition](../tools/datasets.md#composition-versions-and-tiles)
for object-level details.

## Diagnose configuration errors

- **Unset environment value:** provide the variable in the shell or top-level
  `env`, or add a safe default to `{ENV.NAME, default='...'}`. Never use a
  default to conceal a missing secret.
- **Dataset placeholder remains literal:** check that the dataset is defined,
  that the placeholder occupies the entire string, and that a qualified
  prefix matches the exact top-level key spelling. Check the tag syntax if
  the reference carries update assignments.
- **Unknown top-level key or section:** all unreserved keys are treated as
  sections. Correct a misspelled reserved key, or give the section a
  recognized alias or explicit supported engine.
- **Execution option appears to be ignored:** place it inside `exec_options`
  at workflow or section level, not alongside section payload settings.
- **Wrong engine seems selected:** check the section's `engine`, the workflow
  default, and its section-name alias. A present but null or unsupported
  section `engine` falls back to alias matching rather than the workflow
  default.

For dataset, execution-range, and engine-validation failures, follow the
corresponding task page: [Datasets](datasets.md), [Workflows](workflows.md),
or [Troubleshooting](troubleshooting.md). Engine-specific payload validation
belongs to the engine project, not the d3tools configuration contract.
