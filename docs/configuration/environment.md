# Configuration Environment Variables

`{ENV.VAR_NAME}` placeholders let a configuration reference OS environment
variables. They are resolved first in the parsing pipeline, by
`d3tools.config.parsing_pipeline.resolve_env`.

## Syntax

```
{ENV.VAR_NAME}
{ENV.VAR_NAME, default='fallback value'}
{ENV.VAR_NAME, 'fallback value'}
```

- The variable name may contain letters, digits, underscores, `#`, `-`, and `.`.
- A default value is optional, must be wrapped in **single quotes**, and the
  `default=` keyword itself is optional (`{ENV.VAR, 'x'}` and
  `{ENV.VAR, default='x'}` are equivalent).
- If the variable is unset and no default is given, parsing raises:

  ```
  ValueError: Environment variable VAR_NAME is not set and no default value provided
  ```

```json
{
    "tags": {
        "api_key": "{ENV.ERA5_API_KEY}",
        "data_root": "{ENV.DATA_ROOT, default='/data'}"
    }
}
```

`{ENV.VAR}` placeholders can appear anywhere in the config (not just under
`tags`), since `resolve_env` runs over the whole options tree before tags,
datasets, or workflow sections are processed.

## The `env` Top-Level Key

Separately from `{ENV.VAR}` placeholders, a top-level `env` object lets a config
set environment variables directly, before any resolution happens:

```json
{
    "env": {
        "DATA_ROOT": "/mnt/data",
        "LOG_LEVEL": "DEBUG"
    },
    "tags": {
        "data_root": "{ENV.DATA_ROOT}"
    }
}
```

Each entry is applied with `os.environ[key] = str(value)` before `resolve_env`
runs, so a config can define its own environment values instead of (or in
addition to) relying on the shell. This also means values set here affect the
whole process, including code outside of this workflow run.

## Setting Environment Variables Externally

### Shell

```bash
export ERA5_API_KEY="your-api-key"
export DATA_ROOT="/mnt/data"
```

### Python, before loading the workflow

```python
import os
os.environ["ERA5_API_KEY"] = "your-api-key"

from d3tools import WorkflowDefinition
wf = WorkflowDefinition.load("workflow.json")
```

### `.env` files

d3tools does not load `.env` files itself. If you want that workflow, install
`python-dotenv` separately and call it before constructing the workflow:

```python
from dotenv import load_dotenv
load_dotenv()

from d3tools import WorkflowDefinition
wf = WorkflowDefinition.load("workflow.json")
```

Remember to add `.env` to `.gitignore` if you use this pattern.

## Resolution Order

`{ENV.VAR}` is resolved first, before `{now:...}`, tags, and datasets:

1. `env` top-level key applied to `os.environ`
2. `{ENV.VAR}` placeholders
3. `{now:...}` placeholders
4. `{tag}` placeholders
5. `{dataset_key}` placeholders

This means environment variables can be used inside tags and dataset
definitions.

## Best Practices

- Use `{ENV.VAR}` for secrets (API keys, passwords) instead of hardcoding them.
- Provide `default=` values for non-sensitive configuration (log level, temp
  dirs) so a missing variable doesn't break parsing.
- Prefer the `env` top-level key over relying on ambient shell state when you
  want a config file to be self-contained and reproducible; prefer real shell/
  secret-manager environment variables for actual secrets.

See [Configuration Overview](overview.md) for the full parsing pipeline and
[Tags](tags.md) for `{tag}` placeholder syntax.
