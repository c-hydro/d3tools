# D3tools

D3tools provides shared workflow infrastructure and reusable Python components
for operational drought-monitoring systems. It coordinates configuration,
datasets, timestepping, logging, and ordered execution across the companion
DOOR, DAM, and DRYES engines.

## Installation

D3tools requires Python 3.12 or newer:

```bash
python -m pip install cima-d3tools
```

Optional capabilities are installed as extras:

```bash
python -m pip install 'cima-d3tools[remote-data]'  # S3 and SFTP datasets
python -m pip install 'cima-d3tools[more-data]'    # Parquet support
python -m pip install 'cima-d3tools[thumbnails]'   # Thumbnail generation
```

Remote dependencies are not required for local or in-memory datasets. An S3
or SFTP dataset reports how to install `remote-data` when that extra is absent.
The distribution is named `cima-d3tools`; its Python import package remains
`d3tools`.

## Run a workflow

An installed package provides the `d3run` command:

```bash
d3run workflow.json --start 2024-01-01 --end 2024-01-05
```

The equivalent Python API is:

```python
from d3tools import WorkflowDefinition

workflow = WorkflowDefinition.load("workflow.json")
workflow.run(start="2024-01-01", end="2024-01-05")
```

Workflow sections require their corresponding engine package:

- [DOOR](https://github.com/c-hydro/door/tree/main_new) acquires source data.
- [DAM](https://github.com/c-hydro/dam) processes and aggregates data.
- [DRYES](https://github.com/c-hydro/dryes) calculates drought indicators.

Install only the engines used by a workflow.

## Documentation

The complete user, operator, and API documentation is available at
[c-hydro.github.io/d3tools](https://c-hydro.github.io/d3tools/). See the
[changelog](https://github.com/c-hydro/d3tools/blob/main/CHANGELOG.md) for
release details.

## License

D3tools is licensed under the European Union Public Licence 1.2
([EUPL-1.2](https://github.com/c-hydro/d3tools/blob/main/LICENSE)).
