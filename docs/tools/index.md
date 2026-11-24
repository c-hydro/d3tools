# Tools

d3tools provides shared Python building blocks used by DOOR, DAM, and DRYES.
They can also be imported and used without workflow JSON or a workflow engine.
Use these pages for concepts and worked examples; use the linked API reference
for complete signatures and members.

## Tool map

| Tool | What it provides | Import path | API reference |
| --- | --- | --- | --- |
| Timestepping | Time periods, ranges, steps, windows, parsing, and date utilities. | `d3tools.timestepping` | [Timestepping](../reference/timestepping.md) |
| [Spatial](spatial.md) | CRS-aware bounding boxes and raster or xarray cropping helpers. | `d3tools.spatial` | [Spatial](../reference/spatial.md) |
| [Datasets](datasets.md) | Local, memory, S3, and SFTP datasets; formats, catalogues, and templates. | `d3tools.data` | [Data](../reference/data.md) |
| Cases | Layered option combinations and case traversal. | `d3tools.cases` | [Cases](../reference/cases.md) |
| Thumbnails | Single and multi-panel dataset previews. | `d3tools.thumbnails` | [Thumbnails](../reference/thumbnails.md) |
| Exit handling | Deferred callbacks and temporary-path cleanup. | `d3tools.exit` | [Exit](../reference/exit.md) |
| Notification | Experimental SMTP email notifications. | `d3tools.notification` | [Notification](../reference/notification.md) |

The narrative pages for each row are being added during the 2.4.0
documentation refresh. Configuration parsing and workflow logging belong to
the orchestration section's under-the-hood pages rather than the standalone
Tools section.

## Installation extras

The base installation includes timestepping, spatial helpers, cases, local and
memory datasets, raster and vector dependencies, exit handling, and
notification. Install an extra when a tool needs optional dependencies:

```bash
pip install "d3tools[more-data]"    # CSV, Parquet, and shapefile workflows
pip install "d3tools[remote-data]"  # S3 and SFTP datasets
pip install "d3tools[thumbnails]"   # Thumbnail rendering
```

Extras can be combined, for example
`pip install "d3tools[more-data,remote-data,thumbnails]"`.

## Import conventions

Import public objects from their package rather than their implementation
module whenever the package exports them. For example:

```python
from d3tools.data import MemoryDataset
from d3tools.timestepping import Dekad
```

Some lower-level helpers are intentionally imported from their documented
submodules. The API reference shows those module paths. Compatibility aliases
for older dataset and timestepping module paths remain available, but new code
should use the package imports above.

## Minimal example

This example combines a dekadal timestep with an in-memory dataset. Because
the dataset uses the default `end` time signature, the first dekad of January
is rendered as January 10.

```python
from d3tools.data import MemoryDataset
from d3tools.timestepping import Dekad

step = Dekad(2024, 1)
dataset = MemoryDataset(
    "rain-%Y%m%d.txt",
    format="txt",
    keep_after_reading=True,
)

dataset.write_data("12.4 mm", time=step)

assert dataset.get_key(step) == "rain-20240110.txt"
assert dataset.get_data(step) == "12.4 mm"
```

The dedicated Timestepping and Datasets pages will expand this into time-range
iteration, persistent storage, formats, availability queries, and dataset
composition.
