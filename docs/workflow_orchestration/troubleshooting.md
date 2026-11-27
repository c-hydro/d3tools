# Troubleshooting

## Installation and engine imports

Install D3tools, any needed format/storage/thumbnail extras, and the package
for every configured engine. Loading JSON while skipping engine-object
construction is useful for inspection only; it does not make a section
executable. Check the required package installation using the instructions
from [DOOR](https://github.com/c-hydro/door/tree/main_new),
[DAM](https://github.com/c-hydro/dam), or
[DRYES](https://github.com/c-hydro/dryes). See
[installation](getting-started.md#install-d3tools-and-required-extras).

Use the first missing module in the traceback to distinguish these cases:

| Symptom | Cause | Fix |
| --- | --- | --- |
| `WorkflowEngineImportError` names `door`, `dam`, or `dryes` | D3tools selected that engine, but importing the engine or one of its dependencies failed. | Install the named engine and its dependencies in the same environment as D3tools. Inspect the exception's chained error or `original_error` for the first missing transitive dependency. Use `build_workflow_objects=False` only to inspect configuration. |
| `ModuleNotFoundError` names `boto3`, `botocore`, `paramiko`, or `requests` | The S3/SFTP backend dependencies are absent. Because the remote backend module participates in the data API imports, this can surface before a remote dataset is constructed. | Install `d3tools[remote-data]`, then retry the import and workflow load. |
| `ModuleNotFoundError` names `pandas` while importing or loading a workflow | Pandas is imported by ordinary data and template modules, so its absence can prevent general workflow loading before any table dataset is used. | Install `d3tools[more-data]`, then retry the original workflow load. |
| A Parquet operation reports that `pyarrow` or `fastparquet` is unavailable | Pandas cannot find a usable Parquet engine. | Install `d3tools[more-data]` and keep at least one of `pyarrow` or `fastparquet` importable. |
| `ModuleNotFoundError` names `matplotlib`, `img2pdf`, or `PIL` | Thumbnail rendering or collection output was imported without its optional dependencies. | Install `d3tools[thumbnails]`. `PIL` is provided by Pillow. |
| A Rasterio or GeoPandas import reports missing or incompatible GDAL libraries | The active Python geospatial stack is installed incompletely, or its native GDAL libraries do not match. GDAL is not a D3tools extra. | Install compatible system GDAL libraries first, then reinstall the matching Python geospatial packages. Use `gdalinfo --version` to check the system version. D3tools defines and exports `GDAL_ImportError`, but current implementation paths do not raise it; seeing that type would therefore indicate an engine or extension. |
| A large GeoTIFF read requests a chunk manager that is unavailable | Files above the raster reader's chunk threshold are opened with dask chunks, but dask is not installed. | Install a compatible dask release or raise the read's `chunk_threshold` only when reading the file eagerly is safe for available memory. |
| A NetCDF read or write says the `h5netcdf` engine is unavailable | NetCDF I/O explicitly selects xarray's `h5netcdf` engine, which is not supplied by a named D3tools extra. | Install compatible `h5netcdf` and HDF5 Python dependencies in the workflow environment. |

The package metadata defines `more-data`, `remote-data`, and `thumbnails` as
the D3tools extras. Dask and `h5netcdf` are runtime requirements of the
specific paths above, not current D3tools extras. When an import still fails
after installation, confirm that `python`, `pip`, and the workflow runner all
come from the same virtual environment.

## Configuration and dataset failures

- **Missing environment variable:** check shell spelling and exports, or use
  an intentional non-secret default. An unset variable without a default
  fails while loading.
- **Dataset reference does not resolve:** check the dataset name, whether the
  entire string is the reference, and exact capitalization of any
  `datasets.` prefix. Update tags must refer to an existing dataset.
- **Configuration rejects a top-level key:** a typo in a reserved key may be
  interpreted as a section. Ensure every non-reserved key selects DOOR, DAM,
  or DRYES through an alias or supported explicit `engine`.
- **Format cannot be selected:** use a recognised extension or set an
  explicit `format`; check that the required format extra is installed.
- **Availability or time inference fails:** provide an explicit `timestep`
  and verify that matching data is present for the requested dates. Remote
  catalogues can take longer to inspect; sparse keys may not support inference.
- **Thumbnail setup fails:** verify the colour file, output path, optional
  overlay, and thumbnail dependencies. See [dataset thumbnails](datasets.md#configure-thumbnails).

### Spatial-template failures

Raster datasets create or reuse a `TemplateManager` while reading and writing
data. Its errors therefore surface as dataset read/write failures and stop the
owning workflow section.

| Symptom | Cause | Fix |
| --- | --- | --- |
| `TemplateValidationError` lists missing required keys | A template mapping passed to validation lacks one or more of `crs`, `_FillValue`, dimension names, spatial dimensions, dimension starts/ends/steps, or dimension lengths. | Use `TemplateManager.set()` or the dataset's `set_template()` with a valid `DataArray`/`Dataset` instead of assembling internal template dictionaries. If an engine or extension supplied the mapping, report the listed fields to its maintainer. |
| `TemplateValidationError` lists invalid values | The template has no dimensions, does not identify exactly two spatial dimensions, has a missing CRS, or records a non-positive dimension length. | Select a non-empty raster variable and set its x/y spatial dimensions and CRS before creating the template. The exception identifies the spatial key and each invalid value. |
| A cached template is ignored and the dataset tries to derive it again | `TemplateManager.load_from_cache()` found no cache file or could not read valid JSON and returned `False`; cache-load failures are not raised as validation errors. | Check that the source raster exists, then remove that `template_<spatial-key>.json` file or call `clear_cache()` and rebuild the cache. A syntactically valid cache is not revalidated when loaded, so also rebuild it if later reconstruction reports missing or inconsistent fields. |
| `TemplateMemoryError` reports an operation and estimated size | An engine or extension has rejected a template operation as too large. The exception type supplies chunking, smaller tiles, or disk caching as mitigations. | Reduce the tile/grid size, process with dask chunks, or configure template disk caching. Current `TemplateManager` defines and imports this exception but does not itself perform a memory estimate or raise it, so use the traceback to identify the component that did. |

`TemplateNotFoundError` and `TemplateCompatibilityError` are also public
template exception types, but the current `TemplateManager` does not raise
them. If an engine or extension raises either one, follow its traceback rather
than assuming it came from the manager. Direct template setup, cache behaviour,
and reconstruction are described in
[Tools — Spatial templates](../tools/datasets.md#spatial-templates).

For D3tools-facing syntax, use [Configuration](configuration.md) and
[Datasets](datasets.md). Errors inside the section payload or engine
execution are engine-owned; consult that engine's documentation.

## Execution and output failures

Sections run in configuration order, and a failure stops later sections.
Check the failing section's engine log and workflow log, confirm its input
data and run range, and check that output and log destinations are writable.
If a section is skipped, inspect whether its selected range has no remaining
work or a run-state reference contains no range. An unavailable run-state file
or malformed state can fail rather than skip.

Workflow logs and optional run-state output are described in
[Logging](logging.md). Range defaults, skips, and execution-option errors are
described in [Workflows](workflows.md#set-execution-ranges-and-options).
For implementation-level diagnostics, follow the [workflow
lifecycle](under-the-hood/lifecycle.md).
