# d3tools documentation catalogue v2

Status: DRAFT for maintainer review. Supersedes the user/developer split of `guide-outline.md`.

For every entry in the `mkdocs.yml` nav this file lists:

1. **Include**: what the page must contain.
2. **Sources**: existing files that hold that information.
3. **Gaps**: information not in any existing file that must be written (from source code or new examples).

Conventions:

- `OLD/` = `_old_docs/` (outside `docs_dir`, kept only as source material; not built by mkdocs).
- `WO/` = `docs/workflow_orchestration/`; `SRC/` = `src/d3tools/`.
- Each fact has one owning page; other pages link to it.
- Workflow orchestration pages explain what an operator sets in JSON, environment variables or the CLI.
  Python is limited to `WorkflowDefinition.load()` / `run()`.
- Tools pages are Python-first: every page must be usable without any workflow JSON, with importable
  examples. Where a tool is also driven from workflow JSON, the Tools page owns the concept and the
  workflow page owns the JSON keys, linking to each other.
- Engine payload schemas stay owned by DOOR, DAM and DRYES.
- Existing `WO/` and `reference/` pages were moved back from the old guides but not yet revised: titles,
  relative links (e.g. to `../developer/index.md`) and the "User Guide" wording still need updating.

---

## 1. Workflow orchestration

### 1.1 Overview (`WO/index.md`)

1. **Include:** what the orchestrator is (reads workflow JSON, resolves shared configuration, runs sections in
   order and delegates to DOOR/DAM/DRYES); who it is for; a map of the section (install, configuration,
   datasets, workflows, logging, case study, examples, troubleshooting, under the hood); a pointer to
   Tools for standalone Python use and to the API reference.
2. **Sources:** `WO/index.md` (to be retitled and links fixed), `OLD/index.md` (quick links, key features).
3. **Gaps:** one short paragraph on the two roles of the package (orchestrator vs standalone tools) with links
   to both; a "which engine does what" table (DOOR = download, DAM = process/aggregate, DRYES = indicators)
   that is currently only implicit in the case study.

### 1.2 Getting started (`WO/getting-started.md`)

1. **Include:** installation (base and extras for S3, SFTP, GDAL/geospatial); a minimal workflow
   (no engine) loaded and run with `WorkflowDefinition.load()` / `run()`; the CLI
   (`scripts/run_workflow.py`: arguments, exit codes); how engines are discovered/imported.
2. **Sources:** `WO/getting-started.md`, `OLD/getting-started.md` (299 lines, longer variant),
   `scripts/run_workflow.py`, `pyproject.toml` (extras).
3. **Gaps:** verify that the CLI options documented match `scripts/run_workflow.py`; document the optional
   import behaviour of `SRC/errors/gdal_import.py` and `workflow_import.py` (what the user sees when an
   extra or an engine is missing) if not already in troubleshooting.

### 1.3 Configuration (`WO/configuration.md`)

1. **Include:** JSON shape (top-level keys, sections, `env`, tags, `datasets`); environment values
   (`$VAR`, `.env`, `env` key, resolution order); top-level tags and recursive resolution; placeholder
   syntax including time placeholders; referencing and updating datasets from sections; diagnosing
   configuration errors.
2. **Sources:** `WO/configuration.md`; richer material in `OLD/configuration/overview.md`,
   `OLD/configuration/environment.md`, `OLD/configuration/tags.md`, `OLD/configuration/datasets.md`.
3. **Gaps:** check `OLD/configuration/*` for details lost in the 157-line summary (resolution order tables,
   `.env` handling, "common patterns" for reusable paths); an error-message catalogue for configuration
   failures (derive from `SRC/config/parsing_pipeline.py` and `SRC/errors/template_errors.py`).

### 1.4 Datasets (`WO/datasets.md`)

1. **Include:** the JSON side only: defining a dataset (`key_pattern`, `format`, backend/`type`,
   `name`), format and key description, composition (`parents`, `fallbacks`, copies, updates, versions,
   tiles), thumbnails and dataset-log options, time and availability properties. For every concept, a
   one-line definition and a link to the owning section of `tools/datasets.md`.
2. **Sources:** `WO/datasets.md`, `OLD/configuration/datasets.md` (416 lines), `OLD/datasets/*.md`,
   `OLD/thumbnails/overview.md` (JSON keys section).
3. **Gaps:** a complete table of dataset JSON keys by storage backend (local, memory, S3, SFTP) with
   defaults and required/optional status (derive from `SRC/data/datasets/*.py` constructors);
   thumbnail JSON keys currently only in `OLD/thumbnails/overview.md`; trim the Python-oriented
   explanations that move to Tools.

### 1.5 Workflows (`WO/workflows.md`)

1. **Include:** sections and engine selection; section order and list-valued sections; section aliases;
   execution ranges (`time_range`, steps, `run_options`) and `Options`; what stays the responsibility of
   the engine.
2. **Sources:** `WO/workflows.md`, `OLD/configuration/workflow-sections.md`,
   `OLD/developer/workflow-lifecycle.md` (execution ranges part, to be summarised for operators).
3. **Gaps:** the full list of recognised section aliases (`WORKFLOW_SECTION_ALIASES`) with their meaning,
   taken from `SRC/config/workflow_section.py`; behaviour when a section list item fails (stop vs
   continue) if it is not already stated.

### 1.6 Logging (`WO/logging.md`)

1. **Include:** workflow log configuration (file, level, console, naming, rotation/placeholders);
   dataset logs (what gets recorded, where, how to read it); behaviour on failure and at exit.
2. **Sources:** `WO/logging.md`, `OLD/logging/overview.md`, `OLD/logging/dataset-logging.md`,
   `OLD/logging/examples.md`.
3. **Gaps:** `OLD/logging/workflow-log-manager.md` is an empty stub; a reference table of every
   log option (from `SRC/logging/workflow_log_manager.py`, `dataset_log_manager.py`, `utils.py`);
   the dataset-log record format.

### 1.7 Case study (`WO/case-study.md` and `WO/case-study/*.json`)

1. **Include:** the CHIRPS to SPI walk-through (DOOR download, DAM aggregation, DRYES SPI1/SPI3, then the
   combined workflow), with the JSON files embedded via snippets.
2. **Sources:** `WO/case-study.md`, `WO/case-study/*.json`.
3. **Gaps:** none structural; check snippet include paths after the move and that
   `scripts/check_docs.py` still validates them. Features not exercised (fallbacks, versions, tiles,
   remote storage, thumbnails) are listed in `documentation_audits/user-guide-open-questions.md`.

### 1.8 Examples (`WO/examples.md`)

1. **Include:** minimal load-and-run example, multi-dataset example, logging example, each runnable.
2. **Sources:** `WO/examples.md`, `OLD/examples/minimal-workflow.md`, `OLD/examples/multi-dataset.md`,
   `OLD/examples/with-logging.md`.
3. **Gaps:** the multi-dataset example is not in the current page; examples for remote storage,
   fallbacks/versions and thumbnails (see 1.7).

### 1.9 Troubleshooting (`WO/troubleshooting.md`)

1. **Include:** installation and engine import errors; configuration and dataset failures; execution and
   output failures, each as symptom, cause, fix, with links to the owning page.
2. **Sources:** `WO/troubleshooting.md`, `documentation_audits/user-guide-open-questions.md`.
3. **Gaps:** errors raised by `SRC/errors/` (`TemplateValidationError`, `TemplateMemoryError`) and by
   `TemplateManager` surfaced as dataset failures; messages for missing optional extras.

### 1.10 Under the hood: Workflow lifecycle (`WO/under-the-hood/lifecycle.md`, new)

1. **Include:** for technical readers, the life of a workflow definition:
   load (file or dict) → parse stages (environment, top-level tags, dataset construction,
   placeholder substitution) → section construction and dispatch to the engine → execution ranges and
   sequential execution → logging, exception handling and cleanup (`ExitHandler`). Explain the data
   passed between stages, ordering guarantees, where errors are raised, and what is cached.
   Include a mermaid sequence/flow diagram and a "using the parser from Python" subsection
   (`WorkflowDefinition` without `run()`, the parsing pipeline, `Options`).
2. **Sources:** `OLD/developer/workflow-lifecycle.md` (185 lines), `OLD/developer/configuration.md`
   (172 lines: parser stages, standalone parsing, resolution helpers).
3. **Gaps:** the existing text should be checked against current code in `SRC/config/`
   (`parsing_pipeline.py`, `parsers.py`, `workflow_definition.py`, `workflow_section.py`);
   a short description of `parse.config_resolution` (`set_env`, `set_dataset`) and
   `parse.string_rendering` (`substitute_string`, `substitute_values`) which are the configuration
   half of the former "Parsing and templating" topic and have no narrative today; the logging
   side (`WorkflowLogManager`, `configure_logger`, handler factories) from Python, which is only
   documented as JSON options today.

### 1.11 Under the hood: Architecture and engine integration (`WO/under-the-hood/architecture.md`, new)

1. **Include:** package responsibilities and external boundaries (d3tools vs DOOR/DAM/DRYES);
   module map; which surfaces are public, compatibility aliases, helpers or internal; experimental or
   unsupported features; how an engine plugs into `WorkflowSection` (the contract engines must satisfy,
   the dispatch rules, how engines receive datasets and options); contributor checks.
2. **Sources:** `OLD/developer/architecture.md`, `OLD/developer/extensions.md`
   ("Integrate with workflow engines", "Contributor checks and documentation expectations"),
   `OLD/developer/index.md`.
3. **Gaps:** the module map must be rewritten for the Tools/orchestration split (which packages belong
   to which role: orchestration = `config`, `parse` (config half), `logging` (workflow);
   tools = `timestepping`, `spatial`, `data`, `cases`, `thumbnails`, `exit`, `notification`) and this is
   where the planned future separation of the two should be mentioned; refreshed contributor checks
   (`scripts/check_docs.py`, tests) for the new layout; compatibility aliases in `SRC/data/__init__.py`.

---

## 2. Tools

### 2.1 Overview (`tools/index.md`, new)

1. **Include:** purpose of the tools (shared building blocks for DOOR, DAM, DRYES that are usable
   on their own); a table of tools with one-line description, import path and the page that documents
   them; install extras needed per tool; import conventions (what is exported from each package
   `__init__`); a minimal end-to-end snippet combining timestepping and a dataset.
2. **Sources:** `OLD/developer/index.md` (reading paths), `SRC/*/__init__.py` (exports).
3. **Gaps:** all of it; also decide how undocumented-by-page modules are pointed to
   (workflow logging, config, parse → Under the hood; errors → API reference only).

### 2.2 Timestepping (`tools/timestepping.md`, new)

1. **Include:**
   - Concepts: a time range vs individual timesteps; `TimePeriod`, `TimeRange`, `TimeStep`, `TimeWindow`.
   - Creating a `TimeStep` from a unit/start (`from_unit`, `from_string`/parsing), the fixed-number,
     fixed-length and fixed-day-of-year families (`FixedNTimeStep`, `FixedLenTimeStep`,
     fixed DOY), what each does at year boundaries and for leap years.
   - Arithmetic and comparison (`+`, `-`, containment, iteration), start/end semantics (inclusive/exclusive),
     `TimeRange` iteration and `get_timesteps`-style helpers.
   - `TimeWindow` and how windows are built from steps (aggregation windows such as 1 and 3 months on a
     10-day timestep, as in the case study).
   - Parsing and formatting dates and strings (`time_parsing`, `time_utils`), including format
     strings used in dataset keys.
   - Worked examples, edge cases and exceptions.
2. **Sources:** `OLD/timestepping.md` (50 lines), `OLD/developer/python-apis.md` ("Timestepping
   value objects", about 40 lines), docstrings in `SRC/timestepping/**`, `reference/timestepping.md`.
3. **Gaps:** `OLD/developer/timestepping.md` is an empty stub; `time_utils.py` (324 lines) helpers and
   `time_parsing.py` have no narrative; the DOY family and `estimate_timestep` are undocumented outside
   docstrings; rules for time zones/UTC handling (verify in code); a comparison table of the
   timestep families. All examples must be generated by running the code.

### 2.3 Spatial (`tools/spatial.md`, new)

1. **Include:** `BoundingBox` (construction from coordinates/CRS, conversion, intersection/buffering,
   serialisation); `space_utils` (`get_crs`, `crop_to_bb`, `clip_xarray`, `buffer_bbox`) with examples on
   `xarray` data; CRS conventions and axis order. A cross-link to the Templates section of Datasets
   (templates are not documented here).
2. **Sources:** `OLD/developer/python-apis.md` ("Spatial helpers and templates", about 45 lines, the
   helper part only), docstrings in `SRC/spatial/bounding_box.py` and `space_utils.py`,
   `reference/spatial.md`.
3. **Gaps:** `BoundingBox` has no narrative at all (184 lines of code); `clip_xarray` and `crop_to_bb`
   behaviour on different inputs (path vs DataArray vs Dataset); CRS and axis-order caveats.
   Move the `::: d3tools.spatial.template_manager` entry from `reference/spatial.md` to
   `reference/data.md` (`TemplateManager` is re-exported from `d3tools.data`).

### 2.4 Datasets (`tools/datasets.md`, new; one long page)

This page owns the dataset concepts; `WO/datasets.md` links here. Proposed section order:

1. **Include:**
   1. *What a dataset is*: a named, typed collection of files addressed by a key pattern; the class
      hierarchy `Dataset` → `LocalDataset`, `MemoryDataset`, `RemoteDataset` (`S3Dataset`,
      `SFTPDataset`); which one to use; creating datasets from Python and from a dict (same options as
      JSON).
   2. *Key patterns*: placeholder syntax, time formats, tags, static vs time-dependent datasets, runtime
      tags; `KeyParser`/`ParsedKey` for rendering a key from time+tags and parsing a path back;
      `extract_date_and_tags`; examples including multiple tags and tiles. (This is the "Parsing and
      templating" topic for datasets.)
   3. *Reading and writing data*: `get_data`, `write_data`, `get_times`/availability calls, `as_is`
      mode, metadata and validation, `check_data_format`.
   4. *Formats and mixins*: format detection from path, the mixins (raster, vector, table,
      plaintext, structured text, generic file), what each reads/writes and returns, adding extra read/write
      kwargs, `io_utils` / `io_utils_raster` (`read_from_file`, `write_to_file`, `rm_file`).
   5. *Storage backends*: local, memory, S3, SFTP (credentials, caching, the backend contract).
   6. *Time and availability*: time signatures (`time_signature`/`start`/`end`/`timesteps`),
      boundary and availability queries, `DataCatalogue` (what it is, when to use it, enumerating
      storage, query tags).
   7. *Composition*: updates, copies, parents, fallbacks, versions (`increment_version`),
      tiles, relationships.
   8. *Spatial templates*: `TemplateManager` (templates, validation, cache, rebuilding arrays),
      how `Dataset` uses it and the `TemplateValidationError` / `TemplateMemoryError` cases.
   9. *Dataset logging*: `DatasetLogManager` used on a dataset from Python.
   10. *Dataset thumbnails*: `DatasetThumbnailManager` (configuring and generating from a dataset;
       link to Thumbnails for the standalone `Thumbnail`).
   11. *Extending*: writing a new format mixin or storage backend (the extension seams and
       contract, registering them, test expectations).
2. **Sources:**
   - 1, 3, 5, 7: `OLD/developer/datasets.md` ("Construction, composition, and key rendering",
     "Storage backends and format lifecycle"), `OLD/datasets/concepts.md`, `composition.md`,
     `storage-backends.md`.
   - 2: `OLD/datasets/concepts.md` (Key Patterns), `OLD/developer/datasets.md`,
     `OLD/developer/configuration.md`.
   - 4: `OLD/datasets/formats-and-mixins.md`, `OLD/developer/datasets.md`.
   - 6: `OLD/datasets/time-and-availability.md`, `OLD/developer/datasets.md`
     ("Availability and catalogue contract").
   - 8: `OLD/developer/datasets.md` ("Spatial templates and dataset integration"),
     `OLD/developer/python-apis.md` (templates part).
   - 9, 10: `OLD/developer/datasets.md` ("Dataset logging and thumbnails"),
     `OLD/logging/dataset-logging.md`, `OLD/thumbnails/overview.md`.
   - 11: `OLD/developer/extensions.md` ("Extend supported dataset seams").
3. **Gaps:**
   - `DataCatalogue` (870 lines) has only a few lines of narrative; `OLD/datasets/catalogues-and-templates.md`
     and `OLD/datasets/operations.md` are empty stubs. It needs a full section with a runnable example.
   - A runnable standalone example for each backend (local and memory are testable; S3/SFTP need
     mocked or `skip` examples).
   - `KeyParser` (481 lines) has no usage narrative.
   - A table mapping mixin to extensions, input types and return types.
   - `TemplateManager` usage from Python (cache dir, `get`/`set`, applying templates).
   - `DatasetLogManager` and `DatasetThumbnailManager` used as Python objects.
   - The page will be very long: add a table of contents at the top and keep the sections
     independent, so that it can later be split into a `tools/datasets/` folder without rewriting.

### 2.5 Cases (`tools/cases.md`, new)

1. **Include:** what "cases" are (parameter-permutation sets/layered variants of a configuration);
   `Case`, `get_cases`, `CaseManager` (layering, ids, parents), `permutate_options`,
   the `@withcases` decorator, id syntax (`split_id`, `get_parents`, brackets and separator,
   `rand_str`), and how cases interact with workflow sections/engines (where applicable).
   Worked example from a plain Python function.
2. **Sources:** `OLD/developer/python-apis.md` ("Cases and layered case management", about 50 lines),
   docstrings in `SRC/cases/*`, `reference/cases.md`.
3. **Gaps:** motivation and real use (which engine uses cases and how it appears in workflow JSON;
   ask the maintainer); `CaseManager` (323 lines) is only briefly described; id grammar and layering
   rules need to be verified in code and documented; edge cases and errors.

### 2.6 Thumbnails (`tools/thumbnails.md`, new)

1. **Include:** standalone `Thumbnail` from a raster path, `DataArray` or `GeoDataFrame`; QGIS-style
   colour definition files (format, interpolation, labels) and `colors.py` helpers
   (`parse_colors`, `keep_used_colors`, `create_colormap`); overlays, annotations, legends and output
   options; `ThumbnailCollection` for multi-panel output; a pointer to the dataset-driven route
   (`DatasetThumbnailManager` and the JSON keys, owned by Datasets).
2. **Sources:** `OLD/thumbnails/overview.md` (123 lines: colour definitions, JSON keys),
   `OLD/developer/python-apis.md` ("Thumbnail and collection APIs", about 110 lines),
   `reference/thumbnails.md`.
3. **Gaps:** parameter reference for `Thumbnail` (554 lines of code) and the matplotlib/rioxarray
   dependencies; sample colour file and sample output image (generate and commit under `docs/`);
   `ThumbnailCollection` examples; vector rendering options.

### 2.7 Exit handling and notification (`tools/exit-and-notification.md`, new)

1. **Include:** `ExitHandler` and `run_at_exit`, `run_at_exit_first`, `rm_at_exit` (ordering, errors in
   callbacks, when they fire); `EmailNotification` (configuration, attachments, sending, failure
   behaviour, credentials handling). Short page, with examples.
2. **Sources:** `OLD/developer/python-apis.md` ("Errors, exits, and notification boundary", about 20
   lines), docstrings in `SRC/exit/exit_handler.py` and `SRC/notification/email_notification.py`,
   `reference/exit.md`, `reference/notification.md`.
3. **Gaps:** nearly everything beyond signatures: ordering semantics, usage from workflows (is
   notification wired into a workflow section? ask the maintainer), SMTP configuration and security
   notes, error behaviour.

---

## 3. API reference

All pages are `mkdocstrings` autodoc stubs under `docs/reference/`. Each entry:

1. **Include:** the module list for its package, and nothing else (no narrative), plus an
   optional one-line intro linking to the owning Tools or Under-the-hood page.
2. **Sources:** `docs/reference/*.md` (already moved), docstrings in `src/d3tools/`.
3. **Gaps:** per entry below.

| Nav entry | Page | Gaps |
|---|---|---|
| Cases | `reference/cases.md` | add link to `tools/cases.md` |
| Configuration | `reference/config.md` | link to Under the hood (lifecycle) |
| Data | `reference/data.md` | add `d3tools.spatial.template_manager`; add `io_utils` links; consider listing `DatasetLogManager` and `DatasetThumbnailManager` cross-references |
| Errors | `reference/errors.md` | check `gdal_import`/`workflow_import` modules are listed |
| Exit | `reference/exit.md` | link to Tools page |
| Logging | `reference/logging.md` | link to `WO/logging.md` and lifecycle |
| Notification | `reference/notification.md` | link to Tools page |
| Parsing | `reference/parse.md` | confirm `key_parser`, `string_rendering`, `config_resolution`, `parse_utils` are all listed; link to Datasets (key patterns) and lifecycle |
| Spatial | `reference/spatial.md` | remove `template_manager` (moved to Data) |
| Thumbnails | `reference/thumbnails.md` | confirm `colors` and `dataset_thumbnail_manager` are listed |
| Timestepping | `reference/timestepping.md` | link to Tools page |

---

## Cross-cutting work

- **Parsing and templating** has no page of its own. Its parts are placed as follows:
  key patterns/`KeyParser` in Tools > Datasets; `substitute_string`/`set_env`/`set_dataset` in
  Under the hood > Lifecycle; everything else stays reference-only.
- **Workflow-level logging from Python** (`WorkflowLogManager`, `configure_logger`) is documented only in
  Under the hood > Lifecycle. If it grows, promote it to a Tools page.
- **Orphaned or empty sources:** `OLD/datasets/catalogues-and-templates.md`, `OLD/datasets/operations.md`,
  `OLD/developer/timestepping.md`, `OLD/developer/workflow-sections.md`,
  `OLD/logging/workflow-log-manager.md` contain nothing; do not treat them as coverage.
- **Governance files to update before writing:** `.github/instructions/docs.instructions.md` and
  `scripts/check_docs.py` still describe User and Developer Guides and the old paths;
  `documentation_audits/guide-outline.md`, `user-guide.md` and `developer-guide.md` are superseded by
  this file and should be archived or deleted; `mkdocs.yml` `paths: [src]` for `mkdocstrings`
  must be verified from the new layout.
- **Build check:** `mkdocs build` will warn until the new files exist (all `tools/*` and
  `under-the-hood/*` pages are not yet created; the nav already points to them).
- **Verification rule:** every code example on a Tools page is executed (or explicitly marked
  `<!-- check-docs: skip -->` with a reason), as in the existing check script.
