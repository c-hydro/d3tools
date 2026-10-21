Status: DRAFT

# User Guide index

Audience: people who author workflow JSON and run workflows. Explain only d3tools
configuration, CLI behavior, and the `WorkflowDefinition.load()` / `run()` Python
entry points. Engine-specific payload schemas remain owned by DOOR, DAM, and DRYES.

Running case study: the User Guide is anchored on one runnable walk-through,
`docs/user-guide/case-study.md`, that downloads 10-day CHIRPS precipitation (DOOR),
aggregates it to 1-month and 3-month windows on the 10-day timestep (DAM), and computes
SPI1 and SPI3 from pre-computed parameters (DRYES). The workflow files are real JSON
files under `docs/user-guide/case-study/`, included in pages with `pymdownx.snippets`,
so the published text and the runnable files are identical. Other User Guide pages
should illustrate engine-backed behavior with excerpts of these files rather than
inventing engine payloads. Engine blocks are shown as engine-owned: explain only the
d3tools-facing keys (tags, dataset references, ranges, logging) and link to the engine.
The case study does not exercise every feature (e.g. fallbacks, versions, tiles,
remote storage, thumbnails). Where it cannot illustrate a feature, writers use a small
focused snippet (no-engine where possible, or `<!-- check-docs: skip -->` if partial)
and list the feature in `documentation_audits/user-guide-open-questions.md` as a
candidate for an additional runnable example; additional examples are decided later.

## `docs/user-guide/index.md`

### Start here
- **Include:** Orient operators to the workflow-authoring journey and link to install, configuration, datasets, execution, logging, and troubleshooting.
- **Keep out:** Package architecture and Python APIs; see the Developer Guide.
- **Sources:** `docs/index.md` — Quick Links, Key Features, Documentation Structure.

## `docs/user-guide/getting-started.md`

### Install d3tools and required extras
- **Include:** Base install, relevant optional extras, Python requirement, and reminder to install the engine packages used by configured sections.
- **Keep out:** Dependency internals and engine installation instructions; link to the corresponding project.
- **Sources:** `docs/getting-started.md` — Installation.

### Create, load, and run a minimal workflow
- **Include:** A small portable configuration and the supported `WorkflowDefinition.load()` / `run()` path; end by pointing to the case study as the first run that does real work.
- **Keep out:** Full JSON key or engine payload references; see Configuration and Workflows.
- **Sources:** `docs/getting-started.md` — The Configuration Model, Your First Workflow; `docs/examples/minimal-workflow.md` — Configuration, Run It.

### Run from the command line
- **Include:** CLI invocation, optional range arguments, and where to find range rules.
- **Keep out:** CLI implementation and engine dispatch internals; see the Developer Guide.
- **Sources:** `docs/getting-started.md` — Running from the Command Line; `docs/examples/minimal-workflow.md` — Run It.

## `docs/user-guide/configuration.md`

### Workflow JSON shape
- **Include:** Reserved top-level controls, section keys, common value types, and where each configuration task is covered.
- **Keep out:** Parser stages and implementation classes; see Developer Guide — Configuration internals.
- **Sources:** `docs/configuration/overview.md` — Top-Level Keys; `docs/getting-started.md` — The Configuration Model.

### Environment values, tags, and time placeholders
- **Include:** Operator-facing JSON and shell syntax, precedence, recursive/list expansion where supported, safe environment use, and visible missing-value errors; keep Python use to the workflow load/run entry points.
- **Keep out:** Rendering/parser helper APIs and internals; see Developer Guide — Configuration internals and Python APIs.
- **Sources:** `docs/configuration/environment.md` — Syntax, The `env` Top-Level Key, Setting Environment Variables Externally / Shell, Resolution Order, Best Practices; `docs/configuration/tags.md` — Top-Level Configuration Tags, Recursive Resolution, Resolution Order, Placeholder Syntax, Common Patterns.

### Reference and update datasets
- **Include:** Whole-string dataset references, prefix/case rules, update-tag use, and practical consequences for the referenced dataset.
- **Keep out:** Dataset object construction and update/copy implementation; see Developer Guide — Datasets.
- **Sources:** `docs/configuration/datasets.md` — Basic Usage, Referencing Datasets; `docs/configuration/tags.md` — Dataset Update Tags; `docs/getting-started.md` — Dataset References.

### Diagnose configuration errors
- **Include:** Actionable checks for missing environment values, unresolved references, unrecognized section keys, and misplaced options.
- **Keep out:** Stack traces, parsing internals, and engine-specific validation; link to the owning engine or Developer Guide.
- **Sources:** `docs/getting-started.md` — Troubleshooting / Configuration errors; `docs/configuration/overview.md` — Workflow Sections and Aliases, Placeholder Syntax.

## `docs/user-guide/datasets.md`

### Define a dataset
- **Include:** Dataset defaults and the operator-level schema for local, memory, S3-compatible, and SFTP locations.
- **Keep out:** Backend class contracts, caching internals, and storage extension recipes; see Developer Guide — Datasets.
- **Sources:** `docs/configuration/datasets.md` — Basic Usage, Dataset Types, Defaults.

### Choose format and describe keys
- **Include:** `key_pattern` or local `path`/`filename`, time directives, explicit or inferred format, timestep/time signature, tiles, and raster `nan_value` as configurable options.
- **Keep out:** Format mixin lifecycle, key-parser algorithms, or complete format implementation lists beyond operator selection needs.
- **Sources:** `docs/configuration/datasets.md` — Additional Options, Time Options, Datetime Placeholders in Paths; `docs/datasets/concepts.md` — Key Patterns, Static and Time-Dependent Datasets, Time Properties, Tiles, Versions, and Names; `docs/datasets/formats-and-mixins.md` — Format Detection.

### Configure composition and versions
- **Include:** Practical update tags, parent/fallback intent where configurable, file versions, and tile naming.
- **Keep out:** Python copy/update semantics, composition internals, or custom parent functions; see Developer Guide — Datasets.
- **Sources:** `docs/configuration/datasets.md` — Referencing Datasets / With Update Tags; `docs/datasets/composition.md` — Updates, Fallbacks, Versions, Tiles.

### Configure thumbnails
- **Include:** Dataset thumbnail activation, required inputs, and a short link to supported configurable options.
- **Keep out:** Direct Python rendering, collection APIs, and rendering internals; see Developer Guide — Python APIs.
- **Sources:** `docs/configuration/datasets.md` — Thumbnail Definitions; `docs/thumbnails/overview.md` — Dataset JSON Configuration.

### Understand time and availability
- **Include:** Operator-relevant meaning of time signatures, explicit versus inferred timestep, tile-aware availability, and how availability can affect runs.
- **Keep out:** Catalogue object APIs and calendar class detail; see Developer Guide — Datasets and Python APIs.
- **Sources:** `docs/datasets/time-and-availability.md` — Time Signatures, Boundary and Availability Queries, Workflow Use; `docs/configuration/datasets.md` — Time Options.

## `docs/user-guide/workflows.md`

### Define sections and choose an engine
- **Include:** Recognized aliases, explicit/default engine selection, and clear ownership of engine-specific payload syntax.
- **Keep out:** Engine payload schemas, engine construction, and plugin claims; link to DOOR/DAM/DRYES and Developer Guide — Workflow lifecycle.
- **Sources:** `docs/configuration/overview.md` — Workflow Sections and Aliases; `docs/configuration/workflow-sections.md` — Section Aliases.

### Control section order and lists
- **Include:** Configuration order, numbered list entries, and sequential execution (lists are not parallelism).
- **Keep out:** Runtime dispatch implementation; see Developer Guide — Workflow lifecycle.
- **Sources:** `docs/configuration/workflow-sections.md` — Section Order and Lists; case-study combined workflow (`docs/user-guide/case-study/chirps_spi_case_study.json`).

### Set execution ranges and options
- **Include:** CLI/Python run range, environment fallback, defaults and per-section overrides, and documented behavior of `times_from_run`, `all_available`, `repeat_window`, and `split`, including skip/error cases.
- **Keep out:** Timestepping class/API detail and range-resolution internals; see Developer Guide — Workflow lifecycle and Timestepping API.
- **Sources:** `docs/configuration/workflow-sections.md` — Execution Options, Execution Time Range; `docs/timestepping.md` — Main Objects; `docs/getting-started.md` — Running from the Command Line.

### Keep engine responsibilities clear
- **Include:** Explain that d3tools selects and sequences engines while each engine defines its own section schema and behavior.
- **Keep out:** Undocumented engine semantics inferred from example keys; see that engine's documentation.
- **Sources:** `docs/configuration/workflow-sections.md` — opening overview; case-study step files (engine-owned blocks).

## `docs/user-guide/logging.md`

### Configure workflow logs
- **Include:** File/console choices, levels, formats, timestamped paths, run-state output, and visible logging defaults.
- **Keep out:** Generic Python logging tutorial or manager API; see Developer Guide — Workflow lifecycle and generated API reference.
- **Sources:** `docs/logging/overview.md` — Configuration, Formats; `docs/getting-started.md` — Logging; `docs/logging/examples.md` — File and Console, Timestamped Paths, Run-State Output.

### Configure dataset logs
- **Include:** Dataset `log` destination and what an operator can expect to be recorded, with a concise qualification that output-format support varies.
- **Keep out:** Serialization implementation, exact metric calculation, and dataset schema repetition; see Developer Guide — Datasets.
- **Sources:** `docs/logging/dataset-logging.md` — Configuration, When Entries Are Written, Entry Fields; `docs/configuration/datasets.md` — Dataset Logging.

### Logging failures and lifecycle
- **Include:** Visible consequences of unwritable destinations and where operators can find workflow/run-state results; distinguish workflow logs from per-write dataset logs.
- **Keep out:** Handler attachment, cleanup callbacks, and Python logging contracts; see Developer Guide — Workflow lifecycle.
- **Sources:** `docs/logging/overview.md` — Logged Events; `docs/logging/dataset-logging.md` — When Entries Are Written.

## `docs/user-guide/case-study.md`

Each step has its own runnable file under `docs/user-guide/case-study/`
(`01_download_chirps.json`, `02_aggregate.json`, `03_spi.json`) plus the combined
`chirps_spi_case_study.json`. All output paths derive from one root tag so the reader
chooses a single writable folder. Area, period, and grid are fixed by the shipped SPI
parameters (see Decisions 4 and 5).

### Case study overview and prerequisites
- **Include:** Goal and data flow (CHIRPS 10-day precipitation → 1-month and 3-month accumulations on the 10-day timestep → SPI1 and SPI3); required packages (d3tools, DOOR, DAM, DRYES) and that no credentials are needed; network and disk expectations; the fixed area and period; the output folder layout; how to obtain the pre-computed SPI parameters and where to place them; a short statement that parameter calculation is out of scope and owned by DRYES documentation.
- **Keep out:** SPI methodology, parameter fitting, CHIRPS product details beyond what the run needs, engine installation instructions (link to each engine).
- **Sources:** `door/workflow_examples/option_files/chirps_example.json`; `dryes/workflow_examples/option_files/spi_example.json`; Decisions 4 and 5.

### Step 1: download 10-day CHIRPS precipitation (DOOR)
- **Include:** The full `01_download_chirps.json` via snippet; root/tag usage, the destination dataset with time directives and 10-day timestep, the DOOR block as engine-owned, a CLI run with a fixed range, and the resulting files. State that the download range must start at least 3 months (8 dekads) before the first SPI date so the 3-month window is complete.
- **Keep out:** DOOR source/product catalogue, credentials, and DOOR option semantics beyond a one-line note and link.
- **Sources:** `door/workflow_examples/option_files/chirps_example.json`; User Guide Datasets and Workflows pages for d3tools concepts.

### Step 2: aggregate to 1-month and 3-month windows (DAM)
- **Include:** The full `02_aggregate.json` via snippet; reuse of the Step 1 dataset definition as input, an output dataset templated by aggregation window, keeping the 10-day output timestep, the run command, and the resulting files.
- **Keep out:** DAM process catalogue and aggregation semantics; link to DAM. Writers must confirm the DAM settings that keep a 10-day output timestep and record them as an open question if not established.
- **Sources:** `dam/workflow_examples/options_files/era5-wb-aggregation_example.json` (`aggregate_times`).

### Step 3: compute SPI1 and SPI3 (DRYES)
- **Include:** The full `03_spi.json` via snippet; the aggregated input reference, the parameter dataset pointing to the shipped bundle and its update tags (parameter name, aggregation window), 36 timesteps per year for the 10-day step, the run command, and the resulting SPI files. State that parameters must match the case-study area, grid, aggregation, and reference period.
- **Keep out:** How to compute the parameters (link to future DRYES documentation) and statistical background.
- **Sources:** `dryes/workflow_examples/option_files/spi_example.json`; Decision 4.

### Run the whole chain as one workflow
- **Include:** The full `chirps_spi_case_study.json` via snippet: the three sections in order sharing dataset definitions, workflow logging enabled, one CLI or `load()` / `run()` invocation, rerunning a sub-range, and where to inspect outputs and logs; link to Workflows (order, ranges) and Logging.
- **Keep out:** Re-explaining options already covered in Steps 1–3.
- **Sources:** Case-study step files; `docs/examples/with-logging.md` — Configuration, Run It.

## `docs/user-guide/examples.md`

### Minimal load-and-run example
- **Include:** A portable no-engine workflow for checking configuration loading and invoking the workflow API.
- **Keep out:** Claims that the no-section example performs engine work; explain its limited purpose; point to the case study for engine-backed runs.
- **Sources:** `docs/examples/minimal-workflow.md` — Configuration, Run It.

### Logging example
- **Include:** A complete small configuration/run illustrating a workflow log and how to inspect its visible output.
- **Keep out:** Repeating the full option reference; link to Logging.
- **Sources:** `docs/examples/with-logging.md` — Configuration, Run It, Timestamped Files; `docs/logging/examples.md` — Run-State Output.

## `docs/user-guide/troubleshooting.md`

### Installation and engine imports
- **Include:** Check d3tools extras and the required engine package; distinguish inspection-only loading from an executable workflow.
- **Keep out:** Import machinery or unsupported engine workarounds; see Developer Guide — Architecture and Workflow lifecycle.
- **Sources:** `docs/getting-started.md` — Installation, Troubleshooting / Engine import errors; `docs/examples/multi-dataset.md` — Load and Inspect.

### Configuration and dataset failures
- **Include:** Common parse, missing environment/reference, format, and unavailable-data symptoms with links to owning task pages.
- **Keep out:** Assertions about behavior not established in the current documentation; writers must record uncertain cases rather than infer guarantees.
- **Sources:** `docs/getting-started.md` — Troubleshooting / Configuration errors; `docs/configuration/environment.md` — Syntax; `docs/datasets/time-and-availability.md` — Boundary and Availability Queries.

### Execution and output failures
- **Include:** What a sequential section failure or unwritable log means to the operator and where to look next.
- **Keep out:** Internal dispatch, cleanup, or recovery guarantees; see Developer Guide — Workflow lifecycle.
- **Sources:** `docs/configuration/workflow-sections.md` — Execution Time Range, Logging; `docs/logging/dataset-logging.md` — When Entries Are Written.

# Developer Guide index

Audience: people extending d3tools or using it from Python in DOOR, DAM, and DRYES.
Explain architecture, lifecycle, contracts, and supported extension paths in depth;
link to generated reference pages rather than copying signatures or docstrings.

## `docs/developer/index.md`

### Scope and reading paths
- **Include:** Explain the Developer Guide audience, task-oriented guide pages, and relationship to the generated reference.
- **Keep out:** User configuration tutorial; link to `docs/user-guide/index.md`.
- **Sources:** `docs/developer/configuration.md` — Responsibility and Boundaries, API Reference; `docs/developer/workflow-sections.md` — Extension Boundary.

### Generated API reference
- **Include:** Link to the existing `docs/developer/reference/` pages as the source for signatures and member-level documentation.
- **Keep out:** Replanning or copying generated reference pages.
- **Sources:** Existing `docs/developer/reference/*.md` pages (linked only; unchanged).

## `docs/developer/architecture.md`

### Responsibilities and external boundaries
- **Include:** d3tools package responsibilities and boundaries with DOOR, DAM, DRYES, Python host applications, and engine-owned payload behavior.
- **Keep out:** Engine-specific configuration schemas and production-specific examples.
- **Sources:** `docs/index.md` — Architecture Overview; `docs/developer/workflow-sections.md` — opening overview, Extension Boundary; generated API reference.

### Public, compatibility, helper, and internal surfaces
- **Include:** Distinguish canonical imports, supported legacy aliases, helper-level names, internals, and accidental exports; link to exact generated reference modules.
- **Keep out:** Handwritten signatures or turning incidental star-import leaks into promises.
- **Sources:** `docs/developer/configuration.md` — API Reference; `docs/developer/timestepping.md` — API reference; generated reference pages for config, data, errors, spatial, timestepping, exit, and parse.

### Experimental and unsupported features
- **Include:** Mark `EmailNotification` experimental and unsupported for current use; state that no operational guarantee is implied.
- **Keep out:** User-facing recommendation or reliability promise.
- **Sources:** `_working_summaries/documentation_reference.md` — Decisions required before writing, Notification; existing notification API reference (link only).

## `docs/developer/workflow-lifecycle.md`

### Workflow loading and parsing lifecycle
- **Include:** Trace input through parsing, resolution, object creation, section collection, and the `WorkflowDefinition` boundary, with ordering, side effects, and failure points.
- **Keep out:** Repeating the operator JSON reference or writing API signatures.
- **Sources:** `docs/developer/configuration.md` — Responsibility and Boundaries, Parsing Lifecycle, Python Entry Points, Resolution Rules Relevant to Python Code.

### Section construction, dispatch, and engine boundary
- **Include:** Explain section collection, alias/default resolution, preserved order, list flattening, runtime dispatch, and why engine payload semantics stay external.
- **Keep out:** Asserting a general third-party engine plugin contract.
- **Sources:** `docs/developer/workflow-sections.md` — Lifecycle, Python Integration, Extension Boundary; `docs/configuration/workflow-sections.md` — Section Order and Lists.

### Execution ranges and sequential execution
- **Include:** Explain how run arguments, environment, prior state, and availability feed section ranges; document contracts, overrides, skips, errors, and sequential failure behavior.
- **Keep out:** Duplicating the User Guide option table or describing unresolved behavior as guaranteed.
- **Sources:** `docs/developer/workflow-sections.md` — Lifecycle, Python Integration; `docs/configuration/workflow-sections.md` — Execution Options, Execution Time Range; `docs/timestepping.md` — Related Guides.

### Logging, exceptions, and cleanup lifecycle
- **Include:** Explain workflow/section logging integration, run-state output boundary, manager ownership, close/process-exit behavior, and what happens on failure; preserve unresolved cleanup details as open.
- **Keep out:** Generic logging tutorial or unsupported ordering/timing guarantees.
- **Sources:** `docs/logging/overview.md` — Logged Events; `docs/logging/workflow-log-manager.md` — Execution Contexts, Logger Access and Level, Run-State; `_working_summaries/documentation_reference.md` — Logging, Exit/cleanup.

## `docs/developer/configuration.md`

### Configuration contract and parser stages
- **Include:** Detailed parsing stages, resolution dependencies, object boundaries, and distinction between the curated root API and submodule helpers.
- **Keep out:** Duplicating end-user syntax tables; link to the User Guide.
- **Sources:** `docs/developer/configuration.md` — Responsibility and Boundaries, Parsing Lifecycle, Resolution Rules Relevant to Python Code; `docs/configuration/overview.md` — Parsing Pipeline.

### Standalone parsing and Python integration
- **Include:** Explain when `parse_options()` is useful, how mappings relate to JSON, and the supported `WorkflowDefinition` entry path.
- **Keep out:** A second configuration model or unsupported direct construction recipes.
- **Sources:** `docs/developer/configuration.md` — Python Entry Points, Extension Surface.

### Resolution and compatibility helpers
- **Include:** Cover parser and standalone key/string resolution concepts, `KeyParser`/`ParsedKey`, `set_env`/`set_dataset`, and legacy helper boundaries.
- **Keep out:** Workflow JSON operator instructions and undocumented helper guarantees.
- **Sources:** `docs/developer/configuration.md` — Resolution Rules Relevant to Python Code; generated `parse` and `config` API references; `_working_summaries/documentation_reference.md` — Parsing.

## `docs/developer/datasets.md`

### Construction, composition, and key rendering
- **Include:** Dataset construction and copy/update semantics, parent/fallback composition, runtime tags, key rendering, versioning, and relevant extension contracts.
- **Keep out:** Repeating storage-option tables for operators.
- **Sources:** `docs/datasets/concepts.md` — Key Patterns, Static and Time-Dependent Datasets, Runtime Tags, Tiles, Versions, and Names; `docs/datasets/composition.md` — Updates, Copying, Parents, Fallbacks, Versions, Relationships and Updates; `docs/datasets/operations.md` — Key Rendering.

### Availability and catalogue contract
- **Include:** Relate `Dataset`, `DataCatalogue`, `KeyParser`, discovery, time conversion, cache/inference behavior, and tile-aware availability; distinguish convenience methods from direct catalogue access.
- **Keep out:** An operator-only list of JSON options or unverified edge guarantees.
- **Sources:** `docs/datasets/time-and-availability.md` — Boundary and Availability Queries, Enumerating Storage, Query Tags, Catalogue Object; `docs/datasets/catalogues-and-templates.md` — Key Matching, Discovery Methods, Time Conversion, Extending Discovery.

### Storage backends and format lifecycle
- **Include:** Backend responsibilities, remote staging/caching, backend contract, format mixin selection, read/write hooks, validation, metadata, and supported extension seams.
- **Keep out:** Reproducing all user configuration parameters; link to User Guide — Datasets.
- **Sources:** `docs/datasets/storage-backends.md` — Local, Memory, S3, SFTP, Remote Caching, Backend Contract; `docs/datasets/formats-and-mixins.md` — Read and Write Hooks, Format Mixins, Metadata and Validation, `as_is`.

### Spatial templates and dataset integration
- **Include:** Explain `TemplateManager`'s role, preservation, raster integration, schema/validation/cache/reconstruction/application concepts, and the boundary to spatial APIs.
- **Keep out:** General spatial helper reference detail; see Python APIs and generated reference.
- **Sources:** `docs/datasets/catalogues-and-templates.md` — Spatial Templates; `docs/datasets/formats-and-mixins.md` — Raster Templates; generated spatial reference; `_working_summaries/documentation_reference.md` — Spatial.

### Dataset logging and thumbnails
- **Include:** Explain their attachment points in dataset writes, ownership, callbacks, and extension boundaries; link to operator options and Python APIs.
- **Keep out:** Duplicating log-entry tables or thumbnail rendering option tables.
- **Sources:** `docs/datasets/operations.md` — Metadata, Thumbnails, and Logs; `docs/logging/dataset-logging.md` — When Entries Are Written, Entry Fields; `docs/thumbnails/overview.md` — API Usage; `_working_summaries/documentation_reference.md` — Thumbnails, Logging.

## `docs/developer/python-apis.md`

### Timestepping value objects
- **Include:** Explain unit normalization, relative windows, range expansion, fixed step families, and their relationship to dataset/workflow layers.
- **Keep out:** Workflow JSON range controls; link to User Guide — Workflows.
- **Sources:** `docs/developer/timestepping.md` — What the component is responsible for, Core public API (Unit normalization, Relative windows, Range expansion, Examples), Extension points and Python-only features, Relationship with dataset and workflow concerns.

### Cases and layered case management
- **Include:** Cover `Case`, Cartesian case expansion, manager layers/merges, traversal/lookup, `withcases`, and ID helpers; keep unsupported edge claims conservative.
- **Keep out:** Engine-specific interpretation of case payloads.
- **Sources:** Generated `cases` API reference (link only); `_working_summaries/documentation_reference.md` — Cases.

### Spatial helpers and templates
- **Include:** Cover bounding boxes, CRS/crop helpers, and the `TemplateManager` schema, validation, cache, reconstruction, and application contracts.
- **Keep out:** Raster Dataset lifecycle already owned by Datasets; avoid guarantees for untested edge behavior.
- **Sources:** `docs/datasets/catalogues-and-templates.md` — Spatial Templates; generated `spatial` API reference (link only); `_working_summaries/documentation_reference.md` — Spatial.

### Thumbnail and collection APIs
- **Include:** Cover direct rendering APIs, dataset manager integration, collection combine/save behavior, and no-eligible-image failure at a Python-use depth.
- **Keep out:** Operator configuration tables; avoid undocumented rendering behavior.
- **Sources:** `docs/thumbnails/overview.md` — API Usage (Through `Dataset.from_options()`, Through `DatasetThumbnailManager`, Direct `Thumbnail` Usage); generated `thumbnails` reference; `_working_summaries/documentation_reference.md` — Thumbnails.

### Errors, exits, and notification boundary
- **Include:** Link to curated exception exports; explain exit/cleanup API only to the extent established, and keep notification explicitly experimental.
- **Keep out:** Unsupported callback ordering/timing guarantees, hand-copied error signatures, and supported-use claims for notification.
- **Sources:** `docs/logging/workflow-log-manager.md` — Logger Access and Level; generated `errors`, `exit`, and `notification` references; `_working_summaries/documentation_reference.md` — Exit/cleanup, Errors, Notification; `documentation_audits/developer-guide-open-questions.md`.

## `docs/developer/extensions.md`

### Extend supported dataset seams
- **Include:** Supported recipes for backend, format, catalogue, template, thumbnail/log integration, with ownership and compatibility constraints.
- **Keep out:** Unsupported plugin points or API signature duplication.
- **Sources:** `docs/datasets/storage-backends.md` — Backend Contract; `docs/datasets/catalogues-and-templates.md` — Extending Discovery; `docs/datasets/formats-and-mixins.md` — Format Mixins, Metadata and Validation; `docs/developer/workflow-sections.md` — Extension Boundary.

### Integrate with workflow engines
- **Include:** Supported d3tools-side integration boundary, sequential dispatch, payload ownership, and safe extension/change considerations.
- **Keep out:** Claiming arbitrary third-party engines are supported; link to engine projects for their own contracts.
- **Sources:** `docs/developer/workflow-sections.md` — Lifecycle, Extension Boundary; `docs/developer/configuration.md` — Extension Surface.

### Contributor checks and documentation expectations
- **Include:** Testing expectations for public contracts, conservative treatment of unresolved behavior, generated-reference ownership, and documentation/checker workflow.
- **Keep out:** Reproducing repository-wide contributor procedures not present in the allowed sources.
- **Sources:** `docs/developer/configuration.md` — API Reference; generated API references; `_working_summaries/documentation_reference.md` — Cross-cutting API and test backlog; `.github/instructions/docs.instructions.md` — Writing rules and Verification.

# Mapping table

Every heading in the existing non-generated documentation is mapped below. Existing
headings are named as `page § heading`; `§` is the heading text, including page titles.
The mapping does not plan or alter `docs/developer/reference/`.

| Existing page § heading | Destination (page § heading) | Action | Reason |
|---|---|---|---|
| `docs/index.md` § d3tools Documentation | `docs/user-guide/index.md` § Start here | condense | Retain the landing orientation while replacing the mixed-topic directory with task links. |
| `docs/index.md` § Quick Links | `docs/user-guide/index.md` § Start here | merge | Consolidate duplicate entry links in one guide index. |
| `docs/index.md` § Key Features | `docs/user-guide/index.md` § Start here | condense | Preserve only operator-relevant capabilities. |
| `docs/index.md` § Architecture Overview | `docs/developer/architecture.md` § Responsibilities and external boundaries | move | Architecture belongs to developers, not operators. |
| `docs/index.md` § Documentation Structure | `docs/user-guide/index.md` § Start here | condense | Replace the package/topic list with the approved task-shaped nav. |
| `docs/index.md` § Getting Started | `docs/user-guide/getting-started.md` § Create, load, and run a minimal workflow | merge | Index link duplicates the getting-started material. |
| `docs/index.md` § Configuration | `docs/user-guide/configuration.md` § Workflow JSON shape | merge | Put configuration entry links under their task owner. |
| `docs/index.md` § Dataset Reference | `docs/user-guide/datasets.md` § Define a dataset | merge | Consolidate dataset links in the task guide. |
| `docs/index.md` § Logging | `docs/user-guide/logging.md` § Configure workflow logs | merge | Consolidate logging links. |
| `docs/index.md` § Thumbnails | `docs/user-guide/datasets.md` § Configure thumbnails | merge | Thumbnail configuration is a dataset task. |
| `docs/index.md` § Examples | `docs/user-guide/examples.md` § Minimal load-and-run example | merge | Move example directory links to the examples page. |
| `docs/index.md` § Contributing | `docs/developer/extensions.md` § Contributor checks and documentation expectations | condense | Preserve developer-oriented contribution pointer only. |
| `docs/index.md` § Licence | `docs/index.md` § Licence | keep | Keep the existing license link on the site landing page, outside guide prose. |
| `docs/getting-started.md` § Getting Started with d3tools | `docs/user-guide/getting-started.md` § Create, load, and run a minimal workflow | condense | Replace the mixed overview with a short first-run path. |
| `docs/getting-started.md` § Installation | `docs/user-guide/getting-started.md` § Install d3tools and required extras | keep | Operator installation task. |
| `docs/getting-started.md` § The Configuration Model | `docs/user-guide/getting-started.md` § Create, load, and run a minimal workflow | condense | Keep only essential entry-point orientation; detailed JSON belongs in Configuration. |
| `docs/getting-started.md` § Your First Workflow | `docs/user-guide/getting-started.md` § Create, load, and run a minimal workflow | merge | Keep one checked, minimal example; engine payload details link outward. |
| `docs/getting-started.md` § Running from the Command Line | `docs/user-guide/getting-started.md` § Run from the command line | keep | Direct operator task. |
| `docs/getting-started.md` § Tags and Environment Variables | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | move | Configuration task, not onboarding detail. |
| `docs/getting-started.md` § Dataset References | `docs/user-guide/configuration.md` § Reference and update datasets | move | Owns reference syntax. |
| `docs/getting-started.md` § Logging | `docs/user-guide/logging.md` § Configure workflow logs | move | Logging details have a single owner. |
| `docs/getting-started.md` § Troubleshooting | `docs/user-guide/troubleshooting.md` § Installation and engine imports | move | Troubleshooting gets its own concise task page. |
| `docs/getting-started.md` § Engine import errors | `docs/user-guide/troubleshooting.md` § Installation and engine imports | keep | Visible setup failure and remedy. |
| `docs/getting-started.md` § Configuration errors | `docs/user-guide/troubleshooting.md` § Configuration and dataset failures | move | Group symptoms by operator action. |
| `docs/getting-started.md` § Next Steps | `docs/user-guide/index.md` § Start here | condense | Replace the duplicated link list with guide navigation. |
| `docs/configuration/overview.md` § Configuration Overview | `docs/user-guide/configuration.md` § Workflow JSON shape | condense | Keep the operator model; parsing machinery moves to Developer Guide. |
| `docs/configuration/overview.md` § Top-Level Keys | `docs/user-guide/configuration.md` § Workflow JSON shape | keep | This is the operator's JSON structure reference. |
| `docs/configuration/overview.md` § Workflow Sections and Aliases | `docs/user-guide/workflows.md` § Define sections and choose an engine | move | Workflow authoring owns section rules. |
| `docs/configuration/overview.md` § Placeholder Syntax | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Keep syntax overview and link to specific operator guidance. |
| `docs/configuration/overview.md` § Parsing Pipeline | `docs/developer/configuration.md` § Configuration contract and parser stages | move | Internal resolution lifecycle is Developer Guide material. |
| `docs/configuration/overview.md` § Next Steps | `docs/user-guide/configuration.md` § Workflow JSON shape | condense | Remove repetitive link list in favor of in-context links. |
| `docs/configuration/environment.md` § Configuration Environment Variables | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | condense | Keep supported operator syntax and behavior, remove implementation framing. |
| `docs/configuration/environment.md` § Syntax | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | keep | Required placeholder syntax. |
| `docs/configuration/environment.md` § The `env` Top-Level Key | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | keep | Operator-visible process side effect and accepted shape. |
| `docs/configuration/environment.md` § Setting Environment Variables Externally | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Keep safe shell/Python/environment guidance, not a `.env` implementation tutorial. |
| `docs/configuration/environment.md` § Shell | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Small operator usage example. |
| `docs/configuration/environment.md` § Python, before loading the workflow | `docs/developer/configuration.md` § Standalone parsing and Python integration | move | Keep Python environment setup out of the operator guide's limited load/run scope. |
| `docs/configuration/environment.md` § `.env` files | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | condense | Mention only that d3tools does not load them itself if useful to troubleshooting. |
| `docs/configuration/environment.md` § Resolution Order | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Operators need precedence, not parser implementation stages. |
| `docs/configuration/environment.md` § Best Practices | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | keep | Safety and reliable configuration practices. |
| `docs/configuration/tags.md` § Configuration Tags | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | condense | Separate configuration tags from engine cases; avoid long repetition. |
| `docs/configuration/tags.md` § Top-Level Configuration Tags | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | keep | Core operator placeholder behavior. |
| `docs/configuration/tags.md` § Recursive Resolution | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Include recursion as supported behavior. |
| `docs/configuration/tags.md` § Resolution Order | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Keep only configuration-relevant ordering. |
| `docs/configuration/tags.md` § Placeholder Syntax | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | keep | Operator syntax and visible substitution rules. |
| `docs/configuration/tags.md` § Dataset Update Tags | `docs/user-guide/configuration.md` § Reference and update datasets | move | Dataset reference update syntax belongs with dataset references. |
| `docs/configuration/tags.md` § Engine-Specific Cases | `docs/user-guide/workflows.md` § Keep engine responsibilities clear | move | Engine-owned behavior must not be described as d3tools tag semantics. |
| `docs/configuration/tags.md` § Common Patterns | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | condense | Retain at most one useful portable pattern. |
| `docs/configuration/tags.md` § Reusable Paths | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Representative tag usage. |
| `docs/configuration/tags.md` § Combining Environment and Top-Level Tags | `docs/user-guide/configuration.md` § Environment values, tags, and time placeholders | merge | Combine with precedence explanation rather than duplicate example. |
| `docs/configuration/datasets.md` § Configuration Datasets | `docs/user-guide/datasets.md` § Define a dataset | condense | Keep operator schema and remove duplicated API/concept pages. |
| `docs/configuration/datasets.md` § Basic Usage | `docs/user-guide/datasets.md` § Define a dataset | keep | Core dataset declaration and reference task. |
| `docs/configuration/datasets.md` § Dataset Types | `docs/user-guide/datasets.md` § Define a dataset | keep | Operator-facing choices of storage backend. |
| `docs/configuration/datasets.md` § Local Files | `docs/user-guide/datasets.md` § Define a dataset | merge | Local schema and patterns. |
| `docs/configuration/datasets.md` § S3 Buckets | `docs/user-guide/datasets.md` § Define a dataset | merge | S3 configuration under one backend comparison. |
| `docs/configuration/datasets.md` § OVH S3 | `docs/user-guide/datasets.md` § Define a dataset | merge | S3-compatible endpoint configuration. |
| `docs/configuration/datasets.md` § In-Memory Data | `docs/user-guide/datasets.md` § Define a dataset | merge | Include limitations and avoid overstating untested retention behavior. |
| `docs/configuration/datasets.md` § SFTP | `docs/user-guide/datasets.md` § Define a dataset | merge | Keep supported authentication details conservative. |
| `docs/configuration/datasets.md` § Additional Options | `docs/user-guide/datasets.md` § Choose format and describe keys | keep | Operator format, tiles, and raster option summary. |
| `docs/configuration/datasets.md` § Defaults | `docs/user-guide/datasets.md` § Define a dataset | keep | Reduce repeated operator configuration. |
| `docs/configuration/datasets.md` § Time Options | `docs/user-guide/datasets.md` § Choose format and describe keys | merge | Operator time settings. |
| `docs/configuration/datasets.md` § Datetime Placeholders in Paths | `docs/user-guide/datasets.md` § Choose format and describe keys | merge | Key naming configuration. |
| `docs/configuration/datasets.md` § Referencing Datasets | `docs/user-guide/configuration.md` § Reference and update datasets | move | Cross-reference syntax has one owner. |
| `docs/configuration/datasets.md` § In Workflow Sections | `docs/user-guide/configuration.md` § Reference and update datasets | merge | Dataset references at use sites. |
| `docs/configuration/datasets.md` § With Tags | `docs/user-guide/configuration.md` § Reference and update datasets | merge | Keep a concise composition of top-level tags and dataset definitions. |
| `docs/configuration/datasets.md` § With Update Tags | `docs/user-guide/configuration.md` § Reference and update datasets | keep | Operator dataset-reference update syntax. |
| `docs/configuration/datasets.md` § Thumbnail Definitions | `docs/user-guide/datasets.md` § Configure thumbnails | move | Thumbnail option task belongs with datasets. |
| `docs/configuration/datasets.md` § Dataset Logging | `docs/user-guide/logging.md` § Configure dataset logs | move | Logging details have one owner. |
| `docs/configuration/datasets.md` § Dataset API and Concepts | `docs/user-guide/datasets.md` § Define a dataset | condense | Replace package-page directory with links to operator tasks and Developer Guide. |
| `docs/configuration/workflow-sections.md` § Workflow Sections | `docs/user-guide/workflows.md` § Define sections and choose an engine | condense | Retain only JSON/operator-facing behavior. |
| `docs/configuration/workflow-sections.md` § Section Aliases | `docs/user-guide/workflows.md` § Define sections and choose an engine | keep | User-facing engine resolution. |
| `docs/configuration/workflow-sections.md` § Section Order and Lists | `docs/user-guide/workflows.md` § Control section order and lists | keep | Operator ordering and list behavior. |
| `docs/configuration/workflow-sections.md` § Execution Options | `docs/user-guide/workflows.md` § Set execution ranges and options | keep | Operator options, precedence, and visible outcomes. |
| `docs/configuration/workflow-sections.md` § Execution Time Range | `docs/user-guide/workflows.md` § Set execution ranges and options | keep | Operator range selection and failure/skip behavior. |
| `docs/configuration/workflow-sections.md` § Explicit workflow_sections | `docs/developer/workflow-lifecycle.md` § Section construction, dispatch, and engine boundary | move | The page itself says this is an internal parsed-workflow field, not user-authored JSON. |
| `docs/configuration/workflow-sections.md` § Logging | `docs/user-guide/logging.md` § Configure workflow logs | merge | Keep a short link to the logging owner. |
| `docs/configuration/workflow-sections.md` § See Also | `docs/user-guide/workflows.md` § Define sections and choose an engine | condense | Update links in context; remove generic list. |
| `docs/datasets/concepts.md` § Dataset Concepts | `docs/user-guide/datasets.md` § Choose format and describe keys | condense | Translate only configurable concepts for operators; internals move to Developer Guide. |
| `docs/datasets/concepts.md` § Key Patterns | `docs/user-guide/datasets.md` § Choose format and describe keys | merge | Explain operator key patterns. |
| `docs/datasets/concepts.md` § Static and Time-Dependent Datasets | `docs/user-guide/datasets.md` § Choose format and describe keys | merge | Operator-visible naming distinction. |
| `docs/datasets/concepts.md` § Runtime Tags | `docs/developer/datasets.md` § Construction, composition, and key rendering | move | Runtime Dataset API tags are Python behavior, distinct from config placeholders. |
| `docs/datasets/concepts.md` § Time Properties | `docs/user-guide/datasets.md` § Understand time and availability | merge | Keep operator-facing time behavior. |
| `docs/datasets/concepts.md` § Tiles, Versions, and Names | `docs/user-guide/datasets.md` § Choose format and describe keys | merge | Keep operator-facing tile and version configuration; move API details to developer page. |
| `docs/datasets/concepts.md` § Format and Storage | `docs/developer/datasets.md` § Storage backends and format lifecycle | move | The separation and mixin behavior are internal contracts. |
| `docs/datasets/operations.md` § Dataset Operations | `docs/developer/datasets.md` § Construction, composition, and key rendering | condense | Python operations belong to developers; retain only operator-relevant behavior elsewhere. |
| `docs/datasets/operations.md` § Reading | `docs/developer/datasets.md` § Construction, composition, and key rendering | move | API behavior and composition interaction. |
| `docs/datasets/operations.md` § Writing | `docs/developer/datasets.md` § Dataset logging and thumbnails | move | Write lifecycle includes extension hooks. |
| `docs/datasets/operations.md` § Key Rendering | `docs/developer/datasets.md` § Construction, composition, and key rendering | move | Python rendering contract. |
| `docs/datasets/operations.md` § File Operations | `docs/developer/datasets.md` § Construction, composition, and key rendering | move | Python API surface. |
| `docs/datasets/operations.md` § Format Processing | `docs/developer/datasets.md` § Storage backends and format lifecycle | move | Backend/mixin contract. |
| `docs/datasets/operations.md` § Metadata, Thumbnails, and Logs | `docs/developer/datasets.md` § Dataset logging and thumbnails | merge | One dataset write-extension owner. |
| `docs/datasets/time-and-availability.md` § Dataset Time and Availability | `docs/developer/datasets.md` § Availability and catalogue contract | condense | Split operator advice from detailed API/implementation contract. |
| `docs/datasets/time-and-availability.md` § Time Signatures | `docs/user-guide/datasets.md` § Understand time and availability | condense | Keep operator time semantics; catalogue details move to developer page. |
| `docs/datasets/time-and-availability.md` § Boundary and Availability Queries | `docs/developer/datasets.md` § Availability and catalogue contract | move | Dataset API behavior. |
| `docs/datasets/time-and-availability.md` § Enumerating Storage | `docs/developer/datasets.md` § Availability and catalogue contract | move | Discovery API details. |
| `docs/datasets/time-and-availability.md` § Query Tags | `docs/developer/datasets.md` § Availability and catalogue contract | move | Runtime query behavior. |
| `docs/datasets/time-and-availability.md` § Workflow Use | `docs/user-guide/workflows.md` § Set execution ranges and options | merge | Explain only the operator relationship to availability. |
| `docs/datasets/time-and-availability.md` § Catalogue Object | `docs/developer/datasets.md` § Availability and catalogue contract | keep | Direct catalogue/API boundary. |
| `docs/datasets/composition.md` § Dataset Composition and Updates | `docs/developer/datasets.md` § Construction, composition, and key rendering | condense | Separate configurable use from Python semantics. |
| `docs/datasets/composition.md` § Updates | `docs/developer/datasets.md` § Construction, composition, and key rendering | keep | Copy/update and mutation contracts. |
| `docs/datasets/composition.md` § Copying | `docs/developer/datasets.md` § Construction, composition, and key rendering | keep | Python object-copy semantics. |
| `docs/datasets/composition.md` § Parents | `docs/developer/datasets.md` § Construction, composition, and key rendering | keep | Composition API and application-owned parent function. |
| `docs/datasets/composition.md` § Fallbacks | `docs/user-guide/datasets.md` § Configure composition and versions | condense | Explain operator configuration only; string inheritance detail stays in developer explanation. |
| `docs/datasets/composition.md` § Versions | `docs/user-guide/datasets.md` § Configure composition and versions | merge | Operator-visible version behavior. |
| `docs/datasets/composition.md` § Tiles | `docs/user-guide/datasets.md` § Choose format and describe keys | merge | Link tile config with key naming. |
| `docs/datasets/composition.md` § Relationships and Updates | `docs/developer/datasets.md` § Construction, composition, and key rendering | merge | Consolidate propagation/preservation contract. |
| `docs/datasets/storage-backends.md` § Dataset Storage Backends | `docs/developer/datasets.md` § Storage backends and format lifecycle | condense | Put implementation contract with Dataset architecture. |
| `docs/datasets/storage-backends.md` § Local | `docs/user-guide/datasets.md` § Define a dataset | condense | Keep config shape, not duplicate implementation detail. |
| `docs/datasets/storage-backends.md` § Memory | `docs/developer/datasets.md` § Storage backends and format lifecycle | merge | Explain backend behavior and conservative tested scope. |
| `docs/datasets/storage-backends.md` § S3 | `docs/developer/datasets.md` § Storage backends and format lifecycle | merge | Backend details and dependency contract. |
| `docs/datasets/storage-backends.md` § SFTP | `docs/developer/datasets.md` § Storage backends and format lifecycle | merge | Implementation boundary; user config remains in User Guide. |
| `docs/datasets/storage-backends.md` § Remote Caching | `docs/developer/datasets.md` § Storage backends and format lifecycle | keep | Internal lifecycle and connection behavior. |
| `docs/datasets/storage-backends.md` § Backend Contract | `docs/developer/datasets.md` § Storage backends and format lifecycle | keep | Supported extension contract. |
| `docs/datasets/formats-and-mixins.md` § Dataset Formats and Mixins | `docs/developer/datasets.md` § Storage backends and format lifecycle | condense | Format internals belong with Dataset architecture. |
| `docs/datasets/formats-and-mixins.md` § Format Detection | `docs/user-guide/datasets.md` § Choose format and describe keys | condense | Keep only operator selection/detection behavior. |
| `docs/datasets/formats-and-mixins.md` § Read and Write Hooks | `docs/developer/datasets.md` § Storage backends and format lifecycle | keep | Internal lifecycle. |
| `docs/datasets/formats-and-mixins.md` § Format Mixins | `docs/developer/datasets.md` § Storage backends and format lifecycle | keep | Extension and format responsibilities. |
| `docs/datasets/formats-and-mixins.md` § Raster Templates | `docs/developer/datasets.md` § Spatial templates and dataset integration | move | Raster template lifecycle belongs with spatial integration. |
| `docs/datasets/formats-and-mixins.md` § Metadata and Validation | `docs/developer/datasets.md` § Storage backends and format lifecycle | merge | Format contract. |
| `docs/datasets/formats-and-mixins.md` § `as_is` | `docs/developer/datasets.md` § Storage backends and format lifecycle | keep | Advanced Python behavior, not a workflow operator requirement. |
| `docs/datasets/catalogues-and-templates.md` § Dataset Catalogues and Templates | `docs/developer/datasets.md` § Availability and catalogue contract | condense | Catalogue and templates are distinct developer concerns. |
| `docs/datasets/catalogues-and-templates.md` § Key Matching | `docs/developer/datasets.md` § Availability and catalogue contract | keep | Explain catalogue/key parser relationship. |
| `docs/datasets/catalogues-and-templates.md` § Discovery Methods | `docs/developer/datasets.md` § Availability and catalogue contract | merge | Consolidate discovery API behavior. |
| `docs/datasets/catalogues-and-templates.md` § Time Conversion | `docs/developer/datasets.md` § Availability and catalogue contract | merge | Explain logical/storage time mapping. |
| `docs/datasets/catalogues-and-templates.md` § Spatial Templates | `docs/developer/datasets.md` § Spatial templates and dataset integration | move | Template manager belongs in spatial/Dataset integration. |
| `docs/datasets/catalogues-and-templates.md` § Extending Discovery | `docs/developer/extensions.md` § Extend supported dataset seams | move | Extension recipe owner. |
| `docs/timestepping.md` § Timestepping | `docs/developer/python-apis.md` § Timestepping value objects | move | Timestepping classes are Python APIs, not workflow JSON configuration. |
| `docs/timestepping.md` § Main Objects | `docs/developer/python-apis.md` § Timestepping value objects | move | Developer API concepts. |
| `docs/timestepping.md` § Related Guides | `docs/developer/python-apis.md` § Timestepping value objects | condense | Replace with links to user execution ranges and dataset availability. |
| `docs/logging/overview.md` § Logging Overview | `docs/user-guide/logging.md` § Configure workflow logs | condense | Separate operator options from developer logging integration. |
| `docs/logging/overview.md` § Configuration | `docs/user-guide/logging.md` § Configure workflow logs | keep | Workflow logging configuration is an operator task. |
| `docs/logging/overview.md` § Logged Events | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | condense | Keep operator-visible event summary while moving logger details. |
| `docs/logging/overview.md` § Formats | `docs/user-guide/logging.md` § Configure workflow logs | keep | Operator-selectable output formats. |
| `docs/logging/overview.md` § Related Topics | `docs/user-guide/logging.md` § Configure workflow logs | condense | Replace generic list with contextual links. |
| `docs/logging/dataset-logging.md` § Dataset Logging | `docs/user-guide/logging.md` § Configure dataset logs | condense | Split operator setup from serialization internals. |
| `docs/logging/dataset-logging.md` § Configuration | `docs/user-guide/logging.md` § Configure dataset logs | keep | Operator setup. |
| `docs/logging/dataset-logging.md` § When Entries Are Written | `docs/developer/datasets.md` § Dataset logging and thumbnails | merge | Write lifecycle and failure side effects are developer detail; user guide keeps summary. |
| `docs/logging/dataset-logging.md` § Entry Fields | `docs/developer/datasets.md` § Dataset logging and thumbnails | move | Exact serialization and metrics belong to technical contract. |
| `docs/logging/dataset-logging.md` § Ownership | `docs/user-guide/logging.md` § Configure dataset logs | condense | Replace with owner links and remove repeated ownership prose. |
| `docs/logging/workflow-log-manager.md` § WorkflowLogManager | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | condense | Manager API/lifecycle is developer detail; signatures stay in reference. |
| `docs/logging/workflow-log-manager.md` § Configuration Factory | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | merge | Python factory behavior, not operator schema. |
| `docs/logging/workflow-log-manager.md` § Constructor | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | condense | Link to generated reference instead of repeating signature table. |
| `docs/logging/workflow-log-manager.md` § Execution Contexts | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | keep | Python context and workflow lifecycle. |
| `docs/logging/workflow-log-manager.md` § Logger Access and Level | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | merge | Logging ownership, attachment, and cleanup. |
| `docs/logging/workflow-log-manager.md` § Run-State | `docs/user-guide/logging.md` § Configure workflow logs | condense | User config/output belongs in User Guide; internal state contract links to Developer Guide. |
| `docs/logging/examples.md` § Logging Examples | `docs/user-guide/logging.md` § Configure workflow logs | condense | Fold examples into the operator logging task. |
| `docs/logging/examples.md` § File and Console | `docs/user-guide/logging.md` § Configure workflow logs | merge | Short configuration example. |
| `docs/logging/examples.md` § Timestamped Paths | `docs/user-guide/logging.md` § Configure workflow logs | merge | Timestamp option example. |
| `docs/logging/examples.md` § Run-State Output | `docs/user-guide/logging.md` § Configure workflow logs | merge | Run-state operator example. |
| `docs/logging/examples.md` § Python Context Managers | `docs/developer/workflow-lifecycle.md` § Logging, exceptions, and cleanup lifecycle | move | Python API example belongs to Developer Guide. |
| `docs/thumbnails/overview.md` § Thumbnail Generation | `docs/user-guide/datasets.md` § Configure thumbnails | condense | Split operator JSON from Python implementation/API detail. |
| `docs/thumbnails/overview.md` § Colour Definitions | `docs/user-guide/datasets.md` § Configure thumbnails | condense | Give only the operator-needed file contract and link for details. |
| `docs/thumbnails/overview.md` § Dataset JSON Configuration | `docs/user-guide/datasets.md` § Configure thumbnails | keep | Operator thumbnail schema and options. |
| `docs/thumbnails/overview.md` § JSON Keys | `docs/user-guide/datasets.md` § Configure thumbnails | merge | Retain relevant configurable keys/options. |
| `docs/thumbnails/overview.md` § API Usage | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Python entry points are developer use. |
| `docs/thumbnails/overview.md` § Through `Dataset.from_options()` | `docs/developer/python-apis.md` § Thumbnail and collection APIs | merge | Python integration example. |
| `docs/thumbnails/overview.md` § Through `DatasetThumbnailManager` | `docs/developer/python-apis.md` § Thumbnail and collection APIs | merge | Python manager example. |
| `docs/thumbnails/overview.md` § Direct `Thumbnail` Usage | `docs/developer/python-apis.md` § Thumbnail and collection APIs | merge | Lower-level Python use. |
| `docs/thumbnails/overview.md` § Source Data Rules | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Retain source constraints needed for direct Python use, not a long rendering reference. |
| `docs/thumbnails/overview.md` § `ThumbnailCollection` | `docs/developer/python-apis.md` § Thumbnail and collection APIs | keep | Cover collection combine/save behavior and its documented empty-collection failure conservatively. |
| `docs/thumbnails/overview.md` § Raster Sources | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Keep only API-relevant source constraints. |
| `docs/thumbnails/overview.md` § Vector Sources | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Keep only API-relevant source constraints. |
| `docs/thumbnails/overview.md` § Save Options | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Link to generated API; avoid duplicating extensive rendering option details. |
| `docs/thumbnails/overview.md` § Size Options | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Include only supported usage guidance needed by Python callers. |
| `docs/thumbnails/overview.md` § Annotation Options | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Link out rather than repeat option reference tables. |
| `docs/thumbnails/overview.md` § Legend Options | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Link out rather than repeat option reference tables. |
| `docs/thumbnails/overview.md` § Overlay Options | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | Keep only contract-relevant overlay constraints. |
| `docs/thumbnails/overview.md` § All-Missing Data | `docs/developer/python-apis.md` § Thumbnail and collection APIs | condense | State visible all-missing behavior only after writer verification; avoid unnecessary low-level rendering detail. |
| `docs/examples/minimal-workflow.md` § Minimal Workflow Example | `docs/user-guide/examples.md` § Minimal load-and-run example | condense | Keep the example as part of one task-oriented collection. |
| `docs/examples/minimal-workflow.md` § Configuration | `docs/user-guide/examples.md` § Minimal load-and-run example | keep | Portable example configuration. |
| `docs/examples/minimal-workflow.md` § Run It | `docs/user-guide/examples.md` § Minimal load-and-run example | merge | Keep Python and CLI invocation together. |
| `docs/examples/minimal-workflow.md` § Tags and Datasets | `docs/user-guide/configuration.md` § Reference and update datasets | merge | Avoid second tag/dataset tutorial in examples. |
| `docs/examples/minimal-workflow.md` § See Also | `docs/user-guide/examples.md` § Minimal load-and-run example | condense | Replace generic cross-links with contextual guide links. |
| `docs/examples/with-logging.md` § Workflow with Logging | `docs/user-guide/examples.md` § Logging example | condense | Consolidate as one operator example. |
| `docs/examples/with-logging.md` § Configuration | `docs/user-guide/examples.md` § Logging example | merge | Keep as the example config. |
| `docs/examples/with-logging.md` § Run It | `docs/user-guide/examples.md` § Logging example | merge | Pair config and run instructions. |
| `docs/examples/with-logging.md` § Timestamped Files | `docs/user-guide/examples.md` § Logging example | merge | Keep concise timestamped variation if useful. |
| `docs/examples/with-logging.md` § See Also | `docs/user-guide/examples.md` § Logging example | condense | Replace generic example loop links. |
| `docs/examples/multi-dataset.md` § Multi-Dataset Workflow | `docs/user-guide/case-study.md` § Run the whole chain as one workflow | condense | The case study replaces this non-runnable integration example. |
| `docs/examples/multi-dataset.md` § Configuration | `docs/user-guide/case-study.md` § Run the whole chain as one workflow | condense | Keep only the dataset-sharing idea; the placeholder engine payloads are replaced by real case-study files. |
| `docs/examples/multi-dataset.md` § Load and Inspect | `docs/user-guide/troubleshooting.md` § Installation and engine imports | condense | Inspection-only loading is useful when an engine is not installed. |
| `docs/examples/multi-dataset.md` § See Also | `docs/user-guide/case-study.md` § Run the whole chain as one workflow | condense | Replace generic cross-links with task links. |
| `docs/developer/configuration.md` § Configuration Developer Guide | `docs/developer/configuration.md` § Configuration contract and parser stages | condense | Preserve the internals guide, reorganized around developer tasks. |
| `docs/developer/configuration.md` § Responsibility and Boundaries | `docs/developer/configuration.md` § Configuration contract and parser stages | merge | Parser ownership and external engine boundary. |
| `docs/developer/configuration.md` § Parsing Lifecycle | `docs/developer/configuration.md` § Configuration contract and parser stages | keep | Internal ordered lifecycle. |
| `docs/developer/configuration.md` § Python Entry Points | `docs/developer/configuration.md` § Standalone parsing and Python integration | move | Python API integration task. |
| `docs/developer/configuration.md` § Resolution Rules Relevant to Python Code | `docs/developer/configuration.md` § Resolution and compatibility helpers | merge | Place detailed resolution rules alongside parser helper boundaries. |
| `docs/developer/configuration.md` § Extension Surface | `docs/developer/extensions.md` § Integrate with workflow engines | merge | Explain actual extension boundary without implying a parser plugin system. |
| `docs/developer/configuration.md` § API Reference | `docs/developer/index.md` § Generated API reference | merge | Link generated reference; do not repeat module lists/signatures. |
| `docs/developer/workflow-sections.md` § Workflow Sections | `docs/developer/workflow-lifecycle.md` § Section construction, dispatch, and engine boundary | condense | Merge orchestration narrative with complete workflow lifecycle. |
| `docs/developer/workflow-sections.md` § Lifecycle | `docs/developer/workflow-lifecycle.md` § Section construction, dispatch, and engine boundary | merge | Section collection and sequential dispatch. |
| `docs/developer/workflow-sections.md` § Python Integration | `docs/developer/workflow-lifecycle.md` § Execution ranges and sequential execution | merge | Python range contract belongs with execution lifecycle. |
| `docs/developer/workflow-sections.md` § Extension Boundary | `docs/developer/extensions.md` § Integrate with workflow engines | move | Supported engine boundary/extension recipe owner. |
| `docs/developer/timestepping.md` § Timestepping | `docs/developer/python-apis.md` § Timestepping value objects | condense | Retain substantive API narrative, remove duplication with User Guide. |
| `docs/developer/timestepping.md` § What the component is responsible for | `docs/developer/python-apis.md` § Timestepping value objects | merge | Explain value-object role and boundaries. |
| `docs/developer/timestepping.md` § Core public API | `docs/developer/python-apis.md` § Timestepping value objects | keep | Center developer narrative on public concepts and operations. |
| `docs/developer/timestepping.md` § Unit normalization | `docs/developer/python-apis.md` § Timestepping value objects | merge | Core API concept; signatures link to reference. |
| `docs/developer/timestepping.md` § Relative windows | `docs/developer/python-apis.md` § Timestepping value objects | merge | Core API concept. |
| `docs/developer/timestepping.md` § Range expansion | `docs/developer/python-apis.md` § Timestepping value objects | merge | Core API concept. |
| `docs/developer/timestepping.md` § Examples | `docs/developer/python-apis.md` § Timestepping value objects | condense | Keep one concise Python example, verify against reference. |
| `docs/developer/timestepping.md` § Extension points and Python-only features | `docs/developer/python-apis.md` § Timestepping value objects | condense | Describe value-object usage, not an implied plugin mechanism. |
| `docs/developer/timestepping.md` § Relationship with dataset and workflow concerns | `docs/developer/python-apis.md` § Timestepping value objects | merge | Preserve ownership links to dataset and execution-range guides. |
| `docs/developer/timestepping.md` § API reference | `docs/developer/index.md` § Generated API reference | merge | Link only to generated reference. |

# Proposed `mkdocs.yml` nav

```yaml
nav:
  - Home: index.md
  - User Guide:
      - Overview: user-guide/index.md
      - Getting started: user-guide/getting-started.md
      - "Case study: CHIRPS to SPI": user-guide/case-study.md
      - Configuration: user-guide/configuration.md
      - Datasets: user-guide/datasets.md
      - Workflows: user-guide/workflows.md
      - Logging: user-guide/logging.md
      - Examples: user-guide/examples.md
      - Troubleshooting: user-guide/troubleshooting.md
  - Developer Guide:
      - Overview: developer/index.md
      - Architecture: developer/architecture.md
      - Workflow lifecycle: developer/workflow-lifecycle.md
      - Configuration internals: developer/configuration.md
      - Datasets: developer/datasets.md
      - Python APIs: developer/python-apis.md
      - Extensions and contributions: developer/extensions.md
      - API reference:
          - Cases: developer/reference/cases.md
          - Configuration: developer/reference/config.md
          - Data: developer/reference/data.md
          - Errors: developer/reference/errors.md
          - Exit: developer/reference/exit.md
          - Logging: developer/reference/logging.md
          - Notification: developer/reference/notification.md
          - Parsing: developer/reference/parse.md
          - Spatial: developer/reference/spatial.md
          - Thumbnails: developer/reference/thumbnails.md
          - Timestepping: developer/reference/timestepping.md
```

# Decisions for the maintainer

1. **API export boundaries:** Approve documenting only curated/canonical imports and explicitly supported compatibility aliases, while labeling helper-level names and accidental spatial/timestepping wildcard exports as non-contractual until the export findings are resolved. **Recommendation:** approve this conservative rule; do not let documentation turn imported implementation names into public API.
2. **Exit and cleanup contract:** Decide whether `ExitHandler` is public at package level or only through its submodule, and whether callback ordering/timing/exception interruption can be documented. **Recommendation:** treat it as submodule-only/undocumented for now and defer behavioral guarantees pending direct tests.
3. **Exception contract:** Decide whether package-level exception exports are the intended public surface and whether untested attributes/message formatting are supported. **Recommendation:** document package-level names only after confirming the curated exports; avoid untested edge details.
4. **SPI parameter distribution (OPEN, blocks Step 3 of the case study):** Decide how readers obtain the pre-computed SPI parameters (gamma `a`/`loc`/`scale` and `prob0` for 1-month and 3-month windows, one file per dekad of the year). Options: (a) commit a compressed bundle under `docs/user-guide/case-study/`; (b) host it externally (GitHub release asset or Zenodo with DOI) and document the download URL and a checksum. Also record provenance: who generated it, DRYES version, reference period, and CHIRPS terms of use. **Recommendation:** for a small area (about 2°×3° at 0.05°, 288 small GeoTIFFs) the bundle should be a few MB at most, so ship it in the repo if it stays under ~5 MB, otherwise use a release asset or Zenodo. Until decided, writers use a placeholder location and record the gap in `documentation_audits/user-guide-open-questions.md`.
5. **Case-study area and period:** Fix the bounding box, grid, SPI period, and the earlier download start before generating the parameters, because the parameters bind the case study to them. **Recommendation:** reuse the DOOR CHIRPS example bounds (`[8, 38.5, 10, 41.5]`) and a recent complete year of final (non-preliminary) data.
6. **Engine quick-reference appendix (deferred):** A later appendix may summarize DOOR/DAM/DRYES configuration and link to each repository. It is not part of this outline; writers must not create it.
7. **Snippet tooling (prerequisite):** Enable `pymdownx.snippets` in `mkdocs.yml`. `scripts/check_docs.py` does not expand snippets, so case-study JSON files are validated directly (see the docs instructions) until the checker supports them.

Writer follow-up (not a planning-stage investigation): current docs do not fully establish
all engine payload details or every dataset configuration edge case. Writers must use
approved workflow examples and writer-allowed evidence for concrete integrations, link
engine-owned behavior to DOOR/DAM/DRYES, and record unresolved configuration details
rather than inferring contracts. `EmailNotification` remains explicitly experimental.
