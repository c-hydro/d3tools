# Changelog

Notable changes to D3tools are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## [3.0.0] - Unreleased

### Breaking changes

- Python 3.12 or newer is now required.
- Workflow configuration and execution are coordinated by
  `WorkflowDefinition`; the legacy `Options` type remains available for
  configuration mappings but is no longer the workflow runner.
- Workflow loading now builds engine objects and treats engine import failures
  as errors by default. Inspection-only callers must explicitly pass
  `build_workflow_objects=False`.
- Dataset and timestepping implementations have moved into clearer package
  hierarchies. Compatibility aliases preserve the established import paths,
  but code that depended on private module layout should migrate.

### Added

- Added the `d3run` command for running workflow JSON files from an installed
  D3tools package.
- Added ordered multi-engine workflow execution with explicit engine
  selection, execution ranges, split runs, repeat windows, and reuse of time
  ranges recorded by earlier runs.
- Added workflow and dataset logging, including persistent run-state files and
  dataset-backed log destinations.
- Added dataset fallbacks, automatic file-version selection and incrementing,
  memory datasets, and a dedicated data catalogue.
- Added structured key parsing and configuration stages for environment,
  current-time, tag, and dataset-reference resolution.
- Added reusable spatial templates and expanded raster, vector, table, text,
  and file-format handling.
- Added comprehensive user, operator, contributor, and API documentation,
  including a multi-engine CHIRPS-to-SPI case study.

### Changed

- Reworked dataset copying and updating to preserve creation options and
  subclass state consistently.
- Improved dataset availability searches, template lookup bounds, fallback
  handling, metadata preservation, and chunked raster I/O.
- Expanded thumbnail configuration, overlay handling, legends, annotations,
  vector sizing, output validation, and empty-collection behaviour.
- Improved timestepping parsing, aggregation windows, string representations,
  and date-boundary handling.
- Remote storage dependencies are genuinely optional: base imports and local
  datasets no longer require `boto3`, `botocore`, `paramiko`, or `requests`.
  S3 and SFTP backends provide an actionable error when `remote-data` is not
  installed.

### Fixed

- Fixed workflow dispatch and range selection across DOOR, DAM, and DRYES
  sections, including sections with no work and DRYES setup runs.
- Fixed remote key deletion, SFTP private-key detection, paginated S3 lookup,
  and remote staging behaviour.
- Fixed raster dtype selection, infinite-value handling, spatial template
  application, and NetCDF engine selection.
- Fixed dataset placeholder matching, nested dataset defaults, fallback
  availability checks, and versioned template lookup.

[3.0.0]: https://github.com/c-hydro/d3tools/compare/v2.3.0...v3.0.0
