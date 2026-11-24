# Troubleshooting

## Installation and engine imports

Install d3tools, any needed format/storage/thumbnail extras, and the package
for every configured engine. Loading JSON while skipping engine-object
construction is useful for inspection only; it does not make a section
executable. Check the required package installation using the instructions
from [DOOR](https://github.com/c-hydro/door),
[DAM](https://github.com/c-hydro/dam), or
[DRYES](https://github.com/c-hydro/dryes). See
[installation](getting-started.md#install-d3tools-and-required-extras).

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
- **Format cannot be selected:** use a recognized extension or set an
  explicit `format`; check that the required format extra is installed.
- **Availability or time inference fails:** provide an explicit `timestep`
  and verify that matching data is present for the requested dates. Remote
  catalogs can take longer to inspect; sparse keys may not support inference.
- **Thumbnail setup fails:** verify the color file, output path, optional
  overlay, and thumbnail dependencies. See [dataset thumbnails](datasets.md#configure-thumbnails).

For d3tools-facing syntax, use [Configuration](configuration.md) and
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
