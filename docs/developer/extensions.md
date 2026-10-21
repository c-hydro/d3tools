# Extensions and contributions

## Extend supported dataset seams

d3tools does not document a general-purpose plugin registry for Dataset
backends, format handlers, catalogues, or templates. The supported seams are
the existing responsibilities in the Dataset architecture; extending them
means implementing those responsibilities in the relevant class/mixin and
connecting the new behavior through the established construction paths.
Changes to those paths are code changes, not configuration-only plugins.

**Storage backend.**

A backend used by `Dataset` and `DataCatalogue` must provide the operations
needed to read and write a concrete key, remove a key, check whether a key or
prefix exists, and walk a prefix for discovery. Keep storage concerns in the
backend; do not duplicate key-pattern matching or time conversion there.
Applications should normally use Dataset methods rather than call backend
methods directly. To add a backend:

1. Subclass `Dataset`, define a unique lower-case class-level `type`, and
   implement the `key_pattern` getter/setter plus `_read_data()`,
   `_write_data()`, `_rm_data()`, `_check_data()`, and `_walk()`.
   Initialize storage identity before calling the shared initializer, as
   `LocalDataset` and `MemoryDataset` do.
2. Import the defining module before factory construction. `DatasetMeta`
   registers classes defining `type` in `Dataset.subclasses`;
   `Dataset.get_subclass()` uses that mapping. For a built-in backend, wire
   its import into the data package so ordinary configuration loading sees it.
   This class registration is not a general external plugin-discovery system.
3. Construct through `Dataset.from_options()` with the new type. The
   configuration parser selects the registered subclass and attaches the
   existing fallback, log, and thumbnail integrations; the Dataset factory
   combines storage with the selected format mixin.
4. Test round-trip reads/writes, removal, prefix checks/walking, and Dataset
   copy/update reconstruction through that same factory. For file-based
   backends, delegate local format I/O to `_read_from_file()` and
   `_write_to_file()` rather than implementing a second decoder.

See [Datasets — Storage backends and format
lifecycle](datasets.md#storage-backends-and-format-lifecycle) and the
[data reference](reference/data.md).

**Format processing.**

Format mixins own format-aware validation, metadata, file I/O, and
pre-/post-read/write conversion. The Dataset lifecycle invokes these hooks
around backend operations. Keep the public Dataset methods as the normal
application interface; new format behavior should integrate at the format
boundary rather than reimplementing storage access. Current mixin
responsibilities include raster, table, vector, structured text, plain text,
and generic file handling. The [data reference](reference/data.md) is the
source for existing API names and module details. To add a format:

1. Implement a `FormatMixin` subclass with `_init_format_properties()`,
   `_read_from_file()`, `_format_after_read()`, `_format_before_write()`, and
   `_write_to_file()`. File hooks receive local paths, including staged paths
   for remote storage; conversion hooks operate on data values.
2. Override `validate_data()` and the metadata hooks (`get_metadata()`,
   `set_metadata()`, `update_metadata()`) when the format needs them.
   `validate_data()` returns the validated value; the conversion hooks return
   the transformed value. Preserve cooperative `_post_update_init()` behavior
   if format state must survive copies/updates.
3. Export the mixin from the format-mixin package and wire its canonical label
   into `FORMAT_MIXIN_MAP` in `data.io_utils`. Update
   `get_format_from_path()` if an extension needs translation to that label.
   Review `check_data_format()` too: adding a map entry alone does not make a
   new label compatible with the existing object-type checks.
4. Test factory selection with explicit and inferred formats, actual file
   round trips, validation/metadata, and `as_is` behavior. Check memory and
   remote staging separately where relevant; not every path invokes every
   conversion hook. These are coordinated source changes, not a runtime
   format-registration API.

**Catalogue discovery.**

Catalogue queries use backend existence and walking operations together with
`KeyParser`. A backend extension must preserve the catalogue's ability to
enumerate storage; keep logical time conversion, pattern matching, extracted
tags, and availability behavior common through the existing catalogue
interface. For discovery integration:

1. Make `_walk(prefix)` yield concrete keys in the same namespace as
   `key_pattern` and `_check_data(key)`. Support the resolved prefix as well
   as exact-key checks; keep traversal lazy rather than returning parsed
   timestamps from the backend.
2. Let the shared Dataset initializer create `DataCatalogue(self)`. Keep
   `KeyParser` responsible for prefix rendering, matching, extracted tags,
   and logical/storage time conversion.
3. Exercise `get_available_keys()` and `get_available_tags()` through the
   Dataset, then compare direct catalogue queries. Cover time/tag filters,
   first/last dates, `end+1` if relevant, and all-tile availability when tiles
   are configured.
4. If discovery needs behavior beyond backend enumeration, change the shared
   catalogue implementation and its Dataset delegation together. There is no
   documented configuration selector for arbitrary catalogue subclasses;
   preserve one discovery path rather than bolting on a second one.

See [Datasets — Availability and catalogue
contract](datasets.md#availability-and-catalogue-contract).

**Templates and raster integration.**

`TemplateManager` is the existing spatial metadata and cache mechanism used by
raster Dataset formats. Preserve its separation from `DataCatalogue`:
templates describe raster geometry and coordinate alignment, while the
catalogue describes keys and available data. Dataset copies and updates
already have spatial-state preservation behavior; changes to that integration
should cover how template state is retained, cached, loaded, and applied.
There is no evidenced general custom-template plugin contract; treat custom
schema or lifecycle changes as internal API changes and test them explicitly.
See [Datasets — Spatial templates and dataset integration](datasets.md#spatial-templates-and-dataset-integration)
and [Python APIs — Spatial helpers and templates](python-apis.md#spatial-helpers-and-templates).

**Thumbnail and dataset-log integration.**

Dataset writes invoke the configured thumbnail manager before metadata
attachment and storage, and the dataset-log manager after storage. Integrate
with those existing manager seams when extending
write-side behavior; do not assume that a custom callback registry exists.
Dataset logging owns entry construction and quality metrics, while thumbnail
rendering owns image creation. Use the existing manager APIs and generated
[thumbnail](reference/thumbnails.md) and
[logging](reference/logging.md) references. Operator configuration belongs
to [Dataset Logging](../logging/dataset-logging.md) and
[Dataset Configuration](../configuration/datasets.md).
When wiring a manager, retain the configuration parser's manager construction
and the Dataset `_make_thumbnail()` / `_make_log()` delegation rather than
moving these calls into a backend. Test success and failures at each side
effect: a thumbnail failure prevents the data write, while a storage failure
may leave a thumbnail and a log failure may leave written data. The complete
non-transactional write boundary is owned by
[Datasets — Dataset logging and thumbnails](datasets.md#dataset-logging-and-thumbnails).

## Integrate with workflow engines

d3tools currently recognizes section aliases and dispatches to DOOR, DAM, and
DRYES. It collects sections in order, resolves an engine from the explicit
section value, workflow default, or recognized alias, and forwards the
remaining payload to that engine. A list-valued section becomes multiple
sequential section entries. Engine-specific payload schema and behavior remain
owned by each engine project.

The integration boundary is two-sided: d3tools owns section recognition,
ordering, ranges, and dispatch; the engine owns the section payload and its
execution. Changes to aliases, dispatch, section result handling, or range
propagation require d3tools lifecycle coverage and a coordinated update to
the engine-facing examples/documentation. The present implementation and docs
do not establish support for arbitrary third-party engine plugins.

For safe extension work:

1. Keep new workflow-wide behavior in d3tools only when it is shared
   orchestration behavior rather than domain payload semantics.
2. Preserve documented sequential order and the existing engine failure
   boundary unless a deliberate code change and tests establish a new
   contract.
3. Keep options and payload contracts documented by the owning engine. Link
   to [DOOR](https://github.com/c-hydro/door),
   [DAM](https://github.com/c-hydro/dam), and
   [DRYES](https://github.com/c-hydro/dryes) for their complete
   configuration contracts.
4. Do not represent a working example's observed key as a universal
   requirement or claim that it exhausts an engine schema.

The Python-facing lifecycle is detailed in
[Workflow lifecycle](workflow-lifecycle.md); configuration preparation is
described in [Configuration internals](configuration.md).

## Contributor checks and documentation expectations

Test public contracts at the level callers use them. The consolidated API
backlog calls for explicit export boundaries and focused tests around exit,
errors, and selected spatial behavior. Tests should distinguish behavior
intended as a contract from helpers that currently happen to be importable.
Do not promote uncertain behavior into documentation merely because it seems
likely from an implementation detail.

Keep generated API reference pages responsible for signatures and
member-level docstrings. Keep narrative guides responsible for architecture,
lifecycle, extension recipes, and relationships across multiple objects.
Link to the reference instead of copying signatures or full docstrings.

When changing docs:

- respect the approved guide outline and its content ownership;
- keep one owning page for each fact and link to it elsewhere;
- keep complete JSON/Python examples parseable or compilable, and label
  deliberately partial JSON before the fenced block;
- run `scripts/check_docs.py` on every changed Markdown page;
- check local links and `mkdocs.yml` navigation entries against existing
  pages; and
- do not use `mkdocs` as a validation step for this documentation workflow.

For unresolved behavior and export decisions, use the maintained
developer open-question register and keep the prose conservative. See the
[generated API reference index](index.md#generated-api-reference) for the
reference pages.
