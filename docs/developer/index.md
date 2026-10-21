# Developer Guide

## Scope and reading paths

This guide is for maintainers extending d3tools and Python developers integrating
it from DOOR, DAM, DRYES, or another host application. It explains d3tools'
responsibilities, lifecycle, API boundaries, and extension seams. It complements
the [user-facing documentation](../index.md), which owns workflow JSON, CLI use,
and operator configuration.

Follow the path that matches your task:

- Start with [Architecture](architecture.md) to identify package ownership,
  canonical imports, and the engine boundary.
- Use [Workflow lifecycle](workflow-lifecycle.md) to understand parsing,
  section dispatch, execution ranges, logging, and cleanup.
- Use [Configuration internals](configuration.md) when preparing options from
  Python or tracing parser and placeholder behavior.
- Use [Datasets](datasets.md) for object composition, availability, storage,
  format processing, and write integrations.
- Use [Python APIs](python-apis.md) for time values, cases, spatial helpers,
  templates, and thumbnails.
- Use [Extensions and contributions](extensions.md) for supported extension
  seams, engine integration boundaries, and contributor checks.

The narrative pages describe relationships and multi-object behavior. Use the
generated reference for member-level details rather than treating this guide
as a duplicate API catalogue.

## Generated API reference

The [generated API reference](reference/config.md) contains the underlying
signatures and docstring-level descriptions. Its package-shaped pages are:

- [Cases](reference/cases.md)
- [Configuration](reference/config.md)
- [Data](reference/data.md)
- [Errors](reference/errors.md)
- [Exit](reference/exit.md)
- [Logging](reference/logging.md)
- [Notification](reference/notification.md)
- [Parsing](reference/parse.md)
- [Spatial](reference/spatial.md)
- [Thumbnails](reference/thumbnails.md)
- [Timestepping](reference/timestepping.md)

API reference pages are generated documentation targets, not additional
architecture or extension guides.
