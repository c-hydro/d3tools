# Phase 1 Findings: Thumbnails

Scope: `docs/thumbnails/overview.md`.

| Verdict | Documentation claim or gap | Evidence | Action |
|---|---|---|---|
| wrong | The nested dataset JSON example is presented as valid JSON, but it has trailing commas after the `colors`, `destination`, and `overlay` objects. | `docs/thumbnails/overview.md:65-89`; `scripts/check_docs.py` reports an invalid JSON block at line 72. | Remove the trailing commas. |
| wrong | "Additional keys are passed to `Thumbnail.save()` as options" implies arbitrary keys are meaningful, while `Thumbnail.save()` only consumes its recognized options and otherwise ignores them. | `docs/thumbnails/overview.md:121`; `src/d3tools/thumbnails/thumbnail.py:398-449` | Clarify that recognized thumbnail options are forwarded. |
| wrong | Omitted dataset configuration annotation is documented as inferring `source_key` or the raster filename, but dataset-managed generation supplies the output filename when `output_file` is available. | `docs/thumbnails/overview.md:113,275-286`; `src/d3tools/data/datasets/dataset.py:706-713`; direct fallback is `src/d3tools/thumbnails/thumbnail.py:426-433` | Distinguish direct `Thumbnail` inference from dataset-managed annotation behavior. |
| missing | The public `ThumbnailCollection` export and its collection/combine/save behavior are not documented. | `src/d3tools/thumbnails/__init__.py:1-6`; `src/d3tools/thumbnails/thumbnail_collection.py:9-82` | Add a concise API section to the thumbnails page. |

No code-bug? findings.