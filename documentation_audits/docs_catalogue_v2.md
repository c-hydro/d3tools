# d3tools documentation catalogue v2

Status: IMPLEMENTED. This catalogue records the implemented structure and its ownership
boundaries. It supersedes the abandoned User Guide / Developer Guide split.

## Workflow orchestration

| Page | Ownership |
| --- | --- |
| `docs/workflow_orchestration/index.md` | Package roles, engine boundaries, and section map. |
| `getting-started.md` | Installation, minimal loading/running, CLI behavior and exit codes, engine imports. |
| `configuration.md` | Workflow JSON, environment resolution, tags, placeholders, and dataset references. |
| `datasets.md` | Dataset configuration in workflow JSON and links to Python-owned concepts. |
| `workflows.md` | Sections, aliases, engine selection, ordering, ranges, and execution options. |
| `logging.md` | Workflow and dataset log configuration and failure behavior. |
| `case-study.md` | CHIRPS download, aggregation, and SPI using the published parameter bundle. |
| `examples.md` | Runnable minimal, multi-dataset, and logging workflows. |
| `troubleshooting.md` | Symptom/cause/fix guidance for imports, configuration, datasets, templates, execution, and output. |
| `under-the-hood/lifecycle.md` | Parsing, construction, dispatch, ranges, logging, cleanup, and Python inspection. |
| `under-the-hood/architecture.md` | Package/module boundaries, public surfaces, compatibility aliases, and fixed engine integration. |

Workflow pages own what operators set in JSON, environment variables, and the CLI.
Python is limited to workflow loading, inspection, and running. Engine-specific payload
schemas remain owned by DOOR, DAM, and DRYES.

## Tools

| Page | Ownership |
| --- | --- |
| `docs/tools/index.md` | Standalone-tool map, imports, extras, and combined starter example. |
| `timestepping.md` | Time value objects, parsing, arithmetic, windows, families, and edge cases. |
| `spatial.md` | Bounding boxes, CRS handling, cropping, clipping, and buffering. |
| `datasets.md` | Dataset construction, keys, formats, storage, availability, composition, templates, logging, thumbnails, and extension seams. |
| `cases.md` | Cases, permutations, layering, IDs, decorators, and engine interaction. |
| `thumbnails.md` | Standalone thumbnails, color definitions, overlays, annotations, and collections. |
| `exit-and-notification.md` | Exit callbacks, cleanup ordering, and experimental email notification. |

Tools pages own direct Python use. Workflow pages link here for the underlying concept
and retain only the JSON-facing keys and behavior.

## API reference

`docs/reference/` contains module-shaped mkdocstrings pages for cases, configuration,
data, errors, exit handling, logging, notification, parsing, spatial helpers, thumbnails,
and timestepping. Reference pages contain no parallel tutorial narrative; cross-links
point readers to the owning Workflow orchestration or Tools page where needed.

## Cross-cutting decisions

- Dataset key parsing belongs to Tools > Datasets. Workflow substitution and resolution
  belong to Under the hood > Lifecycle.
- Workflow-level logging from Python belongs to Lifecycle unless it grows enough to
  justify a dedicated Tools page.
- `TemplateManager` is documented with datasets and referenced from the Data API page.
- d3tools owns shared orchestration behavior; engine packages own domain payload meaning.
- The CHIRPS-to-SPI parameters are published under the version-specific DOI
  `10.5281/zenodo.24556246` and downloaded with a checksum-verifying helper. Steps 1 and
  2 are runnable and verified; the combined workflow is not claimed as executed end to
  end until that exact run has been repeated with the published bundle.
- Historical source material in `_old_docs/` is not publication content or current truth.

## Quality gates

1. Every `mkdocs.yml` navigation target exists.
2. Run `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py`
   across `docs/` and resolve all errors and warnings.
3. Run `git diff --check` and focused safe example/configuration checks.
4. Review material claims against current source, tests, and relevant engine examples.
5. Do not run MkDocs in automated documentation work. Before publication, the maintainer
   must perform the rendered-site, navigation, generated-reference, and warning check.

## Publication follow-up

- Execute Step 3 and the combined workflow with the published parameter bundle before
  adding an end-to-end verification claim.
- Record code/test failures found during documentation validation in the final release
  status rather than silently changing unrelated implementation.
