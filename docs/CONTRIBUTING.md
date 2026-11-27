# Contributing to the documentation

The documentation is organised by reader need:

- `workflow_orchestration/` explains workflow configuration and operation, including
  datasets in JSON, workflow sections, logging, examples, troubleshooting, and the
  integration boundary with DOOR, DAM, and DRYES.
- `tools/` provides Python-first guidance for the reusable D3tools components. Examples
  should work without workflow JSON unless they explicitly link to a workflow equivalent.
- `reference/` contains concise, module-shaped mkdocstrings pages. Narrative explanation
  belongs in the other sections; do not hand-write API signatures here.

Keep each fact on one owning page and link to it elsewhere. The companion engines own
their complete payload schemas and processing semantics; D3tools documents only their
shared integration boundary. Check claims against `src/d3tools/` and use `tests/` as
evidence of exercised behaviour. Clearly label anything unexecuted or unresolved.

## Language and naming

- Write the human-facing package name as **D3tools**, with a capital `D`. Preserve the
  lowercase form where it is an exact technical identifier, including the Python package
  and import namespace (`d3tools`), installation commands, module paths, URLs, logger
  names, filenames, and filesystem paths.
- Use British English throughout narrative documentation, including forms such as
  `colour`, `behaviour`, `recognise`, `organise`, and `normalise`. Preserve American
  spelling when it is part of an exact API name, configuration key, code sample, command,
  filename, external quotation, or other literal identifier.

## Writing examples

Keep examples small, complete, and portable. Use writable locations such as `/tmp/`,
identify required optional dependencies or engines, and remove credentials, private
hosts, internal paths, and identifying production values. Do not claim that an
engine-backed workflow ran unless that exact configuration and its relevant output were
exercised.

The CHIRPS-to-SPI case study includes its JSON files from
`workflow_orchestration/case-study/` using `pymdownx.snippets`; do not duplicate that JSON
in the narrative page. If its parameter bundle is replaced, update the version-specific
Zenodo DOI in the case study together with the URL, size, and SHA-256 checksum in
`scripts/fetch_case_study_parameters.py`. Never commit the downloaded parameter rasters.

## Validate changes

From the repository root, run:

```bash
/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python tools/check_docs.py
git diff --check
```

The checker validates JSON and Python fences, relevant imports, local links, and heading
anchors. Put `<!-- check-docs: skip -->` immediately before a deliberately partial code
fence or snippet-include fence, and validate any included source file separately. Avoid
using the marker merely to silence a correctable error.

Also inspect `mkdocs.yml` when adding, moving, or removing a navigation target. Zensical
is the production documentation generator; the `classic` theme variant preserves the
site's Material-compatible layout. The maintainer performs the rendered-site check; do
not run Zensical or MkDocs build and preview commands unless explicitly asked.
