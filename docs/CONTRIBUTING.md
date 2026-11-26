# Contributing to the documentation

The documentation is organized by reader need:

- `workflow_orchestration/` explains workflow configuration and operation, including
  datasets in JSON, workflow sections, logging, examples, troubleshooting, and the
  integration boundary with DOOR, DAM, and DRYES.
- `tools/` provides Python-first guidance for the reusable d3tools components. Examples
  should work without workflow JSON unless they explicitly link to a workflow equivalent.
- `reference/` contains concise, module-shaped mkdocstrings pages. Narrative explanation
  belongs in the other sections; do not hand-write API signatures here.

Keep each fact on one owning page and link to it elsewhere. The companion engines own
their complete payload schemas and processing semantics; d3tools documents only their
shared integration boundary. Check claims against `src/d3tools/` and use `tests/` as
evidence of exercised behaviour. Clearly label anything unexecuted or unresolved.

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

Also inspect `mkdocs.yml` when adding, moving, or removing a navigation target. The
maintainer performs the rendered-site check; do not run `mkdocs build`, `mkdocs serve`,
or `mkdocs gh-deploy` unless explicitly asked.
