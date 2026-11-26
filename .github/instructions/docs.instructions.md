---
description: "Conventions and quality gates for d3tools narrative and API documentation."
applyTo: "docs/**"
---
# d3tools documentation conventions

## Documentation structure and ownership

The documentation has three complementary sections:

- `docs/workflow_orchestration/` is for operators who write workflow JSON, set
  environment values, use the CLI, or call the small `WorkflowDefinition.load()` / `run()`
  surface. It owns shared configuration, datasets as configured in JSON, workflow
  sections, logging, examples, troubleshooting, and technical orchestration internals.
- `docs/tools/` is Python-first narrative documentation for the reusable building blocks
  shared by DOOR, DAM, and DRYES. Examples must work without workflow JSON unless a
  cross-link explicitly explains the workflow equivalent.
- `docs/reference/` contains concise mkdocstrings pages. It is module-shaped and links to
  the owning narrative page; it does not duplicate tutorials or hand-written signatures.

Each fact has one owning page. Other pages link to it instead of maintaining a second
explanation. DOOR, DAM, and DRYES own their full engine payload schemas and processing
semantics. d3tools pages explain the shared integration boundary and may use a checked,
sanitized engine example without presenting it as an exhaustive engine contract.

## Evidence and claims

- Treat `src/d3tools/` as implementation truth and `tests/` as evidence of exercised
  behavior. Package docstrings describe the public API but must be checked against code.
- Sanitized examples in the sibling DOOR, DAM, and DRYES repositories are acceptable
  evidence for engine syntax. One observed key is not proof of a universal rule.
- Never use `build/` or `_old_docs/` as current truth. Historical material may help locate
  a topic, but every retained claim must be reverified.
- State exact defaults, ordering, side effects, failure modes, compatibility aliases, and
  experimental status where they affect readers.
- Label unexecuted or unresolved behavior explicitly. Never turn intended behavior into a
  documented guarantee.
- Do not copy credentials, private hosts, tokens, internal paths, or identifying
  production values into examples.

## Writing rules

- Prefer reader tasks and lifecycle relationships over package-by-package narration.
- Keep examples small, portable, and complete. Use user-writable locations such as
  `/tmp/` and identify optional dependencies or engines that an example needs.
- Workflow pages may use Python only for loading, inspecting, and running workflows.
  Direct use of reusable classes and functions belongs under Tools.
- Do not hand-write API signatures in narrative pages. Link to `docs/reference/`.
- Keep API-reference pages limited to a short owning-page link plus mkdocstrings
  directives for the public modules in that package.
- Preserve the fixed CHIRPS-to-SPI case-study files under
  `docs/workflow_orchestration/case-study/`. Include them with `pymdownx.snippets` rather
  than copying their JSON into the page. Keep the version-specific Zenodo DOI and the
  verified downloader checksum synchronized when the parameter bundle is updated. Do not
  commit the downloaded parameter rasters to this repository.

## Verification

Never run `mkdocs build`, `mkdocs serve`, or `mkdocs gh-deploy`; the maintainer performs
the rendered-site check. From this repository, use the shared environment:

```sh
/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py <pages>
```

- Complete JSON must be valid and workflow-shaped JSON must parse in the checker.
- Put `<!-- check-docs: skip -->` immediately before deliberately partial JSON or a
  snippet-include fence, then validate the included JSON file separately.
- Complete Python examples must compile and their d3tools/engine imports must succeed.
  Run safe examples when practical; otherwise state the unverified boundary.
- Check local links and anchors, inspect `mkdocs.yml` to ensure every navigation target
  exists, and run `git diff --check` before handing work off.
- Use `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python` for all Python and
  the corresponding executables from that virtual environment.
- Never claim that an engine-backed workflow ran unless that exact configuration and
  relevant output were exercised. Configuration construction is not execution proof.

## Review and publication

Narrative changes require an independent accuracy review against source/tests and a clean
documentation-checker result. Publication still requires the maintainer's rendered-site
check, because `scripts/check_docs.py` does not validate MkDocs rendering, navigation
presentation, generated-reference warnings, or theme behavior.
