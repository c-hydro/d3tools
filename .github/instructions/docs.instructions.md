---
description: "Conventions and quality gates for writing and auditing the complete d3tools Developer Guide and User Guide."
applyTo: "docs/**"
---
# d3tools documentation workflow

## Deliverables and order

Most content already exists in `docs/`, but pages mix audiences. The work is mainly
**reorganisation**, driven by one approved plan:

1. `docs-guide-planner` writes `documentation_audits/guide-outline.md`: the full index
   of both guides, what each heading includes and keeps out, and where every existing
   section goes. The maintainer approves it before any writing starts.
2. The Developer Guide writer builds the Developer Guide from the outline; one
   independent audit and at most one revision round follow.
3. The User Guide writer builds the User Guide from the outline; one independent audit
   and at most one revision round follow.

The approved outline is binding: writers do not add, drop, or move pages/headings, and
they move each existing section where the outline's mapping table says. If the outline
is wrong, report it instead of deviating.

The docstring writer, auditor, and orchestrator have completed their work and are not part
of this pipeline. Do not invoke or edit them.

## Intent of the two guides

- **User Guide**: for people who write workflow JSON and run it. It answers "what can I
  configure, how do I run it, what do I do when it fails". It explains only what an
  operator needs to write a working configuration. Internal concepts (mixins,
  catalogues, templates machinery, class hierarchies, parsing internals) do not belong
  here; when one explains a visible error or limitation, mention it briefly in
  troubleshooting and link to the Developer Guide. Python is limited to
  `WorkflowDefinition.load()` / `run()`.
- **Developer Guide**: for people who extend d3tools or use it from Python in DOOR, DAM,
  and DRYES. It covers architecture, lifecycle, internal contracts, the Python API, and
  extension recipes, with real detail rather than summaries and links.

If behavior is something an operator sets in JSON, environment variables, or the CLI, it
belongs in the **User Guide**. If it requires Python, extension, maintenance, or
understanding internals, it belongs in the **Developer Guide**.

- Each fact has one owning page; other pages link to it.
- User pages must stand alone for workflow operators.
- Developer pages explain relationships, lifecycle, contracts, and extension work; they
  do not duplicate docstrings.
- Generated API reference under `docs/developer/reference/` is module/package-shaped.
  Narrative guides are task- and architecture-shaped.
- DOOR, DAM, and DRYES own their full engine-specific payload schemas and behavior, but
  their checked-in `workflow_examples/` are approved evidence for realistic d3tools
  integration examples.

## Topics to place

The planner must place these in the outline; the approved outline then defines the
required coverage and depth.

Developer Guide, as a whole:

- architecture, package responsibilities, and external-project boundaries;
- workflow loading, parsing, resolution, section creation, range determination,
  sequential execution, logging, and cleanup;
- configuration and standalone parsing internals;
- dataset construction, composition, key rendering, availability, storage, formats,
  templates, logging, thumbnails, and extension contracts;
- workflow-section/engine integration and execution-range behavior;
- cases, timestepping, spatial/template, logging, parsing, errors, and cleanup APIs;
- canonical imports, compatibility aliases, helper/internal boundaries, and experimental
  APIs;
- supported extension recipes and contributor testing/documentation expectations;
- links into the generated API reference without copied signatures.

User Guide journeys (depth limited to what an operator configures):

- install d3tools plus relevant extras and engine dependencies;
- create and run a minimal workflow through the CLI and `load()` / `run()`;
- use configuration, tags, environment values, and dataset references;
- declare datasets in JSON: location, format, time, tiles, composition as configured;
- define workflow sections, engines, ordering, and execution options/ranges;
- configure workflow and dataset logging, and thumbnails where configurable;
- diagnose common configuration, import, availability, and execution failures;
- follow complete realistic examples without undocumented engine contracts.

## Sources and evidence

- `documentation_reference.md` contains current open findings and decisions. It is not a
  replacement for verification.
- The planner reads existing docs, `documentation_reference.md` and open-question files;
  it does not read source/tests, workflow examples, or production
  configurations. Record unclear configuration details for the writers rather than
  investigating configuration repositories during planning.
- Developer writers use existing docs and the docstrings in `src/d3tools/` (read-only;
  `docs/developer/reference/*.md` only contain mkdocstrings directives). They may read
  tests to confirm a recipe.
- User writers use the Developer Guide, existing pages, and sanitized real
  configurations; they do not read source/tests.
- Both guide writers may inspect the JSON files under
  `/home/luca/Documents/CIMA_code/door/workflow_examples/`,
  `/home/luca/Documents/CIMA_code/dam/workflow_examples/`, and
  `/home/luca/Documents/CIMA_code/dryes/workflow_examples/`. These files demonstrate
  working engine syntax and may be adapted into d3tools integration examples.
- Auditors use `src/d3tools/` as implementation truth and `tests/` as evidence of tested
  contracts. They may inspect docstrings and real configurations as appropriate.
- Never use `build/`; it may be stale.
- Unresolved behavior is a limitation or open question, never a documented guarantee.
- Notification remains experimental until its open code/test findings are resolved.

## Writing rules

- Prefer reader tasks, lifecycle, and extension goals over package-by-package narration.
- Use precise defaults, ordering, ownership, side effects, edge cases, and failure modes.
- Use a table when readers must compare several exact options; use diagrams only for
  genuinely relational flows.
- Keep examples small, realistic, sanitized, and portable.
- When adapting an engine workflow example, retain enough of the real configuration to
  remain executable, name the required engine, record the source example in the writer's
  completion report, and link to that engine's documentation for the complete contract.
- An observed key in one workflow is evidence that the example uses it, not evidence that
  the key is universal, required, or exhaustively understood. Do not infer undocumented
  engine semantics from examples alone.
- Never copy credentials, hosts, tokens, internal paths, or identifying production values
  from real workflow repositories.
- Do not hand-write API signatures. Link to generated reference pages.
- Do not describe intended behavior when current behavior is unresolved.

## Verification

**Never run `mkdocs` (`build`, `serve`, `gh-deploy`) or anything that writes outside
`docs/`, `mkdocs.yml`, and `documentation_audits/`.** The maintainer builds the site.
Generated-reference/Griffe warnings are out of scope.

Run from the d3tools repository with the shared environment:

```sh
/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python scripts/check_docs.py <pages>
```

- Complete JSON must be valid and parse when the checker treats it as a workflow.
- Put `<!-- check-docs: skip -->` immediately before deliberately partial JSON.
- Do not use JavaScript comments or ellipses inside complete JSON.
- Compile or run complete Python snippets when practical.
- Validate adapted workflow JSON with `scripts/check_docs.py`. When the relevant engine
  and dependencies are available, also run the narrowest safe parse/load check that
  exercises that engine configuration; otherwise state that engine-level execution was
  not verified.
- Check local links (they must stay inside `docs/`), headings, and that every
  `mkdocs.yml` nav entry points to an existing page, by reading the file.
- An audit passes with the outline fully implemented, no high/medium findings, and no
  checker failures. Items that need a code or maintainer decision are recorded as open
  questions and do not block the gate.

## Audit reports

Planning and audit files live in `documentation_audits/`:

- `guide-outline.md` (planner; binding once approved)
- `developer-guide.md`
- `user-guide.md`
- optional writer open-question files beside them

Reports contain current findings only. Do not retain corrected findings, old phase
history, “no issue” inventory rows, or verbose transcripts. Merge findings with the same
root cause and cite implementation/tests plus every affected guide location.
