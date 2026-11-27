# d3tools repository guidance

## Scope and source of truth

- `src/d3tools/` is the implementation source of truth. Use `tests/` as evidence of
  exercised behaviour, but do not describe untested intent as a guarantee.
- Keep changes scoped to this repository unless the request explicitly includes the
  sibling `door`, `dam`, or `dryes` repositories.
- Preserve compatibility aliases and public behaviour unless a requested change
  explicitly permits breaking them.

## Python environment and checks

- Run Python and Python-based tools through the shared environment:
  `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python`.
- Run the test suite from the repository root with:
  `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python -m pytest tests -q`.
- Prefer focused tests while developing, followed by the full suite when the scope of
  the change warrants it.
- Run `git diff --check` before handing off changes.

## Python documentation

- Use Google-style docstrings, consistent with the `mkdocstrings` configuration in
  `mkdocs.yml`.
- Check docstrings against signatures, implementations, and relevant tests. Document
  meaningful defaults, return behaviour, exceptions, side effects, and state changes.
- Treat public definitions throughout the recursive package tree as documentation
  candidates, not only names re-exported by an `__init__.py` file.
- Do not invent guarantees or conceal surprising behaviour. Label uncertainty when the
  implementation or tests do not establish a clear contract.
- Keep docstring-only requests limited to docstring edits; report possible code defects
  separately rather than silently changing their behaviour.

## Documentation site

- `docs/workflow_orchestration/` owns workflow configuration and operation guidance.
- `docs/tools/` owns Python-first narrative guidance for reusable components.
- `docs/reference/` contains concise, module-shaped `mkdocstrings` reference pages and
  should not duplicate tutorials or hand-written signatures.
- Keep each fact on one owning page and link to it elsewhere.
- Validate Markdown with:
  `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python tools/check_docs.py`.
- Zensical is the production documentation generator. Do not run documentation build or
  preview commands unless the maintainer explicitly asks for a rendered-site check.
- Never commit credentials, private infrastructure details, or downloaded case-study
  parameter data.

## Repository hygiene

- Preserve unrelated user changes in a dirty worktree.
- Do not use generated directories such as `build/`, caches, or `_old_docs/` as current
  implementation or documentation evidence.
- Do not commit, push, merge, delete tracked material, or rewrite history unless the
  request explicitly authorizes that action.
