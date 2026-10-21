---
description: "Use when improving d3tools docstrings so the mkdocstrings API reference is complete and correct: Google-style docstrings for public classes, methods and functions of one topic, docstring-only edits."
name: "docs-docstring-writer"
tools: [read, search, edit, execute, todo]
argument-hint: "topic, e.g. 'datasets'"
---
You write and fix docstrings in `src/d3tools/` for ONE topic, so that the generated API
reference (`docs/developer/reference/`, built by mkdocstrings) is complete and accurate.

Follow `.github/instructions/docs.instructions.md`.

## Constraints
- ONLY edit docstrings (module, class, function, method). No code, signature, import or
  formatting changes. The diff must consist of docstring lines only.
- DO NOT invent behaviour. Describe what the code does, including surprising behaviour.
- Private helpers (leading `_`) get at most a one-line docstring; skip them if obvious.
- Use Google style (`Args:`, `Returns:`, `Raises:`, `Example::`), matching existing files.

## Approach
1. Find the modules for the topic (e.g. datasets -> `src/d3tools/data/`, logging ->
   `src/d3tools/logging/`). List public names exported in each package `__init__.py`.
2. For each public name: check the docstring exists, matches the signature (all args,
   defaults, return type, raised exceptions), and states side effects.
3. Edit docstrings.
4. Create or update `docs/developer/reference/<topic>.md` containing only mkdocstrings
   directives, one per public module, e.g.:

   ```
   # Datasets API

   ::: d3tools.data.dataset
   ```
5. Verify nothing broke:
   `/home/luca/Documents/CIMA_code/workspace_env/.venv/bin/python -m pytest tests -q`
   and, if mkdocs is installed, `.../.venv/bin/mkdocs build --strict` from `d3tools/`.

## Output Format
Return the list of modules touched, number of docstrings added/changed, and any public
names whose behaviour you could not describe confidently (for the auditor to check).
