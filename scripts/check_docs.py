"""Mechanical consistency checks for the d3tools markdown documentation.

Checks performed on every fenced code block and link in the given markdown files:

- ``json`` blocks: must be valid JSON. Blocks that look like a complete workflow
  config (all top-level keys are reserved keys or section aliases) are also parsed
  with ``WorkflowDefinition(..., build_workflow_objects=False)``.
- ``python`` blocks: must compile, and every ``d3tools``/``door``/``dam``/``dryes``
  import they contain must succeed.
- relative links: the target file must exist, and ``#anchor`` fragments must match
  a heading in the target file.

Put ``<!-- check-docs: skip -->`` on the line right before a fence to skip that
block (use for intentionally partial or illustrative snippets).

Usage::

    python scripts/check_docs.py                 # all of docs/
    python scripts/check_docs.py docs/datasets   # a folder
    python scripts/check_docs.py docs/configuration/tags.md docs/timestepping.md

Exit code is 1 if any ERROR is reported, 0 otherwise.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import importlib
import json
import os
import re
import sys
import tempfile
import warnings
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKED_PACKAGES = {"d3tools", "door", "dam", "dryes"}
SKIP_MARKER = "<!-- check-docs: skip -->"
DUMMY_ENV_VALUE = "__check_docs_dummy__"

FENCE_RE = re.compile(r"^(\s*)```([\w+-]*)\s*$")
LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HEADING_RE = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$")
ENV_RE = re.compile(r"\{ENV\.([\w#\-\.]+)")


@dataclass
class Finding:
    path: Path
    line: int
    level: str  # ERROR | WARN
    kind: str
    message: str

    def __str__(self) -> str:
        rel = self.path.relative_to(REPO_ROOT) if self.path.is_relative_to(REPO_ROOT) else self.path
        return f"{rel}:{self.line}: {self.level} [{self.kind}] {self.message}"


@dataclass
class CodeBlock:
    lang: str
    start_line: int
    text: str
    skipped: bool


def iter_markdown_files(targets: list[str]) -> list[Path]:
    files: list[Path] = []
    for target in targets:
        path = Path(target)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if path.is_dir():
            files.extend(sorted(path.rglob("*.md")))
        elif path.suffix == ".md" and path.exists():
            files.append(path)
        else:
            print(f"warning: skipping {target} (not a markdown file or folder)", file=sys.stderr)
    return files


def extract_blocks(lines: list[str]) -> list[CodeBlock]:
    blocks: list[CodeBlock] = []
    i = 0
    while i < len(lines):
        match = FENCE_RE.match(lines[i])
        if not match:
            i += 1
            continue
        lang = match.group(2).lower()
        skipped = i > 0 and lines[i - 1].strip() == SKIP_MARKER
        start = i + 1
        j = start
        while j < len(lines) and not FENCE_RE.match(lines[j]):
            j += 1
        blocks.append(CodeBlock(lang, start + 1, "\n".join(lines[start:j]), skipped))
        i = j + 1
    return blocks


def slugify(heading: str) -> str:
    """GitHub-style heading anchor."""
    text = re.sub(r"`|\*|_(?=\W)|(?<=\W)_", "", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def heading_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    in_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = HEADING_RE.match(line)
        if match:
            slug = slugify(match.group(1))
            n = counts.get(slug, 0)
            anchors.add(slug if n == 0 else f"{slug}-{n}")
            counts[slug] = n + 1
    return anchors


def check_links(path: Path, lines: list[str]) -> list[Finding]:
    findings: list[Finding] = []
    in_fence = False
    for lineno, line in enumerate(lines, start=1):
        if FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for match in LINK_RE.finditer(line):
            target = match.group(1)
            if re.match(r"^[a-z]+:", target):
                continue
            file_part, _, anchor = target.partition("#")
            target_path = path if not file_part else (path.parent / file_part).resolve()
            if not target_path.exists():
                findings.append(Finding(path, lineno, "ERROR", "link", f"target does not exist: {target}"))
                continue
            if anchor and target_path.suffix == ".md" and anchor not in heading_anchors(target_path):
                findings.append(Finding(path, lineno, "ERROR", "anchor", f"no heading for #{anchor} in {file_part or path.name}"))
    return findings


def looks_like_workflow_config(cfg: object) -> bool:
    from d3tools.config.workflow_definition import WorkflowDefinition
    from d3tools.config.workflow_section import resolve_workflow_section_alias
    from d3tools.parse import normalise_string

    if not isinstance(cfg, dict) or not cfg:
        return False
    reserved = WorkflowDefinition.RESERVED_TOP_LEVEL_KEYS
    return all(
        normalise_string(key) in reserved or resolve_workflow_section_alias(key) is not None
        for key in cfg
    )


@contextlib.contextmanager
def isolated_environment(cfg_text: str):
    """Run in a temp cwd with a restorable os.environ and dummy values for missing ENV vars."""
    saved_env = os.environ.copy()
    saved_cwd = Path.cwd()
    for var in ENV_RE.findall(cfg_text):
        os.environ.setdefault(var, DUMMY_ENV_VALUE)
    with tempfile.TemporaryDirectory() as tmp:
        os.chdir(tmp)
        try:
            yield
        finally:
            os.chdir(saved_cwd)
            os.environ.clear()
            os.environ.update(saved_env)


def check_json_block(path: Path, block: CodeBlock) -> list[Finding]:
    try:
        cfg = json.loads(block.text)
    except json.JSONDecodeError as exc:
        return [Finding(path, block.start_line + exc.lineno - 1, "ERROR", "json",
                        f"invalid JSON ({exc.msg}); fix it or mark the block with {SKIP_MARKER}")]

    if not looks_like_workflow_config(cfg):
        return []

    from d3tools.config.workflow_definition import WorkflowDefinition

    with isolated_environment(block.text), warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            WorkflowDefinition(cfg, build_workflow_objects=False, strict_workflow_imports=False)
        except OSError as exc:
            # absolute paths, remote hosts, etc. that don't exist on this machine
            return [Finding(path, block.start_line, "WARN", "workflow-env",
                            f"could not fully verify on this machine: {type(exc).__name__}: {exc}")]
        except Exception as exc:  # noqa: BLE001 - report any parse failure
            level = "WARN" if DUMMY_ENV_VALUE in str(exc) else "ERROR"
            return [Finding(path, block.start_line, level, "workflow",
                            f"WorkflowDefinition failed: {type(exc).__name__}: {exc}")]
    return [Finding(path, block.start_line, "WARN", "workflow", f"parse warning: {w.message}") for w in caught]


def check_python_block(path: Path, block: CodeBlock) -> list[Finding]:
    try:
        tree = ast.parse(block.text)
    except SyntaxError as exc:
        return [Finding(path, block.start_line + (exc.lineno or 1) - 1, "ERROR", "python",
                        f"syntax error: {exc.msg}")]

    findings: list[Finding] = []
    for node in ast.walk(tree):
        line = block.start_line + getattr(node, "lineno", 1) - 1
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in CHECKED_PACKAGES:
                    findings.extend(_try_import(path, line, alias.name, None))
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module.split(".")[0] in CHECKED_PACKAGES:
                for alias in node.names:
                    findings.extend(_try_import(path, line, node.module, alias.name))
    return findings


def _try_import(path: Path, line: int, module: str, name: str | None) -> list[Finding]:
    label = f"{module}.{name}" if name else module
    try:
        mod = importlib.import_module(module)
    except Exception as exc:  # noqa: BLE001
        return [Finding(path, line, "ERROR", "import", f"cannot import {module}: {type(exc).__name__}: {exc}")]
    if name and name != "*" and not hasattr(mod, name):
        try:
            importlib.import_module(f"{module}.{name}")
        except Exception:  # noqa: BLE001
            return [Finding(path, line, "ERROR", "import", f"{label} does not exist")]
    return []


def check_file(path: Path) -> list[Finding]:
    lines = path.read_text(encoding="utf-8").splitlines()
    findings = check_links(path, lines)
    for block in extract_blocks(lines):
        if block.skipped:
            continue
        if block.lang == "json":
            findings.extend(check_json_block(path, block))
        elif block.lang in ("python", "py"):
            findings.extend(check_python_block(path, block))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("targets", nargs="*", default=[str(REPO_ROOT / "docs")],
                        help="markdown files or folders (default: docs/)")
    args = parser.parse_args()

    files = iter_markdown_files(args.targets)
    findings: list[Finding] = []
    for path in files:
        findings.extend(check_file(path))

    for finding in findings:
        print(finding)
    n_err = sum(f.level == "ERROR" for f in findings)
    n_warn = sum(f.level == "WARN" for f in findings)
    print(f"\nchecked {len(files)} file(s): {n_err} error(s), {n_warn} warning(s)")
    return 1 if n_err else 0


if __name__ == "__main__":
    sys.exit(main())
