"""Restrict guide planner/writers to their approved evidence and output surfaces.

Profiles: ``planner``, ``developer-writer``, ``user-writer``, ``auditor``, or
``orchestrator``. The developer writer and auditor may read ``src/d3tools`` and
``tests``. ``mkdocs build/serve/gh-deploy`` is denied for every profile. The hook only
guards path-bearing tool calls and fails open if the host hook schema cannot be
understood.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable


PATH_KEYS = {
    "filePath",
    "path",
    "includePattern",
    "uri",
    "query",
    "old_path",
    "new_path",
}

COMMON_BLOCKED = re.compile(
    r"(^|/)build(/|$)|"
    r"(^|/)\.github/agents/docs-doc(?:strig|string)-(?:orchestrator|writer|auditor)\.agent\.md$"
)
SOURCE_BLOCKED = re.compile(r"(^|/)(?:src/d3tools|tests)(/|$)")
USER_EXTRA_BLOCKED = re.compile(r"(^|/)docs/developer/reference(/|$)")
MKDOCS_COMMAND = re.compile(r"\bmkdocs\b\s+(?:build|serve|gh-deploy)")


def path_values(obj: object) -> Iterable[str]:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in PATH_KEYS and isinstance(value, str):
                yield value
            else:
                yield from path_values(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from path_values(item)


def deny(reason: str) -> None:
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }))


def main() -> int:
    profile = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_input = payload.get("tool_input", payload.get("toolInput", payload))
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    if isinstance(command, str) and MKDOCS_COMMAND.search(command):
        deny("Do not run mkdocs; the maintainer builds the site.")
        return 0
    for raw_value in path_values(tool_input):
        value = raw_value.replace("\\", "/")
        if COMMON_BLOCKED.search(value):
            deny(
                "Guide agents must not use build output or the protected docstring agents."
            )
            return 0
        if profile not in {"developer-writer", "auditor"} and SOURCE_BLOCKED.search(value):
            deny(
                "Only the Developer Guide writer and auditor may read d3tools source and tests."
            )
            return 0
        if profile == "user-writer" and USER_EXTRA_BLOCKED.search(value):
            deny(
                "The User Guide writer uses the audited narrative Developer Guide, "
                "not the generated API reference."
            )
            return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
