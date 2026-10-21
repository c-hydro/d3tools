"""PreToolUse hook: keep "docs-only" writer agents from reading source code.

Receives the hook payload as JSON on stdin and denies any tool call whose path-like
arguments point into d3tools source, tests, or build output. Fails open (allows) if
the payload can't be understood, so a schema change never blocks normal work.
"""

import json
import re
import sys

BLOCKED = re.compile(r"(^|/)d3tools/(src|tests|build)(/|$)|(^|/)(src|tests|build)/d3tools(/|$)")
PATH_KEYS = {"filePath", "path", "includePattern", "uri", "query", "old_path", "new_path"}


def path_values(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in PATH_KEYS and isinstance(value, str):
                yield value
            else:
                yield from path_values(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from path_values(item)


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_input = payload.get("tool_input", payload.get("toolInput", payload))
    for value in path_values(tool_input):
        if BLOCKED.search(value.replace("\\", "/")):
            print(json.dumps({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": (
                        "This agent writes from documentation only; reading d3tools "
                        "src/, tests/ or build/ is not allowed. Record the gap as an "
                        "open question instead."
                    ),
                }
            }))
            return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
