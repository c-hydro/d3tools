"""Provision isolated documentation topic worktrees without changing existing work."""

import argparse
import subprocess
from pathlib import Path


TOPICS = (
    "configuration",
    "workflow-sections",
    "datasets",
    "timestepping",
    "logging",
    "thumbnails",
    "notification",
    "spatial",
    "cases",
    "parsing",
    "utilities",
    "getting-started",
)
ROOT = Path(__file__).resolve().parents[1]


def git(*args: str, cwd: Path | None = None) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd or ROOT, text=True).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("topics", nargs="+", choices=TOPICS)
    args = parser.parse_args()

    if git("branch", "--show-current") != "docs":
        parser.error("run from the primary docs checkout on the docs branch")
    if git("status", "--porcelain"):
        parser.error("the docs checkout has uncommitted changes; commit or resolve them first")

    entries = git("worktree", "list", "--porcelain").splitlines()
    registered = {}
    current_path = None
    for entry in entries:
        if entry.startswith("worktree "):
            current_path = Path(entry.removeprefix("worktree ")).resolve()
        elif entry.startswith("branch refs/heads/") and current_path is not None:
            registered[current_path] = entry.removeprefix("branch refs/heads/")

    plans = []
    for topic in dict.fromkeys(args.topics):
        branch = f"docs_{topic}"
        path = (ROOT.parent / f"d3tools-docs-{topic}").resolve()
        exists = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
            cwd=ROOT,
            check=False,
        ).returncode == 0
        if path in registered:
            if registered[path] != branch:
                parser.error(f"{path} belongs to {registered[path]}, not {branch}")
            if git("status", "--porcelain", cwd=path):
                parser.error(f"{path} has uncommitted changes; review them before rerunning")
            plans.append((topic, branch, path, "reuse"))
        elif path.exists():
            parser.error(f"{path} exists but is not the registered topic worktree")
        elif branch in registered.values():
            parser.error(f"{branch} is checked out in another worktree")
        else:
            plans.append((topic, branch, path, "attach" if exists else "create"))

    for topic, branch, path, action in plans:
        if action == "create":
            subprocess.run(["git", "worktree", "add", "-b", branch, str(path), "docs"], cwd=ROOT, check=True)
        elif action == "attach":
            subprocess.run(["git", "worktree", "add", str(path), branch], cwd=ROOT, check=True)
        if git("branch", "--show-current", cwd=path) != branch:
            raise RuntimeError(f"{path} did not check out {branch}")
        print(f"{topic}: {branch} at {path} ({action})")


if __name__ == "__main__":
    main()