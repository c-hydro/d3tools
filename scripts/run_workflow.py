"""Compatibility wrapper for the installed ``d3run`` command."""

from d3tools.cli import main, parse_arguments  # noqa: F401


if __name__ == "__main__":
    main()
