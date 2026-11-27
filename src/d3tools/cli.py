"""Command-line interface for running D3tools workflows."""

import argparse
from collections.abc import Sequence

from d3tools import WorkflowDefinition


def parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for a workflow run.

    Args:
        argv: Arguments to parse. When omitted, arguments are read from
            :data:`sys.argv`.

    Returns:
        The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run workflow with specified parameters",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("workflow_json", help="JSON file of the workflow")
    parser.add_argument(
        "-s",
        "--start",
        help="Start date to download the data [YYYY-MM-DD]",
    )
    parser.add_argument(
        "-e",
        "--end",
        help="End date to download the data [YYYY-MM-DD]",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Load and run the workflow selected on the command line.

    Args:
        argv: Arguments to parse. When omitted, arguments are read from
            :data:`sys.argv`.
    """
    args = parse_arguments(argv)
    options: WorkflowDefinition = WorkflowDefinition.load(
        args.workflow_json,
        build_workflow_objects=True,
        strict_workflow_imports=True,
    )

    for workflow_section in options.workflow_sections:
        workflow_section.value.get_last_ts()
        if hasattr(workflow_section.value, "get_first_ts"):
            workflow_section.value.get_first_ts()

    options.run(start=args.start, end=args.end)


if __name__ == "__main__":
    main()
