from unittest.mock import Mock, patch

from d3tools.cli import main, parse_arguments


def test_parse_arguments_accepts_workflow_and_dates():
    args = parse_arguments(
        ["workflow.json", "--start", "2024-01-01", "--end", "2024-01-05"]
    )

    assert args.workflow_json == "workflow.json"
    assert args.start == "2024-01-01"
    assert args.end == "2024-01-05"


@patch("d3tools.cli.WorkflowDefinition.load")
def test_main_loads_checks_and_runs_workflow(load_workflow):
    engine_with_first = Mock()
    engine_without_first = Mock(spec=["get_last_ts"])
    workflow = Mock()
    workflow.workflow_sections = [
        Mock(value=engine_with_first),
        Mock(value=engine_without_first),
    ]
    load_workflow.return_value = workflow

    main(["workflow.json", "-s", "2024-01-01", "-e", "2024-01-05"])

    load_workflow.assert_called_once_with(
        "workflow.json",
        build_workflow_objects=True,
        strict_workflow_imports=True,
    )
    engine_with_first.get_last_ts.assert_called_once_with()
    engine_with_first.get_first_ts.assert_called_once_with()
    engine_without_first.get_last_ts.assert_called_once_with()
    workflow.run.assert_called_once_with(start="2024-01-01", end="2024-01-05")
