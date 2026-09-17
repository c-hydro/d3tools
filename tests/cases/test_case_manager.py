import pytest

from d3tools.cases import CaseManager
from d3tools.cases.case import Case, get_cases
from d3tools.cases.utils import get_parents, permutate_options, split_id


@pytest.fixture
def tiled_manager():
    return CaseManager(
        {
            "tile": {"west": "W", "east": "E"},
            "source": "input",
        }
    )


def test_case_copies_inputs_and_filters_runtime_tags():
    options = {"tile": "W", "source": "input"}
    tags = {"tile": "west", "unused": "ignored"}

    case = Case(options, tags)
    options["tile"] = "changed"
    tags["tile"] = "changed"

    assert case.options == {"tile": "W", "source": "input"}
    assert case.alltags == {"tile": "west", "unused": "ignored"}
    assert case.tags == {"tile": "west"}
    assert str(case) == "tile=west"
    assert repr(case) == "<Case: tile=west>"


def test_case_addition_combines_options_and_tags_without_mutating_operands():
    process_case = Case({"method": "mean"}, {"method": "average"})
    input_case = Case({"tile": "W"}, {"tile": "west"})

    combined = process_case + input_case

    assert combined.options == {"method": "mean", "tile": "W"}
    assert combined.tags == {"method": "average", "tile": "west"}
    assert process_case.options == {"method": "mean"}
    assert input_case.options == {"tile": "W"}


def test_permutate_options_expands_mappings_and_preserves_fixed_values():
    options = {
        "tile": {"west": "W", "east": "E"},
        "window": {"one": 1, "three": 3},
        "source": "input",
    }

    permutations, tags = permutate_options(options)

    assert len(permutations) == 4
    assert {tuple(sorted(item.items())) for item in permutations} == {
        (("source", "input"), ("tile", "W"), ("window", 1)),
        (("source", "input"), ("tile", "W"), ("window", 3)),
        (("source", "input"), ("tile", "E"), ("window", 1)),
        (("source", "input"), ("tile", "E"), ("window", 3)),
    }
    assert {tuple(sorted(item.items())) for item in tags} == {
        (("tile", "west"), ("window", "one")),
        (("tile", "west"), ("window", "three")),
        (("tile", "east"), ("window", "one")),
        (("tile", "east"), ("window", "three")),
    }


def test_get_cases_returns_one_case_when_options_have_no_case_mapping():
    cases = get_cases({"method": "mean"})

    assert len(cases) == 1
    assert cases[0].options == {"method": "mean"}
    assert cases[0].tags == {}


def test_manager_initializes_named_root_layer(tiled_manager):
    assert tiled_manager.layers == ["input"]
    assert tiled_manager.nlayers == 1
    assert tiled_manager["input"] is tiled_manager[0]
    assert len(tiled_manager[0]) == 2
    assert {case.options["tile"] for case in tiled_manager[0].values()} == {"W", "E"}
    assert {case.tags["tile"] for case in tiled_manager[0].values()} == {"west", "east"}


def test_add_layer_builds_cartesian_children_with_parent_prefixes(tiled_manager):
    parent_ids = set(tiled_manager[0])

    tiled_manager.add_layer(
        {"agg_window": {"1m": "one-month", "3m": "three-month"}},
        name="aggregate",
    )

    assert tiled_manager.layers == ["input", "aggregate"]
    assert tiled_manager.nlayers == 2
    assert tiled_manager["aggregate"] is tiled_manager[1]
    assert len(tiled_manager[1]) == 4
    assert {case.options["agg_window"] for case in tiled_manager[1].values()} == {
        "one-month",
        "three-month",
    }
    assert all(any(child_id.startswith(f"{parent_id}/") for parent_id in parent_ids)
               for child_id in tiled_manager[1])
    assert all(sum(child_id.startswith(f"{parent_id}/") for parent_id in parent_ids) == 1
               for child_id in tiled_manager[1])


def test_add_layer_with_fixed_options_keeps_one_child_per_parent(tiled_manager):
    parent_ids = set(tiled_manager[0])

    tiled_manager.add_layer({"method": "mean"}, name="process")

    assert len(tiled_manager["process"]) == len(parent_ids)
    assert all(case.options["method"] == "mean" for case in tiled_manager["process"].values())
    assert all(any(child_id.startswith(f"{parent_id}/") for parent_id in parent_ids)
               for child_id in tiled_manager["process"])


def test_add_layer_ignores_already_expanded_mapping_values(tiled_manager):
    tiled_manager.add_layer(
        {
            "tile": {"west": "different-value", "east": "different-value"},
            "method": {"mean": "mean"},
        },
        name="process",
    )

    assert len(tiled_manager["process"]) == 2
    assert {case.options["tile"] for case in tiled_manager["process"].values()} == {"W", "E"}
    assert all(case.options["method"] == "mean" for case in tiled_manager["process"].values())


def test_add_layer_can_merge_a_dimension_before_processing(tiled_manager):
    root_ids = set(tiled_manager[0])

    tiled_manager.add_layer({"method": {"mean": "mean"}}, name="combine", merge="tile")

    assert len(tiled_manager["combine"]) == 1
    merged_id, merged_case = next(iter(tiled_manager["combine"].items()))
    assert "tile" not in merged_case.options
    assert "tile" not in merged_case.tags
    assert merged_case.options["method"] == "mean"
    assert set(get_parents(merged_id)) == root_ids


def test_find_case_returns_case_and_layer(tiled_manager):
    root_id, expected = next(iter(tiled_manager[0].items()))

    assert tiled_manager.find_case(root_id) is expected
    assert tiled_manager.find_case(root_id, get_layer=True) == (expected, 0)
    assert tiled_manager.find_case("unknown") is None


def test_iterate_tree_yields_each_case_once_in_parent_before_child_order(tiled_manager):
    tiled_manager.add_layer(
        {"agg_window": {"1m": "one-month", "3m": "three-month"}},
        name="aggregate",
    )

    traversed = list(tiled_manager.iterate_tree())

    assert len(traversed) == 6
    assert sum(layer == 0 for _, layer in traversed) == 2
    assert sum(layer == 1 for _, layer in traversed) == 4
    positions = {id(case): index for index, (case, _) in enumerate(traversed)}
    for child_id, child in tiled_manager[1].items():
        parent_id = get_parents(child_id)[0]
        parent = tiled_manager.find_case(parent_id)
        assert positions[id(parent)] < positions[id(child)]


def test_iterate_tree_handles_converging_parents_without_duplicates(tiled_manager):
    tiled_manager.add_layer({"method": {"mean": "mean"}}, name="combine", merge="tile")

    traversed = list(tiled_manager.iterate_tree())

    assert len(traversed) == 3
    assert sum(layer == 0 for _, layer in traversed) == 2
    assert sum(layer == 1 for _, layer in traversed) == 1
    assert len({id(case) for case, _ in traversed}) == 3


def test_get_and_iterate_subtree_respect_depth(tiled_manager):
    tiled_manager.add_layer(
        {"agg_window": {"1m": "one-month", "3m": "three-month"}},
        name="aggregate",
    )
    tiled_manager.add_layer({"method": "mean"}, name="process")
    root_id = next(iter(tiled_manager[0]))

    subtree = tiled_manager.get_subtree(root_id, depth=1)
    descendants = list(tiled_manager.iterate_subtree(root_id, depth=1))

    assert len(subtree) == 1
    assert len(subtree[0]) == 2
    assert all(case_id.startswith(f"{root_id}/") for case_id in subtree[0])
    assert len(descendants) == 2
    assert all(layer == 1 for _, layer in descendants)


def test_get_subtree_rejects_unknown_start_id(tiled_manager):
    with pytest.raises(ValueError, match="Unknown case ID"):
        tiled_manager.get_subtree("unknown")


@pytest.mark.parametrize(
    ("case_id", "expected"),
    [
        ("AA/BB/CC", ["AA", "AA/BB"]),
        ("[AA]&[BB]/CC", ["AA", "BB"]),
        ("AA/[BB/CC]&[DD/EE]/FF", ["AA", "BB", "DD", "BB/CC", "DD/EE"]),
    ],
)
def test_id_parsing_preserves_merged_parent_paths(case_id, expected):
    assert get_parents(case_id) == expected


def test_split_id_ignores_separators_inside_brackets():
    assert split_id("AA/[BB/CC]&[DD/EE]/FF") == ["AA", "[BB/CC]&[DD/EE]", "FF"]