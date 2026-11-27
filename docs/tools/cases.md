# Cases

The `d3tools.cases` package expands named option choices into concrete cases.
It supports two related patterns:

- `get_cases()` creates the Cartesian product of several option dimensions;
- `CaseManager` arranges successive expansions into named layers that can be
  traversed as a tree.

Cases do not prescribe what an option means. A plain Python function, a
Dataset catalogue operation, or an external workflow engine decides how to
use the resolved options and tags.

## Define option dimensions

An option whose value is a dictionary defines a case dimension. Its dictionary
keys become tags and its values become the concrete runtime values. Every
non-dictionary option remains fixed across all generated cases.

```python
from d3tools.cases.case import get_cases

options = {
    "variable": {"rain": "precipitation", "temp": "temperature"},
    "window": {"1m": 1, "3m": 3},
    "year": 2024,
}

cases = get_cases(options)

assert len(cases) == 4
assert {case.name for case in cases} == {
    "variable=rain,window=1m",
    "variable=rain,window=3m",
    "variable=temp,window=1m",
    "variable=temp,window=3m",
}
assert all(case.options["year"] == 2024 for case in cases)
```

For each dimension, the tag is the dictionary key (`rain`) while the resolved
option is its value (`precipitation`). This separation lets tags remain short
and suitable for filenames or query filters while options carry the values
needed for processing.

### Expand options without creating Case objects

`permutate_options()` exposes the two lists used to create cases: resolved
option dictionaries and their corresponding tag dictionaries.

```python
from d3tools.cases.utils import permutate_options

resolved, tags = permutate_options(
    {
        "method": {"avg": "mean", "max": "maximum"},
        "format": "netcdf",
    }
)

assert resolved == [
    {"method": "mean", "format": "netcdf"},
    {"method": "maximum", "format": "netcdf"},
]
assert tags == [{"method": "avg"}, {"method": "max"}]
```

The iteration order follows the insertion order of the option mapping and of
each dimension mapping. If there are no dictionary-valued options,
`get_cases()` returns one case containing the fixed options and no selected
tags.

## Work with Case objects

A `Case` exposes:

| Attribute | Contents |
| --- | --- |
| `options` | The complete resolved option mapping. |
| `tags` | Selected tags whose names also occur in `options`. |
| `alltags` | The original tag mapping passed to the constructor. |
| `name` | A comma-separated `key=value` rendering of `tags`. |

The constructor copies its input mappings. Adding two cases creates a new
case without modifying either operand; values from the right-hand case take
precedence when keys overlap.

```python
from d3tools.cases.case import Case

processing = Case(
    options={"method": "mean", "scale": 1},
    tags={"method": "average"},
)
source = Case(
    options={"tile": "W", "scale": 2},
    tags={"tile": "west"},
)

combined = processing + source

assert combined.options == {"method": "mean", "scale": 2, "tile": "W"}
assert combined.tags == {"method": "average", "tile": "west"}
assert str(combined) == "method=average,tile=west"
```

Case equality compares resolved `options`, not names or tag dictionaries.

## Run a function for several cases

`@withcases` consumes an optional `cases` keyword. When it receives a list of
case dictionaries, it merges each dictionary's `tags` into the call arguments,
runs the decorated function once per case, and returns the results as a list.
Case tags override same-named keyword arguments.

```python
from d3tools.cases.case import get_cases
from d3tools.cases.utils import withcases

cases = get_cases(
    {
        "variable": {"rain": "precipitation", "temp": "temperature"},
        "season": {"wet": "MAM", "dry": "JJA"},
    }
)

@withcases
def output_name(year, variable, season, extension="nc"):
    return f"{variable}_{season}_{year}.{extension}"

names = output_name(
    year=2024,
    cases=[{"tags": case.tags} for case in cases],
)

assert names == [
    "rain_wet_2024.nc",
    "rain_dry_2024.nc",
    "temp_wet_2024.nc",
    "temp_dry_2024.nc",
]
```

The decorator expects dictionaries containing a `tags` mapping, rather than
`Case` objects directly. Other keys in each dictionary are ignored. With no
`cases` keyword, or with `cases=None`, the function runs once normally; an
empty list returns an empty list. Exceptions from the decorated function are
not caught, so processing stops and the exception propagates.

Dataset catalogue methods use this decorator to repeat the same availability
or key operation for several tag sets. See [Datasets — Time and
availability](datasets.md#time-and-availability) for the catalogue interface.

## Build layered cases

`CaseManager` starts with a root layer and can add further option dimensions
as child layers. Each layer is a mapping from generated case IDs to `Case`
objects.

```python
from d3tools.cases import CaseManager

manager = CaseManager(
    {
        "tile": {"west": "W", "east": "E"},
        "source": "input",
    },
    l0_name="inputs",
)
manager.add_layer(
    {"window": {"1m": 1, "3m": 3}},
    name="indicators",
)
manager.add_layer({"format": "netcdf"}, name="outputs")

assert manager.layers == ["inputs", "indicators", "outputs"]
assert manager.nlayers == 3
assert len(manager["inputs"]) == 2
assert len(manager["indicators"]) == 4
assert len(manager["outputs"]) == 4
assert all(case.options["source"] == "input" for case in manager[2].values())
assert all(case.options["format"] == "netcdf" for case in manager[2].values())
```

Layers can be selected by name or integer index. Iterating over the manager
yields the layer mappings themselves, not individual cases. If `name` is
omitted, `add_layer()` assigns a name such as `layer2`.

When a mapping dimension has already been expanded, later layers ignore
values already parsed for that dimension. Prefer introducing each dimension
in one layer; it makes the resulting tree and option inheritance much easier
to reason about.

### IDs and parent paths

Within a manager, each tag/value pair receives a random two-character token.
The same pair reuses its token in later operations. A child ID appends its new
token segment to its parent ID with `/`:

```text
root-token/child-token/grandchild-token
```

IDs are deliberately opaque and random. Use them to navigate one manager
instance, but do not persist them or interpret individual token characters.
`id_map` is available when an application needs to relate the opaque tokens
back to their `tag=value` assignments.

`find_case()` searches all layers and optionally returns the layer index:

```python
from d3tools.cases import CaseManager
from d3tools.cases.utils import get_parents

manager = CaseManager({"tile": {"west": "W", "east": "E"}})
manager.add_layer({"method": {"avg": "mean"}}, name="process")

child_id, child = next(iter(manager["process"].items()))
parent_id = get_parents(child_id)[0]

assert manager.find_case(child_id) is child
assert manager.find_case(parent_id, get_layer=True)[1] == 0
assert manager.find_case("unknown") is None
```

### Merge a dimension

Passing `merge="dimension"` to `add_layer()` combines previous cases across
that dimension before the new layer is created. The merged case no longer has
that dimension in its options or tags. Its ID records every contributing
parent as bracketed paths joined by `&`.

```python
from d3tools.cases import CaseManager
from d3tools.cases.utils import get_parents

manager = CaseManager({"tile": {"west": "W", "east": "E"}})
root_ids = set(manager[0])
manager.add_layer(
    {"method": {"avg": "mean"}},
    name="mosaic",
    merge="tile",
)

merged_id, merged = next(iter(manager["mosaic"].items()))

assert len(manager["mosaic"]) == 1
assert "tile" not in merged.options
assert set(get_parents(merged_id)) == root_ids
```

Merging assumes that the named dimension exists in the previous layer. It is
best used immediately after the layer that introduced that dimension.

## Traverse trees and subtrees

`iterate_tree(depth=None, get_layer=True)` walks all paths through a selected
layer, yielding parents before children and avoiding duplicate ancestors.
`depth` is a zero-based layer index and defaults to the final layer.

`get_subtree(start_id, depth=999)` returns descendant layer mappings below a
case. `iterate_subtree()` yields the same descendants depth-first. Both exclude
the starting case; an unknown start ID raises `ValueError`.

```python
from d3tools.cases import CaseManager

manager = CaseManager({"tile": {"west": "W", "east": "E"}})
manager.add_layer({"window": {"1m": 1, "3m": 3}}, name="indicators")
manager.add_layer({"format": "netcdf"}, name="outputs")

root_id = next(iter(manager[0]))
direct_children = manager.get_subtree(root_id, depth=1)
descendants = list(manager.iterate_subtree(root_id, get_layer=False))
tree = list(manager.iterate_tree())

assert len(direct_children) == 1
assert len(direct_children[0]) == 2
assert len(descendants) == 4  # two children and their two output cases
assert all(layer in {0, 1, 2} for _, layer in tree)
```

Set `get_layer=False` on either iterator when only `Case` objects are needed.

## Parse case IDs

`split_id()` separates a hierarchical ID at top-level `/` characters while
preserving separators inside brackets. `get_parents()` understands both normal
paths and merged parent groups:

```python
from d3tools.cases.utils import get_parents, split_id

case_id = "AA/[BB/CC]&[DD/EE]/FF"

assert split_id(case_id) == ["AA", "[BB/CC]&[DD/EE]", "FF"]
assert get_parents(case_id) == ["AA", "BB", "DD", "BB/CC", "DD/EE"]
```

`split_id()` also accepts a different separator and bracket pair. `rand_str()`
is the lower-level random token helper used by `CaseManager`; application code
normally does not need to call it.

## Limits and error behaviour

- Every dictionary-valued option is treated as a dimension. There is no escape
  syntax for keeping an arbitrary dictionary as one fixed option.
- Case IDs are random between manager instances and runs.
- `find_case()` returns `None` for an unknown ID; subtree methods raise
  `ValueError` for one.
- A missing layer name raises `KeyError`, and an invalid numeric layer index
  raises `IndexError`.
- `@withcases` expects each list item to contain a `tags` mapping. Keep the tag
  keys consistent across a batch so arguments cannot carry over unexpectedly
  between calls.
- Case expansion is in memory. The number of cases is the product of all
  dimension sizes, so large dimension sets can grow quickly.

The cases package defines combination and traversal mechanics only. Any
engine-specific case payload or execution policy belongs to the engine that
consumes it. For complete signatures, see the
[Cases API reference](../reference/cases.md).
