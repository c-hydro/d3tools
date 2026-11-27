"""Utility helpers for case expansion and case-ID traversal.

The helpers in this module support generating case permutations, decorating
functions to execute over multiple case tags, and resolving ancestor IDs for the
hierarchical case trees created by :class:`d3tools.cases.case_manager.CaseManager`.
"""

import copy
import itertools


def withcases(func):
    """Decorate a function so it can be called once per case in a case list.

    When the wrapped function receives a ``cases`` keyword containing a list of
    case dictionaries, it executes the function once per case after merging each
    case's ``tags`` mapping into the call arguments. If ``cases`` is missing or
    ``None``, the function is invoked normally.

    Args:
        func: Callable to wrap.

    Returns:
        Callable: A wrapper that either runs ``func`` normally or expands it over
        a sequence of case-tag dictionaries.

    Example:
        Generate output names for several variable/season cases while sharing
        the year and file format across all runs::

            >>> @withcases
            ... def output_name(variable, season, year, extension):
            ...     return f"{variable}_{season}_{year}.{extension}"
            >>> output_name(
            ...     year=2024,
            ...     extension='nc',
            ...     cases=[
            ...         {'tags': {'variable': 'rain', 'season': 'JJA'}},
            ...         {'tags': {'variable': 'temperature', 'season': 'DJF'}},
            ...     ],
            ... )
            ['rain_JJA_2024.nc', 'temperature_DJF_2024.nc']
    """
    def wrapper(*args, **kwargs):
        if 'cases' in kwargs:
            cases = kwargs.pop('cases')
            if cases is not None:
                these_kwargs = copy.deepcopy(kwargs)
                output = []
                for case in cases:
                    these_kwargs.update(case['tags'])
                    output.append(func(*args, **these_kwargs))
                return output
        return func(*args, **kwargs)
    return wrapper


def permutate_options(options):
    """Expand a mapping of dictionary-valued options into all permutations.

    Args:
        options: Mapping whose dictionary entries define case dimensions; all
            non-dictionary entries remain fixed for every generated case.

    Returns:
        tuple[list[dict], list[dict]]: The first list contains concrete option
        dictionaries for each generated case, and the second contains the tag
        dictionaries that record the selected dimension values.
    """
    # get the options that need to be permutated and the ones that are fixed

    # dictionaries need to be permutated
    to_permutate = {k: list(v.keys()) for k, v in options.items() if isinstance(v, dict)}

    # and everything else is fixed
    fixed_options = {k: v for k, v in options.items() if not isinstance(v, dict)}

    values_to_permutate = [v for v in to_permutate.values()]
    keys = list(to_permutate.keys())

    permutations = [dict(zip(keys, p)) for p in itertools.product(*values_to_permutate)]
    identifiers = copy.deepcopy(permutations)
    for permutation in permutations:
        for k in permutation:
            permutation[k] = options[k][permutation[k]]
        permutation.update(fixed_options)

    return permutations, identifiers


def rand_str(l=4, n=0, skip=None):
    """Generate one or more random uppercase/digit identifiers.

    Args:
        l: Length of each generated identifier.
        n: Number of identifiers to generate. A value of 0 returns a single
            string instead of a list.
        skip: Collection of identifiers that must not be reused.

    Returns:
        str | list[str]: A single identifier when ``n`` is 0, otherwise a list of
        generated identifiers. Values in ``skip`` are avoided when possible.
    """
    import random
    import string

    lst = [''.join(random.choices(string.ascii_uppercase + string.digits, k=l)) for _ in range(max(n, 1))]

    if skip is not None:
        while any([l in skip for l in lst]):
            for s in skip:
                if s in lst:
                    lst.remove(s)
            new = rand_str(l, n - len(lst))
            lst = lst + [new] if isinstance(new, str) else lst + new

    if n == 0:
        return lst[0]

    return lst


def split_id(id, sep='/', bracket=('[', ']')):
    """Split a case ID on top-level separators while preserving grouped values.

    Args:
        id: The case ID to split.
        sep: Separator to split on.
        bracket: Opening and closing bracket characters that keep separator
            characters inside grouped subexpressions from being treated as
            top-level separators.

    Returns:
        list[str]: The ID split into its top-level segments.
    """
    parts = []
    bracket_level = 0
    current_part = []

    for char in id:
        if char == bracket[0]:
            bracket_level += 1
        elif char == bracket[1]:
            bracket_level -= 1
        elif char == sep and bracket_level == 0:
            parts.append(''.join(current_part))
            current_part = []
            continue
        current_part.append(char)

    parts.append(''.join(current_part))
    return parts


def get_parents(id):
    """Return parent case IDs for a hierarchical case ID.

    Args:
        id: Case ID to trace back through its ancestor chain.

    Returns:
        list[str]: Ancestor IDs, ordered from shortest to longest path and with
        duplicates removed. Root-level IDs return an empty list.
    """
    id_sep = split_id(id)
    if len(id_sep) == 1:
        return []

    parents = []
    for i, piece in enumerate(id_sep[:-1]):
        if '&' in piece:
            more_parents = []
            for subpiece in split_id(piece, '&'):
                if subpiece.startswith('[') and subpiece.endswith(']'):
                    subpiece = subpiece[1:-1]
                more_parents.append(subpiece)
            for parent in more_parents:
                parents.extend(get_parents(parent))
            parents.extend(more_parents)
        else:
            parents.append('/'.join(id_sep[:i + 1]))

    # remove duplicates
    parents = list(dict.fromkeys(parents))

    # order by length
    parents.sort(key=lambda x: len(x))

    return parents