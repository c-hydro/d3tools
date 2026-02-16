import itertools
import copy

def withcases(func):
    """
    Decorator that enables case-based expansion of function calls.
    
    This decorator allows a function to be called once but executed multiple times,
    once for each case in a list of cases. Each case contains a set of tag values
    that are unpacked and passed to the decorated function.
    
    When a function is decorated with @withcases, it can accept a 'cases' keyword
    argument. If provided, the function will be executed once per case, with each
    case's tags merged into the function's kwargs.
    
    Use Cases:
        - Processing datasets with multiple tag combinations (e.g., variables, aggregations)
        - Batch operations across different parameter sets
        - Workflows that need to run the same operation with different configurations
    
    Args:
        func: The function to be decorated. Can be any callable.
    
    Returns:
        A wrapper function that handles case expansion.
    
    Behavior:
        - If 'cases' kwarg is present and not None:
            * Executes func once per case
            * Each case's 'tags' dict is unpacked into func's kwargs
            * Returns a list of results, one per case
        - If 'cases' kwarg is absent or None:
            * Executes func normally with provided args/kwargs
            * Returns single result
    
    Case Structure:
        Each case in the cases list should be a dict with at minimum:
        {
            'tags': {
                'tag_name': value,
                ...
            },
            # Optional: other case metadata
        }
    
    Example:
        >>> @withcases
        ... def process_data(time, variable, agg):
        ...     return f"Processing {variable} with {agg} aggregation at {time}"
        
        >>> # Normal call (no cases)
        >>> process_data(time='2024-01', variable='temp', agg='3m')
        'Processing temp with 3m aggregation at 2024-01'
        
        >>> # Call with cases - executes multiple times
        >>> cases = [
        ...     {'tags': {'variable': 'Tmin', 'agg': '3m'}},
        ...     {'tags': {'variable': 'Tmin', 'agg': '6m'}},
        ...     {'tags': {'variable': 'Tmax', 'agg': '3m'}},
        ...     {'tags': {'variable': 'Tmax', 'agg': '6m'}},
        ... ]
        >>> process_data(time='2024-01', cases=cases)
        [
            'Processing Tmin with 3m aggregation at 2024-01',
            'Processing Tmin with 6m aggregation at 2024-01',
            'Processing Tmax with 3m aggregation at 2024-01',
            'Processing Tmax with 6m aggregation at 2024-01'
        ]
    
    Common Workflow Integration:
        Used extensively with DataCatalogue methods to query data across
        multiple tag combinations:
        
        >>> dataset.find_times(
        ...     times=[...],
        ...     cases=[
        ...         {'tags': {'tile': 'h18v04'}},
        ...         {'tags': {'tile': 'h19v04'}},
        ...     ]
        ... )
        # Returns: [[times for h18v04], [times for h19v04]]
    
    Note:
        The 'cases' parameter is consumed by the decorator (popped from kwargs),
        so the decorated function does not need to handle it explicitly.
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

def rand_str(l = 4, n = 0, skip = None):
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

def split_id(id, sep = '/', bracket = ('[', ']')):
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
    id_sep = split_id(id)
    if len(id_sep) == 1:
        return []

    parents = []
    for i, piece in enumerate(id_sep[:-1]):
        if '&' in piece:
            more_parents = []
            for subpiece in split_id(piece,'&'):
                if subpiece.startswith('[') and subpiece.endswith(']'):
                    subpiece = subpiece[1:-1]
                more_parents.append(subpiece)
            for parent in more_parents:
                parents.extend(get_parents(parent))
            parents.extend(more_parents)
        else:
            parents.append('/'.join(id_sep[:i+1]))

    # remove duplicates
    parents = list(dict.fromkeys(parents))

    # order by length
    parents.sort(key = lambda x: len(x))

    return parents