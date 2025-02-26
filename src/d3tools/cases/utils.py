import itertools
import copy

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