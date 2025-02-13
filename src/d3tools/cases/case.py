try:
    from .utils import permutate_options
except ImportError:
    from utils import permutate_options

class Case():

    def __init__(self, options: dict, tags: dict):
        self.options = options.copy()
        self.tags = {}

        for key in self.options:
            if key in tags:
                self.tags[key] = tags[key]

        self.name = ','.join(f'{k}={v}' for k,v in self.tags.items())

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return '<Case: ' + self.name + '>'
    
    def __add__(self, other):
        new_options = {**self.options, **other.options}
        new_tags    = {**self.tags, **other.tags}
        return Case(new_options, new_tags)
    
    def __eq__(self, other):
        return self.options == other.options

def get_cases(options):
    all_options, all_tags = permutate_options(options)

    cases = []
    for opt, tags in zip(all_options, all_tags):
        cases.append(Case(opt, tags))

    return(cases)