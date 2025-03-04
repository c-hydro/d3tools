from typing import Optional, Iterator
import copy

try:
    from .case import Case, get_cases
    from .utils import rand_str
except ImportError:
    from case import Case, get_cases
    from utils import rand_str

class CaseManager():

    _id_len = 2

    def __init__(self, options: dict|None = None, l0_name: str = 'input'):

        if options is None: options = {}
        self.id_map = {}
        
        cases = get_cases(options)
        ids   = self.get_ids(cases)

        self._cases    = [dict(zip(ids, cases))]
        self.options  = [options.copy()]
        self._lyrmap   = {l0_name: 0}

        self._parsed_options = {k:[v for v in options[k].keys()] for k in options.keys() if isinstance(options[k], dict)}
    
    @property
    def layers(self):
        return list(self._lyrmap.keys())
    
    @property
    def nlayers(self):
        return len(self.layers)
    
    def __iter__(self) -> Iterator[Case]:
        return iter(self._cases)

    def __getitem__(self, index: int|str) -> Case:
        if isinstance(index, str):
            index = self._lyrmap[index]
        return self._cases[index]

    def get_ids(self, cases: list[Case]) -> list[str]:
        ids = []
        for case in cases:
            id = ""
            for key, value in case.tags.items():
                id_map_key = "=".join([key, value])
                if id_map_key in self.id_map:
                    id += self.id_map[id_map_key]
                else:
                    new_id = rand_str(self._id_len, 0, skip=self.id_map.values())
                    self.id_map[id_map_key] = new_id
                    id += new_id
            ids.append(id)
        
        return ids

    def add_layer(self, options: dict, name: str|None = None, merge: str|None = None):

        new_options = {}
        for k,v in options.items():
            if k not in self._parsed_options:
                new_options[k] = v
            elif isinstance(options[k], dict) and not any([v in self._parsed_options[k] for v in options[k].keys()]):
                new_options[k] = v

        new_cases = get_cases(new_options)
        new_ids = self.get_ids(new_cases)

        these_cases = {}
        prev_layer = self._cases[-1].copy()

        if merge is not None:
            merge_ids = [v for k,v in self.id_map.items() if k.split('=')[0] == merge]
            groups = [{id.replace(mid, ''): id for id in prev_layer if mid in id} for mid in merge_ids]
            for key in groups[0]:
                prev_keys = [group.get(key) for group in groups]
                case0 = prev_layer[prev_keys[0]]
                case0.options.pop(merge)
                case0.tags.pop(merge)
                casenew = Case(options = case0.options, tags = case0.tags)
                newkey  = '&'.join([f'[{k}]' for k in prev_keys])
                prev_layer[newkey] = casenew
                for key in prev_keys: prev_layer.pop(key)

        for id, case in prev_layer.items():
            for new_id, new_case in zip(new_ids, new_cases):
                _new_case = new_case + case
                _new_id = '/'.join([id, new_id])
                these_cases[_new_id] = _new_case
        
        self._cases.append(these_cases)
        self.options.append(new_options.copy() | self.options[-1])

        if name is None: name = 'layer' + str(self.nlayers)
        self._lyrmap[name] = self.nlayers
        newly_parsed = {k:[v for v in new_options[k].keys()] for k in new_options.keys() if isinstance(new_options[k], dict)}
        for k in newly_parsed:
            if k in self._parsed_options:
                self._parsed_options[k].extend(newly_parsed[k])
            else:
                self._parsed_options[k] = newly_parsed[k]

    def find_case(self, id: str, get_layer = False) -> Optional[Case|tuple[Case]]:
        for lyr_id, lyr_cases in enumerate(self._cases):
            if id in lyr_cases:
                case = lyr_cases[id]
                return case if not get_layer else (case, lyr_id)
        return None
    
    def iterate_tree(self, layer = None, get_layer = True):
        if layer is None: layer = self.nlayers-1
        last_row = self._cases[layer]

        seen_ids = []
        seen_cases = []

        for id, case in last_row.items():
            parents = get_parents(id)
            for parent in parents:
                if parent not in seen_ids:
                    seen_ids.append(parent)
                    seen_cases.append(self.find_case(parent, get_layer = get_layer))

            seen_ids.append(id)
            seen_cases.append((case, layer))

            yield from seen_cases
            seen_cases = []

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

if __name__ == '__main__':
    options = {
        'a': {'a1': 1, 'a2': 2, 'a3': 3},
        'b': {'b1': 3, 'b2': 4}
    }

    cm = CaseManager(options)

    cm.add_layer({'c': {'c1':5, 'c2': 6}})
    cm.add_layer({'d': {'d1':7, 'd2': 8}}, merge='a')
    cm.add_layer({'e': {'e1':9, 'e2': 10}})
    cm.add_layer({'f': {'f1':11, 'f2': 12}})
    
    first_id = list(cm[0].keys())[0]
    for case, layer in cm.iterate_tree(get_layer = True):
        print(case, layer)