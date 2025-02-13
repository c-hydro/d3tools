from typing import Optional, Iterator

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
        self._lyrmap   = {l0_name: 0}

        self._parsed_options = options.keys()
    
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

    def add_layer(self, options: dict, name: str|None = None):

        new_options = {k:v for k,v in options.items() if k not in self._parsed_options}
        new_cases = get_cases(new_options)
        new_ids = self.get_ids(new_cases)

        these_cases = {}
        prev_layer = self._cases[-1]
        for id, case in prev_layer.items():
            for new_id, new_case in zip(new_ids, new_cases):
                _new_case = new_case + case
                _new_id = '/'.join([id, new_id])
                these_cases[_new_id] = _new_case
        
        self._cases.append(these_cases)

        if name is None: name = 'layer' + str(self.nlayers)
        self._lyrmap[name] = self.nlayers
        self._parsed_options = self._parsed_options | options.keys()

    def find_case(self, id: str, layer = False) -> Optional[Case|tuple[Case]]:
        for lyr_id, lyr_cases in enumerate(self._cases):
            if id in lyr_cases:
                case = lyr_cases[id]
                return case if not layer else (case, lyr_id)
        return None

    def get_subtree(self, start_id: str, depth: int = 999):
        _, start_layer = self.find_case(start_id, layer=True)
        end_layer = min(start_layer + depth+1, self.nlayers)

        subtree = []
        for layer_index in range(start_layer+1, end_layer):
            these_cases = {id: case for id, case in self._cases[layer_index].items() if id.startswith(start_id)}
            subtree.append(these_cases)
        
        return subtree
    
    def iterate_subtree(self, start_id: str, depth: int = 999, layer = True):
        children = self.get_subtree(start_id, depth)
        if len(children) == 0: return
        for child in children[0]:
            case, lyr = self.find_case(child, layer=True)
            if layer:
                yield case, lyr
            else:
                yield case
            yield from self.iterate_subtree(child, depth - 1, layer=layer)

if __name__ == '__main__':
    options = {
        'a': {'a1': 1, 'a2': 2}
    }

    cm = CaseManager(options)

    cm.add_layer({'b': {'b1': 3, 'b2': 4}})

    cm.add_layer({'c': {'c1':5, 'c2': 6}})
    
    first_id = list(cm.id_map.values())[0]
    for case, layer in cm.iterate_subtree(first_id, 3, layer = True):
        print(case, layer)