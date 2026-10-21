"""Manage combinations of cases arranged in named layers."""

from typing import Optional, Iterator
import copy

from .case import Case, get_cases
from .utils import rand_str, split_id, get_parents

class CaseManager():
    """Build and traverse layers of cases generated from option mappings.

    A mapping-valued option is expanded into one case per mapping key, with the
    key recorded as a tag and its value used as the option value. Other option
    values are fixed across that expansion. Each layer is stored as a mapping
    from generated case IDs to :class:`Case` objects.

    Case IDs are assembled from short random tokens assigned to tag/value
    pairs. The manager keeps those assignments in ``id_map`` so a pair reuses
    its token within this manager.

    Attributes:
        id_map: Mapping from ``"tag=value"`` strings to generated ID tokens.
        options: Effective option mappings, one per layer.
        tags: Tag names present in each layer.
    """

    _id_len = 2

    def __init__(self, options: dict|None = None, l0_name: str = 'input'):
        """Create the root layer from ``options``.

        Args:
            options: Options for the root layer. Mapping values are expanded
                as case dimensions; other values are fixed options. ``None``
                is treated as an empty mapping.
            l0_name: Name assigned to the root layer.

        Returns:
            None: The constructor has no return value.

        Side Effects:
            Initializes the root layer and generates its case IDs.
        """

        if options is None: options = {}
        self.id_map = {}
        
        cases = get_cases(options)
        ids   = self.get_ids(cases)

        self._cases   = [dict(zip(ids, cases))]
        self.options  = [options.copy()]
        self.tags     = [list(cases[0].tags.keys())]
        self._lyrmap  = {l0_name: 0}

        self._parsed_options = {k:[v for v in options[k].keys()] for k in options.keys() if isinstance(options[k], dict)}
    
    @property
    def layers(self):
        """Return layer names in insertion order.

        Returns:
            list[str]: Layer names in insertion order.
        """
        return list(self._lyrmap.keys())
    
    @property
    def nlayers(self):
        """Return the number of layers currently stored.

        Returns:
            int: Number of stored layers.
        """
        return len(self.layers)
    
    def __iter__(self) -> Iterator[Case]:
        """Iterate over the stored layer mappings.

        Returns:
            Iterator[dict[str, Case]]: An iterator over dictionaries mapping case IDs to ``Case``
            instances, one dictionary per layer. This is the runtime behavior
            even though the return annotation names ``Case`` as the item type.
        """
        return iter(self._cases)

    def __getitem__(self, index: int|str) -> Case:
        """Get a layer by its integer position or its name.

        Args:
            index: Integer layer position (including normal negative list
                indices), or a layer name.

        Returns:
            dict[str, Case]: The selected mapping of case IDs to ``Case`` instances.

        Raises:
            KeyError: If ``index`` is a string that is not a layer name.
            IndexError: If an integer index is outside the stored layer list.
        """
        if isinstance(index, str):
            index = self._lyrmap[index]
        return self._cases[index]

    def get_ids(self, cases: list[Case]) -> list[str]:
        """Generate one ID per case using this manager's tag-token map.

        Args:
            cases: Cases whose tags determine their ID components.

        Returns:
            list[str]: IDs formed by concatenating the tokens for each case's tags in
            tag iteration order. A tag/value pair reuses its token within this
            manager; previously unused pairs receive a random two-character
            token that is not already assigned to another pair.

        Side Effects:
            Adds tokens for previously unseen tag/value pairs to ``id_map``.
        """
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
        """Append a layer by combining new cases with the current final layer.

        Mapping-valued options expand into new cases and fixed values are
        applied to each child. A mapping dimension already parsed in an
        earlier layer is ignored if any of its keys have already been parsed;
        dimensions with no previously parsed keys are accepted. When ``merge``
        names a dimension, cases in the previous layer are first combined
        across that dimension, which is removed from the merged cases.

        Args:
            options: Options to add in this layer. Mapping values associate
                tag values with runtime option values.
            name: Name for the new layer. If omitted, a name of the form
                ``layerN`` is generated from the resulting layer count.
            merge: Optional tag name whose previous-layer values are combined
                before adding the new layer.

        Returns:
            None: The method has no return value.

        Side Effects:
            Appends a case mapping, effective options, and tag list to this
            manager, registers the layer name, and may update ``id_map``.
        """

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
                case0 = copy.deepcopy(prev_layer[prev_keys[0]])
                case0.options.pop(merge)
                case0.tags.pop(merge)
                casenew = Case(options = case0.options, tags = case0.tags)
                newkey  = '&'.join([f'[{k}]' for k in prev_keys])
                prev_layer[newkey] = casenew
                for key in prev_keys: prev_layer.pop(key)

        for id, case in prev_layer.items():
            _new_case = case # this line is only here beacause otherwise line 99 doesn't work when an empty case set is added
            for new_id, new_case in zip(new_ids, new_cases):
                _new_case = new_case + case
                _new_id = '/'.join([id, new_id])
                these_cases[_new_id] = _new_case
        
        self._cases.append(these_cases)
        self.options.append(new_options.copy() | self.options[-1])
        self.tags.append(list(_new_case.tags.keys()))

        if name is None: name = 'layer' + str(self.nlayers)
        self._lyrmap[name] = self.nlayers
        newly_parsed = {k:[v for v in new_options[k].keys()] for k in new_options.keys() if isinstance(new_options[k], dict)}
        for k in newly_parsed:
            if k in self._parsed_options:
                self._parsed_options[k].extend(newly_parsed[k])
            else:
                self._parsed_options[k] = newly_parsed[k]

    def find_case(self, id: str, get_layer = False) -> Optional[Case|tuple[Case]]:
        """Find the first case with ``id`` by scanning layers in order.

        Args:
            id: Case ID to find.
            get_layer (bool): If true, return the layer index together with the case.

        Returns:
            Case | tuple[Case, int] | None: The matching ``Case``, or ``(Case, layer_index)`` when
            ``get_layer`` is true. Returns ``None`` when no layer contains
            the ID.
        """
        for lyr_id, lyr_cases in enumerate(self._cases):
            if id in lyr_cases:
                case = lyr_cases[id]
                return case if not get_layer else (case, lyr_id)
        return None
    
    def iterate_tree(self, depth = None, get_layer = True):
        """Yield cases from the root through the selected layer.

        For each case in the selected layer, its parent cases are yielded
        before it. Parent IDs are deduplicated within that case's traversal,
        allowing converging branches to be represented without repeating a
        shared ancestor for that terminal case.

        Args:
            depth (int | None): Layer index to traverse through, inclusive. Defaults to the
                final layer. Normal list indexing applies to explicit values.
            get_layer (bool): If true, yield ``(case, layer_index)`` pairs; otherwise
                yield cases only.

        Yields:
            tuple[Case, int] | Case: Cases on the traversed paths, in
            parent-before-child order.
        """
        if depth is None: depth = self.nlayers-1
        last_row = self._cases[depth]

        seen_ids = []
        seen_cases = []

        for id, case in last_row.items():
            parents = get_parents(id)
            for parent in parents:
                if parent not in seen_ids:
                    seen_ids.append(parent)
                    seen_cases.append(self.find_case(parent, get_layer = get_layer))

            seen_ids.append(id)
            seen_cases.append((case, depth))

            yield from seen_cases
            seen_cases = []

    def get_subtree(self, start_id: str, depth: int = 999):
        """Return descendant layer mappings below a case.

        Descendants are selected from later layers when their IDs start with
        ``start_id``. The starting case's own layer is excluded, and at most
        ``depth`` subsequent layers are returned.

        Args:
            start_id: ID of the case at which to start.
            depth (int): Maximum number of layers below the starting case to include.
                Defaults to 999.

        Returns:
            list[dict[str, Case]]: A list of dictionaries mapping descendant case IDs to cases, with
            one dictionary per included layer. Returns an empty list when no
            later layer is within the requested depth.

        Raises:
            ValueError: If ``start_id`` does not identify a stored case.
        """
        start = self.find_case(start_id, get_layer=True)
        if start is None:
            raise ValueError(f"Unknown case ID: {start_id}")
        _, start_layer = start
        end_layer = min(start_layer + depth+1, self.nlayers)

        subtree = []
        for layer_index in range(start_layer+1, end_layer):
            these_cases = {id: case for id, case in self._cases[layer_index].items() if id.startswith(start_id)}
            subtree.append(these_cases)

        return subtree
    
    def iterate_subtree(self, start_id: str, depth: int = 999, get_layer = True):
        """Yield descendants of ``start_id`` up to ``depth`` layers below.

        The traversal follows diverging branches and yields each child before
        recursively visiting its descendants. It does not yield the starting
        case.

        Args:
            start_id: ID of the case at which to start.
            depth (int): Maximum number of descendant layers to visit. Defaults to
                999.
            get_layer (bool): If true, yield ``(case, layer_index)`` pairs; otherwise
                yield cases only.

        Yields:
            tuple[Case, int] | Case: Descendant cases in depth-first order, optionally paired with
            their layer indices.

        Raises:
            ValueError: If ``start_id`` does not identify a stored case.
        """
        children = self.get_subtree(start_id, depth)
        if len(children) == 0: return
        for child in children[0]:
            case, lyr = self.find_case(child, get_layer=True)
            if get_layer:
                yield case, lyr
            else:
                yield case
            yield from self.iterate_subtree(child, depth - 1, get_layer=get_layer)
