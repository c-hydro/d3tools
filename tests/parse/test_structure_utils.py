import datetime
import pytest
from d3tools.parse.structure_utils import (
    flatten_dict, make_hashable, transform_back, get_unique_values, format_dict
)

class TestStructureUtils:
    def test_flatten_dict_simple(self):
        d = {'a': {'b': 1, 'c': 2}}
        flat = flatten_dict(d)
        assert flat['a.b'] == 1
        assert flat['a.c'] == 2
        assert flat['b'] == 1
        assert flat['c'] == 2

    def test_flatten_dict_deep(self):
        d = {'a': {'b': {'c': 3}}, 'd': 4}
        flat = flatten_dict(d)
        assert flat['a.b.c'] == 3
        assert flat['b.c'] == 3
        assert flat['c'] == 3
        assert flat['d'] == 4

    def test_flatten_dict_list(self):
        d = {'a': [1, 2], 'b': {'c': 3}}
        flat = flatten_dict(d)
        assert flat['a'] == [1, 2]
        assert flat['b.c'] == 3
        assert flat['c'] == 3

    def test_make_hashable_and_transform_back(self):
        obj = {'a': [1, 2], 'b': {'c': 3}}
        hashable = make_hashable(obj)
        restored = transform_back(hashable)
        assert restored == obj

    def test_make_hashable_with_tuple(self):
        obj = {'a': (1, 2)}
        hashable = make_hashable(obj)
        restored = transform_back(hashable)
        assert restored == {'a': [1, 2]} or restored == {'a': (1, 2)}

    def test_get_unique_values_dicts(self):
        values = [{'a': 1}, {'a': 1}, {'a': 2}]
        unique = get_unique_values(values)
        assert {'a': 1} in unique and {'a': 2} in unique
        assert len(unique) == 2

    def test_get_unique_values_lists(self):
        values = [[1, 2], [1, 2], [2, 3]]
        unique = get_unique_values(values)
        assert [1, 2] in unique and [2, 3] in unique
        assert len(unique) == 2

    def test_get_unique_values_mixed(self):
        values = [{'a': 1}, [1, 2], {'a': 1}, [1, 2], 5, 5]
        unique = get_unique_values(values)
        assert {'a': 1} in unique and [1, 2] in unique and 5 in unique
        assert len(unique) == 3

    def test_format_dict_various_types(self):
        d = {
            'a': 1.234,
            'b': datetime.datetime(2024, 2, 20),
            'c': 'test',
            'd': 42,
            'e': None
        }
        s = format_dict(d)
        assert 'a=1.23' in s
        assert 'b=2024-02-20' in s
        assert 'c=test' in s
        assert 'd=42' in s
        assert 'e=None' in s

    def test_format_dict_empty(self):
        assert format_dict({}) == ''

    def test_format_dict_nested(self):
        d = {'a': {'b': 2}}
        s = format_dict(d)
        assert 'a={"b": 2}' in s or 'a={\'b\': 2}' in s

    def test_flatten_dict_empty(self):
        assert flatten_dict({}) == {}

    def test_make_hashable_edge_cases(self):
        assert make_hashable(None) is None
        assert make_hashable(5) == 5
        assert make_hashable('x') == 'x'

    def test_transform_back_edge_cases(self):
        assert transform_back(('dict',)) == {}
        assert transform_back(('list',)) == []
