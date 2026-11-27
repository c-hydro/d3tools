import pytest
import datetime

from d3tools.parse import *

class DummyDataset:
    def __init__(self):
        self.tags = {}
    def update(self, **tags):
        self.tags.update(tags)
        return self
    
class TestStringSubstitution:
    def test_substitute_string_basic(self):
        assert substitute_string("file_{date}", {"date": "2024-02-20"}) == "file_2024-02-20"

    def test_substitute_string_with_format(self):
        assert substitute_string("file_{date:%Y%m%d}", {"date": "2024-02-20"}) == "file_20240220"

    def test_substitute_string_with_datetime_obj(self):
        dt = datetime.datetime(2024, 2, 20)
        assert substitute_string("file_{date:%Y%m%d}", {"date": dt}) == "file_20240220"

    def test_substitute_string_with_list(self):
        result = substitute_string("tile_{tile}", {"tile": ["A", "B"]})
        assert sorted(result) == ["tile_A", "tile_B"]

    def test_substitute_values_dict(self):
        structure = {"file": "output_{date:%Y%m%d}.tif"}
        tags = {"date": "2024-02-20"}
        assert substitute_values(structure, tags) == {"file": "output_20240220.tif"}

    def test_substitute_values_list(self):
        structure = ["tile_{tile}", "other_{tile}"]
        tags = {"tile": ["A", "B"]}
        assert substitute_values(structure, tags) == [["tile_A", "tile_B"], ["other_A", "other_B"]]

class TestStructureUtils:
    
    def test_make_hashable_and_transform_back_options(self):
        from d3tools.config.options import Options
        obj = Options({'a': [1, 2], 'b': {'c': 3}})
        hashable = make_hashable(obj)
        restored = transform_back(hashable)
        assert isinstance(restored, Options)
        assert restored == obj

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
        assert transform_back((dict,)) == {}
        assert transform_back((list,)) == []


class TestSpecialSubstitutions:
    def test_set_env_basic(self, monkeypatch):
        monkeypatch.setenv('FOO', 'bar')
        assert set_env('Value: {ENV.FOO}') == 'Value: bar'

    def test_set_env_with_default(self, monkeypatch):
        monkeypatch.delenv('BAR', raising=False)
        assert set_env("Value: {ENV.BAR, default = 'baz'}") == 'Value: baz'

    def test_set_env_missing_no_default(self, monkeypatch):
        monkeypatch.delenv('MISSING', raising=False)
        with pytest.raises(ValueError):
            set_env('Value: {ENV.MISSING}')

    def test_set_env_in_dict_and_list(self, monkeypatch):
        monkeypatch.setenv('FOO', 'bar')
        d = {'key': '{ENV.FOO}'}
        l = ['{ENV.FOO}']
        assert set_env(d) == {'key': 'bar'}
        assert set_env(l) == ['bar']

    def test_set_dataset_basic(self):
        obj_dict = {'foo': DummyDataset()}
        result = set_dataset('{foo}', obj_dict)
        assert isinstance(result, DummyDataset)

    def test_set_dataset_with_tag(self):
        obj_dict = {'foo': DummyDataset()}
        result = set_dataset("{foo, tag = 'bar'}", obj_dict)
        assert isinstance(result, DummyDataset)
        assert result.tags['tag'] == 'bar'

    def test_set_dataset_in_dict_and_list(self):
        obj_dict = {'foo': DummyDataset()}
        d = {'ds': "{foo, tag = 'baz'}"}
        l = ["{foo, tag = 'baz'}"]
        result_d = set_dataset(d, obj_dict)
        result_l = set_dataset(l, obj_dict)
        assert isinstance(result_d['ds'], DummyDataset)
        assert result_d['ds'].tags['tag'] == 'baz'
        assert isinstance(result_l[0], DummyDataset)
        assert result_l[0].tags['tag'] == 'baz'

    def test_set_dataset_no_match(self):
        obj_dict = {}
        s = '{bar}'
        assert set_dataset(s, obj_dict) == s
