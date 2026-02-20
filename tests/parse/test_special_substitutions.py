import os
import pytest
from d3tools.parse.special_substitutions import set_env, set_dataset

class DummyDataset:
    def __init__(self):
        self.tags = {}
    def update(self, **tags):
        self.tags.update(tags)
        return self

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
