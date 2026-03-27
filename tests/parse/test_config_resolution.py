"""Tests for parse.config_resolution module."""

import pytest

from d3tools.parse.config_resolution import set_dataset, set_env


class _DummyDataset:
    def __init__(self):
        self.tags = {}

    def update(self, **tags):
        self.tags.update(tags)
        return self


class TestSetEnv:
    """Test environment placeholder resolution behavior."""

    def test_set_env_resolves_existing_variable(self, monkeypatch):
        """Resolve ENV placeholders from current environment values."""
        monkeypatch.setenv("FOO", "bar")
        assert set_env("Value: {ENV.FOO}") == "Value: bar"

    def test_set_env_uses_default_when_variable_missing(self, monkeypatch):
        """Resolve ENV placeholders using provided default value when missing."""
        monkeypatch.delenv("BAR", raising=False)
        assert set_env("Value: {ENV.BAR, default = 'baz'}") == "Value: baz"

    def test_set_env_raises_when_missing_and_no_default(self, monkeypatch):
        """Raise ValueError when ENV variable is missing and no default is defined."""
        monkeypatch.delenv("MISSING", raising=False)
        with pytest.raises(ValueError, match="Environment variable MISSING is not set"):
            set_env("Value: {ENV.MISSING}")

    def test_set_env_resolves_nested_structures(self, monkeypatch):
        """Resolve ENV placeholders recursively in dict/list structures."""
        monkeypatch.setenv("FOO", "bar")
        payload = {"key": "{ENV.FOO}", "items": ["{ENV.FOO}"]}
        assert set_env(payload) == {"key": "bar", "items": ["bar"]}


class TestSetDataset:
    """Test dataset placeholder resolution behavior."""

    def test_set_dataset_resolves_basic_reference(self):
        """Resolve basic dataset placeholder to mapped object."""
        obj = _DummyDataset()
        result = set_dataset("{foo}", {"foo": obj})
        assert result is obj

    def test_set_dataset_applies_tag_updates(self):
        """Apply tag assignments from placeholder syntax via object.update."""
        result = set_dataset("{foo, tag = 'bar'}", {"foo": _DummyDataset()})
        assert isinstance(result, _DummyDataset)
        assert result.tags["tag"] == "bar"

    def test_set_dataset_resolves_nested_structures(self):
        """Resolve dataset placeholders recursively in dict/list structures."""
        payload = {"ds": "{foo, tag = 'baz'}", "items": ["{foo, run = 'r1'}"]}
        result = set_dataset(payload, {"foo": _DummyDataset()})
        assert isinstance(result["ds"], _DummyDataset)
        assert result["ds"].tags["tag"] == "baz"
        assert isinstance(result["items"][0], _DummyDataset)
        assert result["items"][0].tags["run"] == "r1"

    def test_set_dataset_keeps_unresolved_strings(self):
        """Keep placeholder string unchanged when key is not in object map."""
        assert set_dataset("{missing}", {}) == "{missing}"
