"""Tests for parse.string_rendering module."""

import datetime as dt

from d3tools.parse.string_rendering import substitute_string, substitute_values, normalise_string


class TestSubstituteString:
    """Test single-string placeholder rendering behavior."""

    def test_substitute_string_basic_tag(self):
        """Render plain tag placeholders from mapping values."""
        assert substitute_string("file_{date}", {"date": "2024-02-20"}) == "file_2024-02-20"

    def test_substitute_string_datetime_format_from_str(self):
        """Render datetime format placeholders when value is a date-like string."""
        assert substitute_string("file_{date:%Y%m%d}", {"date": "2024-02-20"}) == "file_20240220"

    def test_substitute_string_datetime_format_from_datetime(self):
        """Render datetime format placeholders when value is a datetime object."""
        assert substitute_string("file_{date:%Y%m%d}", {"date": dt.datetime(2024, 2, 20)}) == "file_20240220"

    def test_substitute_string_generates_all_values_for_list_tags(self):
        """Expand list-valued tags recursively into all rendered combinations."""
        rendered = substitute_string("tile_{tile}", {"tile": ["A", "B"]})
        assert sorted(rendered) == ["tile_A", "tile_B"]

    def test_substitute_string_preserves_unknown_placeholders(self):
        """Leave unresolved placeholders unchanged when key is missing."""
        assert substitute_string("file_{unknown}.tif", {"date": "2024-02-20"}) == "file_{unknown}.tif"

    def test_substitute_string_returns_non_string_inputs_unchanged(self):
        """Return non-string input values as-is for compatibility."""
        payload = {"a": 1}
        assert substitute_string(payload, {"a": 1}) is payload


class TestSubstituteValues:
    """Test recursive placeholder rendering across structures."""

    def test_substitute_values_renders_dict_values(self):
        """Render placeholders recursively in dictionary values."""
        structure = {"file": "output_{date:%Y%m%d}.tif"}
        tags = {"date": "2024-02-20"}
        assert substitute_values(structure, tags) == {"file": "output_20240220.tif"}

    def test_substitute_values_renders_list_values(self):
        """Render placeholders recursively in list values with list expansion."""
        structure = ["tile_{tile}", "other_{tile}"]
        tags = {"tile": ["A", "B"]}
        assert substitute_values(structure, tags) == [["tile_A", "tile_B"], ["other_A", "other_B"]]

    def test_substitute_values_renders_dict_keys(self):
        """Render placeholders in dictionary keys as well as values."""
        structure = {"{name}": "value_{name}"}
        tags = {"name": "demo"}
        assert substitute_values(structure, tags) == {"demo": "value_demo"}

    def test_substitute_values_preserves_scalars(self):
        """Keep non-container non-string values unchanged."""
        assert substitute_values(42, {"x": "y"}) == 42

class TestNormaliseString:
    """Test string normalization behavior."""

    def test_normalise_string_strips_whitespace(self):
        """Normalisation should remove leading/trailing whitespace."""
        assert normalise_string("  example  ") == "example"

    def test_normalise_string_replaces_internal_whitespace_with_underscore(self):
        """Normalisation should convert internal whitespace to underscores."""
        assert normalise_string("example string") == "example_string"
        assert normalise_string("example-string") == "example_string"

    def test_normalise_removes_multiple_underscores(self):
        """Normalisation should not return multiple consecutive underscores."""
        assert normalise_string("example   string") == "example_string"
        assert normalise_string("example- -string") == "example_string"

    def test_normalise_string_converts_to_lowercase(self):
        """Normalisation should convert all characters to lowercase."""
        assert normalise_string("ExampleString") == "examplestring"
        assert normalise_string("EXAMPLESTRING") == "examplestring"