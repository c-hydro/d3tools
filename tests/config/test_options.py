"""
Tests for Options class behavior.

The Options class is an enhanced dict with:
- Recursive wrapping of nested structures
- Case-insensitive key lookup
- Attribute-style access for nested keys
"""
import json
import warnings
import pytest

from d3tools import Options


@pytest.fixture
def simple_options():
    """Simple Options instance for testing."""
    return Options({"key": "value", "nested": {"inner": "data"}})


@pytest.fixture
def nested_options():
    """Options with nested structures for testing recursive wrapping."""
    return Options({
        "foo": {"bar": 42},
        "baz": [{"qux": 99}, {"qux": 100}],
        "deep": {"level1": {"level2": {"level3": "deep_value"}}}
    })


@pytest.fixture
def case_sensitive_options():
    """Options for testing case-insensitive lookup."""
    return Options({
        "LowerKey": "lower_value",
        "UpperKey": "upper_value",
        "MixedCase": {"NestedKey": "nested_value"}
    })


class TestOptionsBasicBehavior:
    """Test basic Options dict-like behavior."""

    def test_options_is_dict_subclass(self):
        """Options should be a subclass of dict."""
        assert issubclass(Options, dict)

    def test_options_initialization_from_dict(self, simple_options):
        """Options should initialize from a dict."""
        assert simple_options["key"] == "value"
        assert simple_options["nested"]["inner"] == "data"

    def test_options_dict_methods_work(self, simple_options):
        """Options should support standard dict methods."""
        assert "key" in simple_options
        assert "nonexistent" not in simple_options
        assert simple_options.get("key") == "value"
        assert simple_options.get("nonexistent", "default") == "default"
        assert len(simple_options) == 2

    def test_options_setitem_works(self):
        """Options should support item assignment."""
        opts = Options({})
        opts["new_key"] = "new_value"
        assert opts["new_key"] == "new_value"


class TestOptionsRecursiveWrapping:
    """Test Options recursive wrapping of nested structures."""

    def test_nested_dict_wrapped_as_options(self, nested_options):
        """Nested dicts should be wrapped as Options instances."""
        assert isinstance(nested_options["foo"], Options)
        assert nested_options["foo"]["bar"] == 42

    def test_list_elements_wrapped_as_options(self, nested_options):
        """Dicts in lists should be wrapped as Options instances."""
        assert isinstance(nested_options["baz"], list)
        assert isinstance(nested_options["baz"][0], Options)
        assert nested_options["baz"][0]["qux"] == 99
        assert nested_options["baz"][1]["qux"] == 100

    def test_deep_nesting_wrapped(self, nested_options):
        """Deeply nested structures should be fully wrapped."""
        assert isinstance(nested_options["deep"], Options)
        assert isinstance(nested_options["deep"]["level1"], Options)
        assert isinstance(nested_options["deep"]["level1"]["level2"], Options)
        assert nested_options["deep"]["level1"]["level2"]["level3"] == "deep_value"


class TestOptionsAttributeAccess:
    """Test Options attribute-style access."""

    def test_attribute_access_for_dict_keys(self, nested_options):
        """Options should support attribute access for dict keys."""
        assert nested_options.foo.bar == 42

    def test_attribute_access_for_nested_keys(self, nested_options):
        """Attribute access should work for nested keys."""
        assert nested_options.deep.level1.level2.level3 == "deep_value"

    def test_attribute_access_raises_for_multiple_values(self):
        """Attribute access should raise ValueError for ambiguous keys."""
        opts = Options({"foo": {"bar": 1}, "baz": {"bar": 2}})
        with pytest.raises(ValueError, match="Multiple values found"):
            _ = opts.bar

    def test_attribute_access_raises_for_missing(self, simple_options):
        """Attribute access should raise AttributeError for missing keys."""
        with pytest.raises(AttributeError, match="'Options' object has no attribute"):
            _ = simple_options.nonexistent


class TestOptionsCaseInsensitive:
    """Test Options case-insensitive key lookup."""

    def test_get_case_insensitive(self, case_sensitive_options):
        """get() should support case-insensitive lookup."""
        # Exact case
        assert case_sensitive_options.get("LowerKey") == "lower_value"
        
        # Different case with ignore_case flag
        assert case_sensitive_options.get("lowerkey", ignore_case=True) == "lower_value"
        assert case_sensitive_options.get("LOWERKEY", ignore_case=True) == "lower_value"

    def test_case_sensitive_by_default(self, case_sensitive_options):
        """get() should be case-sensitive by default."""
        assert case_sensitive_options.get("lowerkey") is None
        assert case_sensitive_options.get("LOWERKEY") is None


class TestOptionsDeprecatedLoad:
    """Test deprecated Options.load() method."""

    def test_options_load_emits_deprecation_warning(self, tmp_path):
        """Options.load should emit a deprecation warning."""
        cfg = {"TAGS": {"source": "ERA5"}, "DATASETS": {}}
        cfg_path = tmp_path / "workflow.json"
        cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = Options.load(str(cfg_path))
            
            # Should have a deprecation warning
            assert any(issubclass(warn.category, DeprecationWarning) for warn in w)
            
            # Should return a WorkflowDefinition (as per deprecation behavior)
            from d3tools import WorkflowDefinition
            assert isinstance(result, WorkflowDefinition)


class TestOptionsEdgeCases:
    """Test Options edge cases and corner scenarios."""

    def test_empty_options(self):
        """Empty Options should work correctly."""
        opts = Options({})
        assert len(opts) == 0
        assert list(opts.keys()) == []

    def test_options_with_none_values(self):
        """Options should handle None values correctly."""
        opts = Options({"key": None})
        assert opts["key"] is None
        assert opts.get("key") is None

    def test_options_with_non_dict_values(self):
        """Options should preserve non-dict values."""
        opts = Options({
            "string": "text",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "list_of_primitives": [1, 2, 3]
        })
        
        assert opts["string"] == "text"
        assert opts["number"] == 42
        assert opts["float"] == 3.14
        assert opts["bool"] is True
        assert opts["list_of_primitives"] == [1, 2, 3]

    def test_options_iteration(self, simple_options):
        """Options should support iteration like dict."""
        keys = list(simple_options.keys())
        assert "key" in keys
        assert "nested" in keys
        
        values = list(simple_options.values())
        assert "value" in values

    def test_options_update(self):
        """Options should support update() method."""
        opts = Options({"a": 1})
        opts.update({"b": 2})
        
        assert opts["a"] == 1
        assert opts["b"] == 2
