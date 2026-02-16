"""
Tests for withcases decorator.

Tests the @withcases decorator that enables case-based expansion of function calls.
"""
import pytest
from d3tools.cases.utils import withcases


class TestWithcasesDecorator:
    """Test suite for the @withcases decorator."""
    
    def test_function_without_cases_kwarg(self):
        """Test that decorated function works normally without cases kwarg."""
        @withcases
        def process(value, multiplier=2):
            return value * multiplier
        
        result = process(5, multiplier=3)
        
        assert result == 15
    
    def test_function_with_cases_none(self):
        """Test that cases=None is treated as no cases (normal execution)."""
        @withcases
        def process(value, multiplier=2):
            return value * multiplier
        
        result = process(5, multiplier=3, cases=None)
        
        assert result == 15
    
    def test_function_with_single_case(self):
        """Test function with one case returns list with one result."""
        @withcases
        def process(value, tag1):
            return f"{value}_{tag1}"
        
        cases = [
            {'tags': {'tag1': 'A'}}
        ]
        
        result = process(value='test', cases=cases)
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == 'test_A'
    
    def test_function_with_multiple_cases(self):
        """Test function with multiple cases returns list of results."""
        @withcases
        def process(value, variable, agg):
            return f"{value}_{variable}_{agg}"
        
        cases = [
            {'tags': {'variable': 'Tmin', 'agg': '3m'}},
            {'tags': {'variable': 'Tmin', 'agg': '6m'}},
            {'tags': {'variable': 'Tmax', 'agg': '3m'}},
            {'tags': {'variable': 'Tmax', 'agg': '6m'}},
        ]
        
        result = process(value='data', cases=cases)
        
        assert isinstance(result, list)
        assert len(result) == 4
        assert result[0] == 'data_Tmin_3m'
        assert result[1] == 'data_Tmin_6m'
        assert result[2] == 'data_Tmax_3m'
        assert result[3] == 'data_Tmax_6m'
    
    def test_function_with_empty_cases_list(self):
        """Test that empty cases list returns empty list."""
        @withcases
        def process(value, tag1):
            return f"{value}_{tag1}"
        
        result = process(value='test', cases=[])
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_cases_tags_merged_with_kwargs(self):
        """Test that case tags are merged with other kwargs."""
        @withcases
        def process(value, tag1, tag2, extra='default'):
            return f"{value}_{tag1}_{tag2}_{extra}"
        
        cases = [
            {'tags': {'tag1': 'A', 'tag2': 'X'}},
            {'tags': {'tag1': 'B', 'tag2': 'Y'}},
        ]
        
        result = process(value='test', extra='custom', cases=cases)
        
        assert len(result) == 2
        assert result[0] == 'test_A_X_custom'
        assert result[1] == 'test_B_Y_custom'
    
    def test_cases_do_not_appear_in_decorated_function(self):
        """Test that 'cases' kwarg is consumed by decorator and not passed to function."""
        received_kwargs = []
        
        @withcases
        def process(**kwargs):
            received_kwargs.append(kwargs.copy())
            return kwargs
        
        cases = [
            {'tags': {'tag1': 'A'}},
            {'tags': {'tag1': 'B'}},
        ]
        
        process(value='test', cases=cases)
        
        # Cases should be consumed by decorator
        assert all('cases' not in kw for kw in received_kwargs)
        # But tags should be present
        assert all('tag1' in kw for kw in received_kwargs)
    
    def test_case_tags_override_kwargs(self):
        """Test that case tags override kwargs with same name."""
        @withcases
        def process(tag1):
            return tag1
        
        cases = [
            {'tags': {'tag1': 'from_case'}},
        ]
        
        # tag1 in both cases and kwargs - case should win
        result = process(tag1='from_kwargs', cases=cases)
        
        assert result[0] == 'from_case'
    
    def test_function_with_positional_args(self):
        """Test that positional args work correctly with cases."""
        @withcases
        def process(pos1, pos2, tag1):
            return f"{pos1}_{pos2}_{tag1}"
        
        cases = [
            {'tags': {'tag1': 'A'}},
            {'tags': {'tag1': 'B'}},
        ]
        
        result = process('arg1', 'arg2', cases=cases)
        
        assert len(result) == 2
        assert result[0] == 'arg1_arg2_A'
        assert result[1] == 'arg1_arg2_B'
    
    def test_function_that_returns_list(self):
        """Test decorated function that itself returns a list."""
        @withcases
        def process(tag1):
            return [tag1, tag1.upper()]
        
        cases = [
            {'tags': {'tag1': 'a'}},
            {'tags': {'tag1': 'b'}},
        ]
        
        result = process(cases=cases)
        
        # Should return list of lists
        assert len(result) == 2
        assert result[0] == ['a', 'A']
        assert result[1] == ['b', 'B']
    
    def test_function_that_raises_exception(self):
        """Test that exceptions in decorated function propagate correctly."""
        @withcases
        def process(tag1):
            if tag1 == 'error':
                raise ValueError("Test error")
            return tag1
        
        cases = [
            {'tags': {'tag1': 'ok'}},
            {'tags': {'tag1': 'error'}},
        ]
        
        with pytest.raises(ValueError, match="Test error"):
            process(cases=cases)
    
    def test_case_with_additional_metadata(self):
        """Test that cases can have additional metadata (only tags are used)."""
        @withcases
        def process(tag1):
            return tag1
        
        cases = [
            {
                'tags': {'tag1': 'A'},
                'metadata': {'description': 'First case'},
                'priority': 1
            },
            {
                'tags': {'tag1': 'B'},
                'metadata': {'description': 'Second case'},
                'priority': 2
            },
        ]
        
        result = process(cases=cases)
        
        # Should only use tags, ignore other keys
        assert len(result) == 2
        assert result[0] == 'A'
        assert result[1] == 'B'
