import pytest
from unittest import mock
import datetime

from dam.tools.config.parse_utils import substitute_values, substitute_string, flatten_dict, parse_options

def test_substitute_values():
    structure = {
        "key1": "{tag1}",
        "key2": ["{tag2}"],
        "key3": {
            "key4": "{tag3}"
        }
    }
    tag_dict = {
        "tag1": "value1",
        "tag2": "value2",
        "tag3": "value3"
    }
    result = substitute_values(structure, tag_dict)
    expected = {
        "key1": "value1",
        "key2": ["value2"],
        "key3": {
            "key4": "value3"
        }
    }
    assert result == expected

def test_substitute_string():
    string = "The date is {date:%Y-%m-%d}, {datestr:%Y%m%d}"
    tag_dict = {"date": datetime.datetime(2022, 1, 1),
                "datestr": "2022-01-01"}
    result = substitute_string(string, tag_dict)
    expected = "The date is 2022-01-01, 20220101"
    assert result == expected

def test_flatten_dict():
    nested_dict = {'a': {'b': 1, 'c': 2}, 'd': 3}
    result = flatten_dict(nested_dict)
    expected = {'a.b': 1, 'b': 1, 'a.c': 2, 'c': 2, 'd': 3}
    assert result == expected, f"{expected}, {result}"

def test_parse_options():
    options = {"KEYS":{"key1": "test",
                       "key2": ["{tag2}"],
                       "key3": {"key4": "{key1}"},
                       "key5": "{key3}"},
               "TAGS": {"tag2": "value2"}}


    expected = {"KEYS": {"key1": "test",
                         "key2": ["value2"],
                         "key3": {"key4": "test"},
                         "key5": "{key3}"},
                "TAGS": {"tag2": "value2"}}

    result = parse_options(options)
    assert result == expected, f"{result['key2']}, {expected['key2']}"