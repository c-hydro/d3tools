import pytest
from unittest import mock
import datetime

from config.parse import substitute_values, substitute_string

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
    string = "The date is {date:%Y-%m-%d}"
    tag_dict = {"date": datetime.datetime(2022, 1, 1)}
    result = substitute_string(string, tag_dict)
    expected = "The date is 2022-01-01"
    assert result == expected