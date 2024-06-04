import pytest
import datetime
from timestepping.time_utils import get_date_from_str

def test_get_date_from_str():
    date_str = '2022-01-01'
    expected_date = datetime.datetime(2022, 1, 1)
    assert get_date_from_str(date_str) == expected_date

def test_get_date_from_str_with_format():
    date_str = 'January 2022, 01'
    expected_date = datetime.datetime(2022, 1, 1)
    assert get_date_from_str(date_str, '%B %Y, %d') == expected_date

def test_get_date_from_str_invalid_format():
    date_str = 'January 2022, 01'
    with pytest.raises(ValueError):
        get_date_from_str(date_str)