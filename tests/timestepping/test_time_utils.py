import pytest
import datetime
from timestepping.time_utils import get_date_from_str, get_window

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

def test_get_window_days_start():
    time = datetime.datetime(2022, 1, 1)
    window = get_window(time, 7, 'days', start=True)
    assert window.start == datetime.datetime(2022, 1, 1)
    assert window.end == datetime.datetime(2022, 1, 7)

def test_get_window_days_end():
    time = datetime.datetime(2022, 1, 7)
    window = get_window(time, 7, 'days', start=False)
    assert window.start == datetime.datetime(2022, 1, 1)
    assert window.end == datetime.datetime(2022, 1, 7)

def test_get_window_months_start():
    time = datetime.datetime(2022, 1, 1)
    window = get_window(time, 1, 'months', start=True)
    assert window.start == datetime.datetime(2022, 1, 1)
    assert window.end == datetime.datetime(2022, 1, 31)

def test_get_window_months_end():
    time = datetime.datetime(2022, 2, 1)
    window = get_window(time, 1, 'months', start=False)
    assert window.start == datetime.datetime(2022, 1, 2)
    assert window.end == datetime.datetime(2022, 2, 1)

def test_get_window_dekads_start():
    time = datetime.datetime(2022, 1, 1)
    window = get_window(time, 2, 'dekads', start=True)
    assert window.start == datetime.datetime(2022, 1, 1)
    assert window.end == datetime.datetime(2022, 1, 20)

def test_get_window_dekads_end():
    time = datetime.datetime(2022, 2, 28)
    window = get_window(time, 3, 'dekads', start=False)
    assert window.start == datetime.datetime(2022, 2, 1)
    assert window.end == datetime.datetime(2022, 2, 28)

def test_get_window_invalid_unit():
    time = datetime.datetime(2022, 1, 1)
    with pytest.raises(ValueError):
        get_window(time, 1, 'invalid', start=True)