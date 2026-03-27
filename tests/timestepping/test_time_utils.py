import datetime
import pytest
from d3tools.timestepping.time_utils import (
    get_window, get_md_dates, unit_is_multiple, UNIT_CONVERSIONS
)
from d3tools.timestepping import TimeRange

class DummyDekad:
    def __init__(self, start, end):
        self.start = start
        self.end = end
    @classmethod
    def from_date(cls, date):
        return cls(date, date)
    def __add__(self, n):
        return DummyDekad(self.start, self.end)
    def __sub__(self, n):
        return DummyDekad(self.start, self.end)

class DummyTimeRange:
    def __init__(self, start, end):
        self.start = start
        self.end = end

class DummyYear:
    def __init__(self, y): self.y = y
    def is_leap(self): return self.y % 4 == 0

class TestUnitIsMultiple:
    def test_unit_is_multiple_true(self):
        assert unit_is_multiple('m', 'd')
        assert unit_is_multiple('y', 'm')
        assert unit_is_multiple('y', 'd')

    def test_unit_is_multiple_same(self):
        """A unit should be considered a multiple of itself."""
        assert unit_is_multiple('d', 'd')
        assert unit_is_multiple('m', 'm')
    
    def test_unit_is_multiple_false(self):
        assert not unit_is_multiple('v', 'w')
        assert not unit_is_multiple('m', 'v')
        # 'd','y' will return False, but 'y','d' should return True
        assert not unit_is_multiple('d', 'y')

    def test_unit_is_multiple_invalid(self):
        # if the unit is not recognised it should raise an error
        with pytest.raises(ValueError):
            unit_is_multiple('invalid', 'd')

class TestGetWindow:
    def test_get_window_end(self):
        dt = datetime.datetime(2024, 2, 20) 
        win = get_window(dt, 2, 'm', start = False)
        assert isinstance(win, TimeRange)
        assert win.end == dt
        win = get_window(dt, 2, 't', start = False)
        assert isinstance(win, TimeRange)
        assert win.end == dt
        win = get_window(dt, 2, 'y', start = False)
        assert isinstance(win, TimeRange)
        assert win.end == dt

    def test_get_window_start(self):
        dt = datetime.datetime(2024, 2, 21)
        win = get_window(dt, 2, 'd', start = True)
        assert isinstance(win, TimeRange)
        assert win.start == dt
        win = get_window(dt, 2, 'm', start = True)
        assert isinstance(win, TimeRange)
        assert win.start == dt
        win = get_window(dt, 2, 'w', start = True)
        assert isinstance(win, TimeRange)
        assert win.start == dt

    def test_get_window_invalid_unit(self):
        dt = datetime.datetime(2024, 2, 20)
        with pytest.raises(ValueError):
            get_window(dt, 1, 'invalid')
    
    def test_get_time_window_float_size(self):
        dt = datetime.datetime(2024, 2, 20)
        with pytest.raises(ValueError):
            win = get_window(dt, 1.5, 'd')

class TestGetMDDates:
    def test_get_md_dates_leap(self):
        years = [2020, 2021]
        dates = get_md_dates(years, 2, 29)
        assert len(dates) == 2
        assert dates[0] == datetime.datetime(2020, 2, 29)
        assert dates[1] == datetime.datetime(2021, 2, 28)

    def test_get_md_dates_with_range(self):
        years = range(2020, 2023)
        dates = get_md_dates(years, 3, 15)
        assert len(dates) == 3
        assert dates[0] == datetime.datetime(2020, 3, 15)
        assert dates[-1] == datetime.datetime(2022, 3, 15)
