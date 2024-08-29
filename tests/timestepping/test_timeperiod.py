import pytest
import datetime

from timestepping.timeperiod import TimePeriod

class ConcreteTimePeriod(TimePeriod):
    pass

class TestTimePeriod:
    def setup_method(self):
        self.ts = ConcreteTimePeriod('2022-01-01', '2022-01-02')

    def test_timeperiod_initialization(self):
        assert self.ts.start == datetime.datetime(2022, 1, 1)
        assert self.ts.end == datetime.datetime(2022, 1, 2)

    def test_timeperiod_get_length(self):
        assert self.ts.get_length('days') == 2
        assert self.ts.get_length('hours') == 48
        with pytest.raises(ValueError):
            self.ts.get_length('invalid')

    def test_timeperiod_repr(self):
        assert repr(self.ts) == 'ConcreteTimePeriod (20220101 - 20220102)'
    
    def test_timeperiod_eq(self):
        ts2 = ConcreteTimePeriod('2022-01-01', '2022-01-02')
        assert self.ts == ts2
        ts2 = ConcreteTimePeriod('2022-01-02', '2022-01-03')
        assert self.ts != ts2

    def test_timeperiod_lt(self):
        ts2 = ConcreteTimePeriod('2022-01-03', '2022-01-04')
        assert self.ts < ts2
        ts2 = ConcreteTimePeriod('2022-01-01', '2022-01-02')
        assert not self.ts < ts2

    def test_timeperiod_gt(self):
        ts2 = ConcreteTimePeriod('2022-01-03', '2022-01-04')
        assert not self.ts > ts2
        ts2 = ConcreteTimePeriod('2021-12-30', '2021-12-31')
        assert self.ts > ts2
