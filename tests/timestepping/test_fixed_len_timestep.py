import pytest
from unittest import mock
import datetime

from timestepping.fixed_len_timestep import FixedLenTimeStep
from timestepping import Hour, Day

class ConcreteFixedLenTimeStep(FixedLenTimeStep):
    
    def get_step_from_date(date: datetime.datetime):
        pass

    def get_start(self):
        pass

    def get_end(self):
        pass

class TestFixedLenTimeStep:

    def setup_method(self):
        with mock.patch(f'{__name__}.ConcreteFixedLenTimeStep.get_start') as mock_get_start, \
             mock.patch(f'{__name__}.ConcreteFixedLenTimeStep.get_end') as mock_get_end:
                mock_get_start.return_value = datetime.datetime(2022, 1, 1, 0, 0, 0)
                mock_get_end.return_value = datetime.datetime(2022, 1, 1, 23, 59, 59)
                self.ts = ConcreteFixedLenTimeStep(2022, 1, 1)

    def test_fixed_len_timestep_initialization(self):
        assert self.ts.start == datetime.datetime(2022, 1, 1, 0, 0, 0)
        assert self.ts.end == datetime.datetime(2022, 1, 1, 23, 59, 59)
        assert self.ts.step == 1
        assert self.ts.length == 1
        assert self.ts.year == 2022

    def test_fixed_len_timestep_get_subclass(self):
        assert FixedLenTimeStep.get_subclass(1) == Day
        assert FixedLenTimeStep.get_subclass(1/24) == Hour
        with pytest.raises(ValueError):
            FixedLenTimeStep.get_subclass(0)

    def test_fixed_len_timestep_from_steps(self):
        ts = FixedLenTimeStep.from_step(2022, 1, 1)
        assert isinstance(ts, Day)
        ts = FixedLenTimeStep.from_step(2022, 1, 1/24)
        assert isinstance(ts, Hour)

    def test_fixed_len_timestep_from_date(self):
        ts = FixedLenTimeStep.from_date("2022-01-01", 1)
        assert isinstance(ts, Day)
        ts = FixedLenTimeStep.from_date("2022-01-01 04:00", 1/24)
        assert isinstance(ts, Hour)

    def test_fixed_len_timestep_add(self):
        with mock.patch(f'timestepping.fixed_len_timestep.Day.__init__') as mock_init:
            mock_init.return_value = None
            self.ts + 13
            mock_init.assert_called_once_with(2022, 14)

class TestDay:

    def setup_method(self):
        self.ts1   = Day(2022, 1)
        self.ts365 = Day(2022, 365)
        self.ts366 = Day(2020, 366)

    def test_day_initialization(self):
        assert self.ts1.start == datetime.datetime(2022, 1, 1)
        assert self.ts1.end == datetime.datetime(2022, 1, 1, 23, 59, 59)
        assert self.ts1.day_of_year == 1

    def test_day_get_step_from_date(self):
        assert Day.get_step_from_date(datetime.datetime(2022, 1, 1, 3)) == 1
        assert Day.get_step_from_date(datetime.datetime(2022, 1, 10)) == 10

    def test_day_get_start(self):
        assert self.ts1.get_start() == datetime.datetime(2022, 1, 1)
        assert self.ts365.get_start() == datetime.datetime(2022, 12, 31)
        assert self.ts366.get_start() == datetime.datetime(2020, 12, 31)

    def test_day_get_end(self):
        assert self.ts1.get_end()   == datetime.datetime(2022, 1, 1, 23, 59, 59)
        assert self.ts365.get_end() == datetime.datetime(2022, 12, 31, 23, 59, 59)
        assert self.ts366.get_end() == datetime.datetime(2020, 12, 31, 23, 59, 59)
    
    def test_day_day_of_year(self):
        assert self.ts1.day_of_year   == 1
        assert self.ts365.day_of_year == 365
        assert self.ts366.day_of_year == 366
    
class TestMonth:

    def setup_method(self):
        self.ts = Hour(2022, 12)

    def test_hour_initialization(self):
        assert self.ts.start == datetime.datetime(2022, 1, 1, 11, 0, 0)
        assert self.ts.end == datetime.datetime(2022, 1, 1, 11, 59, 59)
        assert self.ts.step == 12
        assert self.ts.length == 1/24
        assert self.ts.year == 2022

    def test_hour_get_step_from_date(self):
        assert Hour.get_step_from_date(datetime.datetime(2022, 1, 1)) == 1
        assert Hour.get_step_from_date(datetime.datetime(2022, 1, 1, 4)) == 5
    
    def test_hour_get_start(self):
        assert self.ts.get_start() == datetime.datetime(2022, 1, 1, 11, 0, 0)
    
    def test_hour_get_end(self):
        assert self.ts.get_end() == datetime.datetime(2022, 1, 1, 11, 59, 59)

    def test_hour_day_of_year(self):
        assert self.ts.day_of_year == 1
    
    def test_hour_hour_of_year(self):
        assert self.ts.hour_of_year == 12
