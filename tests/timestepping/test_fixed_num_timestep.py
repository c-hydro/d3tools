import pytest
from unittest import mock
import datetime

from timestepping.fixed_num_timestep import FixedNTimeStep
from timestepping import Dekad, Month, Year

class ConcreteFixedNTimeStep(FixedNTimeStep):
    
    def get_step_from_date(date: datetime.datetime):
        pass

    def get_start(self):
        pass

    def get_end(self):
        pass

class TestFixedNTimeStep:

    def setup_method(self):
        with mock.patch(f'{__name__}.ConcreteFixedNTimeStep.get_start') as mock_get_start, \
             mock.patch(f'{__name__}.ConcreteFixedNTimeStep.get_end') as mock_get_end:
                mock_get_start.return_value = datetime.datetime(2022, 1, 1)
                mock_get_end.return_value = datetime.datetime(2022, 1, 31)
                self.ts = ConcreteFixedNTimeStep(2022, 1, 12)

    def test_fixed_num_timestep_initialization(self):
        assert self.ts.start == datetime.datetime(2022, 1, 1)
        assert self.ts.end == datetime.datetime(2022, 1, 31)
        assert self.ts.step == 1
        assert self.ts.n_steps == 12
        assert self.ts.year == 2022

    def test_fixed_num_timestep_get_subclass(self):
        assert FixedNTimeStep.get_subclass(1) == Year
        assert FixedNTimeStep.get_subclass(12) == Month
        assert FixedNTimeStep.get_subclass(36) == Dekad
        with pytest.raises(ValueError):
            FixedNTimeStep.get_subclass(0)

    def test_fixed_num_timestep_from_steps(self):
        ts = FixedNTimeStep.from_step(2022, 1, 1)
        assert isinstance(ts, Year)
        ts = FixedNTimeStep.from_step(2022, 1, 12)
        assert isinstance(ts, Month)
        ts = FixedNTimeStep.from_step(2022, 1, 36)
        assert isinstance(ts, Dekad)

    def test_fixed_num_timestep_from_date(self):
        ts = FixedNTimeStep.from_date("2022-01-01", 1)
        assert isinstance(ts, Year)
        ts = FixedNTimeStep.from_date("2022-01-01", 12)
        assert isinstance(ts, Month)
        ts = FixedNTimeStep.from_date("2022-01-01", 36)
        assert isinstance(ts, Dekad)

    def test_fixed_num_timestep_add(self):
        with mock.patch(f'{__name__}.ConcreteFixedNTimeStep.__init__') as mock_init:
            mock_init.return_value = None
            self.ts + 13
            mock_init.assert_called_once_with(2023, 2)

class TestDekad:

    def setup_method(self):
        self.ts1  = Dekad(2022, 1)
        self.ts6 = Dekad(2022, 6)
        self.ts36 = Dekad(2022, 36)

    def test_dekad_initialization(self):
        assert self.ts1.start == datetime.datetime(2022, 1, 1)
        assert self.ts1.end == datetime.datetime(2022, 1, 10)
        assert self.ts1.month == 1
        assert self.ts1.dekad_of_month == 1
        assert self.ts1.dekad == 1
        assert self.ts1.dekad_of_year == 1

    def test_dekad_get_step_from_date(self):
        assert Dekad.get_step_from_date(datetime.datetime(2022, 1, 1)) == 1
        assert Dekad.get_step_from_date(datetime.datetime(2022, 1, 10)) == 1
        assert Dekad.get_step_from_date(datetime.datetime(2022, 1, 11)) == 2
        assert Dekad.get_step_from_date(datetime.datetime(2022, 1, 20)) == 2
        assert Dekad.get_step_from_date(datetime.datetime(2022, 1, 21)) == 3
        assert Dekad.get_step_from_date(datetime.datetime(2022, 1, 31)) == 3

    def test_dekad_get_start(self):
        assert self.ts1.get_start() == datetime.datetime(2022, 1, 1)
        assert self.ts6.get_start() == datetime.datetime(2022, 2, 21)
        assert self.ts36.get_start() == datetime.datetime(2022, 12, 21)

    def test_dekad_get_end(self):
        # Test when self.dekad_of_month != 3
        assert self.ts1.get_end() == datetime.datetime(2022, 1, 10)

        # Test when self.dekad_of_month == 3 and it's not December
        assert self.ts6.get_end() == datetime.datetime(2022, 2, 28)

        # Test when self.dekad_of_month == 3 and it's December
        assert self.ts36.get_end() == datetime.datetime(2022, 12, 31)

    def test_dekad_month(self):
        assert self.ts1.month == 1
        assert self.ts6.month == 2
        assert self.ts36.month == 12
    
    def test_dekad_dekad_of_month(self):
        assert self.ts1.dekad_of_month == 1
        assert self.ts6.dekad_of_month == 3
        assert self.ts36.dekad_of_month == 3

    def test_dekad_dekad(self):
        assert self.ts1.dekad == 1
        assert self.ts6.dekad == 6
        assert self.ts36.dekad == 36
    
    def test_dekad_of_year(self):
        assert self.ts1.dekad_of_year == 1
        assert self.ts6.dekad_of_year == 6
        assert self.ts36.dekad_of_year == 36
    
class TestMonth:

    def setup_method(self):
        self.ts1 = Month(2022, 1)
        self.ts12 = Month(2022, 12)

    def test_month_initialization(self):
        assert self.ts1.start == datetime.datetime(2022, 1, 1)
        assert self.ts1.end == datetime.datetime(2022, 1, 31)
        assert self.ts1.step == 1
        assert self.ts1.n_steps == 12
        assert self.ts1.year == 2022

    def test_month_get_step_from_date(self):
        assert Month.get_step_from_date(datetime.datetime(2022, 1, 1)) == 1
        assert Month.get_step_from_date(datetime.datetime(2022, 1, 31)) == 1
        assert Month.get_step_from_date(datetime.datetime(2022, 2, 1)) == 2
        assert Month.get_step_from_date(datetime.datetime(2022, 2, 28)) == 2
        assert Month.get_step_from_date(datetime.datetime(2022, 3, 1)) == 3
        assert Month.get_step_from_date(datetime.datetime(2022, 3, 31)) == 3

    def test_month_get_start(self):
        assert self.ts1.get_start() == datetime.datetime(2022, 1, 1)
        assert self.ts12.get_start() == datetime.datetime(2022, 12, 1)

    def test_month_get_end(self):
        assert self.ts1.get_end() == datetime.datetime(2022, 1, 31)
        assert self.ts12.get_end() == datetime.datetime(2022, 12, 31)

    def test_month_month(self):
        assert self.ts1.month == 1
        assert self.ts12.month == 12
    
    def test_month_month_of_year(self):
        assert self.ts1.month_of_year == 1
        assert self.ts12.month_of_year == 12

class TestYear:

    def setup_method(self):
        self.ts = Year(1900)
        self.tsleap = Year(2024)

    def test_year_initialization(self):
        assert self.ts.start == datetime.datetime(1900, 1, 1)
        assert self.ts.end == datetime.datetime(1900, 12, 31)
        assert self.ts.step == 1
        assert self.ts.n_steps == 1
        assert self.ts.year == 1900

    def test_year_get_step_from_date(self):
        assert Year.get_step_from_date(datetime.datetime(1900, 1, 1)) == 1
        assert Year.get_step_from_date(datetime.datetime(1900, 12, 31)) == 1

    def test_year_get_start(self):
        assert self.ts.get_start() == datetime.datetime(1900, 1, 1)
    
    def test_year_get_end(self):
        assert self.ts.get_end() == datetime.datetime(1900, 12, 31)
    
    def test_year_is_leap(self):
        assert self.ts.is_leap() == False
        assert self.tsleap.is_leap() == True
