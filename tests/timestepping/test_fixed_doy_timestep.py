import pytest
from unittest import mock
import datetime

from timestepping.fixed_doy_timestep import FixedDOYTimeStep
from timestepping import ViirsModisTimeStep

class ConcreteFixedDOYTimeStep(FixedDOYTimeStep):
    
    def get_step_from_date(date: datetime.datetime):
        pass

    def get_start(self):
        pass

    def get_end(self):
        pass

class TestFixedDOYTimeStep:

    start_doys = [1, 50, 100, 150, 200, 250, 300, 350]

    def test_fixed_doy_timestep_initialization(self):
        with mock.patch('timestepping.fixed_num_timestep.FixedNTimeStep.__init__') as mock_init:
            ConcreteFixedDOYTimeStep(2022, 1, self.start_doys)
            mock_init.assert_called_once_with(2022, 1, len(self.start_doys))

class TestViirsModisTimeStep:

    def setup_method(self):
        self.ts1  = ViirsModisTimeStep(2022, 1)
        self.ts46_notleap = ViirsModisTimeStep(2022, 46)
        self.ts46_leap = ViirsModisTimeStep(2020, 46)

    def test_viirsmodistimestep_initialization(self):
        assert self.ts1.year == 2022
        assert self.ts1.step == 1
        assert self.ts1.n_steps == 46
    
    def test_viirsmodistimestep_get_period_from_date(self):
        date1 = datetime.datetime(2022, 1, 1)
        date2 = datetime.datetime(2022, 1, 8)
        date3 = datetime.datetime(2022, 1, 9)
        date4 = datetime.datetime(2022, 12, 31)
        assert ViirsModisTimeStep.get_step_from_date(date1) == 1
        assert ViirsModisTimeStep.get_step_from_date(date2) == 1
        assert ViirsModisTimeStep.get_step_from_date(date3) == 2
        assert ViirsModisTimeStep.get_step_from_date(date4) == 46

    def test_viirsmodistimestep_get_start(self):
        assert self.ts1.get_start() == datetime.datetime(2022, 1, 1)
        assert self.ts46_notleap.get_start() == datetime.datetime(2022, 12, 27)
        assert self.ts46_leap.get_start() == datetime.datetime(2020, 12, 26)
    
    def test_viirsmodistimestep_get_end(self):
        assert self.ts1.get_end() == datetime.datetime(2022, 1, 8)
        assert self.ts46_notleap.get_end() == datetime.datetime(2022, 12, 31)
        assert self.ts46_leap.get_end() == datetime.datetime(2020, 12, 31)

    def test_viirsmodistimestep_step_of_year(self):
        assert self.ts1.step_of_year == 1
        assert self.ts46_notleap.step_of_year == 46
        assert self.ts46_leap.step_of_year == 46