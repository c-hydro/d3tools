import pytest
from unittest import mock
import datetime

from timestepping.fixed_doy_timestep import FixedDOYTimeStep
from timestepping import ViirsModisTimeStep

# class ConcreteFixedDOYTimeStep(FixedDOYTimeStep):

#     def get_step_from_date(date: datetime.datetime):
#         pass

#     def get_start(self):
#         pass

#     def get_end(self):
#         pass

class TestFixedDOYTimeStep:

    start_doys = [1, 50, 100, 150, 200, 250, 300, 350]

    def setup_method(self):
        self.ts1 = FixedDOYTimeStep(2022, 1, self.start_doys)
        self.ts46_leap = FixedDOYTimeStep(2020, 46, ViirsModisTimeStep.start_doys)
        self.ts46_notleap = FixedDOYTimeStep(2022, 46, ViirsModisTimeStep.start_doys)

    def test_fixed_doy_timestep_initialization(self):
        assert self.ts1.year == 2022
        assert self.ts1.step == 1
        assert self.ts1.n_steps == 8
        assert self.ts1.start_doys == tuple(self.start_doys)

    def test_fixed_doy_timestep_from_date(self):
        start_doys = ViirsModisTimeStep.start_doys
        tsVIIRSMODIS = FixedDOYTimeStep.from_date(datetime.datetime(2022, 1, 5), start_doys)
        assert tsVIIRSMODIS == ViirsModisTimeStep(2022, 1)
        ts50 = FixedDOYTimeStep.from_date(datetime.datetime(2022, 2, 1), self.start_doys)
        assert ts50 == self.ts1

    def test_fixed_doy_timestep_from_step(self):
        start_doys = ViirsModisTimeStep.start_doys
        tsVIIRSMODIS = FixedDOYTimeStep.from_step(2022, 1, start_doys)
        assert tsVIIRSMODIS == ViirsModisTimeStep(2022, 1)
        ts50 = FixedDOYTimeStep.from_step(2022, 1, self.start_doys)
        assert ts50 == self.ts1

    def test_fixed_doy_timestep_get_start(self):
        assert self.ts1.get_start() == datetime.datetime(2022, 1, 1)
        assert self.ts46_notleap.get_start() == datetime.datetime(2022, 12, 27)
        assert self.ts46_leap.get_start() == datetime.datetime(2020, 12, 26)
    
    def test_fixed_doy_timestep_get_end(self):
        assert self.ts1.get_end() == datetime.datetime(2022, 2, 18)
        assert self.ts46_notleap.get_end() == datetime.datetime(2022, 12, 31)
        assert self.ts46_leap.get_end() == datetime.datetime(2020, 12, 31)

    def test_fixed_doy_timestep_step_of_year(self):
        assert self.ts1.step_of_year == 1
        assert self.ts46_notleap.step_of_year == 46
        assert self.ts46_leap.step_of_year == 46

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