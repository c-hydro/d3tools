import pytest
from unittest import mock
import datetime

from timestepping.timestep import TimeStep

class ConcreteTimeStep(TimeStep):
    
    def __add__(self, n: int):
        pass

class TestTimeStep:
    def setup_method(self):
        self.ts = ConcreteTimeStep('2022-01-01', '2022-01-02')

    def test_timestep_initialization(self):
        assert self.ts.start == datetime.datetime(2022, 1, 1)
        assert self.ts.end == datetime.datetime(2022, 1, 2)

    def test_timestep_length(self):
        assert self.ts.length('days') == 1
        assert self.ts.length('hours') == 24
        with pytest.raises(ValueError):
            self.ts.length('invalid')

    def test_timestep_repr(self):
        assert repr(self.ts) == 'ConcreteTimeStep (20220101 - 20220102)'
    
    def test_timestep_eq(self):
        ts2 = ConcreteTimeStep('2022-01-01', '2022-01-02')
        assert self.ts == ts2
        ts2 = ConcreteTimeStep('2022-01-02', '2022-01-03')
        assert self.ts != ts2

    def test_timestep_lt(self):
        ts2 = ConcreteTimeStep('2022-01-03', '2022-01-04')
        assert self.ts < ts2
        ts2 = ConcreteTimeStep('2022-01-02', '2022-01-03')
        with pytest.raises(ValueError):
            self.ts < ts2
        
    def test_timestep_gt(self):
        ts2 = ConcreteTimeStep('2022-01-03', '2022-01-04')
        assert ts2 > self.ts

    def test_timestep_sub(self):
        with mock.patch(f'{__name__}.ConcreteTimeStep.__add__') as mock_add:
            n = 2
            self.ts - n
            mock_add.assert_called_once_with(-n)
