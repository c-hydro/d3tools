from unittest import mock

from timestepping.timestep import TimeStep

class ConcreteTimeStep(TimeStep):
    def __add__(self, n: int):
        pass

class TestTimeStep:
    def setup_method(self):
        self.ts = ConcreteTimeStep('2022-01-01', '2022-01-02')

    def test_timestep_sub(self):
        with mock.patch(f'{__name__}.ConcreteTimeStep.__add__') as mock_add:
            n = 2
            self.ts - n
            mock_add.assert_called_once_with(-n)
