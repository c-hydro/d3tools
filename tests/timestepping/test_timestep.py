from unittest import mock

from timestepping.timestep import TimeStep

class ConcreteTimeStep(TimeStep):
    def __add__(self, n: int):
        pass

class TestTimeStep:
    def setup_method(self):
        self.ts  = ConcreteTimeStep('2022-01-01', '2022-01-02')
        self.ts2 = ConcreteTimeStep('2022-02-20', '2022-02-28')

    def test_timestep_sub(self):
        with mock.patch(f'{__name__}.ConcreteTimeStep.__add__') as mock_add:
            n = 2
            self.ts - n
            mock_add.assert_called_once_with(-n)

    def test_timestep_set_year(self):
        new_year = 2024
        new_ts = self.ts.set_year(new_year)
        assert new_ts.start.year == new_year
        assert new_ts.end.year == new_year

        new_ts2 = self.ts2.set_year(new_year)
        assert new_ts2.start.year == new_year
        assert new_ts2.end.year == new_year
        assert new_ts2.end.day == 29