import pytest
import datetime

from timestepping import TimeRange, Year, Month, Dekad, Day, Hour, ViirsModisTimeStep, FixedDOYTimeStep

class TestTimeRange:

    def setup_method(self):
        self.tr1 = TimeRange(datetime.datetime(2018, 1, 1), datetime.datetime(2018, 12, 31))
        self.tr2 = TimeRange(datetime.datetime(2018, 1, 20), datetime.datetime(2018, 12, 20))

    def test_gen_timesteps_from_tsnumber(self):
        dekads = self.tr1.gen_timesteps_from_tsnumber(36)
        for i, d in enumerate(dekads):
            assert isinstance(d, Dekad)
            if i == 0:
                assert d == Dekad(2018, 1)
            if i == 35:
                assert d == Dekad(2018, 36)
        else:
            assert i == 35

        months = self.tr2.gen_timesteps_from_tsnumber(12)
        for i, m in enumerate(months):
            assert isinstance(m, Month)
            if i == 0:
                assert m == Month(2018, 1)
            if i == 11:
                assert m == Month(2018, 12)
        else:
            assert i == 11

        days = self.tr1.gen_timesteps_from_tsnumber(365)
        for i, d in enumerate(days):
            assert isinstance(d, Day)
            if i == 0:
                assert d == Day(2018, 1)
            if i == 365:
                assert d == Day(2018, 365)
        else:
            assert i == 364

    def test_gen_ts_from_DOY(self):
        doys = range(1, 366, 8)
        timesteps = self.tr1.gen_timesteps_from_DOY(doys)
        for i, ts in enumerate(timesteps):
            assert isinstance(ts, ViirsModisTimeStep)
            if i == 0:
                assert ts == ViirsModisTimeStep(2018, 1)
            if i == 45:
                assert ts == ViirsModisTimeStep(2018, 46)
        else:
            assert i == 45

        doys = [1, 50, 100, 150, 200, 250, 300, 350]
        timesteps = self.tr2.gen_timesteps_from_DOY(doys)
        for i, ts in enumerate(timesteps):
            assert isinstance(ts, FixedDOYTimeStep)
            if i == 0:
                assert ts == FixedDOYTimeStep.from_step(2018, 1, doys)
            if i == 7:
                assert ts == FixedDOYTimeStep.from_step(2018, 8, doys)
        else:
            assert i == 7

    def test_gen_timesteps_from_issue_hour(self):
        issue_hours = [0,6,12,18]
        expected_n = 365 * 4
        timesteps = self.tr1.gen_timesteps_from_issue_hour(issue_hours)
        for i, ts in enumerate(timesteps):
            assert isinstance(ts, Hour)
            if i == 0:
                assert ts == Hour.from_date("2018-01-01 00:00")
            if i == expected_n-1:
                assert ts == Hour.from_date("2018-12-31 18:00")
        else:
            assert i == expected_n-1

    def test_months(self):
        months = self.tr1.months
        assert months == self.tr1.get_timesteps_from_tsnumber(12)

    def test_years(self):
        years = self.tr1.years
        assert years == self.tr1.get_timesteps_from_tsnumber(1)

    def test_dekads(self):
        dekads = self.tr1.dekads
        assert dekads == self.tr1.get_timesteps_from_tsnumber(36)

    def test_days(self):
        days = self.tr1.days
        assert days == self.tr1.get_timesteps_from_tsnumber(365)

    def test_viirstimes(self):
        viirstimes = self.tr1.viirstimes
        assert viirstimes == self.tr1.get_timesteps_from_DOY(range(1, 366, 8))