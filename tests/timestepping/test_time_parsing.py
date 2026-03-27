import datetime
import pytest
from d3tools.timestepping.time_parsing import get_date_from_str

class TestGetDateFromStr:
    def test_get_date_from_str_basic(self):
        assert get_date_from_str('2024-02-20') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('20240220') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('20/02/2024') == datetime.datetime(2024, 2, 20)
        assert get_date_from_str('2024-02-20 13:45') == datetime.datetime(2024, 2, 20, 13, 45)

    def test_get_date_from_str_end(self):
        dt = get_date_from_str('2024-02-20', end=True)
        assert dt.hour == 23 and dt.minute == 59 and dt.second == 59

    def test_get_date_from_str_invalid(self):
        with pytest.raises(ValueError):
            get_date_from_str('notadate')