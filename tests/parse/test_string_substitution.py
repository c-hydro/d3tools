
import datetime
from d3tools.parse.string_substitution import substitute_string, substitute_values

class TestStringSubstitution:
    def test_substitute_string_basic(self):
        assert substitute_string("file_{date}", {"date": "2024-02-20"}) == "file_2024-02-20"

    def test_substitute_string_with_format(self):
        assert substitute_string("file_{date:%Y%m%d}", {"date": "2024-02-20"}) == "file_20240220"

    def test_substitute_string_with_datetime_obj(self):
        dt = datetime.datetime(2024, 2, 20)
        assert substitute_string("file_{date:%Y%m%d}", {"date": dt}) == "file_20240220"

    def test_substitute_string_with_list(self):
        result = substitute_string("tile_{tile}", {"tile": ["A", "B"]})
        assert sorted(result) == ["tile_A", "tile_B"]

    def test_substitute_values_dict(self):
        structure = {"file": "output_{date:%Y%m%d}.tif"}
        tags = {"date": "2024-02-20"}
        assert substitute_values(structure, tags) == {"file": "output_20240220.tif"}

    def test_substitute_values_list(self):
        structure = ["tile_{tile}", "other_{tile}"]
        tags = {"tile": ["A", "B"]}
        assert substitute_values(structure, tags) == [["tile_A", "tile_B"], ["other_A", "other_B"]]