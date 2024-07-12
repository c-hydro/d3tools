import pytest
from matplotlib.colors import ListedColormap
import numpy as np

from thumbnails import colors as col

def test_parse_line_valid_input():
    line = "10,255,0,0,255,label1"
    expected = ('10', ['255', '0', '0', '255'], 'label1')
    assert col.parse_line(line) == expected

def test_parse_line_empty():
    line = ""
    with pytest.raises(ValueError) as excinfo:
        col.parse_line(line)
    assert "Invalid line format" in str(excinfo.value)

def test_parse_line_invalid_format():
    line = "10,255,0,0,label1"  # Missing one color value
    with pytest.raises(ValueError) as excinfo:
        col.parse_line(line)
    assert "Invalid line format" in str(excinfo.value)

def test_parse_line_invalid_position():
    line = "pos,255,0,0,255,label1"
    with pytest.raises(ValueError) as excinfo:
        col.parse_line(line)
    assert "Invalid position value" in str(excinfo.value)

def test_parse_line_invalid_color_value():
    line = "10,255,-1,300,255,label1"
    with pytest.raises(ValueError) as excinfo:
        col.parse_line(line)
    assert "Invalid color value" in str(excinfo.value)

def create_test_file(tmp_path, content):
    file_path = tmp_path / "test_file.txt"
    file_path.write_text(content)
    return file_path

@pytest.fixture
def basic_file_content():
    return """
    # line 1
    # line 2
    0,255,0,0,255,label1
    10,0,255,0,255,label2
    20,0,0,255,255,label3
    inf,255,255,255,255,label4
    """

def test_parse_txt_basic(tmp_path, basic_file_content):
    file_path = create_test_file(tmp_path, basic_file_content)
    positions, colors = col.parse_txt(file_path)
    assert positions == ('0', '10', '20', 'inf')
    assert colors == (['255', '0', '0', '255'], ['0', '255', '0', '255'], ['0', '0', '255', '255'], ['255', '255', '255', '255'])

def test_parse_txt_empty_file(tmp_path):
    file_path = create_test_file(tmp_path, "")
    with pytest.raises(ValueError):  # Assuming your function raises ValueError for empty input
        col.parse_txt(file_path)

def test_parse_txt_inf_handling(tmp_path):
    content = """
    # line 1
    # line 2
    0,255,0,0,255,label1
    inf,255,255,255,255,label2
    """
    file_path = create_test_file(tmp_path, content)
    positions, colors = col.parse_txt(file_path)
    assert positions == ('0', 'inf')
    assert colors == (['255', '0', '0', '255'], ['255', '255', '255', '255'])

def test_parse_txt_no_inf(tmp_path):
    content = """
    # line 1
    # line 2
    0,255,0,0,255,label1
    10,0,255,0,255,label2
    """
    file_path = create_test_file(tmp_path, content)
    positions, colors = col.parse_txt(file_path)
    assert positions == ('0', '10')
    assert colors == (['255', '0', '0', '255'], ['0', '255', '0', '255'])

def test_create_colormap_with_valid_input():
    # Input: List of RGB colors
    colors = [[255, 0, 0, 255], [0, 255, 0, 255], [0, 0, 255, 255]]
    cmap = col.create_colormap(colors)
    assert isinstance(cmap, ListedColormap), "The function should return a ListedColormap object"
    assert len(cmap.colors) == 4, "Colormap should contain 4 colors"
    np.testing.assert_array_almost_equal(cmap.colors[0], [1.0, 0.0, 0.0, 1.0], err_msg="First color should be red in normalized form")

def test_create_colormap_with_empty_input():
    # Input: Empty list of colors
    with pytest.raises(ValueError):
        col.create_colormap([])

def test_keep_used_colors_with_valid_input():
    unique_values = [1.0, 2.0]
    positions = [1.0, 3.0, 2.0]
    colors = [(255, 0, 0, 255), (0, 255, 0, 255), (0, 0, 255, 255)]
    expected_colors = [(255, 0, 0, 255), (0, 0, 255, 255)]
    result = col.keep_used_colors(unique_values, positions, colors)
    assert result == expected_colors, "Function should remove colors not in unique_values"

def test_keep_used_colors_all_colors_used():
    unique_values = [1.0, 2.0, 3.0]
    positions = [1.0, 2.0, 3.0]
    colors = [(255, 0, 0, 255), (0, 255, 0, 255), (0, 0, 255, 255)]
    expected_colors = colors
    result = col.keep_used_colors(unique_values, positions, colors)
    assert result == expected_colors, "Function should keep all colors when all are used"

def test_keep_used_colors_no_colors_used():
    unique_values = [4.0, 5.0]
    positions = [1.0, 2.0, 3.0]
    colors = [(255, 0, 0, 255), (0, 255, 0, 255), (0, 0, 255, 255)]
    expected_colors = []
    result = col.keep_used_colors(unique_values, positions, colors)
    assert result == expected_colors, "Function should remove all colors when none are used"

def test_keep_used_colors_empty_inputs():
    unique_values = []
    positions = []
    colors = []
    expected_colors = []
    result = col.keep_used_colors(unique_values, positions, colors)
    assert result == expected_colors, "Function should handle empty inputs gracefully"
