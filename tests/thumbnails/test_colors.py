import pytest

from d3tools.thumbnails.colors import keep_used_colors, parse_colors, parse_line


def write_colors(tmp_path, content):
    color_file = tmp_path / "colors.txt"
    color_file.write_text(content)
    return str(color_file)


def test_parse_line_accepts_valid_qgis_color_line():
    position, color, label = parse_line("1,0,128,255,200,high risk")

    assert position == "1"
    assert color == ["0", "128", "255", "200"]
    assert label == "high risk"


@pytest.mark.parametrize(
    "line, message",
    [
        ("1,2,3", "Invalid line format"),
        ("low,0,0,0,255,label", "Invalid position"),
        ("1,256,0,0,255,label", "Invalid color"),
        ("1,red,0,0,255,label", "Invalid color"),
    ],
)
def test_parse_line_rejects_invalid_lines(line, message):
    with pytest.raises(ValueError, match=message):
        parse_line(line)


def test_parse_colors_sorts_finite_breaks_and_keeps_inf_last(tmp_path):
    color_file = write_colors(
        tmp_path,
        "1,0,255,0,255,high\n"
        "inf,0,0,255,255,extreme\n"
        "-1,255,0,0,255,low\n"
        "0,255,255,0,255,normal\n",
    )

    positions, colors, labels = parse_colors(color_file)

    assert positions == ("-1", "0", "1", "inf")
    assert colors == (
        ["255", "0", "0", "255"],
        ["255", "255", "0", "255"],
        ["0", "255", "0", "255"],
        ["0", "0", "255", "255"],
    )
    assert labels == ["low", "normal", "high", "extreme"]


def test_parse_colors_supports_color_file_without_inf(tmp_path):
    color_file = write_colors(
        tmp_path,
        "1,0,255,0,255,high\n"
        "-1,255,0,0,255,low\n"
        "0,255,255,0,255,normal\n",
    )

    positions, colors, labels = parse_colors(color_file)

    assert positions == ("-1", "0", "1")
    assert colors == (
        ["255", "0", "0", "255"],
        ["255", "255", "0", "255"],
        ["0", "255", "0", "255"],
    )
    assert labels == ["low", "normal", "high"]


def test_parse_colors_ignores_malformed_lines_when_valid_entries_remain(tmp_path):
    color_file = write_colors(
        tmp_path,
        "not,a,color,line\n"
        "0,255,255,0,255,normal\n"
        "1,500,0,0,255,bad\n",
    )

    positions, colors, labels = parse_colors(color_file)

    assert positions == ("0",)
    assert colors == (["255", "255", "0", "255"],)
    assert labels == ["normal"]


@pytest.mark.parametrize("content", ["", "not,a,color,line\n1,500,0,0,255,bad\n"])
def test_parse_colors_raises_clear_error_when_no_valid_entries_remain(tmp_path, content):
    color_file = write_colors(tmp_path, content)

    with pytest.raises(ValueError, match="No valid color definitions"):
        parse_colors(color_file)


def test_keep_used_colors_preserves_range_between_used_values():
    colors = [
        ("255", "0", "0", "255"),
        ("255", "255", "0", "255"),
        ("0", "255", "0", "255"),
        ("0", "0", "255", "255"),
    ]

    assert keep_used_colors([1, 3], colors) == colors[1:4]