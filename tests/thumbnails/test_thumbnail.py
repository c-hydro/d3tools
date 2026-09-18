import numpy as np
import geopandas as gpd
import pytest
import xarray as xr
from PIL import Image
from shapely.geometry import LineString, Point, box

from d3tools.thumbnails import Thumbnail


@pytest.fixture
def color_definition_file(tmp_path):
    color_file = tmp_path / "colors.txt"
    color_file.write_text(
        "-1,255,0,0,255,low\n"
        "0,255,255,0,255,normal\n"
        "1,0,255,0,255,high\n"
        "inf,0,0,255,255,extreme\n"
    )
    return str(color_file)


def assert_valid_png(path):
    assert path.exists()
    assert path.stat().st_size > 0

    with Image.open(path) as image:
        assert image.format == "PNG"
        assert image.size[0] > 0
        assert image.size[1] > 0


def dataarray(values):
    return xr.DataArray(np.asarray(values, dtype=float), dims=("y", "x"))


def test_dataarray_without_nodata_saves_thumbnail(color_definition_file, tmp_path):
    thumbnail = Thumbnail(
        dataarray(
            [
                [-2, -1, -0.5, 0],
                [0.5, 1, 1.5, 2],
                [-0.25, 0.25, 0.75, 1.25],
            ]
        ),
        color_definition_file,
    )

    output = tmp_path / "thumbnail.png"
    thumbnail.save(str(output))

    assert thumbnail.allnan is False
    assert_valid_png(output)


def test_save_returns_destination(color_definition_file, tmp_path):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)

    destination = str(tmp_path / "nested" / "thumbnail.png")

    assert thumbnail.save(destination) == destination


def test_save_accepts_basename_destination(color_definition_file, tmp_path, monkeypatch):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    monkeypatch.chdir(tmp_path)

    destination = "thumbnail.png"

    assert thumbnail.save(destination) == destination
    assert_valid_png(tmp_path / destination)


def test_dataarray_nan_without_nodata_uses_missing_class(color_definition_file):
    thumbnail = Thumbnail(
        dataarray(
            [
                [-2, -1, -0.5, 0],
                [0.5, np.nan, 1.5, 2],
                [-0.25, 0.25, 0.75, 1.25],
            ]
        ),
        color_definition_file,
    )

    assert thumbnail.digital_img[1, 1] == 4


def test_singleton_band_dataarray_is_accepted(color_definition_file):
    source = xr.DataArray(
        np.array(
            [
                [
                    [-2, -1, -0.5, 0],
                    [0.5, 1, 1.5, 2],
                    [-0.25, 0.25, 0.75, 1.25],
                ]
            ],
            dtype=float,
        ),
        dims=("band", "y", "x"),
    ).rio.write_nodata(-9999)

    thumbnail = Thumbnail(source, color_definition_file)

    assert thumbnail.shape == (3, 4)


def test_multiband_dataarray_raises_clear_error(color_definition_file):
    source = xr.DataArray(
        np.ones((2, 3, 4), dtype=float),
        dims=("band", "y", "x"),
    ).rio.write_nodata(-9999)

    with pytest.raises(ValueError, match="single band"):
        Thumbnail(source, color_definition_file)


def test_all_nan_dataarray_saves_no_data_thumbnail(color_definition_file, tmp_path):
    thumbnail = Thumbnail(dataarray(np.full((3, 4), np.nan)), color_definition_file)

    output = tmp_path / "all_nan.png"
    thumbnail.save(str(output))

    assert thumbnail.allnan is True
    assert_valid_png(output)


def polygon_geodataframe(crs="EPSG:4326"):
    return gpd.GeoDataFrame(
        {"value": [-0.5, 0.5]},
        geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)],
        crs=crs,
    )


def test_polygon_geodataframe_saves_thumbnail(color_definition_file, tmp_path):
    thumbnail = Thumbnail(polygon_geodataframe(), color_definition_file)

    output = tmp_path / "polygon.png"
    thumbnail.save(str(output))

    assert_valid_png(output)


@pytest.mark.parametrize(
    "geometry",
    [LineString([(0, 0), (1, 1)]), Point(0, 0)],
)
def test_line_and_point_geodataframes_save_thumbnail(
    geometry,
    color_definition_file,
    tmp_path,
):
    source = gpd.GeoDataFrame({"value": [0.5]}, geometry=[geometry], crs="EPSG:4326")
    thumbnail = Thumbnail(source, color_definition_file)

    output = tmp_path / f"{geometry.geom_type.lower()}.png"
    thumbnail.save(str(output))

    assert_valid_png(output)


def test_projected_geodataframe_shape_is_bounded(color_definition_file):
    projected = polygon_geodataframe().to_crs("EPSG:3857")

    thumbnail = Thumbnail(projected, color_definition_file)

    height, width = thumbnail.shape


    assert max(height, width) <= 2400
    assert min(height, width) > 0


def test_degenerate_geodataframe_uses_square_shape(color_definition_file):
    source = gpd.GeoDataFrame(
        {"value": [0.5]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )

    thumbnail = Thumbnail(source, color_definition_file)

    height, width = thumbnail.shape

    assert height == width
    assert height > 0
    assert height <= 2400


def test_all_missing_geodataframe_saves_no_data_thumbnail(color_definition_file, tmp_path):
    source = gpd.GeoDataFrame(
        {"value": [np.nan]},
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    )
    thumbnail = Thumbnail(source, color_definition_file)

    output = tmp_path / "all_missing_vector.png"
    thumbnail.save(str(output))

    assert thumbnail.allnan is True
    assert_valid_png(output)


def test_empty_geodataframe_raises_clear_error(color_definition_file):
    source = gpd.GeoDataFrame({"value": []}, geometry=[], crs="EPSG:4326")

    with pytest.raises(ValueError, match="empty"):
        Thumbnail(source, color_definition_file)


def test_geodataframe_requires_value_column(color_definition_file):
    source = gpd.GeoDataFrame(
        {"not_value": [0.5]},
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="value"):
        Thumbnail(source, color_definition_file)


def test_geodataframe_requires_numeric_value_column(color_definition_file):
    source = gpd.GeoDataFrame(
        {"value": ["medium"]},
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="numeric"):
        Thumbnail(source, color_definition_file)