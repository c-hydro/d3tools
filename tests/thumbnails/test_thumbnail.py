import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import pytest
from rasterio.transform import from_origin
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


def test_raster_path_with_nodata_saves_thumbnail(color_definition_file, tmp_path):
    source = dataarray(
        [
            [0, 1],
            [2, -9999],
        ]
    ).rio.write_crs("EPSG:4326").rio.write_transform(from_origin(0, 2, 1, 1)).rio.write_nodata(-9999)
    raster_path = tmp_path / "source.tif"
    source.rio.to_raster(str(raster_path))

    thumbnail = Thumbnail(str(raster_path), color_definition_file)

    output = tmp_path / "thumbnail.png"
    thumbnail.save(str(output))

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


@pytest.mark.parametrize("annotation", ["none", "NONE", "", "   "])
def test_annotation_disabled_strings_do_not_add_annotation(
    annotation,
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    monkeypatch.setattr(thumbnail, "add_annotation", lambda text, **kwargs: calls.append(text))

    thumbnail.save(str(tmp_path / "thumbnail.png"), annotation=annotation, legend=False)

    assert calls == []


def test_empty_annotation_dict_without_inferred_text_is_disabled(
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    monkeypatch.setattr(thumbnail, "add_annotation", lambda text, **kwargs: calls.append(text))

    thumbnail.save(str(tmp_path / "thumbnail.png"), annotation={}, legend=False)

    assert calls == []


def test_annotation_dict_is_not_mutated(color_definition_file, tmp_path, monkeypatch):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    annotation = {"text": "Forecast", "xy": (0.1, 0.2)}
    expected = annotation.copy()
    monkeypatch.setattr(
        thumbnail,
        "add_annotation",
        lambda text, **kwargs: calls.append((text, kwargs)),
    )

    thumbnail.save(str(tmp_path / "thumbnail.png"), annotation=annotation, legend=False)

    assert calls == [("Forecast", {"xy": (0.1, 0.2)})]
    assert annotation == expected


def test_annotation_dict_empty_text_is_disabled(
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    monkeypatch.setattr(thumbnail, "add_annotation", lambda text, **kwargs: calls.append(text))

    thumbnail.save(str(tmp_path / "thumbnail.png"), annotation={"text": ""}, legend=False)

    assert calls == []


def test_invalid_annotation_type_raises_clear_error(color_definition_file, tmp_path):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)

    with pytest.raises(TypeError, match="annotation"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), annotation=12, legend=False)


@pytest.mark.parametrize("legend", [False, None, "none", "NONE", "  none  "])
def test_legend_disabled_values_do_not_add_legend(
    legend,
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    monkeypatch.setattr(thumbnail, "add_legend", lambda **kwargs: calls.append(kwargs))

    thumbnail.save(str(tmp_path / "thumbnail.png"), legend=legend)

    assert calls == []


@pytest.mark.parametrize("legend", [True, {"loc": "lower left"}])
def test_legend_enabled_values_add_legend(
    legend,
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    monkeypatch.setattr(thumbnail, "add_legend", lambda **kwargs: calls.append(kwargs))

    thumbnail.save(str(tmp_path / "thumbnail.png"), legend=legend)

    if legend is True:
        assert calls == [{}]
    else:
        assert calls == [{"loc": "lower left"}]


def test_legend_dict_is_not_mutated(color_definition_file, tmp_path, monkeypatch):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    legend = {"loc": "lower left", "borderaxespad": 1}
    expected = legend.copy()
    monkeypatch.setattr(thumbnail, "add_legend", lambda **kwargs: calls.append(kwargs))

    thumbnail.save(str(tmp_path / "thumbnail.png"), legend=legend)

    assert calls == [expected]
    assert legend == expected


def test_legend_position_is_applied_to_axes_legend(color_definition_file):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    thumbnail.make_image()

    thumbnail.add_legend(loc="lower left")

    legend = thumbnail.ax.get_legend()
    assert legend is not None
    assert legend._loc == 3
    assert thumbnail.fig.legends == []


def test_legacy_legend_bbox_to_anchor_is_ignored(color_definition_file, monkeypatch):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    thumbnail.make_image()
    calls = []

    def spy_legend(*args, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(thumbnail.ax, "legend", spy_legend)

    thumbnail.add_legend(loc="lower left", bbox_to_anchor=(1, 1))

    assert len(calls) == 1
    assert calls[0]["loc"] == "lower left"
    assert calls[0]["borderaxespad"] == 0
    assert "bbox_to_anchor" not in calls[0]


def test_invalid_legend_string_raises_clear_error(color_definition_file, tmp_path):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)

    with pytest.raises(ValueError, match="legend"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), legend="off")


def test_invalid_legend_type_raises_clear_error(color_definition_file, tmp_path):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)

    with pytest.raises(TypeError, match="legend"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), legend=12)


@pytest.fixture
def overlay_geodataframe():
    return gpd.GeoDataFrame(
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    )


@pytest.mark.parametrize("overlay", [False, None])
def test_overlay_disabled_values_do_not_add_overlay(
    overlay,
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    monkeypatch.setattr(thumbnail, "add_overlay", lambda shp_file, **kwargs: calls.append(shp_file))

    thumbnail.save(str(tmp_path / "thumbnail.png"), overlay=overlay, legend=False)

    assert calls == []


def test_overlay_dict_is_not_mutated(
    color_definition_file,
    overlay_geodataframe,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    calls = []
    overlay = {"shp_file": overlay_geodataframe, "edgecolor": "red"}
    expected = overlay.copy()
    monkeypatch.setattr(
        thumbnail,
        "add_overlay",
        lambda shp_file, **kwargs: calls.append((shp_file, kwargs)),
    )

    thumbnail.save(str(tmp_path / "thumbnail.png"), overlay=overlay, legend=False)

    assert calls == [(overlay_geodataframe, {"edgecolor": "red"})]
    assert overlay == expected


def test_geodataframe_overlay_saves_thumbnail(
    color_definition_file,
    overlay_geodataframe,
    tmp_path,
):
    source = dataarray([[0, 1], [2, 3]]).rio.write_crs("EPSG:4326")
    thumbnail = Thumbnail(source, color_definition_file)

    output = tmp_path / "overlay.png"
    thumbnail.save(str(output), overlay=overlay_geodataframe, legend=False)

    assert_valid_png(output)


def test_invalid_overlay_type_raises_clear_error(color_definition_file, tmp_path):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)

    with pytest.raises(TypeError, match="overlay"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), overlay=12, legend=False)


def test_overlay_requires_source_crs(
    color_definition_file,
    overlay_geodataframe,
    tmp_path,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)

    with pytest.raises(ValueError, match="CRS"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), overlay=overlay_geodataframe, legend=False)


def test_overlay_requires_overlay_crs(color_definition_file, tmp_path):
    source = dataarray([[0, 1], [2, 3]]).rio.write_crs("EPSG:4326")
    thumbnail = Thumbnail(source, color_definition_file)
    overlay = gpd.GeoDataFrame(
        geometry=[box(0, 0, 1, 1)],
    )

    with pytest.raises(ValueError, match="CRS"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), overlay=overlay, legend=False)


def test_save_does_not_reuse_preexisting_figure_state(
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    thumbnail.make_image()
    thumbnail.add_annotation("stale")
    text_before_add = []
    add_annotation = thumbnail.add_annotation

    def spy_add_annotation(text, **kwargs):
        text_before_add.append([item.get_text() for item in thumbnail.ax.texts])
        add_annotation(text, **kwargs)

    monkeypatch.setattr(thumbnail, "add_annotation", spy_add_annotation)

    thumbnail.save(str(tmp_path / "thumbnail.png"), annotation="fresh", legend=False)

    assert text_before_add == [[]]


def test_repeated_saves_create_independent_figures(
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    axes = []
    make_image = thumbnail.make_image

    def spy_make_image(*args, **kwargs):
        make_image(*args, **kwargs)
        axes.append(thumbnail.ax)

    monkeypatch.setattr(thumbnail, "make_image", spy_make_image)

    thumbnail.save(str(tmp_path / "first.png"), annotation="first", legend=False)
    thumbnail.save(str(tmp_path / "second.png"), annotation="second", legend=False)

    assert len(axes) == 2
    assert axes[0] is not axes[1]


def test_save_closes_figure_after_save_failure(
    color_definition_file,
    tmp_path,
    monkeypatch,
):
    thumbnail = Thumbnail(dataarray([[0, 1], [2, 3]]), color_definition_file)
    figures = []
    make_image = thumbnail.make_image

    def spy_make_image(*args, **kwargs):
        make_image(*args, **kwargs)
        figures.append(thumbnail.fig)

        def raise_error(*args, **kwargs):
            raise RuntimeError("save failed")

        monkeypatch.setattr(thumbnail.fig, "savefig", raise_error)

    monkeypatch.setattr(thumbnail, "make_image", spy_make_image)

    with pytest.raises(RuntimeError, match="save failed"):
        thumbnail.save(str(tmp_path / "thumbnail.png"), legend=False)

    assert not hasattr(thumbnail, "fig")
    assert not hasattr(thumbnail, "ax")
    assert not hasattr(thumbnail, "im")
    assert not plt.fignum_exists(figures[0].number)


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


def test_raster_classification_is_right_closed_with_terminal_inf(color_definition_file):
    thumbnail = Thumbnail(
        dataarray(
            [
                [-2, -1, -0.5, 0],
                [0.5, 1, 1.5, np.nan],
            ]
        ),
        color_definition_file,
    )

    np.testing.assert_array_equal(
        thumbnail.digital_img,
        np.array(
            [
                [0, 0, 1, 1],
                [2, 2, 3, 4],
            ]
        ),
    )


def test_vector_classification_is_right_closed_with_terminal_inf(color_definition_file):
    source = gpd.GeoDataFrame(
        {"value": [-2, -1, -0.5, 0, 0.5, 1, 1.5, np.nan]},
        geometry=[Point(i, 0) for i in range(8)],
        crs="EPSG:4326",
    )
    thumbnail = Thumbnail(source, color_definition_file)

    assert thumbnail.digital_src["value_discrete"].to_list() == [0, 0, 1, 1, 2, 2, 3, 4]


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