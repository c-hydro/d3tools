from types import SimpleNamespace

from PIL import Image
import pytest

from d3tools.thumbnails import ThumbnailCollection


@pytest.mark.parametrize("suffix", [".png", ".pdf"])
def test_combine_thumbnail_files_rejects_empty_collection(tmp_path, suffix):
    collection = ThumbnailCollection([], "colors.txt")
    output = tmp_path / f"combined{suffix}"

    with pytest.raises(ValueError, match="No .* thumbnails? .*combine|No thumbnail images"):
        collection.combine_thumbnail_files(str(output))

    assert not output.exists()


def test_combine_pdf_rejects_missing_or_non_png_candidates(tmp_path):
    non_png = tmp_path / "thumbnail.jpg"
    non_png.write_bytes(b"not used for PDF assembly")
    collection = ThumbnailCollection([], "colors.txt")
    collection.thumbnails = [
        SimpleNamespace(thumbnail_file=str(tmp_path / "missing.png")),
        SimpleNamespace(thumbnail_file=str(non_png)),
    ]
    output = tmp_path / "combined.pdf"

    with pytest.raises(ValueError, match="No eligible PNG thumbnails"):
        collection.combine_thumbnail_files(str(output))

    assert not output.exists()


@pytest.mark.parametrize("suffix", [".png", ".pdf"])
def test_combine_thumbnail_files_writes_output(tmp_path, suffix):
    source = tmp_path / "source.png"
    Image.new("RGB", (20, 20), "red").save(source)
    collection = ThumbnailCollection([], "colors.txt")
    collection.thumbnails = [SimpleNamespace(thumbnail_file=str(source))]
    output = tmp_path / f"combined{suffix}"

    collection.combine_thumbnail_files(str(output))

    assert output.is_file()
    assert output.stat().st_size > 0