import tempfile
import img2pdf
import io
from PIL import Image

from .thumbnail import Thumbnail

class ThumbnailCollection:
    def __init__(self, raster_files: list[str], color_definition_files: list[str]|str):
        self.thumbnails = []

        if isinstance(color_definition_files, str):
            color_definition_files = [color_definition_files] * len(raster_files)
        elif len(color_definition_files) != len(raster_files):
            raise ValueError("The number of color definition files must be equal to the number of raster files.")
        
        for raster_file, color_definition_file in zip(raster_files, color_definition_files):
            self.thumbnails.append(Thumbnail(raster_file, color_definition_file))

    def combine_thumbnail_files(self, file_out:str):

        list_of_thumbnails = [thumbnail.thumbnail_file for thumbnail in self.thumbnails]

        with open(file_out, "wb") as f:
            pdf_bytes = []
            for thumbnail in list_of_thumbnails:
                if thumbnail.endswith(".png"):
                    # Open the image file and convert it to RGB
                    img = Image.open(thumbnail).convert("RGB")
                    # Convert the Image object to bytes
                    byte_arr = io.BytesIO()
                    img.save(byte_arr, format='PNG')
                    byte_arr = byte_arr.getvalue()
                    pdf_bytes.append(byte_arr)

            f.write(img2pdf.convert(pdf_bytes))

    def save_as_pdf(self, file:str, **kwargs):
        with tempfile.TemporaryDirectory() as temp_dir:
            for i, thumbnail in enumerate(self.thumbnails):
                thumbnail.save(f'{temp_dir}/thumbnail_{i}.png', **kwargs)

            self.combine_thumbnail_files(file)