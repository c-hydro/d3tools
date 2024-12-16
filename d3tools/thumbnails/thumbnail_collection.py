import tempfile
import img2pdf
import io
from PIL import Image
from typing import Optional
import numpy as np
import os

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

    def combine_thumbnail_files(self, file_out:str, grid: Optional[tuple[int, int]] = None):

        list_of_thumbnails = [thumbnail.thumbnail_file for thumbnail in self.thumbnails]
        os.makedirs(os.path.dirname(file_out), exist_ok=True)

        if file_out.endswith(".pdf"):
            with open(file_out, "wb") as f:
                pdf_bytes = []
                for thumbnail in list_of_thumbnails:
                    if not os.path.exists(thumbnail):
                        continue
                    if thumbnail.endswith(".png"):
                        # Open the image file and convert it to RGB
                        img = Image.open(thumbnail).convert("RGB")
                        # Convert the Image object to bytes
                        byte_arr = io.BytesIO()
                        img.save(byte_arr, format='PNG')
                        byte_arr = byte_arr.getvalue()
                        pdf_bytes.append(byte_arr)
                f.write(img2pdf.convert(pdf_bytes))

        elif file_out.endswith(".png"):
            if grid is None:
                n = len(list_of_thumbnails)
                grid = [1,1]
                grid[0] = int(np.ceil(np.sqrt(n)))
                grid[1] = int(np.ceil(n / grid[0]))
                grid = tuple(grid)

            images = [Image.open(thumbnail).convert("RGB") for thumbnail in list_of_thumbnails if os.path.exists(thumbnail)]
            widths, heights = zip(*(i.size for i in images))

            # check that all images have the same height and width (within a tolerance)
            mean_width = int(np.round(np.mean(widths)))
            mean_height = int(np.round(np.mean(heights)))

            if not all([np.isclose(width, mean_width, rtol = mean_width * .01) for width in widths]) or not all([np.isclose(height, mean_height, mean_height * .01) for height in heights]):
                # if not, save to pdf instead
                ##TODO add a warning
                self.combine_thumbnail_files(file_out.replace(".png", ".pdf"))
                return

            outline_thickness = int(np.ceil(.005 * (mean_width + mean_height) / 2))

            images_with_outline = []
            for image in images:
                new_image = Image.new("RGB", (image.width + 2 * outline_thickness, image.height + 2 * outline_thickness), "white")
                new_image.paste(image, (outline_thickness, outline_thickness))
                images_with_outline.append(new_image)

            total_width  = mean_width  * grid[0] + outline_thickness * 2 * (grid[0])
            total_height = mean_height * grid[1] + outline_thickness * 2 * (grid[1])

            new_im = Image.new('RGB', (total_width, total_height), color = "white")

            x_offset = 0
            y_offset = 0
            for i, im in enumerate(images_with_outline):
                new_im.paste(im, (x_offset, y_offset))
                x_offset += mean_width + outline_thickness * 2
                if (i + 1) % grid[0] == 0:
                    x_offset = 0
                    y_offset += mean_height + outline_thickness * 2

            new_im.save(file_out)

    def save(self, file:str, grid = None, **kwargs):
        self.thumbnail_file = file
        with tempfile.TemporaryDirectory() as temp_dir:
            for i, thumbnail in enumerate(self.thumbnails):
                thumbnail.save(f'{temp_dir}/thumbnail_{i}.png', **kwargs)

            self.combine_thumbnail_files(file, grid = grid)
