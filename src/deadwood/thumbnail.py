from PIL import Image
import rasterio
import numpy as np
from ..settings import settings

Image.MAX_IMAGE_PIXELS = None


def calculate_thumbnail(tiff_file_path, thumbnail_file_path, size=(256, 256)):
    """
    Creates a thumbnail from a TIFF file.

    Args:
        tiff_file_path (str): Path to the TIFF file.
        thumbnail_target_path (str): Path to save the thumbnail.
        size (tuple, optional): Size of the thumbnail. Default is (256, 256).

    Returns:
       None

    """
    with Image.open(tiff_file_path) as img:
            # create the thumbnail
        img.thumbnail(size)
        # Create a new image with a white background (or any other color you prefer)
        thumb = Image.new("RGB", size, (255, 255, 255))

        # Calculate position to center the image
        thumb_width, thumb_height = img.size
        offset = ((size[0] - thumb_width) // 2, (size[1] - thumb_height) // 2)

        # Paste the thumbnail image onto the square background
        thumb.paste(img, offset)

        # save the thumbnail
        thumb.save(thumbnail_file_path)

