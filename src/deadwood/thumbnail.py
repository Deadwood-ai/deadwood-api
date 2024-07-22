from PIL import Image

from ..settings import settings


def calculate_thumbnail(tiff_file_path, thumbnail_target_path, size=(128, 128)):
    """
    Creates a thumbnail from a TIFF file.

    Args:
        tiff_file_path (str): Path to the TIFF file.
        thumbnail_target_path (str): Path to save the thumbnail.
        size (tuple, optional): Size of the thumbnail. Default is (128, 128).

    Returns:
        None

    """
    # open the image
    with Image.open(tiff_file_path) as img:
        # create the thumbnail
        img.thumbnail(size)
        # save the thumbnail
        img.save(thumbnail_target_path)
