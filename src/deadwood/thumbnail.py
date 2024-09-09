from PIL import Image

from ..settings import settings

Image.MAX_IMAGE_PIXELS = None


def calculate_thumbnail(tiff_file_path, thumbnail_file_path, thumbnail_size=(256, 256)):
    """
    Creates a thumbnail from a TIFF file.

    Args:
        tiff_file_path (str): Path to the TIFF file.
        thumbnail_target_path (str): Path to save the thumbnail.
        size (tuple, optional): Size of the thumbnail. Default is (256, 256).

    Returns:
       None

    """
    # open the image
    with Image.open(tiff_file_path) as img:
        # Downscale the image early to reduce memory usage
        downscale_factor = img.width // 1000
        print(f"Downscaling image by factor {downscale_factor}")
        new_size = (img.width // downscale_factor, img.height // downscale_factor)
        print(f"Downscaling image from {img.size} to {new_size}")
        img = img.resize(new_size, Image.LANCZOS)

        # Convert to RGB if necessary (e.g., RGBA or CMYK)
        if img.mode in ("RGBA", "LA", "P"):  # Convert if it has transparency or palette
            print(f"Converting {img.mode} to RGB")
            img = img.convert("RGB")

        # Create a thumbnail with the desired size
        img.thumbnail(thumbnail_size, Image.LANCZOS)

        # Save the thumbnail image
        img.save(thumbnail_file_path, format="JPEG")

        print(f"Thumbnail saved to {thumbnail_file_path}")

