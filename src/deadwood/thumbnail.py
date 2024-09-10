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
    # with rasterio.open(tiff_file_path) as src:
    #     # Read the image data
    #     img_array = src.read()  # Read all bands (might be more than RGB)

    #     # Check if the image has more than 3 bands or is not in a format we expect
    #     # You might have to modify this based on the data structure of your image.
    #     if img_array.shape[0] > 3:
    #         print(f"Dataset has {img_array.shape[0]} bands. Using the first three bands for RGB.")
    #         img_array = img_array[:3, :, :]  # Take only the first three bands (R, G, B)

    #     # Ensure the array is of type uint8, as Pillow works best with 8-bit data
    #     img_array = np.moveaxis(img_array, 0, -1)  # Move the bands to the last dimension
    #     img_array = np.clip(img_array, 0, 255)  # Ensure values are in the 0-255 range
    #     img_array = img_array.astype(np.uint8)  # Convert to uint8

    #     # Convert the numpy array to a Pillow Image
    #     img = Image.fromarray(img_array)

    #     # Resize the image to the thumbnail size while maintaining the aspect ratio
    #     img.thumbnail(thumbnail_size, Image.LANCZOS)

    #     # Create a new image with a white background and paste the thumbnail in the center
    #     background = Image.new("RGB", thumbnail_size, (255, 255, 255))
    #     img_w, img_h = img.size
    #     offset = ((thumbnail_size[0] - img_w) // 2, (thumbnail_size[1] - img_h) // 2)
    #     background.paste(img, offset)

    #     # Save the thumbnail image
    #     background.save(thumbnail_file_path, format="JPEG")

    #     print(f"Thumbnail saved to {thumbnail_file_path}")

