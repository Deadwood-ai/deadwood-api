import rasterio
from rasterio.enums import Resampling
import numpy as np
from PIL import Image
from ..logger import logger


def calculate_thumbnail(tiff_file_path: str, thumbnail_file_path: str, size=(256, 256)):
	"""
	Creates a thumbnail from a GeoTIFF file using rasterio.

	Args:
	    tiff_file_path (str): Path to the TIFF file
	    thumbnail_file_path (str): Path to save the thumbnail
	    size (tuple): Target size for thumbnail (width, height). Default is (256, 256)

	Returns:
	    None
	"""
	try:
		with rasterio.open(tiff_file_path) as src:
			# Calculate scaling factor
			scale_factor = min(size[0] / src.width, size[1] / src.height)

			# Calculate new dimensions (maintaining aspect ratio)
			out_width = int(src.width * scale_factor)
			out_height = int(src.height * scale_factor)

			# Read the data at the new resolution
			data = src.read(out_shape=(src.count, out_height, out_width), resampling=Resampling.lanczos)

			# Normalize the data to 0-255 range for each band
			rgb_data = []
			for band in data:
				band_min = band.min()
				band_max = band.max()
				# Check if band has valid range to avoid division by zero
				if band_max == band_min:
					band_norm = np.zeros_like(band, dtype=np.uint8)
				else:
					band_norm = ((band - band_min) * (255.0 / (band_max - band_min))).astype(np.uint8)
				rgb_data.append(band_norm)

			# Stack bands and transpose to correct shape for PIL
			rgb_array = np.dstack(rgb_data[:3])  # Only use first 3 bands (RGB)

			# Create PIL image
			img = Image.fromarray(rgb_array)

			# Create a new image with white background
			thumb = Image.new('RGB', size, (255, 255, 255))

			# Calculate position to center the image
			offset = ((size[0] - out_width) // 2, (size[1] - out_height) // 2)

			# Paste the thumbnail onto the white background
			thumb.paste(img, offset)

			# Save the thumbnail
			thumb.save(thumbnail_file_path, 'JPEG', quality=85)

	except Exception as e:
		logger.error(
			f'Error creating thumbnail: {str(e)}',
			extra={'tiff_file': tiff_file_path, 'thumbnail_file': thumbnail_file_path},
		)
		raise
