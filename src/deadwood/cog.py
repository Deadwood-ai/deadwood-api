from pathlib import Path

from rio_cogeo.cogeo import cog_translate, cog_validate, cog_info
from rio_cogeo.profiles import cog_profiles

def calculate_cog(tiff_file_path, cog_target_path, profile="webp", overviews=None, skip_recreate: bool = False):
    """
    Converts a TIFF file to a Cloud Optimized GeoTIFF (COG) format using the specified profile and configuration.

    Args:
        tiff_file_path (str): Path to the input TIFF file.
        cog_target_path (str): Path where the output COG file will be saved.
        profile (str, optional): COG profile to use. Default is "webp".
                                 Available profiles: "jpeg", "webp", "zstd", "lzw", "deflate", "packbits", "lzma",
                                 "lerc", "lerc_deflate", "lerc_zstd", "raw".
        overviews (int, optional): Decimation level for generating overviews. If not provided, inferred from data size.
        skip_recreate (bool, optional): If True, skips recreating the COG if it already exists. Default is False.

    Returns:
        dict: Information about the generated COG file.

    Raises:
        RuntimeError: If COG validation fails.

    Notes:
        - The function uses the `cog_translate` function from the rio_cogeo library to perform the conversion.
        - The output COG is validated using the `cog_validate` function.
        - If validation fails, a RuntimeError is raised.
        - The function returns information about the COG using the `cog_info` function.

    Example:
        >>> calculate_cog("input.tif", "output.cog.tif", profile="jpeg", overviews=3)

    """
    # check if the COG already exists
    if skip_recreate and Path(cog_target_path).exists():
        return cog_info(cog_target_path)

    # get the output profile
    output_profile = cog_profiles.get(profile)

    # set the GDAL options directly:
    config = dict(
        # GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_NUM_THREADS="2",
        GDAL_TIFF_INTERNAL_MASK=True,
        # GDAL_TIFF_OVR_BLOCKSIZE=f"{blocksize}",
    )

    # run
    cog_translate(
        tiff_file_path,
        cog_target_path,
        output_profile,
        config=config,
        overview_level=overviews, 
        use_cog_driver=True,
    )

    if not validate(cog_target_path):
        # check if the cog is valid
        raise RuntimeError(f"Validation failed for {cog_target_path}")

    # return info
    return cog_info(cog_target_path)


def validate(cog_path):
    """
    Validate a COG file.

    Args:
        cog_path (str): Path to the COG file.

    Returns:
        bool: True if the COG is valid, False otherwise.

    """
    return cog_validate(cog_path)
