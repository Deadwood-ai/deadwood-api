from pathlib import Path
import subprocess

from rio_cogeo.cogeo import cog_translate, cog_validate, cog_info
from rio_cogeo.profiles import cog_profiles

def calculate_cog(tiff_file_path: str, cog_target_path: str, profile="webp", overviews=None, quality=75, skip_recreate: bool = False):
    """Function to initiate the cog calculation process.

    Args:
        tiff_file_path (str): Path to the geotiff file to be processed
        cog_target_path (str): Path for the finished cog file to save to
        profile (str, optional): Optional base compression profile for the cog calculation. Defaults to "webp".
        overviews (int, optional): Optional overview number. Defaults to None.
        quality (int, optional): Optional overall quality setting (between 0 and 100). Defaults to 75.
        skip_recreate (bool, optional): Option to skip recreate. Defaults to False.

    Returns:
        Function: Returns the cog calculation function the initialized settings
    """
    # we use the gdal
    #return _gdal_calculate_cog(tiff_file_path, cog_target_path, compress=profile, overviews=overviews, quality=quality, skip_recreate=skip_recreate)
    return _rio_calculate_cog(tiff_file_path, cog_target_path, profile=profile, overviews=overviews, quality=quality, skip_recreate=skip_recreate)


def _gdal_calculate_cog(tiff_file_path: str, cog_target_path: str, compress="jpeg", overviews=None, quality=75, skip_recreate: bool = False):
    """Function to calculate a Cloud Optimized Geotiff (cog) from a geotiff using gdal.

    Args:
        tiff_file_path (str): Path to the geotiff file to be processed
        cog_target_path (str): Path for the finished cog file to save to
        compress (str, optional): Optional base compression profile for the cog calculation. Defaults to "jpeg".
        overviews (_type_, optional): Optional overview number. Defaults to None.
        quality (int, optional): Optional overall quality setting (between 0 and 100). Defaults to 75.
        skip_recreate (bool, optional): Option to skip recreate. Defaults to False.
    
    Returns:
        Info: Returns general infos and validation for the calculated Cloud Optimized Geotiff (using the rio_cogeo package)
    """
        # check if the COG already exists
    if skip_recreate and Path(cog_target_path).exists():
        return cog_info(cog_target_path)
    
    # build the gdal command
    cmd_translate = [
        "gdal_translate",
        tiff_file_path,
        cog_target_path,
        "-of", "COG",
        "-co", f"COMPRESS={compress.upper()}",
        "-co", "BLOCKSIZE=512",
        "-co", f"QUALITY={quality}",
        "-co", "OVERVIEW_COMPRESS=JPEG",
        "-co", "OVERVIEWS=IGNORE_EXISTING",
        "--config", "GDAL_TIFF_INTERNAL_MASK", "TRUE",
        
    ]
    if overviews is not None:
        cmd_translate.extend(["-co", f"OVERVIEW_COUNT={overviews}"])

    # apply
    subprocess.run(cmd_translate)

    return cog_info(cog_target_path)


def _rio_calculate_cog(tiff_file_path, cog_target_path, profile="webp", overviews=None, quality=75, skip_recreate: bool = False):
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

    if quality is not None:
        output_profile.update(dict(quality=quality))

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
