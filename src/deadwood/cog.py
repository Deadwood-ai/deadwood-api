from rio_cogeo.cogeo import cog_translate, cog_validate, cog_info
from rio_cogeo.profiles import cog_profiles

def calculate_cog(tiff_file_path, cog_target_path, profile="webp", overviews=None):
    # vorgefertigte Profile sind: 
    # "jpeg", "webp", "zstd", "lzw", "deflate", "packbits", "lzma", "lerc", "lerc_deflate", "lerc_zstd", "raw"
    output_profile = cog_profiles.get(profile)

    # zum nachvollziehen:
    print(output_profile)

    # optionales feintuning der settings:
    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
        # GDAL_TIFF_OVR_BLOCKSIZE=f"{blocksize}",
    )

    cog_translate(tiff_file_path,
    cog_target_path,
    output_profile,
    config=config,
    overview_level=overviews, # OGEO overview (decimation) level. By default, inferred from data size.
    #overview_resampling=, # RasterIO Resampling algorithm for overviews
    #zoom_level_strategy=, # Strategy to determine zoom level (same as in GDAL 3.2).
        # Used only when either "web_optimized" argument is True, or `tms` is not None.
        # LOWER will select the zoom level immediately below the theoretical computed non-integral zoom level, leading to subsampling.
        # On the contrary, UPPER will select the immediately above zoom level, leading to oversampling.
        # Defaults to AUTO which selects the closest zoom level.
    #zoom_level=, # Zoom level number (starting at 0 for coarsest zoom level).
        # If this option is specified, `--zoom-level-strategy` is ignored.
        # In any case, it is used only when either "web_optimized" argument is True, or `tms` is not None.
    #aligned_levels=, #Number of overview levels for which GeoTIFF tile and tiles defined in the tiling scheme match.
        # Used only when either "web_optimized" argument is True, or `tms` is not None.
        # Default is to use the maximum overview levels. Note: GDAL use number of resolution levels instead of overview levels.
    #resampling=, #Warp Resampling algorithm.
    #allow_intermediate_compression=, # Allow intermediate file compression to reduce memory/disk footprint.
        # Note: This could reduce the speed of the process.
    #additional_cog_metadata=, #  Additional dataset metadata to add to the COG.
    use_cog_driver=True, # Use GDAL COG driver if set to True. COG driver is available starting with GDAL 3.1.
    )

    if not cog_validate(cog_target_path):
        # check if the cog is valid
        raise RuntimeError(f"Validation failed for {cog_target_path}")

    # return info
    return cog_info(cog_target_path)

