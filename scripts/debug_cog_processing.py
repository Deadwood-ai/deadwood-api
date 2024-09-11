from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

# Define the destination keyword arguments for COG creation
config = dict(
        # GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_NUM_THREADS="2",
        # GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_OVR_BLOCKSIZE="128"
        # CPL_DEBUG="ON"  # Enable debug output for detailed logging

    )

# Define COG profile
cog_profile = cog_profiles.get("jpeg")
# cog_profile = cog_profiles.get("deflate")


# Call cog_translate with the required arguments
cog_translate(
    "/data/archive/bbac672f-749d-4644-a499-1d2021633819_debug-cog.tif",  # Source file
    "/data/cogs/test-process.tif",  # Destination file
    cog_profile,  # COG profile
    config, 
    overview_level=8,
    use_cog_driver=True # Destination keyword arguments
)


cog_translate(
    "/data/archive/bbac672f-749d-4644-a499-1d2021633819_debug-cog.tif",  # Source file
    # "/data/archive/6adbb2d6-573d-43a9-822c-ec64f8ae205a_uavforsat_2017_CFB014_ortho.tif",
    "/data/cogs/test-process.tif",  # Destination file
    cog_profile,  # COG profile
    indexes=(1, 2, 3)  # Only translate the first 3 bands
)




# /usr/local/lib/python3.12/site-packages/rio_cogeo/cogeo.py:241: UserWarning: Nodata/Alpha band will be translated to an internal mask band.
#   warnings.warn(
# Reading input: /data/archive/bbac672f-749d-4644-a499-1d2021633819_debug-cog.tif
#   [####################################]  100%          
# Adding overviews...
# Updating dataset tags...
# Writing output to: /data/cogs/test-process.tif