"""
Astronomical image processing and FITS file management utilities.

This module provides functions for handling astronomical images captured from
observatory cameras. It manages image directory creation, data type conversion,
and FITS file saving with proper headers and metadata.

Key features:
- Automatic directory creation with date-based naming
- Image data type conversion and array reshaping for FITS compatibility
- FITS file saving with comprehensive metadata and WCS support
- Intelligent filename generation based on observation parameters

The module handles various image types including light frames, bias frames,
dark frames, and calibration images, ensuring proper metadata preservation
and file organization for astronomical data processing pipelines.

Example:
    # Create directory and save an astronomical image
    folder = create_image_dir(schedule_start_time, site_longitude)
    filepath = save_image(
        image_data, image_info, maxadu, header,
        camera_name, obs_time, folder_name
    )
"""

from datetime import UTC, datetime, timedelta
from datetime import time as dttime
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
from alpaca.camera import ImageMetadata
from astropy.io import fits
from astropy.wcs.utils import WCS

from astra import Config

CONFIG = Config()


def create_image_dir(
    schedule_start_time: datetime = datetime.now(UTC),
    site_long: float = 0,
    user_specified_dir: Optional[str] = None,
) -> Path:
    """
    Create a directory for storing astronomical images.

    Creates a directory for image storage using either a user-specified path
    or an auto-generated date-based path. The auto-generated path uses the
    local date calculated from the schedule start time and site longitude.

    Parameters:
        schedule_start_time (datetime, optional): Start time of the observing schedule.
            Defaults to current UTC time.
        site_long (float, optional): Site longitude in degrees for local time conversion.
            Defaults to 0.
        user_specified_dir (str | None, optional): Custom directory path. If provided,
            this overrides auto-generation. Defaults to None.

    Returns:
        Path: Path object pointing to the created directory.

    Note:
        Auto-generated directory format is YYYYMMDD based on local date calculated
        as schedule_start_time + (site_long / 15) hours.
    """

    if user_specified_dir:
        folder = Path(user_specified_dir)
        folder.mkdir(parents=True, exist_ok=True)
    else:
        date_str = (schedule_start_time + timedelta(hours=site_long / 15)).strftime(
            "%Y%m%d"
        )
        folder = CONFIG.paths.images / date_str
        folder.mkdir(exist_ok=True)
    return folder


def transform_image_to_array(
    image: Union[List[int], np.ndarray], maxadu: int, image_info: ImageMetadata
) -> np.ndarray:
    """
    Transform raw image data to a FITS-compatible numpy array.

    Converts raw image data to the appropriate data type and shape for FITS files.
    Handles data type selection based on image element type and maximum ADU value,
    and applies necessary array transpositions for FITS conventions.

    Parameters:
        image (list[int] | np.ndarray): Raw image data as list or numpy array.
        maxadu (int): Maximum ADU (Analog-to-Digital Unit) value for the image.
        image_info (ImageMetadata): Metadata containing ImageElementType (0-3) and
            Rank (2 for grayscale, 3 for color).

    Returns:
        np.ndarray: Properly shaped and typed array ready for FITS file creation.
            2D images are transposed, 3D images use transpose(2, 1, 0).

    Raises:
        ValueError: If ImageElementType is not in range 0-3.

    Note:
        ImageElementType mapping: 0,1→uint16; 2→uint16 (≤65535) or int32 (>65535); 3→float64.
        Transpose operations match FITS conventions where first axis = columns, second = rows.
    """
    if not isinstance(image, np.ndarray):
        image = np.array(image)

    # Determine the image data type
    if image_info.ImageElementType == 0 or image_info.ImageElementType == 1:
        imgDataType = np.uint16
    elif image_info.ImageElementType == 2:
        if maxadu <= 65535:
            imgDataType = np.uint16  # Required for BZERO & BSCALE to be written
        else:
            imgDataType = np.int32
    elif image_info.ImageElementType == 3:
        imgDataType = np.float64
    else:
        raise ValueError(f"Unknown ImageElementType: {image_info.ImageElementType}")

    # Make a numpy array of the correct shape for astropy.io.fits
    if image_info.Rank == 2:
        image_array = np.array(image, dtype=imgDataType).transpose()
    else:
        image_array = np.array(image, dtype=imgDataType).transpose(2, 1, 0)

    return image_array


def save_image(
    image: Union[List[int], np.ndarray],
    image_info: ImageMetadata,
    maxadu: int,
    hdr: fits.Header,
    device_name: str,
    dateobs: datetime,
    image_sequence_n: int,
    folder: str,
    save_config: dict,
    wcs: Optional[WCS] = None
) -> Path:
    """
    Save an astronomical image as a FITS file with proper headers and filename.

    Transforms raw image data, updates FITS headers with observation metadata,
    optionally adds WCS information, and saves as a FITS file with an automatically
    generated filename based on image properties.

    Parameters:
        image (list[int] | np.ndarray): Raw image data to save.
        image_info (ImageMetadata): Image metadata for data type determination.
        maxadu (int): Maximum ADU value for the image.
        hdr (fits.Header): FITS header containing FILTER, IMAGETYP, OBJECT, EXPTIME.
        device_name (str): Camera/device name for filename generation.
        dateobs (datetime): UTC datetime when exposure started.
        image_sequence_n (int): number in sequence of image, for filename formatting.
        folder (str): Subfolder name within the images directory.
        save_config: dict: dictionnary of configs for save format.
        wcs (WCS, optional): World Coordinate System information. Defaults to None.
        site_long (float): longitude of site, for dusk/dawn check for filename

    Returns:
        Path: Path to the saved FITS file.

    Note:
        Filename formats, either:
        - Light frames: "{device}_{filter}_{object}_{exptime}_{timestamp}.fits"
        - Bias/Dark: "{device}_{imagetype}_{exptime}_{timestamp}.fits"
        - Other: "{device}_{filter}_{imagetype}_{exptime}_{timestamp}.fits"
        or uses format defined in config.

        Headers automatically updated with DATE-OBS, DATE, and WCS (if provided).
    """

    # transform image to numpy array
    image_array = transform_image_to_array(
        image, maxadu=maxadu, image_info=image_info
    )  ## TODO: make more efficient?

    # update FITS header
    hdr["DATE-OBS"] = (
        dateobs.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC datetime file written",
    )

    date = datetime.now(UTC)
    hdr["DATE"] = (
        date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC datetime start of exposure",
    )

    # add WCS information
    if wcs:
        hdr.extend(wcs.to_header(), update=True)

    # create FITS HDU
    hdu = fits.PrimaryHDU(image_array, header=hdr)

    # create filename
    # create filename
    filter_name_clean = hdr['FILTER'].replace("'", '')
    filter_name = hdr['FILTER']

    exptime = hdr['EXPTIME']

    timestamp = date.strftime('%Y%m%d_%H%M%S.%f')[:-3]
    timestamp_date = date.strftime('%Y%m%d')
    timestamp_time = date.strftime('%H%M%S')
    image_type = hdr['IMAGETYPE']

    if image_type == 'Light Frame':
        if 'raw_filename_pattern' in save_config:
            # read filename config from config file
            filename_pattern = save_config['raw_filename_pattern']
        else:
            # use default filename
            filename_pattern = '{device}_{filter_clean}_{exptime:.3f}_{timestamp}.fits'

        obj_name = hdr['OBJECT']

        filename = filename_pattern.format(device=device_name,
                                           filter=filter_name,
                                           filter_clean=filter_name_clean,
                                           object_name=obj_name,
                                           exptime=exptime,
                                           imagetype=image_type,
                                           timestamp=timestamp,
                                           timestamp_date=timestamp_date,
                                           timestamp_time=timestamp_time,
                                           sequence=image_sequence_n)
    elif image_type == ['Bias Frame']:
        if 'bias_filename_pattern' in save_config:
            # read filename config from config file
            filename_pattern = save_config['bias_filename_pattern']
        else:
            # use default filename
            filename_pattern = '{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits'

        filename = filename_pattern.format(device=device_name,
                                           filter=filter_name,
                                           exptime=exptime,
                                           timestamp=timestamp,
                                           timestamp_date=timestamp_date,
                                           timestamp_time=timestamp_time,
                                           sequence=image_sequence_n)
    elif image_type == ['Dark Frame']:
        if 'dark_filename_pattern' in save_config:
            # read filename config from config file
            filename_pattern = save_config['dark_filename_pattern']
        else:
            # use default filename
            filename_pattern = '{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits'

        filename = filename_pattern.format(device=device_name,
                                           exptime=exptime,
                                           timestamp=timestamp,
                                           timestamp_date=timestamp_date,
                                           timestamp_time=timestamp_time,
                                           sequence=image_sequence_n)
    else:
        # Flat
        if 'dark_filename_pattern' in save_config:
            # read filename config from config file
            filename_pattern = save_config['flat_filename_pattern']
        else:
            # use default filename
            filename_pattern = '{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits'
            
        # STX has flats marked "Dusk" or "Dawn" depending on when the flat is taken, compute local time of image
        # from UTC to check if it's a dusk or dawn flat
        dusk_dawn = 'DuskDawn'
        if ("utc_offset_hours" in save_config) and (date.time() + timedelta(hours=save_config["utc_offset_hours"]).time() > dttime(hour=12)):
            dusk_dawn = 'Dusk'
        else:
            dusk_dawn = 'Dawn'
            
        filename = filename_pattern.format(device=device_name,
                                           exptime=exptime,
                                           timestamp=timestamp,
                                           timestamp_date=timestamp_date,
                                           timestamp_time=timestamp_time,
                                           sequence=image_sequence_n,
                                           duskdawn=dusk_dawn)

    filepath = CONFIG.folder_images / folder / filename
    # Make directory if not existing
    filepath.parent.mkdir(exists_ok=True, parents=True)

    # save FITS file
    hdu.writeto(filepath)

    return filepath
