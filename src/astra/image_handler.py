from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from alpaca.camera import ImageMetadata
from astropy.io import fits

from astra import CONFIG


def create_image_dir(
    schedule_start_time: datetime = datetime.now(UTC),
    site_long: float = 0,
    user_specified_dir: str = None,
) -> Path:
    """
    Create a directory to store images.

    This function creates a directory to store images. If a user-specified directory is provided, it is used.
    Otherwise, the directory is created in the 'images' folder with a name based on the schedule's beginning
    date (~shifted to local time using site's longitude).

    """

    if user_specified_dir:
        folder = Path(user_specified_dir)
        folder.mkdir(exist_ok=True)
    else:
        date_str = (schedule_start_time + timedelta(hours=site_long / 15)).strftime(
            "%Y%m%d"
        )
        folder = CONFIG.folder_images / date_str
        folder.mkdir(exist_ok=True)
    return folder


def img_transform(img: np.array, maxadu: int, imginfo: ImageMetadata) -> np.array:
    """
    This function takes in a device object, an image object, and a maximum ADU
    value and returns a numpy array of the correct shape for astropy.io.fits.

    Parameters:
        device (AlpacaDevice): A device object that contains the ImageArrayInfo data.
        img (np.array): An image object that contains the image data.
        maxadu (int): The maximum ADU value.

    Returns:
        nda (np.array): A numpy array of the correct shape for astropy.io.fits.
    """

    # Determine the image data type
    if imginfo.ImageElementType == 0 or imginfo.ImageElementType == 1:
        imgDataType = np.uint16
    elif imginfo.ImageElementType == 2:
        if maxadu <= 65535:
            imgDataType = np.uint16  # Required for BZERO & BSCALE to be written
        else:
            imgDataType = np.int32
    elif imginfo.ImageElementType == 3:
        imgDataType = np.float64
    else:
        raise ValueError(f"Unknown ImageElementType: {imginfo.ImageElementType}")

    # Make a numpy array of the correct shape for astropy.io.fits
    if imginfo.Rank == 2:
        nda = np.array(img, dtype=imgDataType).transpose()
    else:
        nda = np.array(img, dtype=imgDataType).transpose(2, 1, 0)

    return nda


def save_image(
    image_array: list[int],
    imginfo: ImageMetadata,
    maxadu: int,
    hdr: fits.Header,
    device_name: str,
    dateobs: datetime,
    folder: str,
    image_sequence_n: int,
    save_config: dict
) -> str:
    """
    Save an image to disk.

    This function retrieves an image from an Alpaca device, transforms it, and saves it to disk in FITS format.
    The filename is generated based on device information and the image's characteristics.

    The FITS header is updated with the 'DATE-OBS' and 'DATE' keywords to record the exposure start time
    and the time when the file was written.

    After saving the image, it is logged, and its file path is returned.

    Parameters:

    Returns:
        str: The file path to the saved image.

    """

    # transform image to numpy array
    img = np.array(image_array)
    nda = img_transform(img, maxadu, imginfo)  ## TODO: make more efficient?

    # update FITS header
    hdr["DATE-OBS"] = (
        dateobs.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC date/time of exposure start",
    )

    date = datetime.now(UTC)
    hdr["DATE"] = (
        date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC date/time when this file was written",
    )

    # create FITS HDU
    hdu = fits.PrimaryHDU(nda, header=hdr)

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
            filename_pattern = '{device}_{filter_clean}_{exptime}_{timestamp}.fits'

        exptime = hdr['EXPTIME']
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
            filename_pattern = '{device}_{imagetype}_{exptime}_{timestamp}.fits'

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
            filename_pattern = '{device}_{imagetype}_{exptime}_{timestamp}.fits'

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
            filename_pattern = '{device}_{imagetype}_{exptime}_{timestamp}.fits'

        # TODO: STX also has flats marked "Dusk" or "Dawn" depending on when the flat is taken
        dusk_dawn = 'Dusk'

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
