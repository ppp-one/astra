from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
from alpaca.camera import ImageMetadata
from astropy.io import fits
from astropy.wcs.utils import WCS

from astra import Config

CONFIG = Config()


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
        folder = CONFIG.paths.images / date_str
        folder.mkdir(exist_ok=True)
    return folder


def transform_image_to_array(
    image: list[int] | np.ndarray, maxadu: int, image_info: ImageMetadata
) -> np.ndarray:
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
    image: list[int] | np.ndarray,
    image_info: ImageMetadata,
    maxadu: int,
    hdr: fits.Header,
    device_name: str,
    dateobs: datetime,
    folder: str,
    wcs: WCS = None,
) -> Path:
    """Save an image to disk.

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
    image_array = transform_image_to_array(
        image, maxadu=maxadu, image_info=image_info
    )  ## TODO: make more efficient?

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

    # add WCS information
    if wcs:
        hdr.extend(wcs.to_header(), update=True)

    # create FITS HDU
    hdu = fits.PrimaryHDU(image_array, header=hdr)

    # create filename
    filter_name = hdr["FILTER"].replace("'", "")
    if hdr["IMAGETYP"] == "Light Frame":
        filename = f"{device_name}_{filter_name}_{hdr['OBJECT']}_{hdr['EXPTIME']:.3f}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
    elif hdr["IMAGETYP"] in ["Bias Frame", "Dark Frame"]:
        filename = f"{device_name}_{hdr['IMAGETYP']}_{hdr['EXPTIME']:.3f}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
    else:
        filename = f"{device_name}_{filter_name}_{hdr['IMAGETYP']}_{hdr['EXPTIME']:.3f}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"

    filepath = CONFIG.paths.images / folder / filename

    # save FITS file
    hdu.writeto(filepath)

    return filepath
