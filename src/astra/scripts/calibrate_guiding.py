"""
Script to calibrate the pulseGuide command

This is done by pointing the telescope 1 hour east of the meridian
and 0 deg Declination. The telescope is then pulseGuided
around the sky in a cross pattern, taking an image at each
location. DONUTS is then used to measure the shift and
determine the camera orientation and pulseGuide conversion
factors
"""

import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Tuple

import astropy.io.fits as fits
import numpy as np
from alpaca.camera import Camera
from alpaca.dome import Dome
from alpaca.telescope import GuideDirections, Telescope
from donuts import Donuts
from donuts.image import Image
from scipy.ndimage import median_filter

from astra import Config

logging.basicConfig(level=logging.INFO)

CONFIG = Config()
OBSERVATORY_CONFIG = CONFIG.load_observatory_config()

GUIDING_CALLIBRATION_DIR = (
    CONFIG.paths.images / "calibrate_guiding" / datetime.now().strftime("%Y%m%d")
)


class CustomImageClass(Image):
    def preconstruct_hook(self):
        clean = median_filter(self.raw_image, size=4, mode="mirror")
        band_corr = np.median(clean, axis=1).reshape(-1, 1)
        band_clean = clean - band_corr
        self.raw_image = band_clean


TELESCOPE_IP = OBSERVATORY_CONFIG["Telescope"][0]["ip"]
TELESCOPE_DEVICE_NUMBER = OBSERVATORY_CONFIG["Telescope"][0]["device_number"]

CAMERA_IP = OBSERVATORY_CONFIG["Camera"][0]["ip"]
CAMERA_DEVICE_NUMBER = OBSERVATORY_CONFIG["Camera"][0]["device_number"]

DOME_IP = OBSERVATORY_CONFIG["Dome"][0]["ip"]
DOME_DEVICE_NUMBER = OBSERVATORY_CONFIG["Dome"][0]["device_number"]


def connect_telescope():
    """A reusable way to connect to ACP telescope"""
    logging.info(
        f"Connecting to telescope at {TELESCOPE_IP} "
        f"with device number {TELESCOPE_DEVICE_NUMBER}..."
    )
    my_telescope = Telescope(TELESCOPE_IP, TELESCOPE_DEVICE_NUMBER)
    try:
        my_telescope.Connected = True
        TELESCOPE_READY = my_telescope.Connected
        my_telescope.Unpark()
        my_telescope.Tracking = True
        logging.info("Telescope connected")
    except Exception as exc:
        logging.warning(f"Cannot connect to telescope {exc}")
        TELESCOPE_READY = False
    return my_telescope, TELESCOPE_READY


def connect_camera():
    """A reusable way of checking camera connection

    The camera needs treated slightly differently. We
    have to try connecting before we can tell if
    connected or not. Annoying!
    """
    logging.info(
        f"Connecting to camera at {CAMERA_IP} with device number {CAMERA_DEVICE_NUMBER}..."
    )
    my_camera = Camera(CAMERA_IP, CAMERA_DEVICE_NUMBER)
    try:
        my_camera.Connected = True
        CAMERA_READY = True
        logging.info("Camera connected")
    except AttributeError:
        logging.warning("Cannot connect to camera.")
        CAMERA_READY = False
    return my_camera, CAMERA_READY


def connect_dome():
    """A reusable way of checking dome connection

    The dome needs treated slightly differently. We
    have to try connecting before we can tell if
    connected or not. Annoying!
    """
    logging.info(
        f"Connecting to dome at {DOME_IP} with device number {DOME_DEVICE_NUMBER}..."
    )
    my_dome = Dome(DOME_IP, DOME_DEVICE_NUMBER)
    try:
        my_dome.Connected = True
        DOME_READY = True
        logging.info("Dome connected")
    except AttributeError:
        logging.warning("Cannot connect to dome.")
        DOME_READY = False
    return my_dome, DOME_READY


def print_yaml_item(key, value, indent=0):
    """Prints a key-value pair in YAML format with proper indentation."""
    indentation = " " * indent
    if isinstance(value, dict):
        print(f"{indentation}{key}:")
        for subkey, subvalue in value.items():
            print_yaml_item(subkey, subvalue, indent + 2)
    else:
        if isinstance(value, str):
            print(f"{indentation}{key}: '{value}'")
        else:
            print(f"{indentation}{key}: {value}")


def open_dome(dome: Dome):
    logging.info("Opening dome...")
    dome.OpenShutter()
    while True:
        if dome.ShutterStatus.value == 0:
            logging.info("The dome is fully opened.")
            break
        elif dome.ShutterStatus.value == 1:
            logging.debug("The dome is opening...")
        time.sleep(1)


def slew_telescope_one_hour_east_of_sidereal_meridian(telescope: Telescope):
    local_sidereal_time = telescope.SiderealTime
    target_right_ascension = local_sidereal_time - 1

    logging.info(f"Local sidereal time: {local_sidereal_time:.2f} hours")
    logging.info(
        f"Slewing one hour east to coordinates: RA = {target_right_ascension:.2f} hours, "
        f"Dec = {0} degrees..."
    )

    telescope.SlewToCoordinatesAsync(
        RightAscension=local_sidereal_time - 1, Declination=0
    )
    while telescope.Slewing:
        logging.debug("Slewing...")
        logging.debug("RA:", telescope.RightAscension)
        logging.debug("Dec:", telescope.Declination)
        logging.debug("Alt:", telescope.Altitude)
        logging.debug("Az:", telescope.Azimuth)
        time.sleep(1)


def perform_exposure(
    camera_object: Camera, image_path: Path, exptime: float = 5, t_settle=10
):
    """
    Take an image with MaxImDL
    """
    logging.info(f"Waiting {t_settle} s to settle...")
    time.sleep(t_settle)

    logging.info("Taking image...")
    dateobs = datetime.now(UTC)
    maxadu = camera_object.MaxADU

    camera_object.StartExposure(Duration=exptime, Light=True)

    while not camera_object.ImageReady:
        time.sleep(0.1)
    logging.info("Image ready...")

    hdr = fits.Header()
    hdr["FILTER"] = ("none", "Filter name")
    hdr["EXPTIME"] = (exptime, "Exposure time (s)")
    hdr["IMAGETYP"] = ("Light", "Image type")

    save_image(camera_object, hdr, dateobs, maxadu, image_path)

    logging.info(f"{image_path} saved...")


def save_image(
    device: Camera, hdr: fits.Header, dateobs: datetime, maxadu: int, filename: Path
):
    """
    Save an image to disk.

    This function retrieves an image from an Alpaca device, transforms it, and saves it to disk in FITS format.
    The filename is generated based on device information and the image's characteristics.

    The FITS header is updated with the 'DATE-OBS' and 'DATE' keywords to record the exposure start time
    and the time when the file was written.

    After saving the image, it is logged, and its file path is returned.

    Parameters:
        device (AlpacaDevice): The camera from which to retrieve the image.
        hdr (fits.Header): The FITS header associated with the image.
        dateobs (datetime): The UTC date and time of exposure start.
        t0 (datetime): The starting time of the image acquisition.
        maxadu (int): The maximum analog-to-digital unit value for the image.
        folder (str): The folder where the image will be saved.

    Returns:
        str: The file path to the saved image.

    """
    if not GUIDING_CALLIBRATION_DIR.exists():
        logging.info(f"Creating directory: {GUIDING_CALLIBRATION_DIR}")
        GUIDING_CALLIBRATION_DIR.mkdir(parents=True)
        logging.info("Directory created")

    arr = device.ImageArray

    img = np.array(arr)

    nda = transform_image_to_array(device, img, maxadu)  ## TODO: make more efficient?

    hdr["DATE-OBS"] = (
        dateobs.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC date/time of exposure start",
    )

    date = datetime.now(UTC)
    hdr["DATE"] = (
        date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "UTC date/time when this file was written",
    )

    hdu = fits.PrimaryHDU(nda, header=hdr)

    hdu.writeto(GUIDING_CALLIBRATION_DIR / filename.name)


def transform_image_to_array(
    device: Camera, img: np.ndarray, maxadu: int
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

    image_info = device.ImageArrayInfo

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

    # Make a numpy array of he correct shape for astropy.io.fits
    if image_info.Rank == 2:
        nda = np.array(img, dtype=imgDataType).transpose()
    else:
        nda = np.array(img, dtype=imgDataType).transpose(2, 1, 0)

    return nda


def pulseGuide(scope: Telescope, direction_int, duration):
    """
    Move the telescope along a given direction
    for the specified amount of time
    """
    logging.info(f"Pulse guiding {direction_int} for {duration} ms")

    if direction_int == 0:
        direction = GuideDirections.guideNorth
    elif direction_int == 1:
        direction = GuideDirections.guideSouth
    elif direction_int == 2:
        direction = GuideDirections.guideEast
    elif direction_int == 3:
        direction = GuideDirections.guideWest
    else:
        logging.error("Invalid direction")

    logging.info(f"Pulse guiding {direction} for {duration} ms")

    scope.PulseGuide(direction, duration)
    while scope.IsPulseGuiding:
        logging.debug("Pulse guiding...")
        time.sleep(0.1)

    while scope.Slewing:
        logging.debug("Slewing...")
        time.sleep(0.1)

    ra = (scope.RightAscension / 24) * 360
    dec = scope.Declination
    logging.info(f"RA: {ra:.8f} deg, DEC: {dec:.8f} deg")


def determineShiftDirectionMagnitude(shft):
    """
    Take a donuts shift object and work out
    the direction of the shift and the distance
    """
    sx = shft.x.value
    sy = shft.y.value
    if abs(sx) > abs(sy):
        if sx > 0:
            direction = "-x"
        else:
            direction = "+x"
        magnitude = abs(sx)
    else:
        if sy > 0:
            direction = "-y"
        else:
            direction = "+y"
        magnitude = abs(sy)
    return direction, magnitude


def newFilename(direction, pulse_time, image_id) -> Tuple[Path, int]:
    """
    Generate new FITS image name
    """
    filename = "step_{:03d}_d{}_{}ms.fits".format(image_id, direction, pulse_time)

    filepath = GUIDING_CALLIBRATION_DIR / filename

    image_id += 1
    return filepath, image_id


if __name__ == "__main__":
    pulse_time = 5000

    # set up objects to hold calib info
    DIRECTION_STORE = defaultdict(list)
    SCALE_STORE = defaultdict(list)
    image_id = 0

    # connect to hardware
    my_telescope, TELESCOPE_READY = connect_telescope()
    my_camera, CAMERA_READY = connect_camera()
    my_dome, DOME_READY = connect_dome()

    open_dome(my_dome)

    # turn tracking on
    my_telescope.Tracking = True
    time.sleep(5)

    slew_telescope_one_hour_east_of_sidereal_meridian(my_telescope)

    # start the calibration run
    logging.info("Starting calibration run...")
    ref_image, image_id = newFilename("R", 0, image_id)
    perform_exposure(my_camera, ref_image)

    # Set up donuts with this reference point. Assume default params for now
    donuts_ref = Donuts(
        ref_image,
        normalise=False,
        subtract_bkg=True,
        downweight_edges=False,
        image_class=CustomImageClass,
    )

    # loop over 10 cycles of the U, D, L, R nudging to determine
    # the scale and orientation of the camera
    # number_of_cycles = 10
    number_of_cycles = 1
    for i in range(number_of_cycles):
        # loop over 4 directions, 0 to 3
        logging.info(f"Starting cycle {i} of {number_of_cycles}.")
        for j in range(4):
            # pulse guide the telescope
            pulseGuide(my_telescope, j, pulse_time)

            # take an image
            check, image_id = newFilename(j, pulse_time, image_id)

            perform_exposure(my_camera, check)

            # now measure the shift
            shift = donuts_ref.measure_shift(check)
            direction, magnitude = determineShiftDirectionMagnitude(shift)

            logging.info(f"Shift in direction {direction} of {magnitude} pixels")
            DIRECTION_STORE[j].append(direction)
            SCALE_STORE[j].append(magnitude)

            # now update the reference image
            donuts_ref = Donuts(
                check,
                normalise=False,
                subtract_bkg=True,
                downweight_edges=False,
                image_class=CustomImageClass,
            )

    # now do some analysis on the run from above
    # check that the directions are the same every time for each orientation
    config = {
        "PIX2TIME": {"+x": None, "-x": None, "+y": None, "-y": None},
        "RA_AXIS": None,
        "DIRECTIONS": {"+x": None, "-x": None, "+y": None, "-y": None},
    }
    logging.info(f"Gathered directions {DIRECTION_STORE}")
    logging.info(f"Gathered scales {SCALE_STORE}")

    logging.info("Checking directions...")

    for i, dir in enumerate(DIRECTION_STORE):
        if len(set(DIRECTION_STORE[dir])) != 1:
            raise ValueError(
                "DIRECTION_STORE should all be in the same direction for each key. "
                f"Key {i} has {DIRECTION_STORE[dir]}."
            )

        xy = DIRECTION_STORE[dir][0]
        if dir == 0:
            direction = "North"
        elif dir == 1:
            direction = "South"
        elif dir == 2:
            direction = "East"
            if xy == "+x" or xy == "-x":
                config["RA_AXIS"] = "x"
            else:
                config["RA_AXIS"] = "y"
        elif dir == 3:
            direction = "West"
        else:
            direction = "Invalid direction"
            logging.warning("Invalid direction")

        config["PIX2TIME"][xy] = pulse_time / np.average(SCALE_STORE[dir])
        config["DIRECTIONS"][xy] = direction

    logging.info("Directions are consistent")

    logging.info("Printing Configuration...")
    for key, value in config.items():
        print_yaml_item(key, value)
