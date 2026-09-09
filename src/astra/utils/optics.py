"""
Pure optics math shared across device-based and FITS-header-based code paths.

`paired_devices.py` derives plate scale from live Alpaca `Camera`/`Telescope`
device properties, while `pointer.py` derives it from FITS header metadata
(`XPIXSZ`/`FOCALLEN`). Both then need the same downstream math (plate scale,
field of view, FWHM in pixels), so that math lives here as plain functions
over floats/arrays with no device or FITS dependencies.
"""

from typing import Union

import numpy as np


def plate_scale(
    pixel_size: Union[float, np.ndarray], focal_length: float
) -> np.ndarray:
    """
    Calculate the plate scale in degrees per pixel.

    Parameters:
        pixel_size: Pixel size in meters. Scalar or array (e.g. [x, y]).
        focal_length (float): Focal length in meters.

    Returns:
        Plate scale in degrees per pixel, same shape as `pixel_size`.
    """
    return np.degrees(np.arctan(np.asarray(pixel_size) / focal_length))


def field_of_view(
    sensor_size: Union[float, np.ndarray], focal_length: float
) -> np.ndarray:
    """
    Calculate the field of view spanned by a sensor, without the small-angle
    approximation.

    Parameters:
        sensor_size: Sensor extent in meters. Scalar or array (e.g. [sx, sy]).
        focal_length (float): Focal length in meters.

    Returns:
        Field of view in degrees, same shape as `sensor_size`.
    """
    return 2.0 * np.degrees(np.arctan(np.asarray(sensor_size) / (2.0 * focal_length)))


def fov_from_plate_scale(
    num_pixels: Union[float, np.ndarray], plate_scale_deg_per_pixel: float
) -> np.ndarray:
    """
    Calculate field of view from a per-pixel plate scale (small-angle approximation).

    Parameters:
        num_pixels: Number of pixels along one or more axes.
        plate_scale_deg_per_pixel (float): Plate scale in degrees per pixel.

    Returns:
        Field of view in degrees, same shape as `num_pixels`.
    """
    return np.asarray(num_pixels) * plate_scale_deg_per_pixel


def fwhm_pixels(plate_scale_deg_per_pixel: float, seeing_arcsec: float = 1) -> float:
    """
    Calculate the expected FWHM in pixels for a given atmospheric seeing.

    Parameters:
        plate_scale_deg_per_pixel (float): Plate scale in degrees per pixel.
        seeing_arcsec (float): Atmospheric seeing in arcseconds.

    Returns:
        float: Expected FWHM in pixels.
    """
    plate_scale_arcsec_per_pixel = plate_scale_deg_per_pixel * 3600.0
    return seeing_arcsec / plate_scale_arcsec_per_pixel
