"""Utility functions for astronomical observations and data processing.

This module provides essential utility functions for the Astra observatory system,
including time conversions, coordinate transformations, database queries, and
telescope error handling. Functions support FITS header processing, flat field
observations, and SPECULOOS telescope operations.

Key capabilities:
    - Julian Day and astronomical time system conversions
    - FITS header time calculations with light travel corrections
    - Solar position analysis for flat field timing
    - Database queries for astronomical catalogs
    - SPECULOOS telescope error checking and acknowledgement
"""

import math
import time
from datetime import datetime
from typing import Any, Tuple

import astropy.units as u
import numpy as np
import pandas as pd
from astropy.coordinates import AltAz, Angle, SkyCoord, get_sun
from astropy.stats import SigmaClip, sigma_clipped_stats
from astropy.time import Time
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage


class CustomImageClass(Image):
    """Enhanced image processing class with background subtraction and cleaning."""

    def preconstruct_hook(self) -> None:
        """
        Apply image preprocessing before Donuts star detection.

        Performs background subtraction, noise reduction, and systematic
        correction to improve star detection reliability.
        """
        # if greater than 1Kx1K, crop to 1Kx1K for speed
        shapex, shapey = self.raw_image.shape
        if shapex > 2048 and shapey > 2048:
            self.raw_image = self.raw_image[
                shapex // 2 - 1024 : shapex // 2 + 1024,
                shapey // 2 - 1024 : shapey // 2 + 1024,
            ]

        self.raw_image = clean_image(self.raw_image)
        mean, median, std = sigma_clipped_stats(self.raw_image, sigma=3.0)

        # remove noise floor
        self.raw_image -= median + 7 * std
        self.raw_image[self.raw_image < 0] = 0


## for final fits header
def interpolate_dfs(index: np.ndarray, *data: pd.DataFrame) -> pd.DataFrame:
    """Interpolate multiple pandas DataFrames onto a common index.

    Merges and interpolates multiple DataFrames using a specified index array,
    commonly used for wavelength-dependent data processing in spectroscopy.

    Args:
        index (np.ndarray): 1D array to interpolate data onto (e.g., wavelength grid).
        *data (pd.DataFrame): Variable number of DataFrames to interpolate.

    Returns:
        pd.DataFrame: Combined DataFrame with all data interpolated onto the common index.
    """
    df = pd.DataFrame({"tmp": index}, index=index)
    for dat in data:
        dat = dat[~dat.index.duplicated(keep="first")]
        df = pd.concat([df, dat], axis=1)
    df = df.sort_index()
    df = df.interpolate(method="index", axis=0).reindex(index)
    df = df.drop(labels="tmp", axis=1)

    return df


def __to_format(jd: float, fmt: str) -> float:
    """Convert Julian Day to specified time format.

    Internal function for converting Julian Day values to different astronomical
    time formats like Modified Julian Day or Reduced Julian Day.

    Args:
        jd (float): Julian Day value to convert.
        fmt (str): Target format ('jd', 'mjd', 'rjd').

    Returns:
        float: Converted time value in specified format.

    Raises:
        ValueError: If format string is not recognized.
    """
    if fmt.lower() == "jd":
        return jd
    elif fmt.lower() == "mjd":
        return jd - 2400000.5
    elif fmt.lower() == "rjd":
        return jd - 2400000
    else:
        raise ValueError("Invalid Format")


def to_jd(dt: datetime, fmt: str = "jd") -> float:
    """Convert datetime object to Julian Day using standard algorithm.

    Converts Python datetime to Julian Day format using the algorithm from
    Wikipedia. Supports conversion to various Julian Day formats.

    Args:
        dt (datetime): Datetime object to convert.
        fmt (str): Output format ('jd', 'mjd', 'rjd'). Defaults to 'jd'.

    Returns:
        float: Julian Day value in specified format.
    """
    a = math.floor((14 - dt.month) / 12)
    y = dt.year + 4800 - a
    m = dt.month + 12 * a - 3

    jdn = (
        dt.day
        + math.floor((153 * m + 2) / 5)
        + 365 * y
        + math.floor(y / 4)
        - math.floor(y / 100)
        + math.floor(y / 400)
        - 32045
    )

    jd = (
        jdn
        + (dt.hour - 12) / 24
        + dt.minute / 1440
        + dt.second / 86400
        + dt.microsecond / 86400000000
    )

    return __to_format(jd, fmt)


def getLightTravelTimes(target: SkyCoord, time_to_correct: Time) -> Tuple[Time, Time]:
    """Calculate light travel times to heliocentric and barycentric frames.

    Computes corrections for light travel time from Earth to the solar system
    barycenter and heliocenter, essential for precise timing in astronomy.

    Args:
        target (SkyCoord): Target celestial coordinates.
        time_to_correct (Time): Observation time requiring correction.
            Must be initialized with an EarthLocation.

    Returns:
        Tuple[Time, Time]: Light travel times as (barycentric, heliocentric).
    """

    ltt_bary = time_to_correct.light_travel_time(target)
    ltt_helio = time_to_correct.light_travel_time(target, "heliocentric")
    return ltt_bary, ltt_helio


def time_conversion(
    jd: float, location: Any, target: SkyCoord
) -> Tuple[float, float, float, str]:
    """Convert time to various astronomical reference frames.

    Transforms Julian Day to heliocentric and barycentric systems, calculates
    local sidereal time and hour angle for astronomical observations.

    Args:
        jd (float): Julian Day to convert.
        location (EarthLocation): Observer's geographic location.
        target (SkyCoord): Target celestial coordinates.

    Returns:
        Tuple[float, float, float, str]: Converted times as
            (hjd, bjd, lst_seconds, hour_angle_string).
    """

    time_inp = Time(jd, format="jd", scale="utc", location=location)

    ltt_bary, ltt_helio = getLightTravelTimes(target, time_inp)

    hjd = (time_inp + ltt_helio).value
    bjd = (time_inp.tdb + ltt_bary).value
    lst = time_inp.sidereal_time("mean")
    lstsec = lst.hour * 3600
    ha = Angle(((((lst - target.ra).hour + 12) % 24) - 12) * u.hourangle).to_string(
        unit=u.hourangle, sep=" ", pad=True
    )

    return hjd, bjd, lstsec, ha


## for flat fielding
def is_sun_rising(obs_location: Any) -> Tuple[bool, bool, AltAz]:
    """Determine solar motion and flat field observation readiness.

    Analyzes sun position and movement to determine if conditions are suitable
    for flat field calibration observations, which require specific twilight conditions.

    Args:
        obs_location (EarthLocation): Observer's geographic location.

    Returns:
        Tuple[bool, bool, AltAz]: Solar status as (rising, flat_ready, position):
            - rising: True if sun is rising, False if setting
            - flat_ready: True if optimal for flats (sun altitude -12° to -1°)
            - position: Current sun position in alt-az coordinates
    """
    # sun's position now
    obs_time0 = Time.now()
    sun_position0 = get_sun(obs_time0)
    sun_altaz0 = sun_position0.transform_to(
        AltAz(obstime=obs_time0, location=obs_location)
    )

    # sun's position in 5 minutes
    obs_time1 = obs_time0 + 5 * u.minute
    sun_position1 = get_sun(obs_time1)
    sun_altaz1 = sun_position1.transform_to(
        AltAz(obstime=obs_time1, location=obs_location)
    )

    # determine if sun is moving up or down by looking at gradient
    sun_altaz_grad = (sun_altaz1.alt.degree - sun_altaz0.alt.degree) / (
        obs_time1 - obs_time0
    ).sec

    sun_rising = None
    if sun_altaz_grad > 0:
        sun_rising = True
    else:
        sun_rising = False

    flat_ready = False

    if sun_altaz0.alt.deg > -12 and sun_altaz0.alt.deg < -1:
        flat_ready = True

    return sun_rising, flat_ready, sun_altaz0


def clean_image(data: np.ndarray) -> np.ndarray:
    """
    Clean an image by subtracting the background.

    Parameters:
        data (np.ndarray): The 2D image data.

    Returns:
        np.ndarray: The background-subtracted image.
    """

    sigma_clip = SigmaClip(sigma=3.0)
    bkg_estimator = MedianBackground()

    # Convert to float32, handling both regular and masked arrays
    data = data.astype(np.float32)
    if np.ma.isMaskedArray(data):
        data = data.filled(fill_value=np.nan)

    bkg = Background2D(
        data,
        (32, 32),
        filter_size=(3, 3),
        sigma_clip=sigma_clip,
        bkg_estimator=bkg_estimator,  # type: ignore
    )

    bkg_clean = data - bkg.background

    med_clean = ndimage.median_filter(
        bkg_clean, size=5, mode="mirror"
    )  # slow but needed

    # add minimum back to avoid negative values
    med_clean += np.abs(np.nanmin(med_clean))

    return med_clean


## SPECULOOS EDIT
def check_astelos_error(
    telescope: Any, close: bool = False
) -> Tuple[bool, pd.DataFrame, str]:
    """Check SPECULOOS telescope status for known acceptable errors.

    Analyzes telescope status messages to identify errors and determines if they
    are in the list of known acceptable errors that can be safely acknowledged.

    Args:
        telescope (Any): Telescope object with get() method for status commands.
        close (bool): Whether to include slit closure errors in acceptable list.

    Returns:
        Tuple[bool, pd.DataFrame, str]: Error analysis as (valid, errors, messages):
            - valid: True if all errors are acceptable, False otherwise
            - errors: DataFrame with columns ['error', 'detail', 'level', 'component']
            - messages: Raw telescope status message string
    """

    allowed_err = [
        [
            "ERR_DeviceError",
            "axis (0) unexpectedly changed to powered on state",
            "2",
            "DOME[0]",
        ],
        [
            "ERR_DeviceError",
            "axis (0) unexpectedly changed to powered on state",
            "2",
            "DOME[1]",
        ],
        [
            "ERR_DeviceError",
            "axis (1) unexpectedly changed to powered on state",
            "2",
            "DOME[0]",
        ],
        [
            "ERR_DeviceError",
            "axis (1) unexpectedly changed to powered on state",
            "2",
            "DOME[1]",
        ],
        [
            "ERR_DeviceError",
            "axis #0\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "HA",
        ],
        [
            "ERR_DeviceError",
            "axis #0\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "DEC",
        ],
        [
            "ERR_DeviceError",
            "axis #1\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "HA",
        ],
        [
            "ERR_DeviceError",
            "axis #1\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "DEC",
        ],
        ["ERR_RunDevError", "Working pressure suddenly lost", "2", "HA"],
        ["ERR_RunDevError", "Working pressure suddenly lost", "2", "DEC"],
        ["ERR_DeviceWarn", "Malformed telegram from GPS", "4", "LOCAL"],
        ["ERR_DeviceError", "axis (1)\\| BOTH LIMITS (code=128)", "2", "DOME[0]"],
        ["ERR_DeviceError", "axis (1)\\| BOTH LIMITS (code=128)", "2", "DOME[1]"],
        ["ERR_DeviceError", "axis (1)\\| EXTERN (code=32)", "2", "DOME[0]"],
        ["ERR_DeviceError", "axis (1)\\| EXTERN (code=32)", "2", "DOME[1]"],
    ]
    if close:
        slit_error = []
        allowed_err.extend(slit_error)

    df_allowed = pd.DataFrame(
        allowed_err, columns=["error", "detail", "level", "component"]
    )
    df_list = pd.DataFrame(columns=["error", "detail", "level", "component"])

    messages = telescope.get("CommandString", Command="TELESCOPE.STATUS.LIST", Raw=True)
    # structure = "<group>|<level>[:<component>|<level>[;<component>...]][:<error>|<detail>|<level>|<component>[;<error>...]][,<group>...]"

    for message in messages.split(","):
        parts = message.split(":")

        # only look parts after "<group>|<level>"
        for part in parts[1:]:
            elements = part.split(";")

            for element in elements:
                error_detail = element.replace("\\|", "[ESCAPED_PIPE]").split("|")
                error_detail = [
                    item.replace("[ESCAPED_PIPE]", "\\|") for item in error_detail
                ]

                if len(error_detail) == 4:
                    if not error_detail[1].isdigit():
                        error = error_detail[0]
                        detail = error_detail[1]
                        error_level = error_detail[2]
                        component = error_detail[3]

                        df_list = pd.concat(
                            [
                                df_list,
                                pd.DataFrame(
                                    {
                                        "error": [error],
                                        "detail": [detail],
                                        "level": [error_level],
                                        "component": [component],
                                    }
                                ),
                            ],
                            ignore_index=True,
                        )

    # check all rows of df_list are in df_allowed
    compare_df = pd.merge(df_list, df_allowed, how="left", indicator="exists")
    exists = compare_df["exists"] == "both"

    # if all of exists is True
    if exists.all():
        return True, df_list, messages
    else:
        return False, df_list, messages


def ack_astelos_error(
    telescope: Any,
    valid: bool,
    all_errors: pd.DataFrame,
    messages: str,
    close: bool = False,
) -> Tuple[bool, str]:
    """Acknowledge acceptable SPECULOOS telescope errors.

    Attempts to clear acceptable telescope errors by sending appropriate
    acknowledgement commands. Continues until all errors are cleared or
    unacceptable errors are encountered.

    Args:
        telescope (Any): Telescope object with get() method for commands.
        valid (bool): Whether errors are acceptable (from check_astelos_error).
        all_errors (pd.DataFrame): Error information with 'level' column.
        messages (str): Original telescope status messages.
        close (bool): Whether to include slit closure errors as acceptable.

    Returns:
        Tuple[bool, str]: Acknowledgement result as (success, final_messages):
            - success: True if all errors cleared, False if unacceptable errors remain
            - final_messages: Updated telescope status messages

    Raises:
        TimeoutError: If error clearing takes longer than 2 minutes.
    """

    start_time = time.time()

    while valid and len(all_errors) > 0:
        # derive system eror level
        sys_level = int(np.sum(np.unique(np.array(all_errors.level.astype(int)))))

        # clear errors
        telescope.get(
            "CommandBlind",
            Command=f"TELESCOPE.STATUS.CLEAR_ERROR={sys_level}",
            Raw=True,
        )
        time.sleep(2)

        # check telescope status
        valid, all_errors, messages = check_astelos_error(telescope, close=close)

        if time.time() - start_time > 120:  # 2 minutes hardcoded limit
            raise TimeoutError("Astelos error acknowledgement timed out")

    if not valid:
        return False, messages

    return True, messages
