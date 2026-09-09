"""Julian Day conversions and astronomical time-frame calculations."""

import math
from datetime import datetime
from typing import Any, Tuple

import astropy.units as u
import numpy as np
import pandas as pd
from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time


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
