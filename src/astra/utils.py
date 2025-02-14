import math
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from typing import Optional, Tuple, Union

import astropy.units as u
import numpy as np
import pandas as pd
import twirl
from astropy.coordinates import AltAz, Angle, SkyCoord, get_sun
from astropy.io import fits
from astropy.stats import SigmaClip
from astropy.time import Time
from astropy.units import Quantity
from astropy.wcs.utils import WCS, pixel_to_skycoord
from photutils.background import Background2D, MedianBackground
from scipy import ndimage

from astra import Config

CONFIG = Config()


## for final fits header
def interpolate_dfs(index, *data):
    """
    Interpolates panda dataframes onto an index, of same index type (e.g. wavelength in microns)
    Parameters
    ----------
    index: 1d array which data is to be interpolated onto
    data:       Pandas dataframes
    Returns
    -------
    df: Interpolated dataframe
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
    """
    Converts a Julian Day object into a specific format.  For
    example, Modified Julian Day.
    Parameters
    ----------
    jd: float
    fmt: str

    Returns
    -------
    jd: float
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
    """
    Converts a given datetime object to Julian date.
    Algorithm is copied from https://en.wikipedia.org/wiki/Julian_day
    All variable names are consistent with the notation on the wiki page.

    Parameters
    ----------
    fmt
    dt: datetime
        Datetime object to convert to MJD

    Returns
    -------
    jd: float
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


def getLightTravelTimes(target, time_to_correct):
    """
    From: https://github.com/WarwickAstro/time-conversions
    Get the light travel times to the helio- and
    barycentres
    Parameters
    ----------
    ra : str
    The Right Ascension of the target in degrees
    dec : str
        The Declination of the target in degrees
    time_to_correct : astropy.Time object
    The time of observation to correct. The astropy.Time
    object must have been initialised with an EarthLocation
    Returns
    -------
    ltt_bary : float
        The light travel time to the barycentre
    ltt_helio : float
        The light travel time to the heliocentre
    Raises
    ------
    None
    """

    ltt_bary = time_to_correct.light_travel_time(target)
    ltt_helio = time_to_correct.light_travel_time(target, "heliocentric")
    return ltt_bary, ltt_helio


def time_conversion(jd, location, target):
    """
    https://github.com/WarwickAstro/time-conversions
    """

    time_inp = Time(jd, format="jd", scale="utc", location=location)

    ltt_bary, ltt_helio = getLightTravelTimes(target, time_inp)

    hjd = (time_inp + ltt_helio).value
    bjd = (time_inp.tdb + ltt_bary).value
    lst = time_inp.sidereal_time("mean")
    lstsec = lst.hour * 3600
    ha = Angle(((((lst - target.ra).hour + 12) % 24) - 12) * u.hourangle).to_string(
        unit=u.hourangle, sep=" "
    )  # MH - not zero padded but will do for now

    return hjd, bjd, lstsec, ha


def hdr_times(hdr, fits_config, location, target):
    dateobs = pd.to_datetime(hdr["DATE-OBS"])

    dateend = dateobs + timedelta(seconds=float(hdr["EXPTIME"]))
    jd = to_jd(dateobs)
    jdend = to_jd(dateend)

    mjd = jd - 2400000.5
    mjdend = jdend - 2400000.5

    hjd, bjd, lstsec, ha = time_conversion(jd, location, target)

    for i, row in fits_config[fits_config["fixed"] == False].iterrows():  # noqa: E712
        if row["device_type"] == "astra":
            if row["header"] == "JD-OBS":
                hdr[row["header"]] = (jd, row["comment"])
            elif row["header"] == "JD-END":
                hdr[row["header"]] = (jdend, row["comment"])
            elif row["header"] == "HJD-OBS":
                hdr[row["header"]] = (hjd, row["comment"])
            elif row["header"] == "BJD-OBS":
                hdr[row["header"]] = (bjd, row["comment"])
            elif row["header"] == "MJD-OBS":
                hdr[row["header"]] = (mjd, row["comment"])
            elif row["header"] == "MJD-END":
                hdr[row["header"]] = (mjdend, row["comment"])
            elif row["header"] == "DATE-END":
                hdr[row["header"]] = (
                    dateend.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    row["comment"],
                )
            elif row["header"] == "LST":
                hdr[row["header"]] = (lstsec, row["comment"])
            elif row["header"] == "HA":
                hdr[row["header"]] = (ha, row["comment"])
            else:
                pass

    z = (90 - hdr["ALTITUDE"]) * np.pi / 180
    hdr["AIRMASS"] = (1.002432 * np.cos(z) ** 2 + 0.148386 * np.cos(z) + 0.0096467) / (
        np.cos(z) ** 3 + 0.149864 * np.cos(z) ** 2 + 0.0102963 * np.cos(z) + 0.000303978
    )  # https://doi.org/10.1364/AO.33.001108, https://en.wikipedia.org/wiki/Air_mass_(astronomy)


## for flat fielding
def is_sun_rising(obs_location):
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


## pointing
def db_query(
    db: str, min_dec: float, max_dec: float, min_ra: float, max_ra: float
) -> pd.DataFrame:
    """
    Queries a federated database for astronomical data within a specified range of declination and right ascension.

    Args:
        db (str): The path to the SQLite database file.
        min_dec (float): The minimum declination value to query.
        max_dec (float): The maximum declination value to query.
        min_ra (float): The minimum right ascension value to query.
        max_ra (float): The maximum right ascension value to query.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the queried astronomical data.
    """

    conn = sqlite3.connect(db)

    if min_dec < -90:
        min_dec = -90

    if max_dec > 90:
        max_dec = 90

    # Determine the relevant shard(s) based on the query parameters.
    arr = np.arange(np.floor(min_dec), np.ceil(max_dec) + 1, 1)
    relevant_shard_ids = set()
    for i in range(len(arr) - 1):
        shard_id = f"{arr[i]:.0f}_{arr[i+1]:.0f}"
        relevant_shard_ids.add(shard_id)

    # Execute the federated query across the relevant shard(s).
    df_total = pd.DataFrame()
    for shard_id in relevant_shard_ids:
        shard_table_name = f"{shard_id}"
        q = f"SELECT * FROM `{shard_table_name}` WHERE dec BETWEEN {min_dec} AND {max_dec} AND ra BETWEEN {min_ra} AND {max_ra}"
        df = pd.read_sql_query(q, conn)
        df_total = pd.concat([df, df_total], axis=0)

    # Close the conn and return the results.
    conn.close()
    return df_total


def gaia_db_query(
    center: Union[Tuple[float, float], SkyCoord],
    fov: Union[float, Quantity],
    limit: int = 1000,
    tmass: bool = False,
    dateobs: Optional[datetime] = None,
) -> np.ndarray:
    """
    Query the Gaia archive to retrieve the RA-DEC coordinates of stars within a given field-of-view (FOV) centered on a given sky position.

    Parameters
    ----------
    center : tuple or astropy.coordinates.SkyCoord
        The sky coordinates of the center of the FOV. If a tuple is given, it should contain the RA and DEC in degrees.
    fov : float or astropy.units.Quantity
        The field-of-view of the FOV in degrees. If a float is given, it is assumed to be in degrees.
    limit : int, optional
        The maximum number of sources to retrieve from the Gaia archive. By default, it is set to 10000.
    circular : bool, optional
        Whether to perform a circular or a rectangular query. By default, it is set to True.
    tmass : bool, optional
        Whether to retrieve the 2MASS J magnitudes catelog. By default, it is set to False.
    dateobs : datetime.datetime, optional
        The date of the observation. If given, the proper motions of the sources will be taken into account. By default, it is set to None.

    Returns
    -------
    np.ndarray
        An array of shape (n, 2) containing the RA-DEC coordinates of the retrieved sources in degrees.

    Raises
    ------
    ImportError
        If the astroquery package is not installed.

    Examples
    --------
    >>> from astropy.coordinates import SkyCoord
    >>> from twirl import gaia_radecs
    >>> center = SkyCoord(ra=10.68458, dec=41.26917, unit='deg')
    >>> fov = 0.1
    >>> radecs = gaia_radecs(center, fov)
    """

    if isinstance(center, SkyCoord):
        ra = center.ra.deg
        dec = center.dec.deg
    else:
        ra, dec = center

    if not isinstance(fov, u.Quantity):
        fov = fov * u.deg

    if fov.ndim == 1:
        ra_fov, dec_fov = fov.to(u.deg).value
    else:
        ra_fov = fov[0].to(u.deg).value
        dec_fov = fov[1].to(u.deg).value

    min_dec = dec - dec_fov / 2
    max_dec = dec + dec_fov / 2
    min_ra = ra - ra_fov / 2
    max_ra = ra + ra_fov / 2

    table = db_query(CONFIG.gaia_db, min_dec, max_dec, min_ra, max_ra)
    if tmass:
        table = table.sort_values(by=["j_m"]).reset_index(drop=True)
    else:
        table = table.sort_values(by=["phot_g_mean_mag"]).reset_index(drop=True)

    table.replace("", np.nan, inplace=True)
    table.dropna(inplace=True)

    # limit number of stars
    table = table[0:limit]

    # add proper motion to ra and dec
    if dateobs is not None:
        # calculate fractional year
        dateobs = dateobs.year + (dateobs.timetuple().tm_yday - 1) / 365.25  # type: ignore

        years = dateobs - 2015.5  # type: ignore
        table["ra"] += years * table["pmra"] / 1000 / 3600
        table["dec"] += years * table["pmdec"] / 1000 / 3600

    return np.array([table["ra"].values, table["dec"].values]).T


def pointing(filepath: str, ra: float, dec: float) -> Tuple[float, float, WCS, float]:
    # open image
    with fits.open(filepath) as hdu:
        header = hdu[0].header
        data = hdu[0].data.astype(np.int16)

    # clean image
    sigma_clip = SigmaClip(sigma=3.0)
    bkg_estimator = MedianBackground()

    bkg = Background2D(
        data,
        (32, 32),
        filter_size=(3, 3),
        sigma_clip=sigma_clip,
        bkg_estimator=bkg_estimator,
    )
    bkg_clean = data - bkg.background

    med_clean = ndimage.median_filter(bkg_clean, size=5, mode="mirror")
    band_corr = np.median(med_clean, axis=1).reshape(-1, 1)
    image_clean = med_clean - band_corr

    # center of image, convert to ra, dec in degrees
    ra_unit = u.deg
    dec_unit = u.deg

    center = SkyCoord(ra, dec, unit=[ra_unit, dec_unit])

    # image fov
    shape = image_clean.shape
    plate_scale = np.arctan((header["XPIXSZ"] * 1e-6) / (header["FOCALLEN"] * 1e-3)) * (
        180 / np.pi
    )  # deg/pixel
    fovx = (1 / np.abs(np.cos(center.dec.rad))) * shape[0] * plate_scale
    fovy = shape[1] * plate_scale

    # detect stars in the image
    stars = twirl.find_peaks(image_clean, threshold=5)

    gaia_limit = len(stars) * 2
    star_limit = len(stars)
    if len(stars) < 4:
        raise Exception("Not enough stars detected for plate solve")
    elif len(stars) > 12:
        gaia_limit = 18
        star_limit = 12

    stars = stars[0:star_limit]

    # get gaia stars in the field of view
    dateobs = pd.to_datetime(header["DATE-OBS"])
    gaias = gaia_db_query(center, (fovx, fovy), tmass=True, dateobs=dateobs)[
        0:gaia_limit
    ]

    wcs = twirl.compute_wcs(stars, gaias)
    real_center = pixel_to_skycoord(
        image_clean.shape[1] / 2, image_clean.shape[0] / 2, wcs
    )
    offset = np.array(
        [real_center.ra.deg - center.ra.deg, real_center.dec.deg - center.dec.deg]
    )

    angular_separation = center.separation(real_center)

    # convert gaia stars to pixel coordinates
    gaias_pixel = np.array(SkyCoord(gaias, unit="deg").to_pixel(wcs)).T

    #  if offset is too large, consider plate solve failed
    if abs(angular_separation.deg) > max(plate_scale * np.array(shape)):
        raise Exception("Plate solve failed, offset larger than field of view")

    # iterate through gaia stars and find the closest star in the image
    count = 0
    for x, y in gaias_pixel:
        for i, j in stars:
            # if the distance between the gaia star and the star
            # in the image is less than 10 pixels, count it as a match
            if np.sqrt((x - i) ** 2 + (y - j) ** 2) < 10:
                count += 1

    # if more than 4 stars match, consider plate solve successful
    if count < 4:
        raise Exception("Plate solve failed, not enough stars matched")

    return offset[0], offset[1], wcs, angular_separation


## SPECULOOS EDIT
def check_astelos_error(telescope, close=False):
    """
    Check astelos telescope status list property
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
        ["ERR_RunDevError", "Working pressure suddenly lost", "2", "HA"],
        [
            "ERR_DeviceError",
            "axis #0\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "DEC",
        ],
        ["ERR_RunDevError", "Working pressure suddenly lost", "2", "DEC"],
        ["ERR_DeviceWarn", "Malformed telegram from GPS", "4", "LOCAL"],
    ]
    if close:
        slit_error = [
            ["ERR_DeviceError", "axis (1)\\| BOTH LIMITS (code=128)", "2", "DOME[0]"],
            ["ERR_DeviceError", "axis (1)\\| BOTH LIMITS (code=128)", "2", "DOME[1]"],
        ]
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
                error_detail = element.split("|")

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


def ack_astelos_error(telescope, valid, all_errors, messages):
    """
    Acknowledge error if valid

    """

    start_time = time.time()

    while valid and len(all_errors) > 0:

        # derive system eror level
        sys_level = int(np.sum(np.unique(np.array(all_errors.level))))

        # clear errors
        telescope.get(
            "CommandBlind",
            Command=f"TELESCOPE.STATUS.CLEAR_ERROR={sys_level}",
            Raw=True,
        )
        time.sleep(2)

        # telescope.get('CommandBlind', Command = "TELESCOPE.STATUS.CLEAR_ERROR=2", Raw = True)
        # time.sleep(5)

        # check telescope status
        valid, all_errors, messages = check_astelos_error(telescope)

        if time.time() - start_time > 120:  # 2 minutes hardcoded limit
            raise TimeoutError("Astelos error acknowledgement timed out")

    if not valid:
        return False, messages

    return True, messages
