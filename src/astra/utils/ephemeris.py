"""Solar position analysis and celestial body coordinate lookup."""

from typing import Any, Tuple

import astropy.units as u
from astropy.coordinates import AltAz, SkyCoord, get_sun
from astropy.time import Time


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


## planet or SIMBAD positions
def get_body_coordinates(body_name: str, obs_time: Time, obs_location: Any) -> SkyCoord:
    """Get the position of a celestial body (Solar System or Deep Sky).

    Calculates the apparent celestial coordinates of a specified solar system body
    or resolves the coordinates of a deep sky object by name.

    Args:
        body_name (str): Name of the body (e.g., 'mars', 'jupiter', 'M31', 'Vega').
        obs_time (Time): Observation time (used for solar system bodies).
        obs_location (EarthLocation): Observer's geographic location (used for solar system bodies).

    Returns:
        SkyCoord: Position of the body in the sky.
    """
    from astropy.coordinates import SkyCoord, get_body, solar_system_ephemeris

    # Check if the body is in the solar system ephemeris (case-insensitive)
    # solar_system_ephemeris.bodies normally contains lowercase strings
    if body_name.lower() in solar_system_ephemeris.bodies:
        return get_body(body_name, obs_time, obs_location)

    # Otherwise, try to resolve as a deep sky object (ICRS)
    return SkyCoord.from_name(body_name)
