"""Solar position analysis and celestial body coordinate lookup.

Also provides pre-computed ephemerides for non-sidereal (moving) targets,
sourced either from astropy's built-in solar system ephemeris or from JPL
Horizons (including TLE-defined satellites).
"""

import logging
from datetime import UTC, datetime
from typing import Any, Tuple

import astropy.units as u
import numpy as np
import requests
from astropy.coordinates import (
    ICRS,
    AltAz,
    CartesianRepresentation,
    EarthLocation,
    SkyCoord,
    get_body,
    get_body_barycentric,
    get_sun,
    solar_system_ephemeris,
)
from astropy.time import Time
from astroquery.jplhorizons import Horizons
from scipy.interpolate import interp1d

logger = logging.getLogger(__name__)

_SOLAR_SYSTEM_BODIES: frozenset[str] = frozenset(solar_system_ephemeris.bodies)
_SOLAR_TO_SIDEREAL = u.Quantity(1, "day").to("sday").value

# The Horizons columns Astra reads: 1 is astrometric RA and Dec, 3 is the rates.
# The full set is 81 columns, and asking for all of them doubles the time the
# query takes, which delays the schedule load. The debug dump written under
# _save_and_log_horizons_output therefore holds these columns rather than all.
_HORIZONS_QUANTITIES = "1,3"

# Ephemeris sampling. The interval is chosen from the target's own sky motion
# unless the caller asks for a specific one. A planet crosses a fraction of an
# arcsecond in a minute, so the default is ample for it, while the ISS crosses
# 30 degrees and its interpolated position lands 8 degrees from the truth.
_DEFAULT_INTERVAL_MINUTES = 1.0
_MIN_INTERVAL_SECONDS = 1.0
# Largest arc a target may cross between two samples. Cubic interpolation over a
# 2 degree arc is accurate to about an arcsecond.
_MAX_ARC_PER_SAMPLE_DEG = 2.0
# Ceiling on the sample count, so a fast target over a long window cannot turn
# into an enormous Horizons query.
_MAX_SAMPLES = 5000
# A coarse grid under-reads the peak rate of a satellite pass, so the interval is
# chosen again on the finer grid it produces. Three passes is enough to settle.
_MAX_INTERVAL_REFINEMENTS = 3
# Where the automatic search starts for a satellite. A TLE always describes an
# Earth satellite, and one in low orbit crosses about 1 degree a second when it
# passes overhead, which is as fast as a satellite gets. Starting here costs one
# query instead of two, which keeps the schedule load quick enough for a sequence
# that starts seconds later.
_TLE_INITIAL_INTERVAL_SECONDS = 2.0
# The search goes back to a coarser grid only when the interval is much shorter
# than the target needs. A small difference is not worth another query.
_INTERVAL_COARSEN_FACTOR = 4.0


class NotMovingBodyError(ValueError):
    """Raised when a lookup_name cannot be resolved as a solar system or minor body."""


def _save_and_log_horizons_output(
    body_name: str, context: str, eph: Any, call_input: dict[str, Any]
) -> None:
    """Persist Horizons diagnostics only when debug logging is enabled."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    try:
        from astra.config import Config

        horizons_dir = Config().paths.logs / "horizons"
        horizons_dir.mkdir(parents=True, exist_ok=True)
        safe_name = body_name.replace(" ", "_").replace("/", "_")
        output_path = horizons_dir / (
            f"{safe_name}_{context}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}.ecsv"
        )
        eph.write(output_path, format="ascii.ecsv", overwrite=True)
        logger.debug(
            "Saved raw Horizons output for %s (%s) to %s",
            body_name,
            context,
            output_path,
        )
    except BaseException as exc:
        logger.debug(
            "Failed to save raw Horizons output for %s (%s): %s",
            body_name,
            context,
            exc,
        )

    try:
        logger.debug(
            "Horizons API call input for %s (%s): %s",
            body_name,
            context,
            call_input,
        )
    except BaseException as exc:
        logger.debug(
            "Failed to log Horizons API call input for %s (%s): %s",
            body_name,
            context,
            exc,
        )


## for flat fielding
def is_sun_rising(obs_location: EarthLocation) -> Tuple[bool, bool, AltAz]:
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


def astrometric_icrs(
    body: SkyCoord, obstime: Time, obs_location: EarthLocation
) -> SkyCoord:
    """Return the astrometric ICRS direction of a solar system body from the observer.

    ``get_body`` returns GCRS coordinates. Those include annual aberration, so they
    differ from a catalogue (ICRS) direction by up to 20 arcseconds. JPL Horizons
    and SIMBAD give astrometric ICRS. This function brings the astropy result onto
    the same footing, so every source Astra uses is in one frame.

    The direction is the light-time corrected barycentric position of the body,
    which astropy provides, minus the barycentric position of the observer. Do not
    use ``transform_to(ICRS())`` on a ``get_body`` result for this. That returns the
    direction from the solar system barycentre, not from the observer.

    Args:
        body: Output of ``get_body``, one or many times.
        obstime: The time(s) the body was evaluated at.
        obs_location: The observer location used for ``get_body``.

    Returns:
        SkyCoord in ICRS with no distance, one entry per input time.
    """
    if not body.cartesian.xyz.unit.is_equivalent(u.m):
        # No distance, so this is already a direction. Nothing to correct.
        return SkyCoord(ra=body.ra, dec=body.dec, frame="icrs")

    body_bary = body.transform_to(ICRS()).cartesian.without_differentials()
    observer_bary = (
        get_body_barycentric("earth", obstime)
        + obs_location.get_gcrs(obstime).cartesian.without_differentials()
    )
    direction = CartesianRepresentation(body_bary.xyz - observer_bary.xyz)
    unit = SkyCoord(direction, frame="icrs")
    return SkyCoord(ra=unit.ra, dec=unit.dec, frame="icrs")


## planet or SIMBAD positions
def get_body_coordinates(
    body_name: str,
    obs_time: Time,
    obs_location: EarthLocation,
) -> SkyCoord:
    """Get the ICRS position of a celestial body (Solar System or Deep Sky).

    Calculates the astrometric ICRS coordinates of a solar system body as seen
    from the observer, or resolves the coordinates of a deep sky object by name.
    Both are in the same frame as the schedule's ``ra``/``dec``.

    This returns a single position and is for targets that are tracked sidereally.
    Moving targets that need differential tracking -- minor bodies and TLE-defined
    satellites -- go through :func:`precompute_ephemeris` instead, which returns
    interpolators over the whole observation window.

    Args:
        body_name (str): Name of the body (e.g., 'mars', 'jupiter', 'M31', 'Vega').
        obs_time (Time): Observation time (used for solar system bodies).
        obs_location (EarthLocation): Observer's geographic location (used for solar system bodies).

    Returns:
        SkyCoord: Position of the body in the sky.
    """
    # Check if the body is in the solar system ephemeris (case-insensitive)
    # solar_system_ephemeris.bodies normally contains lowercase strings
    if body_name.lower() in _SOLAR_SYSTEM_BODIES:
        return astrometric_icrs(
            get_body(body_name, obs_time, obs_location), obs_time, obs_location
        )

    # Otherwise, try to resolve as a deep sky object (ICRS)
    return SkyCoord.from_name(body_name)


def is_solar_system_body(body_name: str) -> bool:
    """Return True if name is a known solar system body in the astropy ephemeris.

    O(1) lookup on a lowercase name against the ephemeris bodies set.
    """
    return body_name.lower() in _SOLAR_SYSTEM_BODIES


def _sample_body(
    body_name: str,
    start_time: Time,
    duration_hours: float,
    obs_location: EarthLocation,
    interval_minutes: float,
    tle_data: str | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray | None, np.ndarray | None]:
    """Sample a moving body's position over a window at one sampling interval.

    Args:
        body_name: Name of the body, or 'TLE' when tle_data is given.
        start_time: Start of the window.
        duration_hours: Length of the window in hours.
        obs_location: Observer EarthLocation.
        interval_minutes: Sampling interval in minutes.
        tle_data: Two element lines separated by a newline, for a satellite.

    Returns:
        (seconds, ra_deg, dec_deg, ra_rate, dec_rate). Seconds are measured from
        start_time and RA is unwrapped, so it is continuous across 0/360. The two
        rates are the Horizons columns in arcseconds per hour, or None for a body
        taken from astropy's built-in ephemeris.

    Raises:
        NotMovingBodyError: If Horizons cannot resolve the name.
        requests.exceptions.RequestException: On a network failure.
    """
    n_points = int(duration_hours * 60 / interval_minutes) + 1
    minutes = np.linspace(0, duration_hours * 60, n_points)
    times = start_time + u.Quantity(minutes, "min")
    stop_time = start_time + duration_hours * u.hour

    ra_rate_as_per_hour = None
    dec_rate_as_per_hour = None

    if body_name.lower() in _SOLAR_SYSTEM_BODIES:
        with solar_system_ephemeris.set("builtin"):
            # Astrometric ICRS, the same frame Horizons and SIMBAD return.
            bodies = astrometric_icrs(
                get_body(body_name, times, obs_location), times, obs_location
            )
        seconds = minutes * 60.0
    else:
        try:
            location = {
                "lon": obs_location.lon.deg,
                "lat": obs_location.lat.deg,
                "elevation": obs_location.height.to(u.km).value,
            }
            epochs = {
                "start": start_time.iso,
                "stop": stop_time.iso,
                "step": str(n_points - 1),  # Horizons returns n+1 rows for n steps
            }

            # Handle TLE data
            if body_name.upper() == "TLE" or tle_data is not None:
                if tle_data is None:
                    raise ValueError(
                        "tle_data parameter is required when body_name is 'TLE'"
                    )
                call_input = {
                    "id": "TLE",
                    "location": location,
                    "epochs": epochs,
                    "optional_settings": {"TLE": tle_data},
                    "quantities": _HORIZONS_QUANTITIES,
                }
                obj = Horizons(id="TLE", location=location, epochs=epochs)
                eph = obj.ephemerides(
                    optional_settings={"TLE": tle_data},
                    quantities=_HORIZONS_QUANTITIES,
                )
            else:
                call_input = {
                    "id": body_name,
                    "location": location,
                    "epochs": epochs,
                    "quantities": _HORIZONS_QUANTITIES,
                }
                obj = Horizons(id=body_name, location=location, epochs=epochs)
                eph = obj.ephemerides(quantities=_HORIZONS_QUANTITIES)
            _save_and_log_horizons_output(
                body_name, "precompute_ephemeris", eph, call_input
            )

            bodies = SkyCoord(ra=eph["RA"].data * u.deg, dec=eph["DEC"].data * u.deg)
            seconds = (Time(eph["datetime_jd"], format="jd") - start_time).to(u.s).value
            if "RA_rate" in eph.colnames and "DEC_rate" in eph.colnames:
                ra_rate_as_per_hour = np.asarray(eph["RA_rate"], dtype=float)
                dec_rate_as_per_hour = np.asarray(eph["DEC_rate"], dtype=float)
        except requests.exceptions.RequestException:
            # Network/HTTP failures are not evidence that the body is fixed --
            # let them propagate so the caller does not silently fall back to
            # sidereal tracking for a genuinely moving target.
            raise
        except Exception as e:
            raise NotMovingBodyError(
                f"'{body_name}' could not be resolved as a solar system or minor body: {e}"
            ) from e

    ra_coords = np.unwrap(bodies.ra.rad) * (180.0 / np.pi)
    dec_coords = bodies.dec.deg
    return seconds, ra_coords, dec_coords, ra_rate_as_per_hour, dec_rate_as_per_hour


def _max_sky_rate_deg_per_s(
    seconds: np.ndarray,
    ra_coords: np.ndarray,
    dec_coords: np.ndarray,
    ra_rate_as_per_hour: np.ndarray | None,
    dec_rate_as_per_hour: np.ndarray | None,
) -> float:
    """Return the fastest apparent sky motion in the sampled window, in deg/s.

    Horizons reports the rate at each sample, which is exact even where the
    samples themselves are too far apart to describe the path. Positions from
    astropy are differentiated instead, which is accurate because every body in
    the built-in ephemeris moves slowly.
    """
    if ra_rate_as_per_hour is not None and dec_rate_as_per_hour is not None:
        # Both columns are arcseconds per hour, and Horizons already projects the
        # RA rate onto the sky by the cos(Dec) factor.
        rates = np.hypot(ra_rate_as_per_hour, dec_rate_as_per_hour) / 3600.0**2
    else:
        if len(seconds) < 2:
            return 0.0
        cos_dec = np.cos(np.radians(dec_coords))
        rates = np.hypot(
            np.gradient(ra_coords, seconds) * cos_dec,
            np.gradient(dec_coords, seconds),
        )
    return float(np.nanmax(np.abs(rates)))


def _interval_floor_seconds(duration_hours: float) -> float:
    """Return the shortest sampling interval allowed over a window of this length.

    One second, or longer where that many samples would exceed the ceiling.
    """
    return max(
        _MIN_INTERVAL_SECONDS, duration_hours * 3600.0 / max(_MAX_SAMPLES - 1, 1)
    )


def _interval_for_sky_rate(rate_deg_per_s: float, duration_hours: float) -> float:
    """Choose a sampling interval in minutes for a target moving at this rate.

    The interval keeps the arc between two samples short enough for the cubic
    interpolation to stay accurate. It is never longer than the default, never
    shorter than one second, and never fine enough to exceed the sample ceiling.
    """
    if rate_deg_per_s <= 0.0:
        wanted_s = _DEFAULT_INTERVAL_MINUTES * 60.0
    else:
        wanted_s = _MAX_ARC_PER_SAMPLE_DEG / rate_deg_per_s

    wanted_s = min(
        max(wanted_s, _interval_floor_seconds(duration_hours)),
        _DEFAULT_INTERVAL_MINUTES * 60.0,
    )
    return wanted_s / 60.0


def _initial_interval_minutes(
    body_name: str, tle_data: str | None, duration_hours: float
) -> float:
    """Return the interval the automatic search starts from.

    A TLE always describes an Earth satellite, so the search starts at an interval
    that suits a low orbit. Every other target starts at the default, which already
    suits a planet, a comet or an asteroid. Each start is the value the search
    settles on for the usual target of its kind, so one query is normally enough.
    """
    is_tle = body_name.upper() == "TLE" or tle_data is not None
    start_s = (
        _TLE_INITIAL_INTERVAL_SECONDS if is_tle else _DEFAULT_INTERVAL_MINUTES * 60.0
    )
    return max(start_s, _interval_floor_seconds(duration_hours)) / 60.0


def _sample_automatically(
    body_name: str,
    start_time: Time,
    duration_hours: float,
    obs_location: EarthLocation,
    tle_data: str | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray | None, np.ndarray | None]:
    """Sample a body at an interval that suits how quickly it moves.

    The search starts from the interval that the kind of target usually needs, and
    each sampled grid says whether that interval was correct. A grid that is too
    coarse for a satellite is also aliased, and the rate read from it is too low.
    So the interval is chosen again from each finer grid, which reads a higher peak
    rate, until the choice settles.
    """
    interval = _initial_interval_minutes(body_name, tle_data, duration_hours)
    sample = _sample_body(
        body_name, start_time, duration_hours, obs_location, interval, tle_data
    )

    for _ in range(_MAX_INTERVAL_REFINEMENTS):
        rate = _max_sky_rate_deg_per_s(*sample)
        wanted = _interval_for_sky_rate(rate, duration_hours)
        too_coarse = wanted < interval * 0.9
        # Much finer than the target needs, which costs query time and memory for
        # no accuracy. A small difference is left alone.
        too_fine = wanted > interval * _INTERVAL_COARSEN_FACTOR
        if not (too_coarse or too_fine):
            break
        logger.info(
            "'%s' moves at up to %.4f deg/s. Sampling its ephemeris every %.1f s "
            "rather than %.1f s.",
            body_name,
            rate,
            wanted * 60.0,
            interval * 60.0,
        )
        interval = wanted
        sample = _sample_body(
            body_name, start_time, duration_hours, obs_location, interval, tle_data
        )

    rate = _max_sky_rate_deg_per_s(*sample)
    if rate > 0.0 and _MAX_ARC_PER_SAMPLE_DEG / rate < _interval_floor_seconds(
        duration_hours
    ):
        logger.warning(
            "'%s' moves at up to %.4f deg/s, which needs an ephemeris sample every "
            "%.2f s. That many samples over this %.1f hour window exceeds the "
            "ceiling of %d, so the interval is held at %.1f s and positions between "
            "samples are less accurate. Shorten the window for a target this fast.",
            body_name,
            rate,
            _MAX_ARC_PER_SAMPLE_DEG / rate,
            duration_hours,
            _MAX_SAMPLES,
            interval * 60.0,
        )
    return sample


def precompute_ephemeris(
    body_name: str,
    start_time: Time,
    duration_hours: float,
    obs_location: EarthLocation,
    interval_minutes: float | None = 1.0,
    tle_data: str | None = None,
    return_rates: bool = False,
) -> (
    tuple["interp1d", "interp1d"]
    | tuple["interp1d", "interp1d", "interp1d", "interp1d"]
):
    """Pre-compute a moving body's sky positions over a time window.

    Samples the body's position over the whole window and returns cubic
    interpolation functions keyed on seconds since start_time. Planets, the Moon
    and the Sun come from vectorised astropy get_body() calls. Minor bodies and
    TLE-defined satellites come from JPL Horizons, in one query, or two where the
    interval is chosen automatically and the target moves fast enough to need a
    finer grid. Reading the interpolators at runtime is much faster than repeated
    position lookups.

    Args:
        body_name: Name of the body (e.g. 'mars', 'moon'). Must be present in
            astropy's built-in solar system ephemeris, resolvable by JPL Horizons,
            or 'TLE' if tle_data is provided.
        start_time: Start of the observation window.
        duration_hours: Length of the window in hours.
        obs_location: Observer EarthLocation.
        interval_minutes: Ephemeris sampling interval in minutes. Pass None to
            choose it from the target's own sky motion, which a fast target needs:
            sampled once a minute, the ISS lands 8 degrees from its true position
            between samples, because cubic interpolation cannot describe an arc it
            never sampled. The search starts at a few seconds for a TLE and at one
            minute for everything else, so one query is normally enough, and then
            checks that against the sampled rates. The interval is held between one
            second and one minute, and never fine enough to exceed the sample
            ceiling.
        tle_data: Two-line element (TLE) data as a string with two lines separated
            by newline. Required when body_name is 'TLE'. Example format:
            "1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927\\n
             2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537"

    Returns:
        If return_rates is False (default):
            (ra_interp, dec_interp): Two callables mapping elapsed seconds to degrees.
            RA is unwrapped (continuous, not modulo 360) to avoid discontinuities at wrap boundaries.

        If return_rates is True:
            (ra_interp, dec_interp, ra_rate_interp, dec_rate_interp), where
            ra_rate_interp and dec_rate_interp map elapsed seconds to ASCOM tracking
            units (RightAscensionRate in seconds of RA per sidereal second and
            DeclinationRate in arcseconds per SI second).

    Raises:
        NotMovingBodyError: If body cannot be resolved as a solar system body,
            minor body, or TLE.
        ValueError: If body_name is 'TLE' but tle_data is not provided.

    Example usage:
    --------
    Get the position of Mars as observed from Greenwich at the current time:
        from astropy.coordinates import get_body, EarthLocation, solar_system_ephemeris
        from astropy.time import Time
        location = EarthLocation.of_site('greenwich')
        ra_interp, dec_interp = precompute_ephemeris('mars', Time.now(), 4, location)

    Get position of ISS using TLE data:
        tle = "1 25544U 98067A   23001.00000000  .00016717  00000-0  29641-3 0  9991\n2 25544  51.6416 339.8014 0002571  235.7582  1.5976 15.54178122381131"
        ra_interp, dec_interp = precompute_ephemeris('TLE', start_time, 4, location, tle_data=tle)
    """
    if interval_minutes is None:
        seconds, ra_coords, dec_coords, ra_rate_as_per_hour, dec_rate_as_per_hour = (
            _sample_automatically(
                body_name, start_time, duration_hours, obs_location, tle_data
            )
        )
    else:
        seconds, ra_coords, dec_coords, ra_rate_as_per_hour, dec_rate_as_per_hour = (
            _sample_body(
                body_name,
                start_time,
                duration_hours,
                obs_location,
                interval_minutes,
                tle_data,
            )
        )

    ra_interp = interp1d(seconds, ra_coords, kind="cubic", fill_value="extrapolate")
    dec_interp = interp1d(seconds, dec_coords, kind="cubic", fill_value="extrapolate")

    if not return_rates:
        return ra_interp, dec_interp

    if ra_rate_as_per_hour is not None and dec_rate_as_per_hour is not None:
        # Horizons RA_rate is dRA*cos(D) in arcsec/hr — the angular velocity projected
        # onto the sky, not the RA coordinate rate.  Divide by cos(Dec) to recover
        # d(RA_coord)/dt before converting to ASCOM RightAscensionRate units
        # (seconds of RA per sidereal second).  Without this factor the mount tracks at
        # cos(Dec) of the required rate, causing steady RA drift between recenters that
        # manifests as a visible jump when each recenter slew corrects the error.
        cos_dec = np.cos(np.radians(dec_coords))
        # Guard against division by zero within ~0.003° of the celestial poles.
        cos_dec = np.where(np.abs(cos_dec) < 5e-5, 5e-5, cos_dec)
        ra_rates = (
            ra_rate_as_per_hour / (15.0 * 3600.0 * cos_dec)
        ) / _SOLAR_TO_SIDEREAL
        # ASCOM DeclinationRate is in arcseconds per SI (solar) second, so no
        # sidereal conversion applies here. Only RightAscensionRate is per
        # sidereal second.
        dec_rates = dec_rate_as_per_hour / 3600.0
    else:
        # Fallback for astropy bodies: derive rates from sampled sky positions.
        ra_rate_deg_per_solar_s = np.gradient(ra_coords, seconds)
        dec_rate_deg_per_solar_s = np.gradient(dec_coords, seconds)
        ra_rates = (ra_rate_deg_per_solar_s * 240.0) / _SOLAR_TO_SIDEREAL
        dec_rates = dec_rate_deg_per_solar_s * 3600.0

    ra_rate_interp = interp1d(
        seconds,
        ra_rates,
        kind="linear",
        fill_value="extrapolate",
    )
    dec_rate_interp = interp1d(
        seconds,
        dec_rates,
        kind="linear",
        fill_value="extrapolate",
    )
    return ra_interp, dec_interp, ra_rate_interp, dec_rate_interp


def compute_nonsidereal_rates_from_interp(
    ra_interp: interp1d,
    dec_interp: interp1d,
    t_seconds: float,
    dt: float = 60.0,
) -> tuple[float, float]:
    """Compute ASCOM RightAscensionRate and DeclinationRate from pre-computed interpolators.

    Uses a finite difference on the interpolated ephemeris so no additional
    get_body() calls are needed at runtime.

    Args:
        ra_interp: RA interpolator (seconds to degrees, unwrapped/continuous).
        dec_interp: Dec interpolator (seconds to degrees).
        t_seconds: Elapsed seconds since the ephemeris start_time.
        dt: Finite-difference step in seconds (default 60).

    Returns:
        (ra_rate, dec_rate) where:
          ra_rate  - seconds of time per sidereal second  (ASCOM RightAscensionRate)
          dec_rate - arcseconds per SI second             (ASCOM DeclinationRate)
    """
    # ASCOM RightAscensionRate is per sidereal second. A sidereal second is shorter
    # than a solar second, so ``dt`` solar seconds span ``dt * _SOLAR_TO_SIDEREAL``
    # sidereal seconds. This matches the ``/ _SOLAR_TO_SIDEREAL`` applied to the RA
    # rates in ``precompute_ephemeris``. DeclinationRate is per SI second, so the
    # Dec rate uses ``dt`` directly.
    dt_in_sidereal_s = dt * _SOLAR_TO_SIDEREAL

    delta_ra_deg = float(ra_interp(t_seconds + dt)) - float(ra_interp(t_seconds))
    delta_dec_deg = float(dec_interp(t_seconds + dt)) - float(dec_interp(t_seconds))

    # Convert to ASCOM units (RA: s/s_sidereal, Dec: as/s_SI)
    ra_rate = (delta_ra_deg * 240.0) / dt_in_sidereal_s
    dec_rate = (delta_dec_deg * 3600.0) / dt
    return ra_rate, dec_rate
