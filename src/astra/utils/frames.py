"""Conversion between Astra's internal ICRS coordinates and the mount's frame.

Astra works in ICRS (J2000) everywhere: schedule ``ra``/``dec``, SIMBAD lookups,
JPL Horizons positions and the astrometric planet positions from astropy. A mount
may expect a different frame. The ASCOM ``EquatorialSystem`` property says which
one. Most modern drivers report ``equTopocentric``, the apparent coordinates of
date (JNow). Sending J2000 coordinates to such a mount misses by the precession
since 2000, about 20 arcminutes in the mid 2020s.

The observatory reads ``EquatorialSystem`` once per mount and converts at the
hardware boundary, just before a slew or sync. A mount that reports J2000, or
whose property cannot be read, gets the ICRS coordinates unchanged.
"""

from enum import IntEnum

from astropy.coordinates import FK4, FK5, ICRS, TETE, EarthLocation, SkyCoord
from astropy.time import Time

__all__ = [
    "EquatorialSystem",
    "parse_equatorial_system",
    "to_mount_frame",
    "from_mount_frame",
    "needs_conversion",
]


class EquatorialSystem(IntEnum):
    """ASCOM ``EquatorialCoordinateType`` values reported by ``EquatorialSystem``."""

    OTHER = 0
    TOPOCENTRIC = 1  # apparent coordinates of date, "JNow"
    J2000 = 2
    J2050 = 3
    B1950 = 4


_ALIASES: dict[str, EquatorialSystem] = {
    "j2000": EquatorialSystem.J2000,
    "icrs": EquatorialSystem.J2000,
    "equj2000": EquatorialSystem.J2000,
    "jnow": EquatorialSystem.TOPOCENTRIC,
    "topocentric": EquatorialSystem.TOPOCENTRIC,
    "equtopocentric": EquatorialSystem.TOPOCENTRIC,
    "equlocaltopocentric": EquatorialSystem.TOPOCENTRIC,
    "j2050": EquatorialSystem.J2050,
    "equj2050": EquatorialSystem.J2050,
    "b1950": EquatorialSystem.B1950,
    "equb1950": EquatorialSystem.B1950,
    "other": EquatorialSystem.OTHER,
    "equother": EquatorialSystem.OTHER,
}


def parse_equatorial_system(value: object) -> EquatorialSystem | None:
    """Parse an ASCOM enum value or a config string into an EquatorialSystem.

    Accepts the integer the driver returns (0 to 4) or a name such as "J2000" or
    "JNow", in any case. Returns None for "auto", for an unknown value, and for
    anything that is not an int or a str. None means "unknown, send ICRS unchanged".
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        try:
            return EquatorialSystem(value)
        except ValueError:
            return None
    if isinstance(value, str):
        return _ALIASES.get(value.strip().lower())
    return None


def needs_conversion(system: EquatorialSystem | None) -> bool:
    """Return True if coordinates must be transformed before reaching the mount."""
    return system in (
        EquatorialSystem.TOPOCENTRIC,
        EquatorialSystem.J2050,
        EquatorialSystem.B1950,
    )


def _mount_frame(
    system: EquatorialSystem, obstime: Time, location: EarthLocation | None
):
    if system == EquatorialSystem.TOPOCENTRIC:
        # True equator and equinox of date. With a location astropy also applies
        # diurnal aberration. This is the ASCOM "apparent, topocentric" place.
        return TETE(obstime=obstime, location=location)
    if system == EquatorialSystem.J2050:
        return FK5(equinox="J2050")
    if system == EquatorialSystem.B1950:
        return FK4(equinox="B1950", obstime=obstime)
    return ICRS()


def to_mount_frame(
    ra_deg: float,
    dec_deg: float,
    system: EquatorialSystem | None,
    obstime: Time | None = None,
    location: EarthLocation | None = None,
) -> tuple[float, float]:
    """Convert an ICRS position to the mount's frame.

    Args:
        ra_deg: ICRS right ascension in degrees.
        dec_deg: ICRS declination in degrees.
        system: The mount's ``EquatorialSystem``. None, J2000 and OTHER return the
            input unchanged.
        obstime: Time of the slew. Defaults to now. Needed for JNow and B1950.
        location: Observer location. Improves the JNow result slightly.

    Returns:
        (ra_deg, dec_deg) in the mount's frame.
    """
    if not needs_conversion(system):
        return ra_deg, dec_deg
    obstime = Time.now() if obstime is None else obstime
    coord = SkyCoord(ra=ra_deg, dec=dec_deg, unit="deg", frame="icrs")
    out = coord.transform_to(_mount_frame(system, obstime, location))
    return float(out.ra.deg) % 360.0, float(out.dec.deg)


def from_mount_frame(
    ra_deg: float,
    dec_deg: float,
    system: EquatorialSystem | None,
    obstime: Time | None = None,
    location: EarthLocation | None = None,
) -> tuple[float, float]:
    """Convert a position read from the mount into ICRS. Inverse of to_mount_frame."""
    if not needs_conversion(system):
        return ra_deg, dec_deg
    obstime = Time.now() if obstime is None else obstime
    coord = SkyCoord(
        ra=ra_deg,
        dec=dec_deg,
        unit="deg",
        frame=_mount_frame(system, obstime, location),
    )
    out = coord.transform_to(ICRS())
    return float(out.ra.deg) % 360.0, float(out.dec.deg)
