"""Tests for astra.utils.frames: ICRS to mount-frame conversion."""

import astropy.units as u
import pytest
from astropy.coordinates import TETE, EarthLocation, SkyCoord, angular_separation
from astropy.time import Time

from astra.utils.frames import (
    EquatorialSystem,
    from_mount_frame,
    needs_conversion,
    parse_equatorial_system,
    to_mount_frame,
)

OBSTIME = Time("2026-09-02T02:00:00")
LOCATION = EarthLocation(lat=47.4 * u.deg, lon=8.5 * u.deg, height=500 * u.m)
VEGA = (279.2347, 38.7837)


def _sep_arcsec(a, b):
    return angular_separation(*[x * u.deg for x in (*a, *b)]).to_value(u.arcsec)


class TestParse:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (0, EquatorialSystem.OTHER),
            (1, EquatorialSystem.TOPOCENTRIC),
            (2, EquatorialSystem.J2000),
            (3, EquatorialSystem.J2050),
            (4, EquatorialSystem.B1950),
            ("J2000", EquatorialSystem.J2000),
            ("icrs", EquatorialSystem.J2000),
            ("JNow", EquatorialSystem.TOPOCENTRIC),
            (" equTopocentric ", EquatorialSystem.TOPOCENTRIC),
            ("B1950", EquatorialSystem.B1950),
        ],
    )
    def test_known_values(self, value, expected):
        assert parse_equatorial_system(value) == expected

    @pytest.mark.parametrize("value", ["auto", "nonsense", 7, None, True, object()])
    def test_unknown_values_are_none(self, value):
        # A MagicMock or a bool must never be mistaken for an enum value.
        assert parse_equatorial_system(value) is None


class TestNeedsConversion:
    def test_only_non_icrs_systems_convert(self):
        assert not needs_conversion(None)
        assert not needs_conversion(EquatorialSystem.J2000)
        assert not needs_conversion(EquatorialSystem.OTHER)
        assert needs_conversion(EquatorialSystem.TOPOCENTRIC)
        assert needs_conversion(EquatorialSystem.J2050)
        assert needs_conversion(EquatorialSystem.B1950)


class TestToMountFrame:
    @pytest.mark.parametrize("system", [None, EquatorialSystem.J2000])
    def test_j2000_mount_gets_icrs_unchanged(self, system):
        assert to_mount_frame(*VEGA, system, OBSTIME, LOCATION) == VEGA

    def test_jnow_matches_astropy_tete(self):
        ra, dec = to_mount_frame(*VEGA, EquatorialSystem.TOPOCENTRIC, OBSTIME, LOCATION)
        expected = SkyCoord(*VEGA, unit="deg", frame="icrs").transform_to(
            TETE(obstime=OBSTIME, location=LOCATION)
        )
        assert _sep_arcsec((ra, dec), (expected.ra.deg, expected.dec.deg)) < 1e-3

    def test_jnow_offset_is_the_expected_precession(self):
        """J2000 sent to a JNow mount would miss by roughly 20 arcminutes."""
        converted = to_mount_frame(
            *VEGA, EquatorialSystem.TOPOCENTRIC, OBSTIME, LOCATION
        )
        offset = _sep_arcsec(VEGA, converted)
        assert 300 < offset < 1500

    def test_ra_is_wrapped_to_0_360(self):
        ra, _ = to_mount_frame(359.99, 0.0, EquatorialSystem.TOPOCENTRIC, OBSTIME, None)
        assert 0.0 <= ra < 360.0

    @pytest.mark.parametrize(
        "system",
        [EquatorialSystem.TOPOCENTRIC, EquatorialSystem.J2050, EquatorialSystem.B1950],
    )
    def test_roundtrip(self, system):
        ra, dec = to_mount_frame(*VEGA, system, OBSTIME, LOCATION)
        back = from_mount_frame(ra, dec, system, OBSTIME, LOCATION)
        assert _sep_arcsec(VEGA, back) < 1e-3
