import math
from datetime import datetime

import astropy.units as u
import numpy as np
import pandas as pd
import pytest
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.table import Table
from astropy.time import Time, TimeDelta

from astra.utils import (
    NotMovingBodyError,
    __to_format,
    compute_nonsidereal_rates_from_interp,
    get_body_coordinates,
    getLightTravelTimes,
    interpolate_dfs,
    is_solar_system_body,
    is_sun_rising,
    precompute_ephemeris,
    time_conversion,
    to_jd,
)


def test_single_dataframe_exact_index():
    idx = np.array([1, 2, 3])
    df = pd.DataFrame({"a": [10, 20, 30]}, index=idx)
    result = interpolate_dfs(idx, df)
    expected = df
    pd.testing.assert_frame_equal(result, expected)


def test_single_dataframe_interpolation():
    idx = np.array([1, 2, 3])
    df = pd.DataFrame({"a": [10, 30]}, index=[1, 3])
    result = interpolate_dfs(idx, df)
    expected = pd.DataFrame({"a": [10.0, 20.0, 30.0]}, index=idx)
    pd.testing.assert_frame_equal(result, expected)


def test_multiple_dataframes_merge_and_interpolate():
    idx = np.array([1, 2, 3])
    df1 = pd.DataFrame({"a": [0, 2]}, index=[1, 3])
    df2 = pd.DataFrame({"b": [10, 30]}, index=[1, 3])
    result = interpolate_dfs(idx, df1, df2)
    expected = pd.DataFrame(
        {"a": [0.0, 1.0, 2.0], "b": [10.0, 20.0, 30.0]},
        index=idx,
    )
    pd.testing.assert_frame_equal(result, expected)


# def test_handles_duplicate_indices():
#     idx = np.array([1, 2, 3])
#     df = pd.DataFrame({"a": [10, 20, 30, 40]}, index=[1, 1, 2, 3])
#     result = interpolate_dfs(idx, df)
#     expected = pd.DataFrame({"a": [10.0, 20.0, 30.0]}, index=idx)
#     pd.testing.assert_frame_equal(result, expected)


# def test_empty_dataframe_input():
#     idx = np.array([1, 2, 3])
#     result = interpolate_dfs(idx)
#     expected = pd.DataFrame(index=idx)
#     pd.testing.assert_frame_equal(result, expected)


def test___to_format_jd_identity():
    jd = 2451545.0
    assert __to_format(jd, "jd") == jd


def test___to_format_mjd():
    jd = 2451545.0
    expected = jd - 2400000.5
    assert __to_format(jd, "mjd") == expected


def test___to_format_rjd():
    jd = 2451545.0
    expected = jd - 2400000
    assert __to_format(jd, "rjd") == expected


def test___to_format_invalid():
    jd = 2451545.0
    with pytest.raises(ValueError, match="Invalid Format"):
        __to_format(jd, "badfmt")


def test_to_jd_epoch_reference():
    dt = datetime(2000, 1, 1, 12, 0, 0)  # J2000.0 epoch
    jd = to_jd(dt, "jd")
    assert math.isclose(jd, 2451545.0, rel_tol=1e-9)


def test_to_jd_with_time_fraction():
    dt = datetime(2000, 1, 1, 18, 0, 0)  # 6 hours later
    jd = to_jd(dt, "jd")
    assert math.isclose(jd, 2451545.25, rel_tol=1e-9)


def test_to_jd_mjd_output():
    dt = datetime(2000, 1, 1, 12, 0, 0)
    mjd = to_jd(dt, "mjd")
    assert math.isclose(mjd, 51544.5, rel_tol=1e-9)


def test_to_jd_rjd_output():
    dt = datetime(2000, 1, 1, 12, 0, 0)
    rjd = to_jd(dt, "rjd")
    assert math.isclose(rjd, 51545.0, rel_tol=1e-9)


##


def test_getLightTravelTimes_returns_tuple():
    loc = EarthLocation.of_site("greenwich")
    time = Time(2451545.0, format="jd", scale="utc", location=loc)
    target = SkyCoord(ra=10 * u.deg, dec=20 * u.deg, frame="icrs")

    ltt_bary, ltt_helio = getLightTravelTimes(target, time)

    assert isinstance(ltt_bary, TimeDelta)
    assert isinstance(ltt_helio, TimeDelta)
    assert np.isfinite(ltt_bary.to(u.s).value)
    assert np.isfinite(ltt_helio.to(u.s).value)


def test_time_conversion_shapes_and_types():
    loc = EarthLocation.of_site("greenwich")
    jd = 2451545.0
    target = SkyCoord(ra=10 * u.deg, dec=20 * u.deg, frame="icrs")

    hjd, bjd, lstsec, ha = time_conversion(jd, loc, target)

    assert isinstance(hjd, float)
    assert isinstance(bjd, float)
    assert isinstance(lstsec, float)
    assert isinstance(ha, str)


def test_time_conversion_reasonable_lst_range():
    loc = EarthLocation.of_site("greenwich")
    jd = 2451545.0
    target = SkyCoord(ra=0 * u.deg, dec=0 * u.deg, frame="icrs")

    _, _, lstsec, _ = time_conversion(jd, loc, target)

    assert 0 <= lstsec < 86400  # must be within one sidereal day


def test_time_conversion_hour_angle_format():
    loc = EarthLocation.of_site("greenwich")
    jd = 2451545.0
    target = SkyCoord(ra=0 * u.deg, dec=0 * u.deg, frame="icrs")

    _, _, _, ha = time_conversion(jd, loc, target)

    parts = ha.split()
    assert len(parts) == 3
    # each part should parse as float (can include decimals and signs)
    for part in parts:
        float(part)


def test_time_conversion_bjd_vs_hjd_difference():
    loc = EarthLocation.of_site("greenwich")
    jd = 2451545.0
    target = SkyCoord(ra=100 * u.deg, dec=20 * u.deg, frame="icrs")

    hjd, bjd, _, _ = time_conversion(jd, loc, target)

    # BJD and HJD should differ slightly but not be identical
    assert not np.isclose(hjd, bjd, rtol=0, atol=0)


@pytest.fixture
def base_inputs():
    hdr = {
        "DATE-OBS": "2000-01-01T12:00:00",
        "EXPTIME": 60.0,
        "ALTITUDE": 45.0,  # degrees
    }
    location = EarthLocation.of_site("greenwich")
    target = SkyCoord(ra=10 * u.deg, dec=20 * u.deg, frame="icrs")
    return hdr, location, target


@pytest.fixture
def location():
    # Greenwich Observatory
    return EarthLocation.of_site("greenwich")


def test_returns_expected_types(location):
    rising, flat_ready, position = is_sun_rising(location)
    assert isinstance(rising, bool)
    assert isinstance(flat_ready, bool)
    assert isinstance(position, SkyCoord)


def test_flat_ready_condition(location, monkeypatch):
    # Force sun altitude into twilight range (-6 degrees)
    class DummyAlt:
        deg = -6.0
        degree = -6.0

    class DummyAltAz:
        alt = DummyAlt()

    def fake_get_sun(time):
        return type("Dummy", (), {"transform_to": lambda self, frame: DummyAltAz()})()

    monkeypatch.setattr("astra.utils.get_sun", fake_get_sun)

    rising, flat_ready, position = is_sun_rising(location)
    assert flat_ready is True


def test_not_flat_ready_outside_range(location, monkeypatch):
    # Force sun altitude = -20 deg (too low)
    class DummyAlt:
        deg = -20.0
        degree = -20.0

    class DummyAltAz:
        alt = DummyAlt()

    def fake_get_sun(time):
        return type("Dummy", (), {"transform_to": lambda self, frame: DummyAltAz()})()

    monkeypatch.setattr("astra.utils.get_sun", fake_get_sun)

    rising, flat_ready, position = is_sun_rising(location)
    assert flat_ready is False


def test_rising_detection(location, monkeypatch):
    # Return alt -10 deg now, -9 deg in 5 min -> rising
    class DummyAlt:
        def __init__(self, degree):
            self.degree = degree
            self.deg = degree

    class DummyAltAz:
        def __init__(self, degree):
            self.alt = DummyAlt(degree)

    values = iter([-10.0, -9.0])

    def fake_get_sun(time):
        return type(
            "Dummy", (), {"transform_to": lambda self, frame: DummyAltAz(next(values))}
        )()

    monkeypatch.setattr("astra.utils.get_sun", fake_get_sun)

    rising, _, _ = is_sun_rising(location)
    assert rising is True


def test_setting_detection(location, monkeypatch):
    # Return alt -5 deg now, -6 deg later -> setting
    class DummyAlt:
        def __init__(self, degree):
            self.degree = degree
            self.deg = degree

    class DummyAltAz:
        def __init__(self, degree):
            self.alt = DummyAlt(degree)

    values = iter([-5.0, -6.0])

    def fake_get_sun(time):
        return type(
            "Dummy", (), {"transform_to": lambda self, frame: DummyAltAz(next(values))}
        )()

    monkeypatch.setattr("astra.utils.get_sun", fake_get_sun)

    rising, _, _ = is_sun_rising(location)
    assert rising is False


def test_get_body_coordinates_solar_system(location, monkeypatch):
    # Mock return value
    expected_coord = SkyCoord(ra=100 * u.deg, dec=20 * u.deg)

    # Mock get_body to avoid ephemeris calculation/download
    monkeypatch.setattr("astra.utils.get_body", lambda name, time, loc: expected_coord)

    # Test with a known solar system object
    result = get_body_coordinates("Jupiter", Time.now(), location)

    assert result.ra == expected_coord.ra
    assert result.dec == expected_coord.dec


def test_get_body_coordinates_deep_sky(location, monkeypatch):
    import astropy.coordinates

    # Mock return value
    expected_coord = SkyCoord(ra=10.68 * u.deg, dec=41.27 * u.deg)

    # Mock SkyCoord.from_name to avoid SIMBAD query
    monkeypatch.setattr(
        astropy.coordinates.SkyCoord, "from_name", lambda name: expected_coord
    )

    # Test with a known deep sky object
    result = get_body_coordinates("M31", Time.now(), location)

    assert result.ra == expected_coord.ra
    assert result.dec == expected_coord.dec


def test_get_body_coordinates_solar_system_case_insensitive(location, monkeypatch):
    expected_coord = SkyCoord(ra=200 * u.deg, dec=-10 * u.deg)

    monkeypatch.setattr("astra.utils.get_body", lambda name, time, loc: expected_coord)

    # Test with uppercase
    result = get_body_coordinates("MARS", Time.now(), location)

    assert result.ra == expected_coord.ra
    assert result.dec == expected_coord.dec


# --- Non-sidereal tracking helpers ---


def test_is_solar_system_body_known_bodies():
    assert is_solar_system_body("mars") is True
    assert is_solar_system_body("jupiter") is True
    assert is_solar_system_body("moon") is True
    assert is_solar_system_body("sun") is True


def test_is_solar_system_body_case_insensitive():
    assert is_solar_system_body("MARS") is True
    assert is_solar_system_body("Jupiter") is True


def test_is_solar_system_body_deep_sky():
    assert is_solar_system_body("M31") is False
    assert is_solar_system_body("Vega") is False
    assert is_solar_system_body("") is False


def test_precompute_ephemeris_returns_callables(location):
    obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris("mars", obs_time, 1.0, location)
    assert callable(ra_interp)
    assert callable(dec_interp)


def test_precompute_ephemeris_return_rates_from_horizons(location, monkeypatch):
    obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")

    class FakeHorizons:
        def __init__(self, id, location, epochs):
            self.id = id

        def ephemerides(self, optional_settings=None):
            return Table(
                {
                    "RA": [10.0, 10.1, 10.2, 10.3],
                    "DEC": [20.0, 20.1, 20.2, 20.3],
                    "datetime_jd": [
                        obs_time.jd,
                        obs_time.jd + 1.0 / 24.0,
                        obs_time.jd + 2.0 / 24.0,
                        obs_time.jd + 3.0 / 24.0,
                    ],
                    "RA_rate": [5400.0, 5400.0, 5400.0, 5400.0],
                    "DEC_rate": [7200.0, 7200.0, 7200.0, 7200.0],
                }
            )

    monkeypatch.setattr("astroquery.jplhorizons.Horizons", FakeHorizons)

    ra_interp, dec_interp, ra_rate_interp, dec_rate_interp = precompute_ephemeris(
        "90000033",
        obs_time,
        3.0,
        location,
        return_rates=True,
    )

    assert callable(ra_interp)
    assert callable(dec_interp)
    assert callable(ra_rate_interp)
    assert callable(dec_rate_interp)
    assert float(ra_rate_interp(0.0)) == pytest.approx(0.0997, abs=1e-3)
    assert float(dec_rate_interp(0.0)) == pytest.approx(1.9945, abs=1e-3)


def test_precompute_ephemeris_ra_dec_ranges(location):
    obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris("mars", obs_time, 1.0, location)

    # Sample several points across the window
    for t in [0, 1800, 3600]:
        ra = float(ra_interp(t))
        dec = float(dec_interp(t))
        assert -180.0 <= ra <= 180.0, f"RA out of range at t={t}: {ra}"
        assert -90.0 <= dec <= 90.0, f"Dec out of range at t={t}: {dec}"


def test_precompute_ephemeris_continuity(location):
    obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris("mars", obs_time, 1.0, location)

    # Adjacent-second values should be very close (no discontinuity)
    for t in [0, 1800]:
        assert abs(float(ra_interp(t + 1)) - float(ra_interp(t))) < 0.01
        assert abs(float(dec_interp(t + 1)) - float(dec_interp(t))) < 0.01


def test_nonsidereal_rates_mars_plausible(location):
    obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris("mars", obs_time, 1.0, location)
    ra_rate, dec_rate = compute_nonsidereal_rates_from_interp(ra_interp, dec_interp, 0)

    # Mars moves slowly; rates should be small but non-zero
    assert abs(ra_rate) < 0.005, f"Mars RA rate unexpectedly large: {ra_rate}"
    assert abs(dec_rate) < 0.05, f"Mars Dec rate unexpectedly large: {dec_rate}"
    assert ra_rate != 0.0 or dec_rate != 0.0, "Both rates are zero — unexpected"


def test_nonsidereal_rates_moon_faster_than_mars(location):
    obs_time = Time("2025-06-01T00:00:00", format="isot", scale="utc")
    ra_mars, dec_mars = precompute_ephemeris("mars", obs_time, 1.0, location)
    ra_moon, dec_moon = precompute_ephemeris("moon", obs_time, 1.0, location)

    mars_ra_rate, _ = compute_nonsidereal_rates_from_interp(ra_mars, dec_mars, 0)
    moon_ra_rate, _ = compute_nonsidereal_rates_from_interp(ra_moon, dec_moon, 0)

    assert abs(moon_ra_rate) > abs(mars_ra_rate), (
        f"Moon RA rate ({moon_ra_rate}) should exceed Mars rate ({mars_ra_rate})"
    )


@pytest.mark.network
def test_precompute_ephemeris_minor_body(location):
    """precompute_ephemeris should resolve a JPL Horizons minor body via astroquery."""
    obs_time = Time("2025-01-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris(
        "90000033", obs_time, duration_hours=1.0, obs_location=location
    )

    assert callable(ra_interp)
    assert callable(dec_interp)

    # Spot-check a few points across the window
    for t in [0, 1800, 3600]:
        ra = float(ra_interp(t))
        dec = float(dec_interp(t))
        assert -360.0 <= ra <= 360.0, f"RA out of range at t={t}: {ra}"
        assert -90.0 <= dec <= 90.0, f"Dec out of range at t={t}: {dec}"

    # Adjacent-second values should be smooth (no discontinuity)
    assert abs(float(ra_interp(1)) - float(ra_interp(0))) < 0.01
    assert abs(float(dec_interp(1)) - float(dec_interp(0))) < 0.01

@pytest.mark.network
def test_precompute_ephemeris_tle(location):
    """precompute_ephemeris should resolve a TLE via astroquery.jplhorizons."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    tle_data = "1 25544U 98067A   26084.45430866  .00012951  00000-0  24673-3 0  9999\n2 25544  51.6344 354.4276 0006215 231.1671 128.8763 15.48531543558777"
    ra_interp, dec_interp = precompute_ephemeris(
        "TLE", obs_time, duration_hours=1.0, obs_location=location, tle_data=tle_data)
    assert callable(ra_interp)
    assert callable(dec_interp)

    # Spot-check a few points across the window
    for t in [0, 1800, 3600]:
        ra = float(ra_interp(t))
        dec = float(dec_interp(t))
        assert -360.0 <= ra <= 360.0, f"RA out of range at t={t}: {ra}"
        assert -90.0 <= dec <= 90.0, f"Dec out of range at t={t}: {dec}"

@pytest.mark.network
def test_precompute_ephemeris_tle_missing_data(location):
    """Should raise ValueError if body_name is 'TLE' but tle_data is None."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    
    with pytest.raises(ValueError, match="tle_data parameter is required"):
        precompute_ephemeris("TLE", obs_time, 1.0, location)

@pytest.mark.network
def test_precompute_ephemeris_tle_case_insensitive(location):
    """Should accept 'TLE', 'tle', 'Tle' etc."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    tle_data = "1 25544U 98067A   26084.45430866  .00012951  00000-0  24673-3 0  9999\n2 25544  51.6344 354.4276 0006215 231.1671 128.8763 15.48531543558777"
    
    for name_variant in ["TLE", "tle", "Tle"]:
        ra_interp, dec_interp = precompute_ephemeris(
            name_variant, obs_time, 1.0, location, tle_data=tle_data
        )
        assert callable(ra_interp)
        assert callable(dec_interp)

@pytest.mark.network
def test_precompute_ephemeris_tle_invalid_data(location):
    """Should raise NotMovingBodyError for malformed TLE data."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    invalid_tle = "this is not valid tle data"
    
    with pytest.raises(NotMovingBodyError):
        precompute_ephemeris("TLE", obs_time, 1.0, location, tle_data=invalid_tle)

@pytest.mark.network
def test_precompute_ephemeris_tle_rates_plausible(location):
    """ISS (TLE) should have appreciable tracking rates."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    tle_data = "1 25544U 98067A   26084.45430866  .00012951  00000-0  24673-3 0  9999\n2 25544  51.6344 354.4276 0006215 231.1671 128.8763 15.48531543558777"
    
    ra_interp, dec_interp = precompute_ephemeris(
        "TLE", obs_time, 1.0, location, tle_data=tle_data
    )
    ra_rate, dec_rate = compute_nonsidereal_rates_from_interp(ra_interp, dec_interp, 0)
    
    # ISS moves much faster than planets
    assert abs(ra_rate) > 0.01, "ISS should have significant RA rate"

@pytest.mark.network
def test_precompute_ephemeris_minor_body_fallback(location):
    """Should fallback to id_type='smallbody' for objects like comets."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris(
        "C/2023 A3", obs_time, 1.0, location  # Comet example
    )
    assert callable(ra_interp)
    assert callable(dec_interp)

@pytest.mark.network
def test_precompute_ephemeris_unresolvable_body(location):
    """Should raise NotMovingBodyError with descriptive message."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    
    with pytest.raises(NotMovingBodyError, match="could not be resolved"):
        precompute_ephemeris("12345INVALID", obs_time, 1.0, location)

@pytest.mark.network
def test_precompute_ephemeris_short_interval(location):
    """Should handle very short observation windows with fine resolution."""
    obs_time = Time("2026-04-01T00:00:00", format="isot", scale="utc")
    ra_interp, dec_interp = precompute_ephemeris(
        "mars", obs_time, duration_hours=0.1, interval_minutes=0.5, obs_location=location
    )
    assert callable(ra_interp)
    assert callable(dec_interp)
    
    # Should produce consistent values
    t1 = float(ra_interp(0))
    t2 = float(ra_interp(60))
    assert abs(t2 - t1) < 0.1  # Should change smoothly

@pytest.mark.network
def test_precompute_ephemeris_different_tle_objects(location):
    """Should work with different satellite TLEs."""
    obs_time = Time("2010-01-01T00:00:00", format="isot", scale="utc")
    
    # Example: Hubble Space Telescope TLE
    tle_hst = "1 20580U 90037B   10001.00000000  .00001524  00000-0  75821-5 0  8017\n2 20580  28.4698 279.0467 0002853 247.4627 112.6160 15.09299652680691"
    
    ra_interp, dec_interp = precompute_ephemeris(
        "TLE", obs_time, 1.0, location, tle_data=tle_hst
    )
    assert callable(ra_interp)
    assert callable(dec_interp)