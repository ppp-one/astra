"""Test visibility check functionality for ObjectActionConfig."""

from datetime import UTC, datetime, timedelta

import astropy.units as u
import pytest
from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time

from astra.action_configs import ObjectActionConfig


@pytest.fixture
def observatory_location():
    """Create an observatory location (example: somewhere in Chile)."""
    return EarthLocation(
        lat=-24.625 * u.deg,
        lon=-70.403 * u.deg,
        height=2400 * u.m,
    )


@pytest.fixture
def observation_times():
    """Create start and end times for observations."""
    start_time = Time(datetime.now(UTC))
    end_time = Time(datetime.now(UTC) + timedelta(hours=2))
    return start_time, end_time


@pytest.fixture
def visible_target_coords(observatory_location, observation_times):
    """Find a target that's actually visible (near zenith) at the observation time."""
    start_time, _ = observation_times

    # Get near zenith coordinates at observation time
    altaz_frame = AltAz(obstime=start_time, location=observatory_location)
    near_zenith = SkyCoord(alt=85 * u.deg, az=0 * u.deg, frame=altaz_frame)
    near_zenith_radec = near_zenith.transform_to("icrs")

    return near_zenith_radec.ra.deg, near_zenith_radec.dec.deg


@pytest.fixture
def invisible_target_coords(observatory_location, observation_times):
    """Find a target that's below the horizon at the observation time."""
    start_time, _ = observation_times

    # Get coordinates well below horizon (nadir direction)
    altaz_frame = AltAz(obstime=start_time, location=observatory_location)
    below_horizon = SkyCoord(alt=-45 * u.deg, az=180 * u.deg, frame=altaz_frame)
    below_radec = below_horizon.transform_to("icrs")

    return below_radec.ra.deg, below_radec.dec.deg


def test_visible_target_passes_validation(
    observatory_location, observation_times, visible_target_coords
):
    """Test that a visible target passes validation."""
    start_time, end_time = observation_times
    ra, dec = visible_target_coords

    config_visible = ObjectActionConfig(
        object="Visible Target",
        exptime=60.0,
        ra=ra,
        dec=dec,
    )

    # Should not raise any exception
    config_visible.validate_visibility(
        start_time=start_time,
        end_time=end_time,
        observatory_location=observatory_location,
        min_altitude=0.0,
    )


def test_invisible_target_fails_validation(
    observatory_location, observation_times, invisible_target_coords
):
    """Test that an invisible target (below horizon) fails validation."""
    start_time, end_time = observation_times
    ra, dec = invisible_target_coords

    config_invisible = ObjectActionConfig(
        object="Invisible Target",
        exptime=60.0,
        ra=ra,
        dec=dec,
    )

    # Should raise ValueError with visibility information
    with pytest.raises(ValueError, match="is not visible during observation window"):
        config_invisible.validate_visibility(
            start_time=start_time,
            end_time=end_time,
            observatory_location=observatory_location,
            min_altitude=0.0,
        )


def test_no_coordinates_skips_validation(observatory_location, observation_times):
    """Test that targets without RA/Dec skip visibility check."""
    start_time, end_time = observation_times

    config_no_coords = ObjectActionConfig(
        object="No Coords Target",
        exptime=60.0,
    )

    # Should not raise any exception (check is skipped)
    config_no_coords.validate_visibility(
        start_time=start_time,
        end_time=end_time,
        observatory_location=observatory_location,
        min_altitude=0.0,
    )


def test_visibility_check_with_custom_min_altitude(
    observatory_location, observation_times, visible_target_coords
):
    """Test visibility check with custom minimum altitude."""
    start_time, end_time = observation_times
    ra, dec = visible_target_coords

    config = ObjectActionConfig(
        object="Low Target",
        exptime=60.0,
        ra=ra,
        dec=dec,
    )

    # Should pass with min_altitude=0
    config.validate_visibility(
        start_time=start_time,
        end_time=end_time,
        observatory_location=observatory_location,
        min_altitude=0.0,
    )

    # Should fail with very high min_altitude since target is at ~85° not 89°
    with pytest.raises(ValueError, match="is not visible during observation window"):
        config.validate_visibility(
            start_time=start_time,
            end_time=end_time,
            observatory_location=observatory_location,
            min_altitude=89.0,  # Very high minimum
        )


def test_visibility_error_message_format(
    observatory_location, observation_times, invisible_target_coords
):
    """Test that visibility error messages contain useful information."""
    start_time, end_time = observation_times
    ra, dec = invisible_target_coords

    config = ObjectActionConfig(
        object="Test Target",
        exptime=60.0,
        ra=ra,
        dec=dec,
    )

    with pytest.raises(ValueError) as exc_info:
        config.validate_visibility(
            start_time=start_time,
            end_time=end_time,
            observatory_location=observatory_location,
            min_altitude=0.0,
        )

    error_message = str(exc_info.value)
    # Check that error message contains important information
    assert "Test Target" in error_message
    assert "altitude" in error_message.lower()
    # Check for the RA/Dec values (format may vary)
    assert str(round(ra, 2)) in error_message or str(round(ra, 1)) in error_message
    assert str(round(dec, 2)) in error_message or str(round(dec, 1)) in error_message


def test_visibility_resolution_from_altaz(observatory_location, observation_times):
    """Test visibility check when resolving from Alt/Az."""
    start_time, end_time = observation_times

    # Define an Alt/Az that is currently below horizon (-20 deg)
    alt = -20.0
    az = 180.0

    config = ObjectActionConfig(object="AltAz Target", exptime=60.0, alt=alt, az=az)

    # Should fail validation because the resolved target is below horizon
    with pytest.raises(ValueError) as exc_info:
        config.validate_visibility(
            start_time=start_time,
            end_time=end_time,
            observatory_location=observatory_location,
            min_altitude=0.0,
        )

    error_msg = str(exc_info.value)
    # The error message should report RA/Dec coordinates, not just "None"
    # because the code computed them
    assert "RA=" in error_msg
    assert "Dec=" in error_msg
    assert "altitude -20" in error_msg


def test_visibility_resolution_from_lookup_name(
    observatory_location, observation_times
):
    """Test visibility check when resolving from lookup_name."""
    start_time, end_time = observation_times

    # We mock coordinate resolution to ensure deterministic behavior and speed.
    # This avoids:
    # 1. Network calls to SIMBAD for deep sky objects.
    # 2. Complex ephemeris calculations for solar system bodies.
    # 3. Flakiness due to changing object positions over time.

    from unittest.mock import MagicMock

    import astropy.units as u
    from astropy.coordinates import SkyCoord

    # The method-under-test imports get_body_coordinates internally:
    #     from astra.utils import get_body_coordinates
    # To intercept this, we must ensure 'astra.utils' is loaded in sys.modules
    # and patch the function on the module object itself.

    # 1. Define coordinates that are guaranteed to be known:
    # Start with a Visible target near Zenith (Alt=85 degrees)
    # We calculate the ICRS RA/Dec for this Alt/Az position at the specific test time.
    altaz_frame = AltAz(obstime=start_time, location=observatory_location)
    zenith = SkyCoord(alt=85 * u.deg, az=0 * u.deg, frame=altaz_frame)
    zenith_radec = zenith.transform_to("icrs")

    mock_get_body = MagicMock(return_value=zenith_radec)

    import astra.utils

    original_func = astra.utils.get_body_coordinates
    astra.utils.get_body_coordinates = mock_get_body

    try:
        config = ObjectActionConfig(
            object="Mock Body", exptime=60.0, lookup_name="MockObject"
        )

        # Should pass validation (visible)
        config.validate_visibility(
            start_time=start_time,
            end_time=end_time,
            observatory_location=observatory_location,
            min_altitude=0.0,
        )

        # Verify our mock was actually used
        mock_get_body.assert_called_once()
        args, kwargs = mock_get_body.call_args
        assert kwargs["body_name"] == "MockObject"

        # 2. Test failure case (Invisible object)
        # Update mock to return coordinates below the horizon (Alt=-45 degrees)
        below_horizon = SkyCoord(alt=-45 * u.deg, az=180 * u.deg, frame=altaz_frame)
        below_radec = below_horizon.transform_to("icrs")
        mock_get_body.reset_mock()
        mock_get_body.return_value = below_radec

        config_invisible = ObjectActionConfig(
            object="Invisible Mock Body", exptime=60.0, lookup_name="InvisibleObject"
        )

        with pytest.raises(
            ValueError, match="is not visible during observation window"
        ):
            config_invisible.validate_visibility(
                start_time=start_time,
                end_time=end_time,
                observatory_location=observatory_location,
                min_altitude=0.0,
            )

    finally:
        # Restore the original function to avoid side effects on other tests
        astra.utils.get_body_coordinates = original_func
