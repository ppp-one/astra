"""Test visibility check functionality for ObjectActionConfig."""

import unittest.mock
from datetime import UTC, datetime, timedelta

import astropy.units as u
import numpy as np
import pytest
from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time

from astra.action_configs import ObjectActionConfig, ephemeris_window_hours
from astra.utils.ephemeris import NotMovingBodyError


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

    # 1. Define coordinates that are guaranteed to be known:
    # Start with a Visible target near Zenith (Alt=85 degrees)
    # We calculate the ICRS RA/Dec for this Alt/Az position at the specific test time.
    altaz_frame = AltAz(obstime=start_time, location=observatory_location)
    zenith = SkyCoord(alt=85 * u.deg, az=0 * u.deg, frame=altaz_frame)
    zenith_radec = zenith.transform_to("icrs")

    mock_get_body = MagicMock(return_value=zenith_radec)

    import astra.action_configs

    with (
        unittest.mock.patch.object(
            astra.action_configs, "get_body_coordinates", mock_get_body
        ),
        unittest.mock.patch.object(
            astra.action_configs,
            "precompute_ephemeris",
            side_effect=NotMovingBodyError("mock"),
        ),
    ):
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
        _, kwargs = mock_get_body.call_args
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

    with (
        unittest.mock.patch.object(
            astra.action_configs, "get_body_coordinates", mock_get_body
        ),
        unittest.mock.patch.object(
            astra.action_configs,
            "precompute_ephemeris",
            side_effect=NotMovingBodyError("mock"),
        ),
    ):
        with pytest.raises(
            ValueError, match="is not visible during observation window"
        ):
            config_invisible.validate_visibility(
                start_time=start_time,
                end_time=end_time,
                observatory_location=observatory_location,
                min_altitude=0.0,
            )


class TestNonsiderealCapabilityGate:
    """validate_visibility gates on whether the name resolved to a moving body.

    ``nonsidereal_supported`` reflects whether any telescope in the observatory
    reports ASCOM CanSetRightAscensionRate and CanSetDeclinationRate.
    """

    def _config(self, **kwargs):
        fields = {"lookup_name": "mars", **kwargs}
        return ObjectActionConfig(object="Test Target", exptime=60.0, **fields)

    def _validate(self, config, observatory_location, observation_times, **kwargs):
        start_time, end_time = observation_times
        config.validate_visibility(
            start_time=start_time,
            end_time=end_time,
            observatory_location=observatory_location,
            min_altitude=0.0,
            **kwargs,
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"nonsidereal_recenter_interval": 300},
            {"lookup_name": "TLE", "tle": "1 25544U ...\n2 ..."},
        ],
        ids=["bare_lookup_name", "recenter_interval_set", "tle_supplied"],
    )
    def test_moving_body_on_unsupported_mount_raises(
        self, observatory_location, observation_times, kwargs
    ):
        """A moving target that no mount can track is a schedule error.

        This does not depend on how the action was written: recenter_interval is a
        cadence knob with a default of 0, so requiring it here would let a comet
        scheduled with default settings degrade to sidereal in silence.
        """
        import astra.action_configs

        interp = unittest.mock.MagicMock(return_value=0.0)
        with unittest.mock.patch.object(
            astra.action_configs,
            "precompute_ephemeris",
            return_value=(interp, interp, interp, interp),
        ):
            config = self._config(**kwargs)
            with pytest.raises(ValueError, match="differential tracking rates"):
                self._validate(
                    config,
                    observatory_location,
                    observation_times,
                    nonsidereal_supported=False,
                )

    def test_fixed_target_on_unsupported_mount_is_fine(
        self, observatory_location, observation_times
    ):
        """A name that resolves as fixed needs no rates, so it must not be rejected."""
        import astra.action_configs

        zenith = SkyCoord(
            alt=85 * u.deg,
            az=180 * u.deg,
            frame=AltAz(obstime=observation_times[0], location=observatory_location),
        ).transform_to("icrs")

        with (
            unittest.mock.patch.object(
                astra.action_configs,
                "precompute_ephemeris",
                side_effect=NotMovingBodyError("fixed"),
            ),
            unittest.mock.patch.object(
                astra.action_configs,
                "get_body_coordinates",
                unittest.mock.MagicMock(return_value=zenith),
            ),
        ):
            config = self._config()
            self._validate(
                config,
                observatory_location,
                observation_times,
                nonsidereal_supported=False,
            )

        assert config._nonsidereal is False

    def test_supported_mount_allows_moving_body(
        self, observatory_location, observation_times
    ):
        import astra.action_configs

        interp = unittest.mock.MagicMock(return_value=0.0)
        with unittest.mock.patch.object(
            astra.action_configs,
            "precompute_ephemeris",
            return_value=(interp, interp, interp, interp),
        ) as precompute:
            config = self._config()
            try:
                self._validate(
                    config,
                    observatory_location,
                    observation_times,
                    nonsidereal_supported=True,
                )
            except ValueError as e:
                assert "differential tracking rates" not in str(e)

        precompute.assert_called_once()
        assert config._nonsidereal is True


class TestAltitudeSampling:
    """The altitude check samples the whole window, not only three moments.

    A target whose altitude changes monotonically is described by its start,
    middle and end. A satellite is not: it rises and sets several times in a long
    window, so it can be above the horizon at those three moments and below it for
    most of the sequence.
    """

    @staticmethod
    def _ephemeris_for(alt_profile, start_time, observatory_location):
        """Return RA and Dec interpolators tracing a chosen altitude profile.

        Args:
            alt_profile: Callable mapping elapsed seconds to altitude in degrees.
            start_time: Epoch the interpolators are keyed to.
            observatory_location: Observer location.

        Returns:
            (ra_interp, dec_interp), each taking elapsed seconds and returning
            degrees, as the real ephemeris interpolators do.
        """

        def coords(elapsed):
            scalar = np.ndim(elapsed) == 0
            elapsed = np.atleast_1d(np.asarray(elapsed, dtype=float))
            times = start_time + elapsed * u.s
            radec = SkyCoord(
                alt=u.Quantity(alt_profile(elapsed), u.deg),
                az=u.Quantity(np.full(elapsed.shape, 180.0), u.deg),
                frame=AltAz(obstime=times, location=observatory_location),
            ).transform_to("icrs")
            return radec, scalar

        def ra_interp(elapsed):
            radec, scalar = coords(elapsed)
            return radec.ra.deg[0] if scalar else radec.ra.deg

        def dec_interp(elapsed):
            radec, scalar = coords(elapsed)
            return radec.dec.deg[0] if scalar else radec.dec.deg

        return ra_interp, dec_interp

    def _validate(
        self, alt_profile, start_time, end_time, observatory_location, min_altitude=0.0
    ):
        """Validate a satellite action whose ephemeris follows alt_profile."""
        config = ObjectActionConfig(
            object="Moving Target", exptime=1.0, lookup_name="TLE", tle="1 ...\n2 ..."
        )
        ra_interp, dec_interp = self._ephemeris_for(
            alt_profile, start_time, observatory_location
        )
        rate = unittest.mock.MagicMock(return_value=0.0)
        with unittest.mock.patch(
            "astra.action_configs.precompute_ephemeris",
            return_value=(ra_interp, dec_interp, rate, rate),
        ):
            config.validate_visibility(
                start_time=start_time,
                end_time=end_time,
                observatory_location=observatory_location,
                min_altitude=min_altitude,
                nonsidereal_supported=True,
            )
        return config

    def test_target_below_horizon_between_the_sampled_ends_is_caught(
        self, observatory_location, observation_times
    ):
        """The case three samples cannot see.

        The profile puts the target at 20 degrees at the start, the middle and the
        end, and at -20 degrees a quarter and three quarters of the way through.
        """
        start_time, end_time = observation_times
        span_s = (end_time - start_time).to_value(u.s)

        def profile(elapsed):
            return 20.0 * np.cos(4.0 * np.pi * elapsed / span_s)

        # The three moments the old check looked at are all well above the horizon.
        assert profile(np.array([0.0, span_s / 2, span_s])).min() > 19.0

        with pytest.raises(ValueError) as exc_info:
            self._validate(profile, start_time, end_time, observatory_location)

        message = str(exc_info.value)
        assert "below the limit at" in message
        assert "worst -20" in message

    def test_start_and_end_are_reported_by_name(
        self, observatory_location, observation_times
    ):
        """The exact start and end are always sampled and named in the error."""
        start_time, end_time = observation_times
        span_s = (end_time - start_time).to_value(u.s)

        # Sets during the window: above the limit at the start, below it at the end.
        with pytest.raises(ValueError) as exc_info:
            self._validate(
                lambda elapsed: 30.0 - 40.0 * elapsed / span_s,
                start_time,
                end_time,
                observatory_location,
            )

        message = str(exc_info.value)
        assert "end: altitude -10" in message
        assert "start:" not in message

    def test_target_above_the_limit_throughout_passes(
        self, observatory_location, observation_times
    ):
        start_time, end_time = observation_times
        self._validate(
            lambda elapsed: np.full(np.shape(elapsed), 40.0),
            start_time,
            end_time,
            observatory_location,
        )

    def test_sample_times_span_the_window_at_a_fixed_cadence(self):
        """The first and last samples are the exact ends of the window."""
        start_time = Time("2026-09-08 00:00:00")
        end_time = start_time + 2 * u.hour
        elapsed, times = ObjectActionConfig._visibility_sample_times(
            start_time, end_time
        )

        assert elapsed[0] == 0.0
        assert elapsed[-1] == pytest.approx(7200.0)
        assert len(elapsed) == 241  # every 30 s
        assert times[0].isclose(start_time)
        assert times[-1].isclose(end_time)

    def test_sample_count_is_capped_for_a_long_window(self):
        start_time = Time("2026-09-08 00:00:00")
        elapsed, _ = ObjectActionConfig._visibility_sample_times(
            start_time, start_time + 30 * u.day
        )
        assert len(elapsed) == 2000


class TestMovingBodyWithFixedCoordinates:
    """A moving target cannot also carry a fixed coordinate.

    At runtime the mount is driven from the ephemeris and the fixed coordinate is
    ignored, so validating the fixed one reports on a position never visited.
    """

    def _moving(self):
        """Patch the resolver so the name comes back as a moving body."""
        interp = unittest.mock.MagicMock(return_value=0.0)
        return unittest.mock.patch(
            "astra.action_configs.precompute_ephemeris",
            return_value=(interp, interp, interp, interp),
        )

    @pytest.mark.parametrize(
        "coords",
        [{"ra": 10.0, "dec": 20.0}, {"alt": 80.0, "az": 0.0}],
        ids=["ra_dec", "alt_az"],
    )
    def test_moving_lookup_name_with_fixed_coordinates_is_rejected(
        self, observatory_location, observation_times, coords
    ):
        start_time, end_time = observation_times
        config = ObjectActionConfig(
            object="Mars", exptime=60.0, lookup_name="mars", **coords
        )
        with self._moving():
            with pytest.raises(ValueError, match="resolves to a moving body"):
                config.validate_visibility(
                    start_time=start_time,
                    end_time=end_time,
                    observatory_location=observatory_location,
                    min_altitude=0.0,
                    nonsidereal_supported=True,
                )

    def test_fixed_lookup_name_with_fixed_coordinates_is_allowed(
        self, observatory_location, observation_times, visible_target_coords
    ):
        """A star named alongside its coordinates stays valid."""
        start_time, end_time = observation_times
        ra, dec = visible_target_coords
        config = ObjectActionConfig(
            object="M31", exptime=60.0, lookup_name="M31", ra=ra, dec=dec
        )
        with unittest.mock.patch(
            "astra.action_configs.precompute_ephemeris",
            side_effect=NotMovingBodyError("fixed"),
        ):
            config.validate_visibility(
                start_time=start_time,
                end_time=end_time,
                observatory_location=observatory_location,
                min_altitude=0.0,
            )
        assert config._nonsidereal is False


class TestEphemerisWindow:
    """The ephemeris covers the action and a margin past its end.

    The margin is scaled to the action. A satellite pass is sampled every few
    seconds, so a fixed half-hour margin would be thousands of points that the
    sequence never reaches, and fetching them delays the schedule load.
    """

    @pytest.mark.parametrize(
        "minutes, expected_hours",
        [
            (1, 1 / 60 + 5 / 60),  # short: the five minute floor
            (60, 1.25),  # medium: a quarter of the action
            (600, 10.5),  # long: the half hour ceiling
        ],
        ids=["short", "medium", "long"],
    )
    def test_margin_scales_with_the_action(self, minutes, expected_hours):
        start_time = Time("2026-09-08 00:00:00")
        window = ephemeris_window_hours(start_time, start_time + minutes * u.min)
        assert window == pytest.approx(expected_hours)

    def test_never_longer_than_the_old_fixed_margin(self):
        """The window never grows, so no action pays more than it used to."""
        start_time = Time("2026-09-08 00:00:00")
        for minutes in (0, 1, 10, 60, 240, 600):
            end_time = start_time + minutes * u.min
            span_hours = minutes / 60
            assert ephemeris_window_hours(start_time, end_time) <= span_hours + 0.5


class TestDefaultObjectName:
    """object may be left out when lookup_name names the target."""

    ISS_TLE = (
        "1 25544U 98067A   24001.50000000  .00016717  00000-0  10270-3 0  9005\n"
        "2 25544  51.6400 208.9163 0006317  69.9862  25.2906 15.49560020432139"
    )

    def test_object_defaults_to_lookup_name(self):
        config = ObjectActionConfig(exptime=60.0, lookup_name="mars")
        assert config.object == "mars"

    def test_given_object_is_kept(self):
        config = ObjectActionConfig(object="Mars", exptime=60.0, lookup_name="mars")
        assert config.object == "Mars"

    def test_tle_object_defaults_to_norad_number(self):
        config = ObjectActionConfig(exptime=1.0, lookup_name="TLE", tle=self.ISS_TLE)
        assert config.object == "NORAD 25544"

    def test_tle_without_readable_line_1_is_rejected(self):
        with pytest.raises(ValueError, match="NORAD catalog number"):
            ObjectActionConfig(exptime=1.0, lookup_name="TLE", tle="not a tle\nat all")

    def test_no_object_and_no_lookup_name_is_rejected(self):
        with pytest.raises(ValueError, match="'object', or 'lookup_name'"):
            ObjectActionConfig(exptime=60.0, ra=10.0, dec=20.0)
