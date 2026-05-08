"""Unit tests for NonSiderealManager.

Covers the logic in nonsidereal.py that is not exercised by the integration test
in test_observatory_running_schedule.py:

  - _setup guard conditions (inactive by default, calibration, disable_telescope_movement,
    missing interpolators)
  - is_active property
  - apply_rates / reset_rates telescope interactions and error handling
  - should_recenter interval logic
  - recenter slew + rate refresh + timestamp update + error path
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import astropy.units as u
from scipy.interpolate import interp1d
from astropy.time import Time

from astra.nonsidereal import NonSiderealManager
from astra.utils import compute_nonsidereal_rates_from_interp
from astra.scheduler import Action


def _make_interp(slope=0.0, intercept=0.0):
    """Return a trivial linear interp1d (no real ephemeris needed)."""
    ts = np.array([0.0, 3600.0])
    vals = intercept + slope * ts
    return interp1d(ts, vals, kind="linear", fill_value="extrapolate")


def _make_action(
    action_type="object",
    nonsidereal=True,
    disable_telescope_movement=False,
    ra_interp=None,
    dec_interp=None,
    ra_rate_interp=None,
    dec_rate_interp=None,
    recenter_interval=0,
    lookup_name="mars",
    start_time=None,
):
    """Build a minimal Action suitable for NonSiderealManager._setup."""
    if start_time is None:
        start_time = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
    end_time = datetime(2025, 6, 1, 1, 0, 0, tzinfo=UTC)

    action_value = MagicMock()
    action_value.get = lambda key, default=None: {
        "_nonsidereal": nonsidereal,
        "disable_telescope_movement": disable_telescope_movement,
        "_ra_interp": ra_interp,
        "_dec_interp": dec_interp,
        "_ra_rate_interp": ra_rate_interp,
        "_dec_rate_interp": dec_rate_interp,
        "nonsidereal_recenter_interval": recenter_interval,
        "lookup_name": lookup_name,
    }.get(key, default)

    return Action(
        device_name="cam1",
        action_type=action_type,
        action_value=action_value,
        start_time=start_time,
        end_time=end_time,
    )


def _make_active_manager(recenter_interval=0, ra_slope=1e-4, dec_slope=0.0):
    ra_interp = _make_interp(slope=ra_slope, intercept=100.0)
    dec_interp = _make_interp(slope=dec_slope, intercept=20.0)
    action = _make_action(
        ra_interp=ra_interp,
        dec_interp=dec_interp,
        recenter_interval=recenter_interval,
    )
    return NonSiderealManager(action, MagicMock())


class TestSetup:
    def test_inactive_when_nonsidereal_false(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.is_active

    def test_inactive_for_calibration_action(self):
        action = _make_action(action_type="calibration")
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.is_active

    def test_inactive_when_telescope_movement_disabled(self):
        action = _make_action(disable_telescope_movement=True)
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.is_active

    def test_inactive_and_logs_error_when_interps_missing(self):
        logger = MagicMock()
        action = _make_action(ra_interp=None, dec_interp=None)
        mgr = NonSiderealManager(action, logger)
        assert not mgr.is_active
        logger.error.assert_called_once()

    def test_active_with_valid_config(self):
        mgr = _make_active_manager()
        assert mgr.is_active

    def test_active_logs_recenter_disabled(self):
        logger = MagicMock()
        action = _make_action(
            ra_interp=_make_interp(),
            dec_interp=_make_interp(),
            recenter_interval=0,
        )
        NonSiderealManager(action, logger)
        logger.info.assert_called_once()
        assert "re-centering disabled" in logger.info.call_args[0][0]

    def test_active_logs_recenter_interval(self):
        logger = MagicMock()
        action = _make_action(
            ra_interp=_make_interp(),
            dec_interp=_make_interp(),
            recenter_interval=300,
        )
        NonSiderealManager(action, logger)
        assert "300s" in logger.info.call_args[0][0]


class TestApplyRates:
    def test_sets_rates_on_telescope(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        assert telescope.set.call_count == 2
        keys_set = {c.args[0] for c in telescope.set.call_args_list}
        assert keys_set == {"RightAscensionRate", "DeclinationRate"}

    def test_noop_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        telescope = MagicMock()
        mgr.apply_rates(telescope)
        telescope.set.assert_not_called()

    def test_warns_and_does_not_raise_on_telescope_error(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        telescope.set.side_effect = RuntimeError("device offline")
        # Should not propagate
        mgr.apply_rates(telescope)
        mgr.logger.warning.assert_called_once()

    @patch("astra.utils.compute_nonsidereal_rates_from_interp")
    def test_uses_precomputed_rate_interpolators_when_available(self, rate_fn):
        action = _make_action(
            ra_interp=_make_interp(slope=1e-4, intercept=100.0),
            dec_interp=_make_interp(slope=0.0, intercept=20.0),
            ra_rate_interp=_make_interp(slope=0.0, intercept=0.123),
            dec_rate_interp=_make_interp(slope=0.0, intercept=-4.56),
        )
        mgr = NonSiderealManager(action, MagicMock())
        telescope = MagicMock()

        mgr.apply_rates(telescope)

        telescope.set.assert_any_call("RightAscensionRate", 0.123)
        telescope.set.assert_any_call("DeclinationRate", -4.56)
        rate_fn.assert_not_called()


class TestNonsiderealRateHelper:
    def test_compute_nonsidereal_rates_uses_sidereal_second_conversion(self):
        ra_interp = _make_interp(slope=1.0 / 60.0, intercept=0.0)
        dec_interp = _make_interp(slope=0.0, intercept=0.0)

        ra_rate, dec_rate = compute_nonsidereal_rates_from_interp(
            ra_interp,
            dec_interp,
            t_seconds=0.0,
            dt=60.0,
        )

        assert ra_rate == pytest.approx(4.010951637, rel=1e-9)
        assert dec_rate == pytest.approx(0.0, abs=1e-12)


class TestPrepointAndActivation:
    def test_prepoint_coordinates_return_offset_position(self):
        mgr = _make_active_manager(ra_slope=1e-3, dec_slope=2e-3)

        ra_deg, dec_deg = mgr.prepoint_coordinates(lead_time_seconds=60.0)

        assert ra_deg == pytest.approx((100.0 + 0.001 * 60.0) % 360.0)
        assert dec_deg == pytest.approx(20.0 + 0.002 * 60.0)

    def test_prepoint_coordinates_none_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.prepoint_coordinates() is None

    def test_tracking_activation_time_is_start_plus_offset(self):
        start = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
        action = _make_action(
            start_time=start,
            ra_interp=_make_interp(slope=1e-4, intercept=100.0),
            dec_interp=_make_interp(slope=0.0, intercept=20.0),
        )
        mgr = NonSiderealManager(action, MagicMock())

        activation_time = mgr.tracking_activation_time(lead_time_seconds=60.0)

        assert activation_time is not None
        assert activation_time.unix == pytest.approx((Time(start) + 60.0 * u.s).unix)

    def test_tracking_activation_time_none_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.tracking_activation_time() is None


class TestResetRates:
    def test_zeros_both_rates(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        mgr.reset_rates(telescope)
        telescope.set.assert_any_call("RightAscensionRate", 0.0)
        telescope.set.assert_any_call("DeclinationRate", 0.0)

    def test_noop_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        telescope = MagicMock()
        mgr.reset_rates(telescope)
        telescope.set.assert_not_called()

    def test_warns_and_does_not_raise_on_telescope_error(self):
        mgr = _make_active_manager()
        telescope = MagicMock()
        telescope.set.side_effect = RuntimeError("device offline")
        mgr.reset_rates(telescope)
        mgr.logger.warning.assert_called_once()


class TestShouldRecenter:
    def test_false_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert not mgr.should_recenter()

    def test_false_when_interval_is_zero(self):
        mgr = _make_active_manager(recenter_interval=0)
        assert not mgr.should_recenter()

    def test_false_before_interval_elapses(self):
        mgr = _make_active_manager(recenter_interval=300)
        # last_recenter_time was just set — well within 300 s
        assert not mgr.should_recenter()

    def test_true_after_interval_elapses(self):
        mgr = _make_active_manager(recenter_interval=300)
        # Wind the clock back so the interval appears to have passed
        mgr._state.last_recenter_time -= 301
        assert mgr.should_recenter()


class TestRecenter:
    def _make_paired_devices(self):
        telescope = MagicMock()
        pd = MagicMock()
        pd.telescope = telescope
        return pd

    def test_returns_false_when_inactive(self):
        action = _make_action(nonsidereal=False)
        mgr = NonSiderealManager(action, MagicMock())
        assert mgr.recenter(MagicMock(), MagicMock()) is False

    def test_slews_to_current_position(self):
        mgr = _make_active_manager()
        paired_devices = self._make_paired_devices()
        wait_fn = MagicMock()

        with patch("time.sleep"):
            mgr.recenter(paired_devices, wait_fn)

        paired_devices.telescope.get.assert_called_once()
        call_kwargs = paired_devices.telescope.get.call_args
        assert call_kwargs.args[0] == "SlewToCoordinatesAsync"
        ra_hours = call_kwargs.kwargs["RightAscension"]
        dec_deg = call_kwargs.kwargs["Declination"]
        assert 0.0 <= ra_hours < 24.0
        assert -90.0 <= dec_deg <= 90.0

    def test_calls_wait_fn_and_reapplies_rates(self):
        mgr = _make_active_manager()
        paired_devices = self._make_paired_devices()
        wait_fn = MagicMock()

        with patch("time.sleep"):
            result = mgr.recenter(paired_devices, wait_fn)

        assert result is True
        wait_fn.assert_called_once_with(paired_devices)
        # apply_rates sets RightAscensionRate and DeclinationRate
        keys_set = {c.args[0] for c in paired_devices.telescope.set.call_args_list}
        assert "RightAscensionRate" in keys_set
        assert "DeclinationRate" in keys_set

    def test_updates_last_recenter_time(self):
        mgr = _make_active_manager(recenter_interval=300)
        mgr._state.last_recenter_time -= 400  # pretend it's been a while
        paired_devices = self._make_paired_devices()

        with patch("time.sleep"):
            mgr.recenter(paired_devices, MagicMock())

        assert not mgr.should_recenter()  # timestamp was refreshed

    def test_returns_false_and_warns_on_error(self):
        mgr = _make_active_manager()
        paired_devices = self._make_paired_devices()
        paired_devices.telescope.get.side_effect = RuntimeError("slew failed")

        result = mgr.recenter(paired_devices, MagicMock())

        assert result is False
        mgr.logger.warning.assert_called_once()
