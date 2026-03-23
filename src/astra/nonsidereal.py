"""Non-sidereal (solar system body) tracking for observatory imaging sequences.

This module provides the machinery for tracking solar system objects (comets,
asteroids, planets) whose celestial coordinates change significantly over short
timescales. It uses high-precision cubic interpolation of pre-computed ephemerides
to provide smooth, differential tracking rates to the telescope mount via ASCOM.

Key Capabilities:
    - Ephemeris Pre-computation: Uses Astropy to generate a sequence of
      positions for a given object over the duration of an observation.
    - Cubic Interpolation: Provides sub-second precision for RA/Dec coordinates
      without requiring repeated, expensive lookups.
    - Differential Tracking: Calculates and applies the exact RA/Dec rates
      required for the mount to follow the target (blind tracking).
    - Periodic Re-centering: Automatically re-slews the telescope to the latest
      ephemeris position at user-defined intervals to correct for long-term drift.

Integration Notes:
    - Non-sidereal tracking is generally incompatible with standard autoguiding.
      The system is designed to disable guiding when active.
    - Requires ASCOM drivers that support `RightAscensionRate` and
      `DeclinationRate` properties.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import astropy.units as u
from astropy.time import Time

import astra.utils
from astra.alpaca_device_process import AlpacaDevice
from astra.paired_devices import PairedDevices
from astra.scheduler import Action

__all__ = ["NonSiderealManager"]


@dataclass
class _NonSiderealState:
    """
    Holds pre-computed ephemeris state for a single imaging sequence.

    Attributes:
        body_name (str): Name of the solar system body being tracked.
        ra_interp (Any): Scipy interpolator for Right Ascension (degrees).
        dec_interp (Any): Scipy interpolator for Declination (degrees).
        sequence_start_time (Time): The exact Astropy Time the sequence began.
        recenter_interval (int): Seconds between re-centering slews.
        last_recenter_time (float): Unix timestamp of the last successful re-center.
    """

    body_name: str
    ra_interp: Any
    dec_interp: Any
    sequence_start_time: Time
    recenter_interval: int
    last_recenter_time: float = field(default_factory=time.time)


class NonSiderealManager:
    """Manages non-sidereal tracking operations for a single imaging sequence.

    This class encapsulates the mathematical and operational complexity of
    differential tracking, allowing the main observatory loop to remain
    focused on hardware orchestration.

    Usage::

        manager = NonSiderealManager(action, logger)
        if manager.is_active:
            manager.apply_rates(telescope)

        try:
            # ... exposure loop ...
            if manager.should_recenter():
                manager.recenter(paired_devices, wait_for_slew_fn)
        finally:
            manager.reset_rates(telescope)
    """

    def __init__(
        self,
        action: Action,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger
        self._state: _NonSiderealState | None = self._setup(action)

    @property
    def is_active(self) -> bool:
        return self._state is not None

    def apply_rates(self, telescope: AlpacaDevice) -> None:
        """Push current differential RA/Dec rates to the mount."""
        if self._state is None:
            return
        self._apply_rates(telescope, self._state)

    def should_recenter(self) -> bool:
        """Return True if the recenter interval has elapsed."""
        if self._state is None or self._state.recenter_interval <= 0:
            return False
        return (
            time.time() - self._state.last_recenter_time > self._state.recenter_interval
        )

    def recenter(
        self,
        paired_devices: PairedDevices,
        wait_for_slew_fn: Callable[[PairedDevices], None],
    ) -> bool:
        """Slew to the updated ephemeris position and refresh tracking rates.

        Args:
            paired_devices: PairedDevices for the sequence.
            wait_for_slew_fn: Callable(paired_devices) that blocks until slew completes.

        Returns:
            True if re-centering was performed (caller should reset guiding flag).
        """
        if self._state is None:
            return False

        state = self._state
        try:
            t_seconds = (Time.now() - state.sequence_start_time).to(u.s).value
            ra_deg = float(state.ra_interp(t_seconds)) % 360.0  # unwrap → [0, 360)
            dec_deg = float(state.dec_interp(t_seconds))
            telescope = paired_devices.telescope
            self.logger.info(
                f"Re-centering on {state.body_name} at RA={ra_deg:.3f}°, Dec={dec_deg:.3f}°"
            )
            telescope.get(
                "SlewToCoordinatesAsync",
                RightAscension=ra_deg / 15.0,  # ASCOM expects RA in hours [0, 24)
                Declination=dec_deg,
            )
            time.sleep(1)
            wait_for_slew_fn(paired_devices)
            self._apply_rates(telescope, state)
            state.last_recenter_time = time.time()
            return True
        except Exception as e:
            self.logger.warning(f"Non-sidereal re-centering failed: {e}")
            return False

    def reset_rates(self, telescope: AlpacaDevice) -> None:
        """Reset differential tracking rates to zero.

        Safe to call even when not active (no-op if non-sidereal tracking was
        never started, so the telescope is never touched); always call this in
        a finally block.
        """
        if self._state is None:
            return
        try:
            telescope.set("RightAscensionRate", 0.0)
            telescope.set("DeclinationRate", 0.0)
            self.logger.info("Non-sidereal tracking rates reset to zero")
        except Exception as e:
            self.logger.warning(f"Could not reset non-sidereal tracking rates: {e}")

    def _setup(self, action: Action) -> _NonSiderealState | None:
        """Build state from pre-computed ephemeris in the action config.

        Returns None if non-sidereal tracking is not active (``_nonsidereal`` is False)
        or telescope movement is disabled.  The ephemeris interpolators are computed
        once at schedule load time (in ``ObjectActionConfig.validate_visibility``) and
        read here at sequence start — no repeated network or ephemeris calls at runtime.
        """
        if (
            not action.action_value.get("_nonsidereal", False)
            or action.action_type == "calibration"
            or action.action_value.get("disable_telescope_movement", False)
        ):
            return None

        lname = action.action_value.get("lookup_name")
        ra_interp = action.action_value.get("_ra_interp")
        dec_interp = action.action_value.get("_dec_interp")
        recenter_interval = int(
            action.action_value.get("nonsidereal_recenter_interval", 0)
        )

        if ra_interp is None or dec_interp is None:
            self.logger.error(
                f"Non-sidereal tracking requested for '{lname}' but ephemeris "
                "interpolators are missing. This should never happen if the schedule "
                "was validated correctly at load time."
            )
            return None

        recenter_msg = (
            f"re-centering every {recenter_interval}s"
            if recenter_interval > 0
            else "re-centering disabled"
        )
        self.logger.info(f"Non-sidereal tracking active for '{lname}', {recenter_msg}")
        return _NonSiderealState(
            body_name=lname,
            ra_interp=ra_interp,
            dec_interp=dec_interp,
            sequence_start_time=Time(action.start_time),
            recenter_interval=recenter_interval,
        )

    def _apply_rates(self, telescope: AlpacaDevice, state: _NonSiderealState) -> None:
        """Set ASCOM RightAscensionRate / DeclinationRate from the interpolated ephemeris."""
        try:
            t_seconds = (Time.now() - state.sequence_start_time).to(u.s).value
            ra_rate, dec_rate = astra.utils.compute_nonsidereal_rates_from_interp(
                state.ra_interp, state.dec_interp, t_seconds
            )
            telescope.set("RightAscensionRate", ra_rate)
            telescope.set("DeclinationRate", dec_rate)
            self.logger.info(
                f"Non-sidereal tracking rates: dRA={ra_rate:.6f} s/s, dDec={dec_rate:.6f} as/s"
            )
        except Exception as e:
            if not telescope.get("CanSetRightAscensionRate"):
                self.logger.warning(
                    "Mount does not support RightAscensionRate "
                    "(CanSetRightAscensionRate=False). "
                    "Non-sidereal tracking is unavailable."
                )
            else:
                self.logger.warning(f"Could not set non-sidereal tracking rates: {e}")
