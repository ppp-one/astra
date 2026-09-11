"""Non-sidereal (moving target) tracking for observatory imaging sequences.

This module tracks solar system objects and Earth-orbiting objects. Their sky
coordinates change over the length of an exposure. The module reads positions
from a pre-computed ephemeris and sends differential tracking rates to the mount
through ASCOM.

What it does:
    - Reads the ephemeris that ``ObjectActionConfig`` computed at schedule load
      time, from Astropy or from JPL Horizons.
    - Interpolates RA/Dec and their rates at any moment without new lookups.
    - Sends the ASCOM ``RightAscensionRate`` and ``DeclinationRate`` the mount needs
      to follow the target (open-loop tracking).
    - Re-slews the mount to the current ephemeris position at a set interval, to
      remove drift that the rates cannot correct.

Notes:
    - Autoguiding does not work with non-sidereal tracking. The observatory
      disables guiding when this tracking is active.
    - The mount must report ``CanSetRightAscensionRate`` and
      ``CanSetDeclinationRate``.
"""

import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Callable

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from astra.utils import ephemeris
from astra.alpaca_device_process import AlpacaDevice
from astra.paired_devices import PairedDevices
from astra.scheduler import Action

__all__ = ["NonSiderealManager"]

# Issuing a rate command interrupts tracking on some mounts, so the two settings
# below govern how rarely we can get away with it.
#
# Never send rates more often than this, however fast the caller asks. apply_rates
# is driven from the exposure and image-save loops at up to 10 Hz, which is far
# beyond what any ephemeris justifies: outside low Earth orbit the required rate
# changes over minutes, not milliseconds. A schedule overrides this default with
# the action value "nonsidereal_rate_update_interval".
_MIN_RATE_UPDATE_INTERVAL_S = 10.0

# Beyond the interval floor, only send a rate once keeping the current one would
# trail the target by more than this much. This is an angle on the sky, not a
# percentage of the rate. A percentage is undefined when a rate passes through
# zero, which happens at a target's maximum declination, and would then fire
# continuously over changes far too small to see.
_RATE_UPDATE_TOLERANCE_ARCSEC = 0.5

# How far the target may drift past the intended start of imaging before the mount
# is re-pointed. Expressed as an angle rather than as a grace period, because no
# single period suits every target: a comet takes most of an hour to move this far,
# while a satellite in low orbit covers it in a fraction of a second.
_MAX_START_DRIFT_ARCSEC = 60.0

# Seconds to wait after issuing an asynchronous slew before the caller starts
# polling ``Slewing``; some drivers do not raise the flag immediately. Mirrors
# ``observatory.SLEW_POLL_START_DELAY``, which cannot be imported here because
# observatory imports this module. The post-slew settle is applied by the
# ``wait_for_slew_fn`` passed into ``recenter``.
_SLEW_POLL_START_DELAY = 1.0

# How far the sequence start may sit from the epoch the ephemeris interpolators
# were computed for. These are the same timestamp in a correctly ordered load, so
# any real drift is a re-timed schedule, not rounding.
_MAX_EPHEMERIS_EPOCH_DRIFT_S = 1.0


# Skip the second, fine re-centering slew when the target moved less than this
# during the first slew. A planet moves well under an arcsecond in that time, so
# the second slew would only add settle time.
_RECENTER_CORRECTION_MIN_ARCSEC = 1.0


@dataclass
class _NonSiderealState:
    """
    Holds pre-computed ephemeris state for a single imaging sequence.

    Attributes:
        body_name (str): Name of the solar system body being tracked.
        ra_interp (Any): Scipy interpolator for Right Ascension (degrees).
        dec_interp (Any): Scipy interpolator for Declination (degrees).
        ra_rate_interp (Any): Scipy interpolator for ASCOM RightAscensionRate
            (seconds of RA per sidereal second), or None to derive rates from
            ``ra_interp`` and ``dec_interp`` by finite difference.
        dec_rate_interp (Any): Scipy interpolator for ASCOM DeclinationRate
            (arcseconds per SI second), or None as above.
        sequence_start_time (Time): The epoch the interpolators are keyed to (t=0).
        recenter_interval (int): Seconds between re-centering slews. 0 disables them.
        last_recenter_time (float | None): Unix timestamp of the last re-center.
            None until tracking rates are first applied, so the interval counts
            from the start of tracking rather than from sequence setup.
        last_applied_ra_rate (float | None): Last RightAscensionRate sent to the mount.
        last_applied_dec_rate (float | None): Last DeclinationRate sent to the mount.
        last_rate_update_time (float | None): Unix timestamp of the last rate command.
        last_rate_check_time (float | None): Unix timestamp of the last time the
            rates were evaluated and found not worth sending. Gates re-evaluation
            so that a steady target is not re-checked at the caller's 10 Hz.
    """

    body_name: str
    ra_interp: Any
    dec_interp: Any
    ra_rate_interp: Any
    dec_rate_interp: Any
    sequence_start_time: Time
    recenter_interval: int
    last_recenter_time: float | None = None
    last_applied_ra_rate: float | None = None
    last_applied_dec_rate: float | None = None
    last_rate_update_time: float | None = None
    last_rate_check_time: float | None = None


class NonSiderealManager:
    """Manages non-sidereal tracking for a single imaging sequence.

    This class computes the tracking rates and the re-center positions from the
    pre-computed ephemeris. The observatory loop only controls the hardware.

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
        telescope: AlpacaDevice | None = None,
        to_mount_frame: Callable[[float, float], tuple[float, float]] | None = None,
    ) -> None:
        """
        Args:
            action: The scheduled action for this sequence.
            logger: Observatory logger.
            telescope: Mount this sequence will run on. When given, its ASCOM
                ``CanSetRightAscensionRate`` / ``CanSetDeclinationRate`` flags decide
                whether non-sidereal tracking can run at all. This is the authoritative
                check: schedule validation only knows whether *some* mount in the
                observatory supports differential rates, not which one the action is
                paired with.
            to_mount_frame: Optional ``(ra_deg, dec_deg) -> (ra_deg, dec_deg)`` that
                converts an ICRS position into the mount's frame. The ephemeris is
                ICRS; a JNow mount needs the conversion before every re-centering
                slew. None sends ICRS unchanged. The observatory supplies
                ``to_mount_coordinates`` for the paired telescope.
        """
        self.logger = logger
        self._to_mount_frame = to_mount_frame

        # The schedule sets the shortest time between rate commands. Increase it for
        # a mount that moves incorrectly when it receives a rate command. Decrease it
        # for a target whose rate changes over seconds.
        requested = action.action_value.get("nonsidereal_rate_update_interval")
        self.min_rate_update_interval = (
            _MIN_RATE_UPDATE_INTERVAL_S if requested is None else float(requested)
        )
        self._state: _NonSiderealState | None = self._setup(action, telescope)

    @property
    def is_active(self) -> bool:
        return self._state is not None

    def apply_rates(self, telescope: AlpacaDevice) -> None:
        """Push current differential RA/Dec rates to the mount."""
        if self._state is None:
            return
        self._apply_rates(telescope, self._state)

    def prepoint_coordinates(
        self, lead_time_seconds: float = 0.0
    ) -> tuple[float, float] | None:
        """Return RA/Dec for an initial lead-pointing slew.

        Args:
            lead_time_seconds: Seconds after sequence start used as the pre-pointing
                target. Defaults to 0, which points at the target's position at the
                start of the sequence.

        Returns:
            Tuple of (ra_deg, dec_deg) or None when non-sidereal tracking is inactive.
        """
        if self._state is None:
            return None

        state = self._state
        ra_deg = float(state.ra_interp(lead_time_seconds)) % 360.0
        dec_deg = float(state.dec_interp(lead_time_seconds))
        return (ra_deg, dec_deg)

    def tracking_activation_time(self, lead_time_seconds: float = 0.0) -> Time | None:
        """Return the time when non-sidereal rates and imaging should begin."""
        if self._state is None:
            return None
        return self._state.sequence_start_time + lead_time_seconds * u.s

    def should_recenter(self) -> bool:
        """Return True if the recenter interval has elapsed since tracking started.

        The interval is counted from the first rate command, not from when the
        manager was built. Sequence setup (slew, filter, focus, lead-time wait)
        can take longer than a short interval, and a re-center on the first
        exposure would waste two slews on a mount that just arrived on target.
        """
        if self._state is None or self._state.recenter_interval <= 0:
            return False
        if self._state.last_recenter_time is None:
            return False
        return (
            time.time() - self._state.last_recenter_time > self._state.recenter_interval
        )

    def drift_between(self, start: Time, end: Time) -> float | None:
        """Angular distance the target moves between two times, in arcsec.

        Returns None when non-sidereal tracking is inactive.
        """
        if self._state is None:
            return None

        state = self._state

        def position(when: Time) -> SkyCoord:
            t = (when - state.sequence_start_time).to_value(u.s)
            return SkyCoord(
                ra=(float(state.ra_interp(t)) % 360.0) * u.deg,
                dec=float(state.dec_interp(t)) * u.deg,
            )

        return float(position(start).separation(position(end)).arcsec)

    def recenter_if_late(
        self,
        activation_time: Time,
        paired_devices: PairedDevices,
        wait_for_slew_fn: Callable[[PairedDevices], None],
        can_slew: Callable[[], bool] | None = None,
    ) -> bool:
        """Re-point the mount if the target has drifted since ``activation_time``.

        The mount is pre-pointed at where the target would be at ``activation_time``.
        If the sequence reaches that point late, the target has moved on, and how
        much that matters depends entirely on the target: the same delay is
        imperceptible for a comet and puts a satellite degrees outside the field. The
        decision is therefore made on the distance the target has actually travelled,
        not on how late the sequence is.

        Returns True if a re-centre was performed.
        """
        if self._state is None:
            return False

        now = Time.now()
        late_s = (now - activation_time).to_value(u.s)
        if late_s <= 0:
            return False

        drift = self.drift_between(activation_time, now)
        if drift is None or drift < _MAX_START_DRIFT_ARCSEC:
            return False

        self.logger.warning(
            f"Imaging of {self._state.body_name} starts {late_s:.0f}s after the "
            f'intended moment. The target has moved {drift:.0f}" since then. '
            "Re-centering."
        )
        return self.recenter(paired_devices, wait_for_slew_fn, can_slew=can_slew)

    def recenter(
        self,
        paired_devices: PairedDevices,
        wait_for_slew_fn: Callable[[PairedDevices], None],
        can_slew: Callable[[], bool] | None = None,
    ) -> bool:
        """Slew to the current ephemeris position and refresh the tracking rates.

        The first slew goes to the target's position at the moment the slew is
        commanded. A fast target moves on while the mount is slewing, so a second
        slew corrects for that motion. The second slew is skipped when the target
        moved less than ``_RECENTER_CORRECTION_MIN_ARCSEC`` during the first one.

        Args:
            paired_devices: PairedDevices for the sequence.
            wait_for_slew_fn: Callable(paired_devices) that blocks until slew completes.
            can_slew: Optional predicate checked before each slew. Conditions can
                change between the two slews. A weather alert parks the mount, and a
                parked mount rejects a slew. So it is checked before each slew, not
                only on entry.

        Returns:
            True if re-centering was performed.
        """
        if self._state is None:
            return False

        state = self._state

        def unsafe_to_slew() -> bool:
            if can_slew is None or can_slew():
                return False
            self.logger.info(
                f"Conditions are no longer safe for movement. Abandoning the "
                f"re-center on {state.body_name}."
            )
            return True

        def slew_to_current_position(label: str, level: int) -> Time:
            """Slew to the target's position now and return the time used."""
            now = Time.now()
            t_seconds = (now - state.sequence_start_time).to(u.s).value
            ra_deg = float(state.ra_interp(t_seconds)) % 360.0  # unwrap to [0, 360)
            dec_deg = float(state.dec_interp(t_seconds))
            self.logger.log(
                level,
                f"{label} on {state.body_name} at RA={ra_deg:.3f}°, Dec={dec_deg:.3f}°",
            )
            if self._to_mount_frame is not None:
                ra_deg, dec_deg = self._to_mount_frame(ra_deg, dec_deg)
            telescope.get(
                "SlewToCoordinatesAsync",
                RightAscension=ra_deg / 15.0,  # ASCOM expects RA in hours [0, 24)
                Declination=dec_deg,
            )
            time.sleep(_SLEW_POLL_START_DELAY)
            wait_for_slew_fn(paired_devices)
            return now

        try:
            telescope = paired_devices.telescope

            if unsafe_to_slew():
                return False

            # Coarse slew to the target's current position.
            slew_time = slew_to_current_position("Re-centering", logging.INFO)

            if unsafe_to_slew():
                return False

            # Fine correction: the target moved during the coarse slew. Re-target
            # the current position so the tracking rates do not lock in the offset.
            moved = self.drift_between(slew_time, Time.now()) or 0.0
            if moved >= _RECENTER_CORRECTION_MIN_ARCSEC:
                slew_to_current_position("Re-centering correction", logging.DEBUG)
            else:
                self.logger.debug(
                    f"Skipping re-centering correction on {state.body_name}: the "
                    f'target moved only {moved:.2f}" during the slew.'
                )

            # Force the post-slew rate push regardless of delta or interval. The
            # mount has just moved and needs a known-good rate applied immediately.
            state.last_applied_ra_rate = None
            state.last_applied_dec_rate = None
            state.last_rate_update_time = None
            state.last_rate_check_time = None
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
            self._state.last_applied_ra_rate = None
            self._state.last_applied_dec_rate = None
            self._state.last_rate_update_time = None
            self._state.last_rate_check_time = None
            self.logger.info("Non-sidereal tracking rates reset to zero")
        except Exception as e:
            self.logger.warning(f"Could not reset non-sidereal tracking rates: {e}")

    def _setup(
        self, action: Action, telescope: AlpacaDevice | None = None
    ) -> _NonSiderealState | None:
        """Build state from the pre-computed ephemeris in the action config.

        Returns None if non-sidereal tracking is not active (``_nonsidereal`` is
        False), if telescope movement is disabled, or if the mount cannot accept
        differential tracking rates. The ephemeris interpolators are computed once
        at schedule load time, in ``ObjectActionConfig.validate_visibility``, and
        read here at sequence start. There are no network or ephemeris calls at
        runtime.
        """
        if (
            not action.action_value.get("_nonsidereal", False)
            or action.action_type == "calibration"
            or action.action_value.get("disable_telescope_movement", False)
        ):
            return None

        lname = action.action_value.get("lookup_name")

        if telescope is not None and not self._telescope_supports_rates(telescope):
            # Reaching here means the schedule resolved this target as a moving body,
            # so running the sequence sidereally would trail it. logger.error clears
            # error_free, stopping the sequence via the usual check_conditions() path.
            # Schedule validation catches this for the observatory as a whole; it can
            # still fire here when the action is paired with a different mount than
            # the one validation happened to sample.
            self.logger.error(
                f"'{lname}' needs non-sidereal tracking, but mount "
                f"{getattr(telescope, 'device_name', '')} does not support "
                "differential tracking rates (CanSetRightAscensionRate / "
                "CanSetDeclinationRate are False)."
            )
            return None

        ra_interp = action.action_value.get("_ra_interp")
        dec_interp = action.action_value.get("_dec_interp")
        ra_rate_interp = action.action_value.get("_ra_rate_interp")
        dec_rate_interp = action.action_value.get("_dec_rate_interp")
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

        # The interpolators are keyed to the epoch they were computed for. If the
        # sequence is starting at a different time, every position and rate read out
        # of them is for the wrong moment, and far enough out the cubic fit is
        # extrapolating. Refuse rather than slew somewhere confidently wrong.
        sequence_start_time = Time(action.start_time)
        epoch = action.action_value.get("_ephemeris_epoch")
        if epoch is not None:
            drift_s = abs((sequence_start_time - epoch).to_value(u.s))
            if drift_s > _MAX_EPHEMERIS_EPOCH_DRIFT_S:
                self.logger.error(
                    f"Ephemeris for '{lname}' was computed for {epoch.isot} but the "
                    f"sequence starts at {sequence_start_time.isot} ({drift_s:.0f}s "
                    "later). The schedule was re-timed after validation; refusing to "
                    "track from a stale ephemeris."
                )
                return None
            sequence_start_time = epoch

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
            ra_rate_interp=ra_rate_interp,
            dec_rate_interp=dec_rate_interp,
            sequence_start_time=sequence_start_time,
            recenter_interval=recenter_interval,
        )

    def _apply_rates(self, telescope: AlpacaDevice, state: _NonSiderealState) -> None:
        """Set ASCOM RightAscensionRate / DeclinationRate from the interpolated ephemeris.

        Called at up to 10 Hz from the exposure and image-save loops. Most of those
        calls must not reach the mount: rate commands interrupt tracking on some
        mounts, and the required rate changes much more slowly than that. Two gates
        decide whether a call gets through. The first is a minimum interval since
        the last rate command or the last rate check. The second is whether keeping
        the current rate would trail the target, see ``_projected_drift_arcsec``.
        """
        now = time.time()
        last_visit = max(
            state.last_rate_update_time or 0.0, state.last_rate_check_time or 0.0
        )
        if last_visit > 0.0 and now - last_visit < self.min_rate_update_interval:
            # Cheap early exit before touching the interpolators, since this is the
            # branch the high-frequency callers take almost every time.
            return

        if state.last_recenter_time is None:
            # First rate command of the sequence: tracking starts now, so the
            # re-center interval starts counting from here.
            state.last_recenter_time = now

        try:
            t_seconds = (Time.now() - state.sequence_start_time).to(u.s).value
            if state.ra_rate_interp is not None and state.dec_rate_interp is not None:
                ra_rate = float(state.ra_rate_interp(t_seconds))
                dec_rate = float(state.dec_rate_interp(t_seconds))
            else:
                ra_rate, dec_rate = ephemeris.compute_nonsidereal_rates_from_interp(
                    state.ra_interp, state.dec_interp, t_seconds
                )

            drift = self._projected_drift_arcsec(
                state, ra_rate, dec_rate, t_seconds, now
            )
            if drift is not None and drift < _RATE_UPDATE_TOLERANCE_ARCSEC:
                # Record the check so the next evaluation waits a full interval
                # instead of running at the caller's rate.
                state.last_rate_check_time = now
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug(
                        "Keeping current non-sidereal rates. Updating them would "
                        f'recover only {drift:.4f}" of trailing '
                        f"(dRA={ra_rate:.6f} s/s, dDec={dec_rate:.6f} as/s)"
                    )
                return

            telescope.set("RightAscensionRate", ra_rate)
            telescope.set("DeclinationRate", dec_rate)
            state.last_applied_ra_rate = ra_rate
            state.last_applied_dec_rate = dec_rate
            state.last_rate_update_time = now
            self.logger.info(
                f"Non-sidereal tracking rates: dRA={ra_rate:.6f} s/s, dDec={dec_rate:.6f} as/s"
            )
        except Exception as e:
            # Rate support was verified in _setup, so this is a transient device
            # or ephemeris failure. Do not re-query the mount here: if the link is
            # down that raises a second exception out of the handler.
            self.logger.warning(f"Could not set non-sidereal tracking rates: {e}")

    def _telescope_supports_rates(self, telescope: AlpacaDevice) -> bool:
        """Return True if the mount can accept differential RA and Dec tracking rates.

        Both ASCOM capability flags are required: a mount that can offset only one
        axis cannot follow a moving target. If the flags cannot be read the mount is
        assumed capable, so a transient query failure degrades to the previous
        behaviour (attempt the rates, warn if they are rejected) rather than
        silently disabling tracking.
        """
        try:
            can_ra = bool(telescope.get("CanSetRightAscensionRate"))
            can_dec = bool(telescope.get("CanSetDeclinationRate"))
        except Exception as e:
            self.logger.warning(
                f"Could not query differential tracking rate support: {e}. "
                "Assuming the mount supports it."
            )
            return True
        return can_ra and can_dec

    def _projected_drift_arcsec(
        self,
        state: _NonSiderealState,
        ra_rate: float,
        dec_rate: float,
        t_seconds: float,
        now: float,
    ) -> float | None:
        """Trailing, in arcsec, that keeping the current rates would cause.

        Measured over the time until rates could next be sent. This answers the
        question that matters: how far off target the mount drifts if it is not
        updated. It does not measure how much the rate number changed.

        Returns None when no rates have been applied yet, meaning the caller should
        send them unconditionally.
        """
        if state.last_applied_ra_rate is None or state.last_applied_dec_rate is None:
            return None

        # RightAscensionRate is in seconds of RA, DeclinationRate in arcsec. Put both
        # on the sky in arcsec so they are comparable and the tolerance means one
        # thing on each axis.
        dec_deg = float(state.dec_interp(t_seconds))
        cos_dec = max(abs(math.cos(math.radians(dec_deg))), 1e-6)
        d_ra_arcsec_s = abs(ra_rate - state.last_applied_ra_rate) * 15.0 * cos_dec
        d_dec_arcsec_s = abs(dec_rate - state.last_applied_dec_rate)

        elapsed = now - (state.last_rate_update_time or now)
        horizon_s = max(elapsed, self.min_rate_update_interval)
        return math.hypot(d_ra_arcsec_s, d_dec_arcsec_s) * horizon_s
