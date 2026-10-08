"""Guiding calibration system for telescope autoguiding setup.

This module provides automated calibration of telescope guiding systems by
measuring pixel-to-time scales and determining camera orientation relative
to telescope mount axes. It performs systematic nudges in cardinal directions
and analyzes the resulting star field shifts to create calibration parameters.

Classes:
    CustomImageClass: Enhanced image processing with background subtraction
    GuidingCalibrator: Main calibration orchestrator for guiding systems
"""

import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

import numpy as np
from alpaca.telescope import GuideDirections
from donuts import Donuts
from ruamel.yaml import YAML

import astra
from astra.config import Config
from astra.image_handler import ImageHandler
from astra.paired_devices import PairedDevices
from astra.scheduler import Action
from astra.utils.image import CustomImageClass

GUIDE_DIRECTION_NAMES = ("North", "South", "East", "West")

# largest allowed difference between the rotation of the RA and the Dec moves;
# a larger difference means that the moves are not perpendicular
MAX_AXIS_ANGLE_DIFFERENCE = 20.0  # degrees


def calibration_from_shifts(
    shifts: dict[str, Any], pulse_time: float, binning: int = 1
) -> dict[str, Any]:
    """
    Guider calibration from the star shift that each guide direction causes.

    The camera axes do not need to be aligned with RA and Dec. Each mount axis
    is assigned to the camera axis it is closest to, and the remaining
    rotation is returned as ANGLE, which the guider removes before it computes
    the corrections.

    Parameters:
        shifts (dict): For "North", "South", "East" and "West", the mean shift
            (x, y) in pixels that Donuts measured after a pulse of
            ``pulse_time`` milliseconds in that direction.
        pulse_time (float): Pulse duration in milliseconds.
        binning (int, optional): Camera binning during the calibration.

    Returns:
        dict: PIX2TIME, RA_AXIS, DIRECTIONS and ANGLE (degrees) for the guider
            configuration.

    Raises:
        ValueError: If the shifts do not fit two perpendicular axes.
    """
    v = {name: np.asarray(shifts[name], dtype=float) for name in GUIDE_DIRECTION_NAMES}
    for a, b in (("East", "West"), ("North", "South")):
        if np.dot(v[a], v[b]) >= 0:
            raise ValueError(
                f"{a} and {b} pulses did not move the stars in opposite directions "
                f"({a}: {v[a].round(2)}, {b}: {v[b].round(2)} pixels)."
            )

    # each mount axis goes to the camera axis it is closest to, RA first, so
    # that the two never share an axis
    ra_axis = (
        "x"
        if abs(v["East"][0] - v["West"][0]) >= abs(v["East"][1] - v["West"][1])
        else "y"
    )
    dec_axis = "y" if ra_axis == "x" else "x"

    # a pulse that gives a positive shift on its axis is the "-" direction
    directions = {}
    for name, opposite, axis in (
        ("East", "West", ra_axis),
        ("North", "South", dec_axis),
    ):
        i = 0 if axis == "x" else 1
        sign, other = ("-", "+") if v[name][i] - v[opposite][i] > 0 else ("+", "-")
        directions[sign + axis] = name
        directions[other + axis] = opposite

    pix2time = {
        label: float(pulse_time / np.linalg.norm(v[name]) / binning)
        for label, name in directions.items()
    }

    # rotation of the move of a "+x" and a "+y" pulse from the camera axes
    angles = []
    for axis, ideal in (("x", (-1.0, 0.0)), ("y", (0.0, -1.0))):
        move = v[directions["+" + axis]] - v[directions["-" + axis]]
        cross = ideal[0] * move[1] - ideal[1] * move[0]
        angles.append(np.arctan2(cross, ideal[0] * move[0] + ideal[1] * move[1]))
    difference = np.degrees(np.angle(np.exp(1j * (angles[0] - angles[1]))))
    if abs(difference) > MAX_AXIS_ANGLE_DIFFERENCE:
        raise ValueError(
            f"RA and Dec pulses did not move the stars at right angles "
            f"(difference {difference:.1f} degrees). Check the calibration images."
        )
    angle = np.degrees(np.angle(np.exp(1j * angles[0]) + np.exp(1j * angles[1])))

    return {
        "PIX2TIME": pix2time,
        "RA_AXIS": ra_axis,
        "DIRECTIONS": directions,
        "ANGLE": round(float(angle), 3),
    }


class GuidingCalibrator:
    """Automated telescope guiding calibration system.

    Orchestrates the complete guiding calibration process by systematically
    pulsing the telescope mount in cardinal directions and measuring the
    resulting star field shifts to determine pixel-to-time scales and
    camera orientation relative to mount axes.

    Attributes:
        astra_observatory: Observatory instance for device control.
        action: Action instance containing calibration information.
        paired_devices: Dictionary of paired device names.
        hdr: FITS header data for images.
        save_path: Directory for saving calibration data and images.
        pulse_time: Duration of guide pulses in milliseconds.
        exptime: Exposure time for calibration images.
        settle_time: Wait time after pulses before exposing.
        number_of_cycles: Number of calibration cycles to perform.
    """

    def __init__(
        self,
        astra_observatory: "astra.observatory.Observatory",  # type: ignore
        action: Action,
        paired_devices: Dict[str, str],
        image_handler: ImageHandler,
        save_path: Path | None = None,
        pulse_time: float = 5000,
        exptime: float = 5,
        settle_time: float = 10,
        number_of_cycles: int = 10,
    ):
        self.astra_observatory = astra_observatory
        self.action = action
        self.paired_devices = paired_devices
        self.image_handler = image_handler
        self.image_handler.image_directory = (
            save_path if save_path is not None else (Config().paths.images)
        )

        self.pulse_time = action.action_value.get("pulse_time", pulse_time)
        self.exptime = action.action_value.get("exptime", exptime)
        self.settle_time = action.action_value.get("settle_time", settle_time)
        self.number_of_cycles = action.action_value.get(
            "number_of_cycles", number_of_cycles
        )
        self.binning = action.action_value.get("bin", 1)
        self._shifts = defaultdict(list)
        self._calibration_config = {}
        self._camera = astra_observatory.devices["Camera"][action.device_name]
        self._telescope = astra_observatory.devices["Telescope"][
            paired_devices["Telescope"]
        ]
        self.image_handler.image_directory.mkdir(parents=True, exist_ok=True)

    def run(self) -> None:
        """Execute complete guiding calibration sequence.

        Performs telescope slewing, calibration cycles, configuration
        completion, and saves results to observatory configuration.
        """
        self.slew_telescope_one_hour_east_of_sidereal_meridian()
        success = self.perform_calibration_cycles()
        if success:
            self.complete_calibration_config()
            self.save_calibration_config()
            self.update_observatory_config()

    def slew_telescope_one_hour_east_of_sidereal_meridian(self) -> None:
        """Position telescope one hour east of meridian for calibration.

        Slews telescope to RA = LST - 1 hour, Dec = 0 degrees to provide
        optimal conditions for guiding calibration with good star tracking
        and minimal atmospheric effects.

        Raises:
            ValueError: If telescope slewing fails.
        """
        local_sidereal_time = self._telescope.get("SiderealTime")
        target_right_ascension = local_sidereal_time - 1
        # Normalize RA to 0-24 hours
        if target_right_ascension < 0:
            target_right_ascension += 24
        elif target_right_ascension >= 24:
            target_right_ascension -= 24

        self.astra_observatory.logger.info(
            f"Local sidereal time: {local_sidereal_time:.2f} hours. "
            f"Slewing one hour east to: RA = {target_right_ascension:.2f} hours, "
            "Dec = 0 degrees..."
        )

        try:
            self.astra_observatory.execute_and_monitor_device_task(
                "Telescope",
                "Tracking",
                True,
                "Tracking",
                device_name=self.paired_devices["Telescope"],
                log_message=f"Setting Telescope {self.paired_devices['Telescope']} tracking to True",
            )
            self._telescope.get(
                "SlewToCoordinatesAsync",
                RightAscension=target_right_ascension,
                Declination=0,
            )
            time.sleep(1)

            # Wait for slew to finish
            self.astra_observatory.wait_for_slew(self.paired_devices)

        except Exception as e:
            raise ValueError(f"Failed to slew telescope: {e}")

    def perform_calibration_cycles(self) -> None:
        """Execute systematic guiding calibration cycles.

        Performs multiple cycles of telescope nudges in North, South, East,
        and West directions, measuring star field shifts to determine pixel
        scales and camera orientation. Each cycle improves measurement accuracy.
        """
        success = False

        image_path = self._perform_exposure()

        if image_path is None:
            return success

        donuts_ref = self._apply_donuts(image_path)

        for i in range(self.number_of_cycles):
            self.astra_observatory.logger.info(
                f"=== Starting cycle {i + 1} of {self.number_of_cycles} ==="
            )
            for direction in [
                GuideDirections.guideNorth,
                GuideDirections.guideSouth,
                GuideDirections.guideEast,
                GuideDirections.guideWest,
            ]:
                # Nudging to determine the scale and orientation of the camera
                self._pulse_guide_telescope(direction, self.pulse_time)
                image_path = self._perform_exposure()

                if image_path is None:
                    return success

                shift = donuts_ref.measure_shift(image_path)

                direction_name = direction.name.removeprefix(
                    "guide"
                )  # North, South, East, West
                self._shifts[direction_name].append(
                    (float(shift.x.value), float(shift.y.value))
                )
                self.astra_observatory.logger.info(
                    f"Shift {direction_name}: x={shift.x.value:.2f}, "
                    f"y={shift.y.value:.2f} pixels"
                )

                donuts_ref = self._apply_donuts(image_path)

        self.astra_observatory.logger.info("Calibration cycles complete.")
        self.astra_observatory.logger.info(f"Shifts: {dict(self._shifts)}")

        success = True

        return success

    def complete_calibration_config(self) -> None:
        """Generate final calibration configuration from measurements.

        Averages the shift of each guide direction over all cycles and derives
        PIX2TIME, RA_AXIS, DIRECTIONS and the camera ANGLE from them.

        Raises:
            ValueError: If a direction has no measurements, or the shifts do
                not fit two perpendicular axes.
        """
        missing = [name for name in GUIDE_DIRECTION_NAMES if not self._shifts[name]]
        if missing:
            raise ValueError(f"Calibration error: no shifts measured for {missing}.")

        mean_shifts = {
            name: np.mean(self._shifts[name], axis=0) for name in GUIDE_DIRECTION_NAMES
        }
        calibration_config = calibration_from_shifts(
            mean_shifts, self.pulse_time, self.binning
        )
        self.astra_observatory.logger.info(
            f"Guiding calibration: RA on the camera {calibration_config['RA_AXIS']} "
            f"axis, camera angle {calibration_config['ANGLE']:.2f} degrees"
        )
        self._calibration_config.update(calibration_config)

    def save_calibration_config(self) -> None:
        """Save calibration configuration to YAML file with nice formatting.

        Uses ruamel.yaml to create readable output with proper indentation,
        preserved structure, and better formatting for nested dictionaries.
        """
        output_path = self.image_handler.image_directory / "calibration_config.yaml"

        yaml_writer = YAML()
        yaml_writer.default_flow_style = False
        yaml_writer.preserve_quotes = True
        yaml_writer.indent(mapping=2, sequence=2, offset=0)
        yaml_writer.width = 4096  # Prevent line wrapping

        with open(output_path, "w") as file:
            yaml_writer.dump(self._calibration_config, file)

        self.astra_observatory.logger.info(f"Calibration config saved to {output_path}")

    def update_observatory_config(self) -> None:
        """Update observatory configuration with calibration results.

        Integrates the calculated calibration parameters into the observatory
        configuration file for the specific camera being calibrated.
        """
        paired_devices = PairedDevices.from_observatory(
            observatory=self.astra_observatory,
            camera_name=self.action.device_name,
        )
        telescope_config = paired_devices.get_device_config("Telescope")
        telescope_config["guider"].update(self._calibration_config)
        paired_devices.observatory_config.save()
        self.astra_observatory.logger.info("Observatory config updated.")

    def _pulse_guide_telescope(
        self, guide_direction: GuideDirections, duration: float
    ) -> None:
        """Execute telescope guide pulse in specified direction.

        Sends guide pulse command to telescope mount and waits for completion.
        Logs telescope position after pulse for verification.

        Args:
            guide_direction (GuideDirections): Cardinal direction for guide pulse from GuideDirections enum.
            duration (float): Pulse duration in milliseconds.

        Raises:
            ValueError: If guide direction is invalid.
        """
        if guide_direction not in GuideDirections:
            raise ValueError("Invalid direction")

        self.astra_observatory.logger.info(
            f"Pulse guiding {guide_direction.name} for {duration} ms"
        )

        self._telescope.get("PulseGuide", Direction=guide_direction, Duration=duration)
        while self._telescope.get("IsPulseGuiding"):
            self.astra_observatory.logger.debug("Pulse guiding...")
            time.sleep(0.1)

        while self._telescope.get("Slewing"):
            self.astra_observatory.logger.debug("Slewing...")
            time.sleep(0.1)

        ra = (self._telescope.get("RightAscension") / 24) * 360
        dec = self._telescope.get("Declination")
        self.astra_observatory.logger.info(f"RA: {ra:.8f} deg, DEC: {dec:.8f} deg")

    @staticmethod
    def _apply_donuts(image_path: Path) -> Donuts:
        """Create Donuts instance for image shift measurement.

        Configures Donuts with custom image processing for accurate
        star shift detection during guiding calibration.

        Args:
            image_path (Path): Path object pointing to FITS image file.

        Returns:
            Donuts: Configured Donuts instance for shift measurements.
        """
        return Donuts(
            image_path,
            normalise=False,
            subtract_bkg=False,
            downweight_edges=False,
            image_class=CustomImageClass,
        )

    def _perform_exposure(self) -> Path:
        """Capture calibration image with specified parameters.

        Waits for telescope settling, then captures image using configured
        exposure time and saves to calibration directory.

        Returns:
            Path: Path to captured FITS image file.

        Raises:
            ValueError: If image exposure fails.
        """
        self.astra_observatory.logger.info(f"Waiting {self.settle_time} s to settle...")
        time.sleep(self.settle_time)

        success, file_path = self.astra_observatory.perform_exposure(
            camera=self._camera,
            exptime=self.exptime,
            maxadu=self._camera.get("MaxADU"),
            action=self.action,
            use_light=True,
            log_option=None,
            maximal_sleep_time=0.1,
            wcs=None,
        )
        if not success:
            return None

        return file_path
