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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import yaml
from alpaca.telescope import GuideDirections
from astropy.stats import SigmaClip
from donuts import Donuts
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage

import astra
from astra.image_handler import ImageHandler
from astra.config import Config
from astra.scheduler import Action
from astra.paired_devices import PairedDevices


class CustomImageClass(Image):
    """Enhanced image processing class with background subtraction and cleaning.

    Extends the donuts Image class to provide sophisticated background
    subtraction, median filtering, and banding correction for improved
    star detection and shift measurements during guiding calibration.
    """

    def preconstruct_hook(self):
        """Apply background subtraction and image cleaning preprocessing.

        Performs sigma-clipped background estimation, median filtering,
        and horizontal banding correction to prepare images for accurate
        star position measurements during guiding calibration.
        """
        sigma_clip = SigmaClip(sigma=3.0)
        bkg_estimator = MedianBackground()

        self.raw_image = self.raw_image.astype(np.int16)

        bkg = Background2D(
            self.raw_image,
            (32, 32),
            filter_size=(3, 3),
            sigma_clip=sigma_clip,
            bkg_estimator=bkg_estimator,  # type: ignore
        )
        bkg_clean = self.raw_image - bkg.background

        med_clean = ndimage.median_filter(bkg_clean, size=5, mode="mirror")
        band_corr = np.median(med_clean, axis=1).reshape(-1, 1)  # type: ignore
        image_clean = med_clean - band_corr

        self.raw_image = np.clip(image_clean, 1, None)


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
        self.inage_handler.image_directory = (
            save_path
            if save_path is not None
            else (
                Config().paths.images
                / "calibrate_guiding"
                / datetime.now(UTC).strftime("%Y%m%d")
            )
        )

        self.pulse_time = action.action_value.get("pulse_time", pulse_time)
        self.exptime = action.action_value.get("exptime", exptime)
        self.settle_time = action.action_value.get("settle_time", settle_time)
        self.number_of_cycles = action.action_value.get(
            "number_of_cycles", number_of_cycles
        )
        self._directions = defaultdict(list)
        self._scales = defaultdict(list)
        self._calibration_config = {}
        self._camera = astra_observatory.devices["Camera"][action.device_name]
        self._telescope = astra_observatory.devices["Telescope"][
            paired_devices["Telescope"]
        ]
        self.inage_handler.image_directory.mkdir(parents=True, exist_ok=True)

    def run(self) -> None:
        """Execute complete guiding calibration sequence.

        Performs telescope slewing, calibration cycles, configuration
        completion, and saves results to observatory configuration.
        """
        self.slew_telescope_one_hour_east_of_sidereal_meridian()
        self.perform_calibration_cycles()
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

        self.astra_observatory.logger.info(
            f"Local sidereal time: {local_sidereal_time:.2f} hours."
            f"Slewing one hour east to: RA = {target_right_ascension:.2f} hours, "
            "Dec = 0 degrees..."
        )

        try:
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
        image_path = self._perform_exposure()
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

                shift = donuts_ref.measure_shift(image_path)
                direction_literal, magnitude = self._determine_shift_direction(shift)

                direction_name = direction.name.removeprefix(
                    "guide"
                )  # North, South, East, West
                self._directions[direction_name].append(direction_literal)
                self._scales[direction_name].append(magnitude)
                self.astra_observatory.logger.info(
                    f"Shift {direction_name} results in direction {direction_literal} "
                    f"of {magnitude} pixels."
                )

                donuts_ref = self._apply_donuts(image_path)

        self.astra_observatory.logger.info("Calibration cycles complete.")
        self.astra_observatory.logger.info(
            f"Directions: {str(self._directions)}; Scales: {str(self._scales)}"
        )

    def complete_calibration_config(self) -> None:
        """Generate final calibration configuration from measurements.

        Processes collected direction and scale measurements to create
        PIX2TIME conversion factors, determine RA axis orientation,
        and validate measurement consistency across cycles.

        Raises:
            ValueError: If direction measurements are inconsistent across cycles.
        """
        calibration_config = {
            "PIX2TIME": {"+x": None, "-x": None, "+y": None, "-y": None},
            "RA_AXIS": None,
            "DIRECTIONS": {"+x": None, "-x": None, "+y": None, "-y": None},
        }

        self.astra_observatory.logger.info("Checking directions...")
        for direction_name in self._directions:
            # Check that the directions are the same every time for each orientation
            if len(set(self._directions[direction_name])) != 1:
                raise ValueError(
                    "Directions must be the same across all cycles. "
                    f"Direction number {direction_name} has {self._directions[direction_name]}."
                )

            direction_literal = self._directions[direction_name][0]
            if direction_name == "East":
                calibration_config["RA_AXIS"] = "x" if "x" in direction_literal else "y"

            calibration_config["PIX2TIME"][direction_literal] = float(
                self.pulse_time / np.average(self._scales[direction_name])
            )
            calibration_config["DIRECTIONS"][direction_literal] = direction_name

        self.astra_observatory.logger.info("Directions are consistent")
        self._calibration_config.update(calibration_config)

    def save_calibration_config(self) -> None:
        """Save calibration configuration to YAML file."""
        with open(
            self.inage_handler.image_directory / "calibration_config.yaml", "w"
        ) as file:
            yaml.dump(self._calibration_config, file)

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

    @staticmethod
    def _determine_shift_direction(shift: Any) -> Tuple[str, float]:
        """Analyze donuts shift measurement to determine direction and magnitude.

        Processes shift measurements to identify the primary axis of movement
        and calculate the pixel displacement magnitude for calibration.

        Args:
            shift (Any): Donuts shift measurement object with x and y value attributes.

        Returns:
            Tuple[str, float]: Direction literal ('+x', '-x', '+y', '-y') and
                              pixel displacement magnitude.
        """
        sx = shift.x.value
        sy = shift.y.value
        if abs(sx) > abs(sy):
            if sx > 0:
                direction_literal = "-x"
            else:
                direction_literal = "+x"
            magnitude = abs(sx)
        else:
            if sy > 0:
                direction_literal = "-y"
            else:
                direction_literal = "+y"
            magnitude = abs(sy)

        return direction_literal, magnitude

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

        self._telescope.get("PulseGuide")(guide_direction, duration)
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
            raise ValueError("Exposure failed.")

        return file_path
