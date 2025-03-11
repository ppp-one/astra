import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import yaml
from alpaca.telescope import GuideDirections
from donuts import Donuts
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage
from astropy.stats import SigmaClip

from astra.config import Config, ObservatoryConfig

CONFIG = Config()
OBSERVATORY_CONFIG = ObservatoryConfig.from_config(CONFIG)


class CustomImageClass(Image):
    def preconstruct_hook(self):
        sigma_clip = SigmaClip(sigma=3.0)
        bkg_estimator = MedianBackground()

        self.raw_image = self.raw_image.astype(np.int16)

        bkg = Background2D(
            self.raw_image,
            (32, 32),
            filter_size=(3, 3),
            sigma_clip=sigma_clip,
            bkg_estimator=bkg_estimator,
        )
        bkg_clean = self.raw_image - bkg.background

        med_clean = ndimage.median_filter(bkg_clean, size=5, mode="mirror")
        band_corr = np.median(med_clean, axis=1).reshape(-1, 1)
        image_clean = med_clean - band_corr

        self.raw_image = np.clip(image_clean, 1, None)


class GuidingCalibrator:
    def __init__(
        self,
        astra_observatory: "astra.observatory.Observatory",
        row,
        paired_devices: dict,
        action_value: dict,
        hdr,
        save_path: Path = (
            CONFIG.paths.images
            / "calibrate_guiding"
            / datetime.now(UTC).strftime("%Y%m%d")
        ),
        pulse_time: float = 5000,
        exptime: float = 5,
        settle_time: float = 10,
        number_of_cycles: int = 10,
    ):
        self.astra_observatory = astra_observatory
        self.row = row
        self.paired_devices = paired_devices
        self.action_value = action_value
        self.hdr = hdr
        self.save_path = save_path
        self.pulse_time = action_value.get("pulse_time", pulse_time)
        self.exptime = action_value.get("exptime", exptime)
        self.settle_time = action_value.get("settle_time", settle_time)
        self.number_of_cycles = action_value.get("number_of_cycles", number_of_cycles)
        self._directions = defaultdict(list)
        self._scales = defaultdict(list)
        self._calibration_config = {}
        self._camera = astra_observatory.devices["Camera"][row["device_name"]]
        self._telescope = astra_observatory.devices["Telescope"][
            paired_devices["Telescope"]
        ]
        save_path.mkdir(parents=True, exist_ok=True)

    def run(self):
        self.slew_telescope_one_hour_east_of_sidereal_meridian()
        self.perform_calibration_cycles()
        self.complete_calibration_config()
        self.save_calibration_config()
        self.update_observatory_config()

    def slew_telescope_one_hour_east_of_sidereal_meridian(self):
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

    def perform_calibration_cycles(self):
        """Nudge camera in direction U, D, L, R to determine its scale and orientation."""
        image_path = self._perform_exposure()
        donuts_ref = self._apply_donuts(image_path)

        for i in range(self.number_of_cycles):
            self.astra_observatory.logger.info(
                f"=== Starting cycle {i+1} of {self.number_of_cycles} ==="
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

    def complete_calibration_config(self):
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

    def save_calibration_config(self):
        with open(self.save_path / "calibration_config.yaml", "w") as file:
            yaml.dump(self._calibration_config, file)

    def update_observatory_config(self):
        camera_index = self.astra_observatory.get_cam_index(self.row["device_name"])

        observatory_config = ObservatoryConfig.from_config(CONFIG)
        observatory_config["Telescope"][camera_index]["guider"].update(
            self._calibration_config
        )
        observatory_config.save()
        self.astra_observatory.logger.info("Observatory config updated.")

    @staticmethod
    def _determine_shift_direction(shift):
        """Take a donuts shift object and work out the direction of the shift and the distance"""
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

    def _pulse_guide_telescope(self, guide_direction: GuideDirections, duration: float):
        """
        Move the telescope along a given direction
        for the specified amount of time
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
    def _apply_donuts(image_path):
        return Donuts(
            image_path,
            normalise=False,
            subtract_bkg=False,
            downweight_edges=False,
            image_class=CustomImageClass,
        )

    def _perform_exposure(self):
        self.astra_observatory.logger.info(f"Waiting {self.settle_time} s to settle...")
        time.sleep(self.settle_time)

        success, file_path = self.astra_observatory.perform_exposure(
            camera=self._camera,
            exptime=self.exptime,
            maxadu=self._camera.get("MaxADU"),
            row=self.row,
            hdr=self.hdr,
            folder=self.save_path,
            use_light=True,
            log_option=None,
            maximal_sleep_time=0.1,
            wcs=None,
        )
        if not success:
            raise ValueError("Exposure failed.")

        return file_path
