import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import astropy.units as u
import numpy as np
import yaml
from alpaca.telescope import GuideDirections
from astrafocus.targeting.airmass_models import \
    find_airmass_threshold_crossover
from astrafocus.targeting.zenith_neighbourhood_query import \
    ZenithNeighbourhoodQuery
from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from donuts import Donuts
from donuts.image import Image
from scipy.ndimage import median_filter

from astra.config import Config, ObservatoryConfig

CONFIG = Config()
OBSERVATORY_CONFIG = ObservatoryConfig.from_config(CONFIG)


class CustomImageClass(Image):
    def preconstruct_hook(self):
        clean = median_filter(self.raw_image, size=4, mode="mirror")
        band_corr = np.median(clean, axis=1).reshape(-1, 1)
        band_clean = clean - band_corr
        self.raw_image = band_clean


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
        calibration_coordinates=None,
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
        self.calibration_coordinates = calibration_coordinates
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
        if not self.calibration_coordinates:
            self.determine_guider_calibration_field()
        self._slew_telescope(
            ra=self.calibration_coordinates.ra.deg,
            dec=self.calibration_coordinates.dec.deg,
        )
        self.perform_calibration_cycles()
        self.complete_calibration_config()
        self.save_calibration_config()
        self.update_observatory_config()

    def determine_guider_calibration_field(self):
        """Determine the calibration field for the guider calibration field.

        It uses the following parameters from the action_value:
            - 'gaia_tmass_db_path': The path to the Gaia-Tmass database.
            - 'maximal_zenith_angle': The maximal zenith angle in degrees. Default is None.
            - 'airmass_threshold': The airmass threshold. Default is 1.01.
            - 'g_mag_range': The range of g magnitudes. Default is (0, 10).
            - 'j_mag_range': The range of j magnitudes. Default is (0, 10).
            - 'fov_height': The height of the field of view in argmins. Default is 11.666666 / 60.
            - 'fov_width': The width of the field of view in argmins. Default is 11.666666 / 60.
            - 'selection_method': The method for selecting the calibration field.

        In broad terms, the function determines the zenith neighbourhood of the observatory
        and selects a star from it. The selection method can be one of the following:
            - 'single': Select the star closest to zenith within the desired magnitude range
               that is alone in the fov.
            - 'maximal': Select the star closest to zenith within the desired magnitude range
               that has the maximal number of neighbours in the fov.
            - 'any': Select the star closest to zenith within the desired magnitude range.

        If the selection method is unsuccessful, the function will attempt
        to calibrate the guider at zenith.

        Raises:
            ValueError: If no observatory location is found in the header.
            ValueError: check_conditions return false.

        """
        # Find observatory location
        row, action_value, hdr = self.row, self.action_value, self.hdr

        if "ra" in action_value and "dec" in action_value:
            self.astra_observatory.logger.info(
                "Using user-specified calibration coordinates for autofocus."
            )
            self.calibration_coordinates = SkyCoord(
                ra=float(action_value["ra"]) * u.deg,
                dec=float(action_value["dec"]) * u.deg,
            )
            return

        self.astra_observatory.logger.info("Determining guider calibration field.")
        try:
            if not self.astra_observatory.check_conditions(row=row):
                raise ValueError("Autofocus aborted due to bad conditions.")
            observatory_location = EarthLocation(
                lat=hdr["LAT-OBS"] * u.deg,
                lon=hdr["LONG-OBS"] * u.deg,
                height=hdr["ALT-OBS"] * u.m,
            )
            self.astra_observatory.logger.info(
                f"Observatory location determined to be at {observatory_location}."
            )
        except Exception as e:
            raise ValueError(f"Error determining observatory location: {str(e)}.")

        try:
            if not CONFIG.gaia_db.exists() or not action_value.get("use_gaia", True):
                raise ValueError("gaia_tmass_db_path not specified in config.")

            maximal_zenith_angle = action_value.get("maximal_zenith_angle", None)
            if action_value.get("maximal_zenith_angle", None) is None:
                maximal_zenith_angle = (
                    find_airmass_threshold_crossover(
                        airmass_threshold=action_value.get("airmass_threshold", 1.01)
                    )
                    * 180
                    / np.pi
                    * u.deg
                )
            self.astra_observatory.logger.info(
                f"Computing coordinates for the guider calibration target with maximal zenith angle of "
                f"{maximal_zenith_angle}."
                # f"and selection method '{selection_method}'."
            )

            zenith_neighbourhood_query = (
                ZenithNeighbourhoodQuery.create_from_location_and_angle(
                    db_path=str(CONFIG.gaia_db),
                    observatory_location=observatory_location,
                    observation_time=action_value.get("observation_time", None),
                    maximal_zenith_angle=maximal_zenith_angle,
                )
            )

            self.astra_observatory.logger.info(
                "Zenith was determined to be at "
                f"{zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs}. "
                f"RA: {zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs.ra.value:.8f} deg, "
                f"DEC: {zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs.dec.value:.8f} deg."
            )

            znqr_full = zenith_neighbourhood_query.query_shardwise(n_sub_div=20)
            self.astra_observatory.logger.info(
                f"Retrieved {len(znqr_full)} stars in the neighbourhood of the zenith from the database.",
            )

            znqr = znqr_full.mask_by_magnitude(
                g_mag_range=action_value.get("g_mag_range", (0, 10)),
                j_mag_range=action_value.get("j_mag_range", (0, 10)),
            )
            self.astra_observatory.logger.info(
                f"Retrieved {len(znqr)} stars in the neighbourhood of the zenith from the database "
                "within the desired magnitude ranges.",
            )
            if not self.astra_observatory.check_conditions(row=row):
                raise ValueError("Guiding calibration aborted due to bad conditions.")

            # Determine the number of stars that would be on the ccd
            # if the telescope was centred on a given star
            znqr.determine_stars_in_neighbourhood(
                height=action_value.get("fov_height", 11.666666 / 60),
                width=action_value.get("fov_width", 11.666666 / 60),
            )
            if not self.astra_observatory.check_conditions(row=row):
                raise ValueError("Guiding calibration aborted due to bad conditions.")

            # Find the desired field of calibration
            znqr.sort_values(["zenith_angle", "n"], ascending=[True, True])

            selection_method = action_value.get("selection_method", "maximal")
            if selection_method == "single":
                centre_coordinates = znqr.get_sky_coord_of_select_star(
                    np.argmax(znqr.n == 1)
                )
            elif selection_method == "maximal":
                centre_coordinates = znqr.get_sky_coord_of_select_star(
                    np.argmax(znqr.n)
                )
            elif selection_method == "any":
                centre_coordinates = znqr.get_sky_coord_of_select_star(0)
            else:
                self.astra_observatory.logger.warning(
                    f"Unknown selection_method: {selection_method}. Fall back to 'single'."
                )
                centre_coordinates = znqr.get_sky_coord_of_select_star(
                    np.argmax(znqr.n == 1)
                )

            if centre_coordinates is None or not isinstance(
                centre_coordinates, SkyCoord
            ):
                raise ValueError("No suitable calibration field found.")

        except Exception as e:
            if not self.astra_observatory.check_conditions(row=row):
                raise ValueError("Guider calibration aborted due to bad conditions.")
            self.astra_observatory.logger.warning(
                f"Error determining guider calibration target coordinates: {str(e)}. "
                "Attempt to calibrate guider at zenith.",
            )
            # Try to calibrate the guider at zenith.
            try:
                centre_coordinates = SkyCoord(
                    AltAz(
                        obstime=Time.now(),
                        location=observatory_location,
                        alt=90 * u.deg,
                        az=0 * u.deg,
                    )
                ).icrs
                self.astra_observatory.logger.info(
                    "Guider calibration target coordinates set to zenith."
                )
            except Exception as e:
                raise ValueError(
                    f"Error determining zenith: {str(e)}."
                    "This is likely due to an error in the observatory location in the header."
                )

        self.calibration_coordinates = centre_coordinates

    def perform_calibration_cycles(self):
        """Nudge camera in direction U, D, L, R to determine its scale and orientation."""
        image_path = self._perform_exposure()
        donuts_ref = self._apply_donuts(image_path)

        for i in range(self.number_of_cycles):
            self.astra_observatory.logger.info(
                f"Starting cycle {i} of {self.number_of_cycles}."
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

                self._directions[direction_literal].append(direction_literal)
                self._scales[direction_literal].append(magnitude)
                self.astra_observatory.logger.info(
                    f"Shift {direction.name} is in direction {direction_literal} "
                    "of {magnitude} pixels."
                )

                donuts_ref = self._apply_donuts(image_path)

        self.astra_observatory.logger.info("Calibration cycles complete.")
        self.astra_observatory.logger.debug(
            f"Directions: {self._directions}", f"Scales: {self._scales}"
        )

    def complete_calibration_config(self):
        calibration_config = {
            "PIX2TIME": {"+x": None, "-x": None, "+y": None, "-y": None},
            "RA_AXIS": None,
            "DIRECTIONS": {"+x": None, "-x": None, "+y": None, "-y": None},
        }

        self.astra_observatory.logger.info("Checking directions...")
        for direction_index, direction in enumerate(self._directions):
            # check that the directions are the same every time for each orientation
            if len(set(self._directions[direction])) != 1:
                raise ValueError(
                    "Directions must be the same across all cycles. "
                    f"Direction number {direction_index} has {self._directions[direction]}."
                )

            direction_literal = self._directions[direction][0]
            if direction == 0:
                direction_name = "North"
            elif direction == 1:
                direction_name = "South"
            elif direction == 2:
                direction_name = "East"
                calibration_config["RA_AXIS"] = "x" if "x" in direction_literal else "y"
            elif direction == 3:
                direction_name = "West"
            else:
                direction_name = "Invalid direction"
                logging.warning("Invalid direction")

            calibration_config["PIX2TIME"][direction_literal] = (
                float(self.pulse_time / np.average(self._scales[direction]))
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
            f"Pulse guiding {guide_direction} for {duration} ms"
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
            subtract_bkg=True,
            downweight_edges=False,
            image_class=CustomImageClass,
        )

    def _slew_telescope(self, ra, dec, **kwargs):
        self.action_value["ra"] = ra
        self.action_value["dec"] = dec
        try:
            self.astra_observatory.setup_observatory(
                self.paired_devices, self.action_value
            )
        except Exception as e:
            self.astra_observatory.error_source.append(
                {
                    "device_type": "GuidingCalibrator",
                    "device_name": self.paired_devices["Telescope"],
                    "error": str(e),
                }
            )

    def _slew_telescope_one_hour_east_of_sidereal_meridian(self):
        local_sidereal_time = self._telescope.get("SiderealTime")
        target_right_ascension = local_sidereal_time - 1

        self.astra_observatory.logger.info(
            f"Local sidereal time: {local_sidereal_time:.2f} hours"
        )
        self.astra_observatory.logger.info(
            f"Slewing one hour east to coordinates: RA = {target_right_ascension:.2f} hours, "
            f"Dec = {0} degrees..."
        )

        self._slew_telescope(ra=target_right_ascension, dec=0)

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
