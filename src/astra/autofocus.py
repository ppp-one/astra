"""Automated telescope autofocus system for astronomical observations.

This module provides a comprehensive autofocus system for astronomical telescopes,
integrating with ALPACA devices and the astrafocus library. It supports various
focus measurement operators, automatic target selection using Gaia catalog data,
and sophisticated focusing algorithms for optimal image quality.

Key components:

- Camera, focuser, and telescope interfaces for ALPACA devices
- Automated target selection using Gaia star catalog
- Multiple focus measurement algorithms (HFR, FFT, variance-based)
- Defocusing and refocusing capabilities
- Comprehensive logging and result visualization

"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

import astropy.io.fits as fits
import astropy.units as u
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from astrafocus import ExtremumEstimatorRegistry, FocusMeasureOperatorRegistry
from astrafocus.autofocuser import (
    AnalyticResponseAutofocuser,
    NonParametricResponseAutofocuser,
)
from astrafocus.interface import (
    AutofocusDeviceManager,
    CameraInterface,
    FocuserInterface,
    TelescopeInterface,
)
from astrafocus.star_size_focus_measure_operators import StarSizeFocusMeasure
from astrafocus.targeting import (
    ZenithNeighbourhoodQuery,
)
from astropy.coordinates import AltAz, Angle, SkyCoord
from astropy.time import Time
from scipy import ndimage
from scipy.ndimage import median_filter

import astra
from astra.action_configs import AutofocusConfig, SelectionMethod
from astra.alpaca_device_process import AlpacaDevice
from astra.config import Config
from astra.logger import DatabaseLoggingHandler
from astra.paired_devices import PairedDevices
from astra.scheduler import Action

__all__ = ["Autofocuser"]


class AstraCamera(CameraInterface):
    """Camera interface for autofocus operations with ALPACA devices.

    Provides image acquisition capabilities for autofocus sequences,
    including exposure control, hot pixel removal, and data type handling.

    Args:
        observatory: Observatory instance for device control and logging.
        alpaca_device_camera (AlpacaDevice): ALPACA camera device instance.
        action (Action): Action configuration for the camera.
        maxadu: Maximum ADU value for camera, auto-detected if None.
    """

    def __init__(
        self,
        observatory: Any,
        alpaca_device_camera: AlpacaDevice,
        action: Action,
        maxadu: Optional[int] = None,
    ) -> None:
        self.observatory = observatory
        self.alpaca_device_camera = alpaca_device_camera

        self.action = action
        self.success = True

        self.maxadu = (
            maxadu if maxadu is not None else alpaca_device_camera.get("MaxADU")
        )

        super().__init__()

    def perform_exposure(
        self, texp: float, log_option: Optional[str] = None, use_light: bool = True
    ) -> np.ndarray:
        """Capture image with specified exposure time and settings.

        Args:
            texp (float): Exposure time in seconds.
            log_option (Optional[str]): Logging option for the exposure.
            use_light (bool): Whether to use light frame (vs bias frame).

        Returns:
            np.ndarray: Captured image data as numpy array.
        """
        exposure_successful, filepath = self._perform_exposure(
            texp=texp, log_option=log_option, use_light=use_light
        )
        if exposure_successful:
            with fits.open(filepath) as hdul:
                image = hdul[0].data  # type: ignore

            # image = self.remove_hot_pixels(image, kernel_size=5)
        else:
            image = np.array([])

        return image

    @staticmethod
    def remove_hot_pixels(
        image: np.ndarray, threshold: float = 5, kernel_size: int = 3
    ) -> np.ndarray:
        """Remove hot pixels from image using median filtering.

        Identifies and replaces hot pixels by comparing with a smoothed version
        of the image using median filtering.

        Args:
            image (np.ndarray): 2D image array to process.
            threshold (float): Threshold factor to identify hot pixels.
            kernel_size (int): Size of median filter kernel.

        Returns:
            np.ndarray: Image with hot pixels replaced by smoothed values.
        """
        # Create a copy of the image to avoid modifying the original
        cleaned_image = np.copy(image)

        # Create a smoothed version of the image using a median filter
        smoothed_image = median_filter(cleaned_image, size=kernel_size)

        # Calculate the difference between the original and smoothed images
        difference = cleaned_image - smoothed_image

        # Identify hot pixels based on the difference
        hot_pixels = difference > (threshold * np.std(difference))

        # Replace hot pixels with the smoothed value
        cleaned_image[hot_pixels] = smoothed_image[hot_pixels]

        return cleaned_image

    def _perform_exposure(
        self, texp: float, log_option: Optional[str] = None, use_light: bool = True
    ) -> tuple[bool, Path | None]:
        """Internal method to perform camera exposure.

        Handles the actual camera exposure operation and updates success status.

        Args:
            texp (float): Exposure time in seconds.
            log_option (Optional[str]): Logging option for the exposure.
            use_light (bool): Whether to use light frame (vs bias frame).
        """
        exposure_successful, filepath = self.observatory.perform_exposure(
            camera=self.alpaca_device_camera,
            exptime=texp,
            maxadu=self.maxadu,
            action=self.action,
            use_light=use_light,
            log_option=log_option,
        )
        self.success = exposure_successful

        if not self.success:
            self.success = False
            self.observatory.logger.warning("Exposure failed.")
            # raise ValueError("Exposure failed.")

        return exposure_successful, filepath


class AstraFocuser(FocuserInterface):
    """Focuser interface for Astra observatory automation system.

    Provides position control interface for telescope focuser through ALPACA protocol.

    Args:
        observatory (astra.Observatory): Main Astra instance for system coordination.
        alpaca_device_focuser (AlpacaDevice): ALPACA focuser device interface.
        action (Action): Action configuration for the focuser.
    """

    def __init__(
        self,
        observatory: "astra.observatory.Observatory",
        alpaca_device_focuser: AlpacaDevice,
        action: Optional[Action] = None,
        settle_time: int = 3,
    ) -> None:
        if not alpaca_device_focuser.get("Absolute"):
            raise ValueError("Focuser must be absolute for autofocusing to work.")

        self.observatory = observatory
        self.action = action
        self.alpaca_device_focuser = alpaca_device_focuser
        self.settle_time = settle_time

        current_position = self.get_current_position()
        allowed_range = (0, alpaca_device_focuser.get("MaxStep"))
        super().__init__(current_position=current_position, allowed_range=allowed_range)

    def move_focuser_to_position(
        self, new_position: int, hard_timeout: float = 120
    ) -> None:
        """Move focuser to specified position with timeout protection.

        Moves the focuser to the target position and waits for completion.
        Includes range checking and timeout protection.

        Args:
            new_position (int): Target focuser position.
            hard_timeout (float): Maximum time to wait for movement in seconds.

        Raises:
            TimeoutError: If movement takes longer than hard_timeout.
        """
        new_position = self._project_to_allowed_range(new_position)

        self.alpaca_device_focuser.get("Move", Position=new_position)
        start_time = time.time()
        while self.alpaca_device_focuser.get("IsMoving"):
            if time.time() - start_time > hard_timeout:
                raise TimeoutError("Slew timeout")
            time.sleep(0.1)

        time.sleep(self.settle_time)
        return None

    def get_current_position(self) -> int:
        """Get the current focuser position.

        Returns:
            int: Current position of the focuser.
        """
        return self.alpaca_device_focuser.get("Position")

    def _project_to_allowed_range(self, new_position: int) -> int:
        """Project position to the allowed focuser range.

        Ensures the requested position is within the focuser's allowed range,
        adjusting to boundaries if necessary with warning messages.

        Args:
            new_position (int): Requested focuser position.

        Returns:
            int: Position adjusted to allowed range.
        """
        if self.allowed_range[0] is not None and new_position < self.allowed_range[0]:
            new_position = self.allowed_range[0]
            self.observatory.logger.warning(
                f"Requested focuser position {new_position} is below the allowed range. "
                f"Moving focuser to {self.allowed_range[0]} instead."
            )
        if self.allowed_range[1] is not None and new_position > self.allowed_range[1]:
            new_position = self.allowed_range[1]
            self.observatory.logger.warning(
                f"Requested focuser position {new_position} is above the allowed range. "
                f"Moving focuser to MaxStep {self.allowed_range[1]} instead."
            )

        return new_position


class AstraTelescope(TelescopeInterface):
    """Telescope interface for Astra observatory automation system.

    Provides coordinate control and pointing interface for telescope through ALPACA protocol.

    Args:
        observatory (astra.observatory.Observatory): Observatory instance.
        alpaca_device_telescope (AlpacaDevice): ALPACA telescope device interface.
        action (Action): Action configuration for the telescope.
    """

    def __init__(
        self,
        observatory: "astra.observatory.Observatory",
        alpaca_device_telescope: AlpacaDevice,
        action: Action,
    ) -> None:
        self.observatory = observatory
        self.alpaca_device_telescope = alpaca_device_telescope

        self.action = action
        super().__init__()

    def set_telescope_position(
        self, coordinates: SkyCoord, hard_timeout: float = 120
    ) -> None:
        """Move telescope to specified celestial coordinates.

        Args:
            coordinates (SkyCoord): Target celestial coordinates.
            hard_timeout (float): Maximum time to wait for slew completion in seconds.
        """
        self.alpaca_device_telescope.get(
            "SlewToCoordinatesAsync",
            RightAscension=coordinates.ra.hour,
            Declination=coordinates.dec.deg,
        )

        # Wait for slew to finish
        start_time = time.time()
        while self.alpaca_device_telescope.get("Slewing"):
            if time.time() - start_time > hard_timeout:
                raise TimeoutError("Slew timeout")
            if not self.observatory.check_conditions(self.action):
                break

            time.sleep(1)


class AstraAutofocusDeviceManager(AutofocusDeviceManager):
    """Device manager for Astra autofocus operations.

    Coordinates camera, focuser, and telescope for automated focusing operations
    using the astrafocus library framework.

    Args:
        observatory (astra.observatory.Observatory): Observatory instance.
        action_value (dict): Configuration values for autofocus action.
        action (Action): Action configuration for the autofocus.
        astra_camera (AstraCamera): Camera interface for image capture.
        astra_focuser (AstraFocuser): Focuser interface for position control.
        astra_telescope (Optional[AstraTelescope]): Telescope interface for positioning.
    """

    def __init__(
        self,
        observatory: "astra.observatory.Observatory",
        action_value: dict,
        action: Action,
        astra_camera: AstraCamera,
        astra_focuser: AstraFocuser,
        astra_telescope: Optional[AstraTelescope] = None,
    ) -> None:
        self.observatory = observatory
        self.action_value = action_value
        self.action = action
        super().__init__(
            camera=astra_camera, focuser=astra_focuser, telescope=astra_telescope
        )

    @classmethod
    def from_action(
        cls,
        observatory: "astra.observatory.Observatory",
        action: Action,
        paired_devices: PairedDevices,
    ) -> "AstraAutofocusDeviceManager":
        """Create device manager from the scheduled action.

        Factory method to construct an autofocus device manager from
        observatory configuration and device mappings.

        Args:
            observatory (astra.observatory.Observatory): Astra Observatory instance.
            action (Action): Scheduled autofocus action.
            paired_devices (PairedDevices): Device manager instance.

        Returns:
            AstraAutofocusDeviceManager: Configured device manager instance.
        """
        action_value = action.action_value

        alpaca_device_camera = observatory.devices["Camera"][action.device_name]
        alpaca_device_focuser = observatory.devices["Focuser"][
            paired_devices["Focuser"]
        ]
        alpaca_device_telescope = observatory.devices["Telescope"][
            paired_devices["Telescope"]
        ]

        astra_camera = AstraCamera(
            observatory,
            alpaca_device_camera=alpaca_device_camera,
            action=action,
        )
        astra_focuser = AstraFocuser(
            observatory,
            alpaca_device_focuser=alpaca_device_focuser,
            action=action,
            settle_time=paired_devices.get_device_config("Focuser").get(
                "settle_time", 3
            ),
        )
        astra_telescope = AstraTelescope(
            observatory, alpaca_device_telescope=alpaca_device_telescope, action=action
        )

        return cls(
            observatory=observatory,
            action_value=action_value,
            action=action,
            astra_camera=astra_camera,
            astra_focuser=astra_focuser,
            astra_telescope=astra_telescope,
        )

    def check_conditions(self) -> bool:
        """Check if observatory conditions are suitable for autofocus.

        Returns:
            bool: True if conditions are acceptable, False otherwise.
        """
        return self.observatory.check_conditions(action=self.action)


class Defocuser:
    """Defocusing control for creating intentionally out-of-focus images.

    Used for specific observational techniques requiring defocused stellar images,
    such as photometry of very bright stars or specific calibration procedures.

    Args:
        observatory (astra.observatory.Observatory): Astra observatory instance.
        paired_devices (PairedDevices): Device manager for observatory components.
        action (Optional[Action]): Action configuration for the focuser.
    """

    def __init__(
        self,
        observatory: Any,
        paired_devices: PairedDevices,
        action: Optional[Action] = None,
    ) -> None:
        self.observatory = observatory
        self.action = action
        self.paired_devices = paired_devices

        self._focuser = AstraFocuser(
            observatory=observatory,
            alpaca_device_focuser=paired_devices.focuser,
            action=action,
        )
        self.best_focus_position = self.load_best_focus_position_from_config()

    @property
    def focuser_name(self) -> str:
        """Get the name of the connected focuser device.

        Returns:
            str: Name identifier of the focuser device.
        """
        return self.paired_devices["Focuser"]

    def load_best_focus_position_from_config(self) -> int:
        """Load the best focus position from device configuration.

        Retrieves the stored focus position from the focuser configuration.

        Returns:
            int: Best focus position from configuration.

        Raises:
            ValueError: If focuser configuration is not found or missing focus_position.
        """
        focuser_config = self.paired_devices.get_device_config("Focuser")
        if "focus_position" not in focuser_config:
            self.observatory.logger.warning(
                "No best focus position found in focuser configuration. "
                "Using current position as best focus position."
                f" Focuser: {self.focuser_name}"
                f"focuser_config: {focuser_config}"
            )
            raise ValueError("Focuser configuration not found in paired devices.")

        return focuser_config["focus_position"]

    @property
    def current_position(self) -> int:
        """Get the current focuser position.

        Returns:
            int: Current position of the focuser.
        """
        return self._focuser.get_current_position()

    def defocus(self, position: int) -> None:
        """Move focuser to a defocused position.

        Intentionally moves the focuser away from the best focus position
        for specialized observing techniques.

        Args:
            position (int): Target defocus position.
        """
        if position == self.current_position:
            self.observatory.logger.debug(
                f"Focuser {self.focuser_name} already at position "
                f"{position}. No change of focus needed."
            )
            return

        current_position = self._focuser.get_current_position()
        shift = position - current_position
        self.observatory.logger.info(
            f"Defocusing by {shift} steps from current position {current_position} "
            f"to new position {position}."
        )

        self._focuser.move_focuser_to_position(position)

    def refocus(self) -> None:
        """Return focuser to the best focus position.

        Moves the focuser back to the stored best focus position
        after defocusing operations.
        """
        if self.current_position == self.best_focus_position:
            self.observatory.logger.debug(
                f"Focuser {self.focuser_name} already at best "
                f"focus position {self.best_focus_position}. No refocusing needed."
            )
            return
        self.observatory.logger.info(
            f"Refocusing from current position {self._focuser.get_current_position()} "
            f"to the best focus position: {self.best_focus_position}."
        )
        self._focuser.move_focuser_to_position(self.best_focus_position)


class Autofocuser:
    """Main autofocus orchestration class for Astra observatory.

    Coordinates complete autofocus operations including target selection,
    focus measurement, and optimization using multiple algorithms.

    Args:
        observatory (Any): Main Astra instance for system coordination.
        action (Action): Action configuration for the autofocus.
        paired_devices (PairedDevices): Device manager for observatory components.
        action_value (dict): Configuration values for autofocus action.
        save_path (Optional[Path]): Directory path for saving autofocus data.
        autofocuser (Optional[Union[NonParametricResponseAutofocuser, AnalyticResponseAutofocuser]]):
            Specific autofocus algorithm instance.
        success (bool): Success flag for autofocus operation.
    """

    def __init__(
        self,
        observatory: Any,
        action: Action,
        paired_devices: PairedDevices,
        autofocuser: Optional[
            Union[NonParametricResponseAutofocuser, AnalyticResponseAutofocuser]
        ] = None,
        success: bool = True,
    ) -> None:
        self.observatory = observatory
        self.action = action
        self.paired_devices = paired_devices
        self.action_value = action.action_value
        self.autofocuser = autofocuser
        self.success = success
        self.config: AutofocusConfig = action.action_value  # type: ignore

        default_dict = paired_devices.get_device_config("Camera").get("autofocus", {})
        logging.info(f"default_dict {default_dict}")

        if (
            self.config.calibration_field.fov_width == 0
            or self.config.calibration_field.fov_height == 0
        ):
            fov_width, fov_height = self.determine_default_field_of_view(
                paired_devices, default_dict
            )
            self.config.calibration_field.fov_width = fov_width
            self.config.calibration_field.fov_height = fov_height

        self._initialise_logging()
        self.observatory.logger.info(f"Loaded action values {self.action_value}")
        self.observatory.logger.info(f"Autofocus configuration: {self.config}.")

    @property
    def best_focus_position(self) -> int:
        """Get the best focus position found by the autofocuser.

        Returns:
            int: Optimal focus position determined by autofocus algorithm.

        Raises:
            ValueError: If autofocuser has not been set up yet.
        """
        if self.autofocuser is None:
            raise ValueError("Autofocuser has not been set up yet.")
        return self.autofocuser.best_focus_position

    def run(self) -> bool:
        """Execute the autofocus sequence.

        Runs the complete autofocus operation using the configured algorithm.

        Returns:
            bool: True if autofocus completed successfully, False otherwise.
        """
        if not self.success or not self.observatory.check_conditions(
            action=self.action
        ):
            return False

        try:
            if not self.autofocuser:
                raise ValueError("Autofocuser has not been set up yet.")
            return self.autofocuser.run()

        except Exception as e:
            self.observatory.logger.report_device_issue(
                device_type="Autofocuser",
                device_name=self.paired_devices["Telescope"],
                message="Error running autofocus",
                exception=e,
            )
            self.success = False
            return False

    def setup(self) -> None:
        """Set up the autofocus system with devices and algorithms.

        Configures camera, focuser, telescope interfaces and initializes
        the autofocus algorithm with specified parameters.
        """
        if not self._check_conditions():
            return

        try:
            self._setup()
        except Exception as e:
            self.observatory.logger.report_device_issue(
                device_type="Autofocuser",
                device_name=self.paired_devices["Telescope"],
                message="Error extracting action_value for autofocus",
                exception=e,
            )
            self.success = False

    def _setup(self) -> None:
        """Internal setup method for autofocus configuration.

        Configures save paths, device managers, focus measure operators,
        and autofocus algorithms based on action values.
        """
        autofocus_device_manager = AstraAutofocusDeviceManager.from_action(
            self.observatory,
            action=self.action,
            paired_devices=self.paired_devices,
        )
        focus_measure_operator = self.determine_focus_measure_operator()

        # Reduce exposure time if necessary
        if self.config.reduce_exposure_time:
            # Clean image, CustomImageClass
            self.config.exptime = self.reduce_exposure_time(
                autofocus_device_manager=autofocus_device_manager,
                exposure_time=self.config.exptime,
                reduction_factor=2,
                max_reduction_steps=5,
                minimal_exposure_time=0.1,
            )

        initial_focus_position = None
        if (
            self.config.search_range_is_relative
            and self.config.search_range is not None
        ):
            initial_focus_position = self.paired_devices.get_device_config(
                "Focuser"
            ).get("focus_position", None)
            if initial_focus_position is None:
                initial_focus_position = (
                    autofocus_device_manager.focuser.get_current_position()
                )
                self.observatory.logger.info(
                    "No best focus position found in focuser configuration. "
                    f"Using current position {initial_focus_position} "
                    "to define autofocus search range instead."
                )

        autofocus_args = dict(
            autofocus_device_manager=autofocus_device_manager,
            search_range=self.config.search_range,  # None defaults to allowed focuser range
            n_steps=self.config.n_steps,
            n_exposures=self.config.n_exposures,
            decrease_search_range=self.config.decrease_search_range,
            exposure_time=self.config.exptime,
            # save_path=self.config.save_path,
            secondary_focus_measure_operators=self.config._secondary_focus_measure_operators,
            focus_measure_operator_kwargs=self.config.focus_measure_operator_kwargs,
            search_range_is_relative=self.config.search_range_is_relative,
            initial_position=initial_focus_position,
            keep_images=True,
        )
        self.observatory.logger.debug(f"Autofocus arguments: {autofocus_args}")

        if issubclass(focus_measure_operator, StarSizeFocusMeasure):
            autofocuser = AnalyticResponseAutofocuser(
                focus_measure_operator=focus_measure_operator,
                percent_to_cut=self.config.percent_to_cut,
                **autofocus_args,
            )
            self.observatory.logger.info(
                f"Using the focus_measure_operator {self.config.focus_measure_operator_name} "
            )
        else:
            extremum_estimator = self.determine_extremum_estimator()
            autofocuser = NonParametricResponseAutofocuser(
                focus_measure_operator=focus_measure_operator(),
                extremum_estimator=extremum_estimator,
                **autofocus_args,
            )
            self.observatory.logger.info(
                f"Using the extremum_estimator {extremum_estimator}"
            )

        self.observatory.logger.debug(f"Using autofocuser {autofocuser}.")

        self.autofocuser = autofocuser

    def determine_autofocus_calibration_field(self):
        """Determine optimal celestial coordinates for autofocus operation.

        Selects suitable star field for autofocus using Gaia catalog data and
        zenith neighborhood analysis. Uses various selection criteria including
        magnitude ranges, field of view, and star density preferences.

        Selection methods:
            - 'single': Isolated star closest to zenith
            - 'maximal': Star with maximum neighbors in field
            - 'any': Any suitable star closest to zenith

        Updates self.config.calibration_field.coordinates with selected coordinates or
        sets self.success to False if no suitable field found.
        """
        if not self._check_conditions():
            return None

        try:
            self._determine_autofocus_calibration_field()
            if not isinstance(self.config.calibration_field.coordinates, SkyCoord):
                self.success = False

        except Exception as e:
            self.success = False
            self.observatory.logger.report_device_issue(
                device_type="Autofocuser",
                device_name=self.paired_devices["Telescope"],
                message="Error determining autofocus calibration field",
                exception=e,
            )

    def _determine_autofocus_calibration_field(self) -> None:
        """Determine the calibration field for the autofocus using config attributes."""
        calibration_config = self.config.calibration_field

        # Use user-specified coordinates if present
        if calibration_config.ra is not None and calibration_config.dec is not None:
            self.observatory.logger.info(
                "Using user-specified calibration coordinates for autofocus."
            )
            calibration_config.coordinates = SkyCoord(
                ra=Angle(float(calibration_config.ra), u.deg),
                dec=Angle(float(calibration_config.dec), u.deg),
            )
            return

        self.observatory.logger.info("Determining autofocus calibration field.")
        try:
            observatory_location = (
                self.observatory.image_handler.get_observatory_location()
            )
            logging.info(
                f"Observatory location determined to be at {observatory_location}."
            )
        except Exception as e:
            raise ValueError(f"Error determining observatory location: {str(e)}.")

        try:
            if not Config().gaia_db.exists() or not calibration_config.use_gaia:
                raise ValueError("gaia_tmass_db_path not specified in config.")

            self.observatory.logger.info(
                f"Computing coordinates for the autofocus target with maximal zenith angle of "
                f"{calibration_config.maximal_zenith_angle}."
            )
            zenith_neighbourhood_query = (
                ZenithNeighbourhoodQuery.create_from_location_and_angle(
                    db_path=Config().gaia_db,
                    observatory_location=observatory_location,
                    observation_time=calibration_config.observation_time,
                    maximal_zenith_angle=calibration_config.maximal_zenith_angle,
                    maximal_number_of_stars=calibration_config.maximal_number_of_stars,
                )
            )

            self.observatory.logger.info(
                "Zenith was determined to be at "
                f"{zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs!r}."
            )

            min_phot_g_mean_mag, max_phot_g_mean_max = calibration_config.g_mag_range
            min_j_m, max_j_m = calibration_config.j_mag_range
            znqr = zenith_neighbourhood_query.query_shardwise(
                n_sub_div=20,
                min_phot_g_mean_mag=min_phot_g_mean_mag,
                max_phot_g_mean_mag=max_phot_g_mean_max,
                min_j_m=min_j_m,
                max_j_m=max_j_m,
            )

            self.observatory.logger.info(
                f"Retrieved {len(znqr)} stars in the neighbourhood of the zenith from the database "
                "within the desired magnitude ranges.",
            )
            if not self.observatory.check_conditions(action=self.action):
                return

            znqr.determine_stars_in_neighbourhood(
                height=calibration_config.fov_height,
                width=calibration_config.fov_width,
            )
            if not self.observatory.check_conditions(action=self.action):
                return

            znqr.sort_values(["zenith_angle", "n"], ascending=[True, True])

            selection_method = calibration_config.selection_method
            if selection_method == SelectionMethod.SINGLE:
                centre_coordinates = znqr.get_sky_coord_of_select_star(
                    np.argmax(znqr.n == 1)
                )
            elif selection_method == SelectionMethod.MAXIMAL:
                centre_coordinates = znqr.get_sky_coord_of_select_star(
                    np.argmax(znqr.n)
                )
            elif selection_method == SelectionMethod.ANY:
                centre_coordinates = znqr.get_sky_coord_of_select_star(0)
            else:
                # This should never happen due to enum selection
                raise ValueError(f"Unknown selection_method: {selection_method}")

            if centre_coordinates is None or not isinstance(
                centre_coordinates, SkyCoord
            ):
                raise ValueError("No suitable calibration field found.")

            calibration_config.coordinates = centre_coordinates

        except Exception as e:
            if not self.observatory.check_conditions(action=self.action):
                return
            self.observatory.logger.warning(
                f"Error determining autofocus target coordinates: {str(e)}. "
                "Attempt to autofocus at zenith.",
            )
            try:
                calibration_config.coordinates = SkyCoord(
                    AltAz(
                        obstime=Time.now(),
                        location=observatory_location,
                        alt=Angle(90.0, unit=u.deg),
                        az=Angle(0.0, unit=u.deg),
                    )
                ).icrs  # type: ignore
                self.observatory.logger.info(
                    "Autofocus target coordinates set to zenith."
                )
            except Exception as e:
                raise ValueError(
                    f"Error determining zenith: {str(e)}."
                    "This is likely due to an error in the observatory location in the header."
                )

    def slew_to_calibration_field(self) -> None:
        """Slew telescope to the determined autofocus calibration field.

        Moves the telescope to the coordinates selected for autofocus operations.
        Updates action_value with target coordinates and initiates observatory setup.
        """
        if not self.success:
            return None

        self.observatory.logger.debug(
            "Slewing to autofocus calibration field: "
            f"{self.config.calibration_field.coordinates!r}"
        )
        self.action_value["ra"] = self.config.calibration_field.coordinates.ra.deg
        self.action_value["dec"] = self.config.calibration_field.coordinates.dec.deg
        try:
            self.observatory.setup_observatory(self.paired_devices, self.action_value)
        except Exception as e:
            self.observatory.logger.report_device_issue(
                device_type="Autofocuser",
                device_name=self.paired_devices["Telescope"],
                message="Error slewing to autofocus calibration field",
                exception=e,
            )
            self.success = False

    def reduce_exposure_time(
        self,
        autofocus_device_manager: AstraAutofocusDeviceManager,
        exposure_time: float,
        reduction_factor: float = 2,
        max_reduction_steps: int = 5,
        minimal_exposure_time: float = 0.1,
    ) -> float:
        """Automatically reduce exposure time to prevent saturation.

        Takes test exposures and progressively reduces exposure time if saturation
        is detected, ensuring optimal image quality for focus measurements.

        Args:
            autofocus_device_manager (AstraAutofocusDeviceManager): Device manager for camera access.
            exposure_time (float): Initial exposure time in seconds.
            reduction_factor (float): Factor by which to reduce exposure time.
            max_reduction_steps (int): Maximum number of reduction iterations.
            minimal_exposure_time (float): Minimum allowed exposure time.

        Returns:
            float: Optimal exposure time that avoids saturation.
        """
        new_exposure_time = exposure_time
        for _ in range(max_reduction_steps):
            if new_exposure_time < minimal_exposure_time:
                self.observatory.logger.warning(
                    f"Minimal exposure time of {minimal_exposure_time} reached. "
                    f"Cannot reduce exposure time further. Image might still be saturated.",
                )
                return new_exposure_time * reduction_factor

            image = autofocus_device_manager.camera.perform_exposure(
                texp=new_exposure_time, use_light=True
            )

            clean = ndimage.median_filter(image, size=4, mode="mirror")
            band_corr = np.median(clean, axis=1).reshape(-1, 1)
            band_clean = clean - band_corr

            if band_clean.max() > 0.9 * autofocus_device_manager.camera.maxadu:
                new_exposure_time = new_exposure_time / reduction_factor
            else:
                break

        if band_clean.max() > 0.9 * autofocus_device_manager.camera.maxadu:
            self.observatory.logger.warning(
                f"Reduced exposure time of {exposure_time} s is still saturating. "
            )
        elif new_exposure_time != exposure_time:
            self.observatory.logger.warning(
                f"Reduced exposure time from {exposure_time} to {new_exposure_time} "
                f"to avoid saturation.",
            )

        return new_exposure_time

    def make_summary_plot(self) -> None:
        """Create visualization plot of autofocus results.

        Generates a summary plot showing focus measure vs focuser position
        with the determined best focus position marked. Saves plot to the
        autofocus results directory.
        """
        try:
            if self.success is False:
                return

            # Determine directory to write the summary to. Prefer configured save_path;
            # if not provided, fall back to the directory containing the last saved
            # autofocus image for this camera (if available).
            save_dir: Path | None = None
            if self.config.save_path is not None:
                save_dir = Path(self.config.save_path)

            if save_dir is None:
                # try to find last image saved by this camera
                try:
                    image_handler = self.observatory.get_image_handler(
                        self.action.device_name
                    )
                    last_image_path = getattr(image_handler, "last_image_path", None)
                except Exception:
                    last_image_path = None

                if last_image_path is None:
                    self.observatory.logger.warning(
                        "Skipping creation of summary plot: no save_path configured and no last image available."
                    )
                    return

                save_dir = last_image_path.parent

            # Obtain focus record dataframe. Prefer in-memory record from the
            # astrafocus autofocuser instance; if not available, try reading CSVs
            # from the chosen directory.
            df: pd.DataFrame | None = None
            if (
                hasattr(self, "autofocuser")
                and getattr(self.autofocuser, "focus_record", None) is not None
            ):
                try:
                    df = self.autofocuser.focus_record
                except Exception:
                    df = None

            if df is None:
                assert save_dir is not None
                csv_files = sorted(
                    [p for p in Path(save_dir).iterdir() if p.suffix == ".csv"],
                    key=lambda p: p.stat().st_mtime,
                )
                if not csv_files:
                    self.observatory.logger.error(
                        f"No focus record CSV found in {save_dir}. Skipping summary plot."
                    )
                    return
                df = pd.read_csv(csv_files[-1])

            df = df.sort_values("focus_pos")

            matplotlib.use("Agg")
            _, ax = plt.subplots(dpi=300)
            ax.plot(
                df["focus_pos"], df["focus_measure"], color="black", marker=".", ls=""
            )
            ax.set_xlabel("Focuser position")
            ax.set_ylabel(f"Focus measure ({self.config.focus_measure_operator_name})")

            ax.axvline(
                self.best_focus_position,
                color="red",
                ls="--",
                zorder=-1,
                label="Best focus position",
            )
            ax.legend()

            # Build output filename: if we read a CSV file use its stem, otherwise timestamp it.
            if "csv_files" in locals() and csv_files:
                out_name = f"{csv_files[-1].stem}.png"
            else:
                out_name = (
                    f"autofocus_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                )

            assert save_dir is not None
            out_path = Path(save_dir) / out_name
            plt.savefig(out_path)
            plt.close()
        except Exception as e:
            self.observatory.logger.exception(f"Error creating summary plot: {str(e)}")

    def create_result_file(self) -> None:
        """Create text file with autofocus results summary.

        Writes a summary file containing the best focus position, algorithm used,
        and autofocuser configuration for future reference.
        """
        if self.success is False:
            return
        save_dir: Path | None = None
        if self.config.save_path is not None:
            save_dir = Path(self.config.save_path)

        if save_dir is None:
            try:
                image_handler = self.observatory.get_image_handler(
                    self.action.device_name
                )
                last_image_path = getattr(image_handler, "last_image_path", None)
            except Exception:
                last_image_path = None

            if last_image_path is None:
                self.observatory.logger.error(
                    "Skipping creation of log file: no save_path configured and no last image available."
                )
                return

            save_dir = last_image_path.parent

        # derive a timestring for filename; prefer an adjacent CSV if present
        timestr = None
        assert save_dir is not None
        csv_files = sorted(
            [p for p in Path(save_dir).iterdir() if p.suffix == ".csv"],
            key=lambda p: p.stat().st_mtime,
        )
        if csv_files:
            timestr = csv_files[-1].stem.split("_")[0]

        if not timestr:
            timestr = datetime.now().strftime("%Y%m%d_%H%M%S")

        result_file_path = Path(save_dir) / f"{timestr}_result.txt"
        try:
            with open(result_file_path, "w") as result_file:
                result_file.write(f"Best focus position: {self.best_focus_position}\n")
                result_file.write(
                    f"Focus measure operator: {self.config.focus_measure_operator_name}\n"
                )
                result_file.write(f"Autofocuser: {self.autofocuser}\n")
        except Exception as e:
            self.observatory.logger.exception(f"Error creating log file: {str(e)}")

    def _initialise_logging(self) -> None:
        """Set up logging integration with astrafocus library.

        Configures the astrafocus logger to use Astra's logging system
        for consistent log formatting and storage.
        """
        if logging.getLogger("astrafocus").hasHandlers():
            logging.getLogger("astrafocus").handlers.clear()
        logging.getLogger("astrafocus").addHandler(
            DatabaseLoggingHandler(self.observatory.database_manager)
        )

    def _check_conditions(self) -> bool:
        """Verify observatory conditions are suitable for autofocus.

        Checks weather, equipment status, and other conditions before
        proceeding with autofocus operations.

        Returns:
            bool: True if conditions are acceptable, False otherwise.
        """
        if not self.observatory.check_conditions(action=self.action):
            self.observatory.logger.error("Autofocus aborted due to bad conditions.")
            self.success = False

        return self.success

    def save_best_focus_position(self) -> None:
        """Save determined best focus position to observatory configuration.

        Updates the focuser configuration with the optimal focus position
        found during autofocus operation for future use.
        """
        if not self.success or not self.config.save:
            return

        self.observatory.logger.info(
            f"Saving best focus position {self.best_focus_position} "
            f"of type {type(self.best_focus_position)} "
        )
        self.paired_devices.get_device_config("Focuser")["focus_position"] = int(
            self.best_focus_position
        )
        self.paired_devices.observatory_config.save()

    def determine_focus_measure_operator(self):
        """Select focus measurement algorithm from configuration.

        Determines the appropriate focus measure operator based on user preferences,
        supporting various algorithms like HFR, 2D Gaussian, FFT, and variance-based methods.

        Returns:
            Tuple[Any, str]: Focus measure operator class and descriptive name.
        """
        focus_measure_operator = FocusMeasureOperatorRegistry.from_name(
            self.config.focus_measure_operator
        )

        return focus_measure_operator

    def determine_extremum_estimator(
        self,
    ):
        """Select extremum estimation algorithm for focus curve analysis.

        Chooses appropriate curve fitting method for determining optimal focus
        from focus measure vs position data. Supports LOWESS, median filter,
        spline, and RBF methods.

        Returns:
            astrafee.RobustExtremumEstimator: Configured extremum estimator instance.
        """
        extremum_estimator_class = ExtremumEstimatorRegistry.from_name(
            self.config.extremum_estimator
        )
        extremum_estimator = extremum_estimator_class(
            **self.config.extremum_estimator_kwargs
        )
        self.observatory.logger.info(
            f"Initialised extremum estimator: {extremum_estimator.__class__.__name__} "
            f"with parameters: {self.config.extremum_estimator_kwargs}"
        )
        return extremum_estimator

    def calculate_field_of_view(self, paired_devices):
        """
        Calculate the field of view of the camera-telescope system.
        """
        try:
            camera = paired_devices.camera
            telescope = paired_devices.telescope

            # Convert microns to meters
            pixel_size = 1e-6 * np.array(
                [camera.get("PixelSizeX"), camera.get("PixelSizeY")]
            )
            number_of_pixels = np.array([camera.get("NumX"), camera.get("NumY")])

            focal_length = telescope.get("FocalLength")  # meters
            # plate_scale = np.arctan(pixel_size / focal_length)

            # field_of_view = plate_scale * number_of_pixels
            sensor_size = pixel_size * number_of_pixels  # [sx, sy]

            fov = 2.0 * np.arctan(sensor_size / (2.0 * focal_length)) * (180.0 / np.pi)
            return fov

        except Exception as e:
            field_of_view = np.array([np.nan, np.nan])
            self.observatory.error(
                f"Error calculating field of view from paired devices. Exception: {e}"
            )

        return field_of_view

    def determine_default_field_of_view(self, paired_devices, default_dict):
        field_of_view = self.calculate_field_of_view(paired_devices)
        fov_width = float(default_dict.get("fov_width", field_of_view[0]))
        fov_height = float(default_dict.get("fov_height", field_of_view[1]))

        self.observatory.logger.info(
            f"Determined field of view width={fov_width}, height={fov_height}."
        )
        return fov_width, fov_height
