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

Classes:
    AstraCamera: Camera interface for autofocus operations
    AstraFocuser: Focuser interface with position control
    AstraTelescope: Telescope interface for positioning
    AstraAutofocusDeviceManager: Device coordination for autofocus
    Defocuser: Controlled defocusing and refocusing operations
    Autofocuser: Main autofocus orchestrator with target selection
"""

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, Union

import astrafocus.extremum_estimators as astrafee
import astrafocus.focus_measure_operators as astrafmo
import astrafocus.star_size_focus_measure_operators as astrasfmo
import astropy.units as u
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from astrafocus.autofocuser import (
    AnalyticResponseAutofocuser,
    NonParametricResponseAutofocuser,
)
from astrafocus.interface.camera import CameraInterface
from astrafocus.interface.device_manager import AutofocusDeviceManager
from astrafocus.interface.focuser import FocuserInterface
from astrafocus.interface.telescope import TelescopeInterface

# from astrafocus.interface import CameraInterface, AutofocusDeviceManager, FocuserInterface, TelescopeInterface
from astrafocus.targeting.airmass_models import find_airmass_threshold_crossover
from astrafocus.targeting.zenith_neighbourhood_query import ZenithNeighbourhoodQuery
from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from scipy import ndimage
from scipy.ndimage import median_filter

from astra.alpaca_device_process import AlpacaDevice
from astra.config import Config, ObservatoryConfig
from astra.logging_handler import LoggingHandler
from astra.paired_devices import PairedDevices

CONFIG = Config()

__all__ = ["AstraAutofocusDeviceManager", "Autofocuser"]


class AstraCamera(CameraInterface):
    """Camera interface for autofocus operations with ALPACA devices.

    Provides image acquisition capabilities for autofocus sequences,
    including exposure control, hot pixel removal, and data type handling.

    Args:
        astra: Observatory instance for device control and logging.
        alpaca_device_camera (AlpacaDevice): ALPACA camera device instance.
        row (Dict[str, Any]): Camera configuration parameters.
        folder (str): Directory path for saving images.
        hdr (Dict[str, Any]): FITS header data for images.
        image_data_type: NumPy data type for images, auto-detected if None.
        maxadu: Maximum ADU value for camera, auto-detected if None.
    """

    def __init__(
        self,
        astra: Any,
        alpaca_device_camera: AlpacaDevice,
        row: Dict[str, Any],
        folder: str,
        hdr: Dict[str, Any],
        image_data_type: Optional[np.dtype] = None,
        maxadu: Optional[int] = None,
    ) -> None:
        self.astra = astra
        self.alpaca_device_camera = alpaca_device_camera

        self.row = row
        self.folder = folder
        self.hdr = hdr
        self.success = True

        if image_data_type is None or maxadu is None:
            self.determine_image_data_type_and_maxadu()
        else:
            self.image_data_type = image_data_type
            self.maxadu = maxadu

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
        self._perform_exposure(texp=texp, log_option=log_option, use_light=use_light)
        image = np.array(
            self.alpaca_device_camera.get("ImageArray"), dtype=self.image_data_type
        )

        # image = self.remove_hot_pixels(image, kernel_size=5)

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
    ) -> None:
        """Internal method to perform camera exposure.

        Handles the actual camera exposure operation and updates success status.

        Args:
            texp (float): Exposure time in seconds.
            log_option (Optional[str]): Logging option for the exposure.
            use_light (bool): Whether to use light frame (vs bias frame).
        """
        exposure_successful, filepath = self.astra.perform_exposure(
            camera=self.alpaca_device_camera,
            exptime=texp,
            row=self.row,
            hdr=self.hdr,
            use_light=use_light,
            log_option=log_option,
            maximal_sleep_time=0.1,
            folder=self.folder,
            maxadu=self.alpaca_device_camera.get("MaxADU"),
        )
        self.success = exposure_successful

        if not self.success:
            self.success = False
            self.astra.logger.warning("Exposure failed.")
            # raise ValueError("Exposure failed.")

    def determine_image_data_type_and_maxadu(self) -> None:
        """Determine camera image data type and maximum ADU value.

        Takes a test exposure to determine the camera's image data type
        and maximum ADU value for proper image processing.
        """
        self._perform_exposure(
            texp=0.0,
            log_option=None,
            use_light=False,
        )
        self.alpaca_device_camera.get("ImageArray")

        imginfo = self.alpaca_device_camera.get("ImageArrayInfo")
        maxadu = self.alpaca_device_camera.get("MaxADU")

        if imginfo.ImageElementType == 0 or imginfo.ImageElementType == 1:
            image_data_type = np.uint16
        elif imginfo.ImageElementType == 2:
            if maxadu <= 65535:
                image_data_type = np.uint16
            else:
                image_data_type = np.int32
        elif imginfo.ImageElementType == 3:
            image_data_type = np.float64
        else:
            raise ValueError(f"Unknown ImageElementType: {imginfo.ImageElementType}")

        self.image_data_type = image_data_type
        self.maxadu = maxadu


class AstraFocuser(FocuserInterface):
    """Focuser interface for Astra observatory automation system.

    Provides position control interface for telescope focuser through ALPACA protocol.

    Args:
        astra (Any): Main Astra instance for system coordination.
        alpaca_device_focuser (AlpacaDevice): ALPACA focuser device interface.
        row (dict): Configuration parameters for focuser setup.
    """

    def __init__(
        self,
        astra: Any,
        alpaca_device_focuser: AlpacaDevice,
        row: Optional[dict] = None,
    ) -> None:
        if not alpaca_device_focuser.get("Absolute"):
            raise ValueError("Focuser must be absolute for autofocusing to work.")

        self.astra = astra
        self.row = row
        self.alpaca_device_focuser = alpaca_device_focuser

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

        time.sleep(3)  # settle time
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
            self.astra.logger.warning(
                f"Requested focuser position {new_position} is below the allowed range. "
                f"Moving focuser to {self.allowed_range[0]} instead."
            )
        if self.allowed_range[1] is not None and new_position > self.allowed_range[1]:
            new_position = self.allowed_range[1]
            self.astra.logger.warning(
                f"Requested focuser position {new_position} is above the allowed range. "
                f"Moving focuser to MaxStep {self.allowed_range[1]} instead."
            )

        return new_position


class AstraTelescope(TelescopeInterface):
    """Telescope interface for Astra observatory automation system.

    Provides coordinate control and pointing interface for telescope through ALPACA protocol.

    Args:
        astra (Any): Main Astra instance for system coordination.
        alpaca_device_telescope (AlpacaDevice): ALPACA telescope device interface.
        row (dict): Configuration parameters for telescope setup.
    """

    def __init__(
        self, astra: Any, alpaca_device_telescope: AlpacaDevice, row: dict
    ) -> None:
        self.astra = astra
        self.alpaca_device_telescope = alpaca_device_telescope

        self.row = row
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
            if not self.astra.check_conditions(self.row):
                break

            time.sleep(1)


class AstraAutofocusDeviceManager(AutofocusDeviceManager):
    """Device manager for Astra autofocus operations.

    Coordinates camera, focuser, and telescope for automated focusing operations
    using the astrafocus library framework.

    Args:
        astra (Any): Main Astra instance for system coordination.
        action_value (dict): Configuration values for autofocus action.
        row (dict): Observatory configuration parameters.
        astra_camera (AstraCamera): Camera interface for image capture.
        astra_focuser (AstraFocuser): Focuser interface for position control.
        astra_telescope (Optional[AstraTelescope]): Telescope interface for positioning.
    """

    def __init__(
        self,
        astra: Any,
        action_value: dict,
        row: dict,
        astra_camera: AstraCamera,
        astra_focuser: AstraFocuser,
        astra_telescope: Optional[AstraTelescope] = None,
    ) -> None:
        self.astra = astra
        self.action_value = action_value
        self.row = row
        super().__init__(
            camera=astra_camera, focuser=astra_focuser, telescope=astra_telescope
        )

    @classmethod
    def from_row(
        cls, astra: Any, folder: str, row: dict, paired_devices: PairedDevices
    ) -> "AstraAutofocusDeviceManager":
        """Create device manager from configuration row.

        Factory method to construct an autofocus device manager from
        observatory configuration and device mappings.

        Args:
            astra (Any): Main Astra instance.
            folder (str): Directory path for saving images.
            row (dict): Configuration row with device settings.
            paired_devices (PairedDevices): Device manager instance.

        Returns:
            AstraAutofocusDeviceManager: Configured device manager instance.
        """
        # action_value, _, hdr = astra.pre_sequence(row, paired_devices)
        action_value = row["action_value"]
        hdr = astra.base_header(paired_devices, action_value)

        alpaca_device_camera = astra.devices["Camera"][row["device_name"]]
        alpaca_device_focuser = astra.devices["Focuser"][paired_devices["Focuser"]]
        alpaca_device_telescope = astra.devices["Telescope"][
            paired_devices["Telescope"]
        ]

        astra_camera = AstraCamera(
            astra,
            alpaca_device_camera=alpaca_device_camera,
            row=row,
            folder=folder,
            hdr=hdr,
        )
        astra_focuser = AstraFocuser(
            astra, alpaca_device_focuser=alpaca_device_focuser, row=row
        )
        astra_telescope = AstraTelescope(
            astra, alpaca_device_telescope=alpaca_device_telescope, row=row
        )

        return cls(
            astra=astra,
            action_value=action_value,
            row=row,
            astra_camera=astra_camera,
            astra_focuser=astra_focuser,
            astra_telescope=astra_telescope,
        )

    def check_conditions(self) -> bool:
        """Check if observatory conditions are suitable for autofocus.

        Returns:
            bool: True if conditions are acceptable, False otherwise.
        """
        return self.astra.check_conditions(row=self.row)


class Defocuser:
    """Defocusing control for creating intentionally out-of-focus images.

    Used for specific observational techniques requiring defocused stellar images,
    such as photometry of very bright stars or specific calibration procedures.

    Args:
        astra (Any): Main Astra instance for system coordination.
        paired_devices (PairedDevices): Device manager for observatory components.
        row (Optional[dict]): Configuration parameters for defocusing operation.
    """

    def __init__(
        self,
        astra: Any,
        paired_devices: PairedDevices,
        row: Optional[dict] = None,
    ) -> None:
        self.astra = astra
        self.row = row
        self.paired_devices = paired_devices

        self._focuser = AstraFocuser(
            astra=astra,
            alpaca_device_focuser=paired_devices.focuser,
            row=row,
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
        if focuser_config is None or "focus_position" not in focuser_config:
            self.astra.logger.warning(
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
            self.astra.logger.debug(
                f"Focuser {self.focuser_name} already at position "
                f"{position}. No change of focus needed."
            )
            return

        current_position = self._focuser.get_current_position()
        shift = position - current_position
        self.astra.logger.info(
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
            self.astra.logger.debug(
                f"Focuser {self.focuser_name} already at best "
                f"focus position {self.best_focus_position}. No refocusing needed."
            )
            return
        self.astra.logger.info(
            f"Refocusing from current position {self._focuser.get_current_position()} "
            f"to the best focus position: {self.best_focus_position}."
        )
        self._focuser.move_focuser_to_position(self.best_focus_position)


class Autofocuser:
    """Main autofocus orchestration class for Astra observatory.

    Coordinates complete autofocus operations including target selection,
    focus measurement, and optimization using multiple algorithms.

    Args:
        astra (Any): Main Astra instance for system coordination.
        row (dict): Observatory configuration parameters.
        paired_devices (PairedDevices): Device manager for observatory components.
        action_value (dict): Configuration values for autofocus action.
        hdr (Any): Header information for image metadata.
        save_path (Optional[Path]): Directory path for saving autofocus data.
        calibration_coordinates (Optional[Any]): Specific coordinates for calibration.
        focus_measure_operator_name (Optional[str]): Name of focus measurement algorithm.
        autofocuser (Optional[Union[NonParametricResponseAutofocuser, AnalyticResponseAutofocuser]]):
            Specific autofocus algorithm instance.
        success (bool): Success flag for autofocus operation.
    """

    def __init__(
        self,
        astra: Any,
        row: dict,
        paired_devices: PairedDevices,
        action_value: dict,
        hdr: Any,
        save_path: Optional[Path] = None,
        calibration_coordinates: Optional[Any] = None,
        focus_measure_operator_name: Optional[str] = None,
        autofocuser: Optional[
            Union[NonParametricResponseAutofocuser, AnalyticResponseAutofocuser]
        ] = None,
        success: bool = True,
    ) -> None:
        self.astra = astra
        self.row = row
        self.paired_devices = paired_devices
        self.action_value = action_value
        self.hdr = hdr
        self.save_path = save_path
        self.focus_measure_operator_name = focus_measure_operator_name
        self.calibration_coordinates = calibration_coordinates
        self.autofocuser = autofocuser
        self.success = success

        self._initialise_logging()

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
        if not self.success or not self.astra.check_conditions(row=self.row):
            return False

        try:
            return self.autofocuser.run()

        except Exception as e:
            self.astra.error_source.append(
                {
                    "device_type": "Autofocuser",
                    "device_name": self.paired_devices["Telescope"],
                    "error": str(e),
                }
            )
            self.astra.logger.exception(f"Error running autofocus: {str(e)}")
            self.success = False
            return False

    def setup(self) -> None:
        """Set up the autofocus system with devices and algorithms.

        Configures camera, focuser, telescope interfaces and initializes
        the autofocus algorithm with specified parameters.
        """
        if not self._check_conditions():
            return False

        try:
            self._setup()
        except Exception as e:
            self.astra.error_source.append(
                {
                    "device_type": "Autofocuser",
                    "device_name": self.paired_devices["Telescope"],
                    "error": str(e),
                }
            )
            self.astra.logger.exception(
                f"Error extracting action_value for autofocus: {str(e)}"
            )
            self.success = False

    def _setup(self) -> None:
        """Internal setup method for autofocus configuration.

        Configures save paths, device managers, focus measure operators,
        and autofocus algorithms based on action values.
        """
        action_value = self.action_value

        date = datetime.now().strftime("%Y%m%d")

        self.save_path = CONFIG.paths.images / "autofocus_ref" / date
        self.save_path.mkdir(exist_ok=True, parents=True)

        autofocus_device_manager = AstraAutofocusDeviceManager.from_row(
            self.astra,
            folder=self.save_path,
            row=self.row,
            paired_devices=self.paired_devices,
        )
        focus_measure_operator, focus_measure_operator_name = (
            self.determine_focus_measure_operator(action_value)
        )
        self.focus_measure_operator_name = focus_measure_operator_name

        # Reduce exposure time if necessary
        if action_value.get("reduce_exposure_time", False):
            # Clean image, CustomImageClass
            exposure_time = self.reduce_exposure_time(
                autofocus_device_manager=autofocus_device_manager,
                exposure_time=action_value.get("exptime", 3.0),
                reduction_factor=2,
                max_reduction_steps=5,
                minimal_exposure_time=0.1,
            )
        else:
            exposure_time = action_value.get("exptime", 3.0)

        search_range = action_value.get("search_range", None)
        search_range_is_relative = action_value.get("search_range_is_relative", False)
        initial_focus_position = None
        if search_range_is_relative and search_range is not None:
            initial_focus_position = self.paired_devices.get_device_config(
                "Focuser"
            ).get("focus_position", None)
            if initial_focus_position is None:
                initial_focus_position = (
                    autofocus_device_manager.focuser.get_current_position()
                )
                self.astra.logger.info(
                    "No best focus position found in focuser configuration. "
                    f"Using current position {initial_focus_position} "
                    "to define autofocus search range instead."
                )

        autofocus_args = dict(
            autofocus_device_manager=autofocus_device_manager,
            search_range=search_range,  # None defaults to allowed focuser range
            n_steps=action_value.get("n_steps", (30, 20)),
            n_exposures=action_value.get("n_exposures", (1, 1)),
            decrease_search_range=action_value.get("decrease_search_range", True),
            exposure_time=exposure_time,
            save_path=self.save_path,
            secondary_focus_measure_operators={
                "FFTTan2022": astrafmo.FFTFocusMeasureTan2022(),
                "NormalisedVariance": astrafmo.NormalizedVarianceFocusMeasure(),
                "Tennegrad": astrafmo.TenengradFocusMeasure(),
            },
            focus_measure_operator_kwargs={
                "star_find_threshold": action_value.get("star_find_threshold", 5.0),
                "fwhm": action_value.get("fwhm", 8),
            },
            search_range_is_relative=search_range_is_relative,
            initial_position=initial_focus_position,
            keep_images=True,
        )
        self.astra.logger.debug(f"Autofocus arguments: {autofocus_args}")

        if issubclass(focus_measure_operator, astrasfmo.StarSizeFocusMeasure):
            autofocuser = AnalyticResponseAutofocuser(
                focus_measure_operator=focus_measure_operator,
                percent_to_cut=action_value.get("percent_to_cut", 60),
                **autofocus_args,
            )
            self.astra.logger.info(
                f"Using the focus_measure_operator {focus_measure_operator_name} "
            )
        else:
            extremum_estimator = self.determine_extremum_estimator()
            autofocuser = NonParametricResponseAutofocuser(
                focus_measure_operator=focus_measure_operator(),
                extremum_estimator=extremum_estimator,
                **autofocus_args,
            )
            self.astra.logger.info(f"Using the extremum_estimator {extremum_estimator}")

        self.astra.logger.debug(f"Using autofocuser {autofocuser}.")

        self.autofocuser = autofocuser

    def determine_autofocus_calibration_field(self) -> None:
        """Determine optimal celestial coordinates for autofocus operation.

        Selects suitable star field for autofocus using Gaia catalog data and
        zenith neighborhood analysis. Uses various selection criteria including
        magnitude ranges, field of view, and star density preferences.

        Selection methods:
            - 'single': Isolated star closest to zenith
            - 'maximal': Star with maximum neighbors in field
            - 'any': Any suitable star closest to zenith

        Updates self.calibration_coordinates with selected coordinates or
        sets self.success to False if no suitable field found.
        """
        if not self._check_conditions():
            return False

        try:
            coords = self._determine_autofocus_calibration_field(
                self.row, self.action_value, self.hdr
            )
            if coords:
                self.calibration_coordinates = coords
            else:
                self.success = False

        except Exception as e:
            self.astra.logger.error(
                f"Error determining autofocus calibration field: {str(e)}."
            )
            self.success = False
            self.astra.error_source.append(
                {
                    "device_type": "Autofocuser",
                    "device_name": self.paired_devices["Telescope"],
                    "error": str(e),
                }
            )

    def _determine_autofocus_calibration_field(
        self, row, action_value, hdr
    ) -> SkyCoord:
        """Determine the calibration field for the autofocus.

        This function determines the calibration field for the autofocus. It uses the following
        parameters from the action_value:
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

        If the selection method is unsuccessful, the function will attempt to autofocus at zenith.

        Raises:
            ValueError: If no observatory location is found in the header.
            ValueError: check_conditions return false.

        # TODO: Verify action values.
        """
        # Find observatory location

        if "ra" in action_value and "dec" in action_value:
            self.astra.logger.info(
                "Using user-specified calibration coordinates for autofocus."
            )
            calibration_coordinates = SkyCoord(
                ra=float(action_value["ra"]) * u.deg,
                dec=float(action_value["dec"]) * u.deg,
            )
            return calibration_coordinates

        self.astra.logger.info("Determining autofocus calibration field.")
        try:
            if not self.astra.check_conditions(row=row):
                return False
            observatory_location = EarthLocation(
                lat=hdr["LAT-OBS"] * u.deg,
                lon=hdr["LONG-OBS"] * u.deg,
                height=hdr["ALT-OBS"] * u.m,
            )
            logging.info(
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
            self.astra.logger.info(
                f"Computing coordinates for the autofocus target with maximal zenith angle of "
                f"{maximal_zenith_angle}."
                # f"and selection method '{selection_method}'."
            )

            zenith_neighbourhood_query = (
                ZenithNeighbourhoodQuery.create_from_location_and_angle(
                    db_path=str(CONFIG.gaia_db),
                    observatory_location=observatory_location,
                    observation_time=action_value.get("observation_time", None),
                    maximal_zenith_angle=maximal_zenith_angle,
                    maximal_number_of_stars=action_value.get(
                        "maximal_number_of_stars", 100_000
                    ),
                )
            )

            self.astra.logger.info(
                "Zenith was determined to be at "
                f"{zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs!r}."
                # f"ra={zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs.ra.value}, "
                # f"dec={zenith_neighbourhood_query.zenith_neighbourhood.zenith.icrs.dec.value}."
            )

            min_phot_g_mean_mag, max_phot_g_mean_max = action_value.get(
                "g_mag_range", (0, 10)
            )
            min_j_m, max_j_m = action_value.get("j_mag_range", (0, 10))
            znqr = zenith_neighbourhood_query.query_shardwise(
                n_sub_div=20,
                min_phot_g_mean_mag=min_phot_g_mean_mag,
                max_phot_g_mean_mag=max_phot_g_mean_max,
                min_j_m=min_j_m,
                max_j_m=max_j_m,
            )

            self.astra.logger.info(
                f"Retrieved {len(znqr)} stars in the neighbourhood of the zenith from the database "
                "within the desired magnitude ranges.",
            )
            if not self.astra.check_conditions(row=row):
                return False

            # Determine the number of stars that would be on the ccd
            # if the telescope was centred on a given star
            znqr.determine_stars_in_neighbourhood(
                height=action_value.get("fov_height", 11.666666 / 60),
                width=action_value.get("fov_width", 11.666666 / 60),
            )
            if not self.astra.check_conditions(row=row):
                return False

            # Find the desired field of calibration
            znqr.sort_values(["zenith_angle", "n"], ascending=[True, True])

            selection_method = action_value.get("selection_method", "single")
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
                self.astra.logger.warning(
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
            if not self.astra.check_conditions(row=row):
                return False
            self.astra.logger.warning(
                f"Error determining autofocus target coordinates: {str(e)}. "
                "Attempt to autofocus at zenith.",
            )
            # Try to use the autofocus function at zenith.
            try:
                centre_coordinates = SkyCoord(
                    AltAz(
                        obstime=Time.now(),
                        location=observatory_location,
                        alt=90 * u.deg,
                        az=0 * u.deg,
                    )
                ).icrs
                self.astra.logger.info("Autofocus target coordinates set to zenith.")
            except Exception as e:
                raise ValueError(
                    f"Error determining zenith: {str(e)}."
                    "This is likely due to an error in the observatory location in the header."
                )

        return centre_coordinates

    def slew_to_calibration_field(self) -> None:
        """Slew telescope to the determined autofocus calibration field.

        Moves the telescope to the coordinates selected for autofocus operations.
        Updates action_value with target coordinates and initiates observatory setup.
        """
        if not self.success:
            return False

        self.astra.logger.debug(
            f"Slewing to autofocus calibration field: {self.calibration_coordinates!r}"
        )
        self.action_value["ra"] = self.calibration_coordinates.ra.deg
        self.action_value["dec"] = self.calibration_coordinates.dec.deg
        try:
            self.astra.setup_observatory(self.paired_devices, self.action_value)
        except Exception as e:
            self.astra.error_source.append(
                {
                    "device_type": "Autofocuser",
                    "device_name": self.paired_devices["Telescope"],
                    "error": str(e),
                }
            )
            self.astra.logger.error(
                f"Error slewing to autofocus calibration field: {str(e)}."
            )
            self.success = False

    def determine_focus_measure_operator(self, action_value: dict) -> Tuple[Any, str]:
        """Select focus measurement algorithm from configuration.

        Determines the appropriate focus measure operator based on user preferences,
        supporting various algorithms like HFR, 2D Gaussian, FFT, and variance-based methods.

        Args:
            action_value (dict): Configuration containing focus_measure_operator preference.

        Returns:
            Tuple[Any, str]: Focus measure operator class and descriptive name.
        """
        focus_measure_operator_name = action_value.get("focus_measure_operator", "HFR")

        if not isinstance(focus_measure_operator_name, str):
            self.astra.logger.warning(
                f"Invalid focus_measure_operator: {focus_measure_operator_name}."
                " Using HFR."
            )
            focus_measure_operator = astrasfmo.HFRStarFocusMeasure
            focus_measure_operator_name = "HFR"
        elif focus_measure_operator_name.lower() == "hfr":
            focus_measure_operator = astrasfmo.HFRStarFocusMeasure
            focus_measure_operator_name = "HFR"
        elif focus_measure_operator_name.lower() in ["gauss", "2dgauss"]:
            focus_measure_operator = astrasfmo.GaussianStarFocusMeasure
            focus_measure_operator_name = "2D Gaussian"
        # elif focus_measure_operator_name.lower() == 'moffat':  # TODO
        #     focus_measure_operator = astrasfmo.MOFFAATStarFocusMeasure
        elif focus_measure_operator_name.lower() in ["ffttan2022", "fft"]:
            focus_measure_operator = astrafmo.FFTFocusMeasureTan2022
            focus_measure_operator_name = "FFT Tan 2022"
        elif focus_measure_operator_name.lower() in [
            "nv",
            "var",
            "normavar",
            "normalizedvariance",
        ]:
            focus_measure_operator = astrafmo.NormalizedVarianceFocusMeasure
            focus_measure_operator_name = "Normalized Variance"
        else:
            self.astra.logger.warning(
                f"Unknown focus_measure_operator: {focus_measure_operator_name}."
                " Using HFR."
            )
            focus_measure_operator = astrasfmo.HFRStarFocusMeasure
            focus_measure_operator_name = "HFR"
        return focus_measure_operator, focus_measure_operator_name

    def determine_extremum_estimator(self) -> astrafee.RobustExtremumEstimator:
        """Select extremum estimation algorithm for focus curve analysis.

        Chooses appropriate curve fitting method for determining optimal focus
        from focus measure vs position data. Supports LOWESS, median filter,
        spline, and RBF methods.

        Returns:
            astrafee.RobustExtremumEstimator: Configured extremum estimator instance.
        """
        action_value = self.action_value
        extremum_estimator = action_value.get("extremum_estimator", "LOWESS")
        if not isinstance(extremum_estimator, str):
            self.astra.logger.warning(
                f"Unknown extremum_estimator: {extremum_estimator}. Using LOWESS.",
            )

        if extremum_estimator.lower() in ["lowess", "loess"]:
            extremum_estimator = astrafee.LOWESSExtremumEstimator(
                frac=action_value.get("frac", 0.4), it=action_value.get("it", 5)
            )
        elif extremum_estimator.lower() in ["medianfilter", "medfil", "median"]:
            astrafee.MedianFilterExtremumEstimation(size=action_value.get("size", 5))
        elif extremum_estimator.lower() in ["spline"]:
            astrafee.SplineExtremumEstimator(k=action_value.get("k", 3))
        elif extremum_estimator.lower() in ["rbf"]:
            astrafee.RBFExtremumEstimator(
                kernel=action_value.get("kernel", "linear"),
                smoothing=action_value.get("smoothing", 5),
            )
        else:
            self.astra.logger.warning(
                f"Unknown extremum_estimator: {extremum_estimator}. Using LOWESS.",
            )
            extremum_estimator = astrafee.LOWESSExtremumEstimator(
                frac=action_value.get("frac", 0.4), it=action_value.get("it", 5)
            )
        return extremum_estimator

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
                self.astra.logger.warning(
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
            self.astra.logger.warning(
                f"Reduced exposure time of {exposure_time} s is still saturating. "
            )
        elif new_exposure_time != exposure_time:
            self.astra.logger.warning(
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
            if self.save_path is None:
                self.astra.logger.error(
                    "Skipping creation of summary plot, as save_path is unspecified."
                )
                return

            focus_record = sorted(
                [item for item in os.listdir(self.save_path) if item.endswith(".csv")]
            )[-1]

            df = pd.read_csv(self.save_path / focus_record)
            df = df.sort_values("focus_pos")

            matplotlib.use("Agg")
            _, ax = plt.subplots(dpi=300)
            ax.plot(
                df["focus_pos"], df["focus_measure"], color="black", marker=".", ls=""
            )
            ax.set_xlabel("Focuser position")
            ax.set_ylabel(f"Focus measure ({self.focus_measure_operator_name})")

            ax.axvline(
                self.best_focus_position,
                color="red",
                ls="--",
                zorder=-1,
                label="Best focus position",
            )
            ax.legend()

            plt.savefig(self.save_path / f"{focus_record.removesuffix('.csv')}.png")
            plt.close()
        except Exception as e:
            self.astra.logger.exception(f"Error creating summary plot: {str(e)}")

    def create_result_file(self) -> None:
        """Create text file with autofocus results summary.

        Writes a summary file containing the best focus position, algorithm used,
        and autofocuser configuration for future reference.
        """
        if self.success is False:
            return
        if self.save_path is None:
            self.astra.logger.error(
                "Skipping creation of log file, as save_path is unspecified."
            )
            return

        focus_record = sorted(
            [item for item in os.listdir(self.save_path) if item.endswith(".csv")]
        )[-1]
        timestr = focus_record.split("_")[0]
        if not timestr:
            timestr = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file_path = self.save_path / f"{timestr}_result.txt"
        try:
            with open(result_file_path, "w") as result_file:
                result_file.write(f"Best focus position: {self.best_focus_position}\n")
                result_file.write(
                    f"Focus measure operator: {self.focus_measure_operator_name}\n"
                )
                result_file.write(f"Autofocuser: {self.autofocuser}\n")
        except Exception as e:
            self.astra.logger.exception(f"Error creating log file: {str(e)}")

    def _initialise_logging(self) -> None:
        """Set up logging integration with astrafocus library.

        Configures the astrafocus logger to use Astra's logging system
        for consistent log formatting and storage.
        """
        if logging.getLogger("astrafocus").hasHandlers():
            logging.getLogger("astrafocus").handlers.clear()
        logging.getLogger("astrafocus").addHandler(LoggingHandler(self.astra))

    def _check_conditions(self) -> bool:
        """Verify observatory conditions are suitable for autofocus.

        Checks weather, equipment status, and other conditions before
        proceeding with autofocus operations.

        Returns:
            bool: True if conditions are acceptable, False otherwise.
        """
        if not self.astra.check_conditions(row=self.row):
            self.astra.logger.error("Autofocus aborted due to bad conditions.")
            self.success = False

        return self.success

    def save_best_focus_position(self) -> None:
        """Save determined best focus position to observatory configuration.

        Updates the focuser configuration with the optimal focus position
        found during autofocus operation for future use.
        """
        if not self.success or not self.action_value.get("save", True):
            return

        camera_index = self.astra.get_cam_index(self.row["device_name"])
        observatory_config = ObservatoryConfig.from_config(CONFIG)
        observatory_config["Focuser"][camera_index][
            "focus_position"
        ] = self.best_focus_position
        observatory_config.save()
