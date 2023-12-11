import time
from datetime import datetime
from typing import Callable, Optional, Tuple, Union

import astropy
import astropy.units as u
import numpy as np
from alpaca_device_process import AlpacaDevice
from astrafocus.interface.camera import CameraInterface
from astrafocus.interface.device_manager import AutofocusDeviceManager
from astrafocus.interface.focuser import FocuserInterface
from astrafocus.interface.telescope import TelescopeInterface
from astrafocus.targeting.airmass_models import find_airmass_threshold_crossover
from astrafocus.targeting.zenith_neighbourhood_query import ZenithNeighbourhoodQuery
from astropy.coordinates import SkyCoord

# from astra import Astra

__all__ = ["AstraAutofocusDeviceManager"]


class AstraCamera(CameraInterface):
    """
    A camera interface for the Astra autofocus system.
    """

    def __init__(
        self,
        astra: "Astra",
        alpaca_device_camera: AlpacaDevice,
        row: dict,
        folder: str,
        hdr,
        image_data_type=None,
        maxadu=None,
    ):
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
        """
        Calling this function should take as long as it takes to make the observation.
        """
        self._perform_exposure(texp=texp, log_option=log_option, use_light=use_light)
        image = np.array(self.alpaca_device_camera.get("ImageArray"), dtype=self.image_data_type)

        t0 = datetime.utcnow()
        dateobs = self.astra.get_last_exposure_start_time(
            self.alpaca_device_camera, self.row["device_name"]
        )

        # TODO Save image (disabled for now, to increase speed)
        self.astra.save_image(
            self.alpaca_device_camera, self.hdr, dateobs, t0, self.maxadu, self.folder
        )

        return image

    def _perform_exposure(
        self, texp: float, log_option: Optional[str] = None, use_light: bool = True
    ):
        self.success = self.astra.perform_exposure(
            camera=self.alpaca_device_camera,
            exptime=texp,
            row=self.row,
            hdr=self.hdr,
            use_light=use_light,
            log_option=log_option,
            maximal_sleep_time=0.01,
        )
        if not self.success:
            raise ValueError("Exposure failed.")

    def determine_image_data_type_and_maxadu(self):
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
    def __init__(self, astra, alpaca_device_focuser: AlpacaDevice, row: dict):
        if not alpaca_device_focuser.get("Absolute"):
            raise ValueError("Focuser must be absolute for autofocusing to work.")

        self.astra = astra
        self.row = row
        self.alpaca_device_focuser = alpaca_device_focuser

        current_position = self.get_current_position()
        allowed_range = (0, alpaca_device_focuser.get("MaxStep"))
        super().__init__(current_position=current_position, allowed_range=allowed_range)

    def move_focuser_to_position(self, new_position: int):
        """
        Calling this function should take as long as it takes to move the focuser to the desired position.
        """
        self.alpaca_device_focuser.get("Move", Position=new_position)
        while self.alpaca_device_focuser.get("IsMoving"):
            if not self.astra.check_conditions(self.row):
                raise ValueError("Focuser move aborted due to bad conditions.")
            time.sleep(0.1)

        return None

    def get_current_position(self):
        return self.alpaca_device_focuser.get("Position")


class AstraTelescope(TelescopeInterface):
    def __init__(self, astra: "Astra", alpaca_device_telescope: AlpacaDevice, row: dict):
        self.astra = astra
        self.alpaca_device_telescope = alpaca_device_telescope

        self.row = row
        super().__init__()

    def set_telescope_position(self, coordinates: SkyCoord, hard_timeout: float = 120):
        """
        Calling this function should take as long as it takes to move the telescope to the desired position.
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
                raise ValueError("Slew aborted due to bad conditions.")

            time.sleep(1)


class AstraAutofocusDeviceManager(AutofocusDeviceManager):
    """
    Autofocus device manager for the Astra autofocus system.

    Parameters:
        astra (Astra): The Astra instance.
        action_value (dict): Dictionary containing action values.
        row (dict): A dictionary containing configuration parameters.
        astra_camera (AstraCamera): The Astra camera instance.
        astra_focuser (AstraFocuser): The Astra focuser instance.
        astra_telescope (Optional[AstraTelescope]): The Astra telescope instance (optional).

    Methods:
        check_conditions() -> bool:
            Check the conditions for autofocus.
    """

    def __init__(
        self,
        astra: "Astra",
        action_value: dict,
        row: dict,
        astra_camera: AstraCamera,
        astra_focuser: AstraFocuser,
        astra_telescope: Optional[AstraTelescope] = None,
    ):
        self.astra = astra
        self.action_value = action_value
        self.row = row
        super().__init__(camera=astra_camera, focuser=astra_focuser, telescope=astra_telescope)

    @classmethod
    def from_row(cls, astra, row, paired_devices):
        action_value, folder, hdr = astra.pre_sequence(row, paired_devices)

        alpaca_device_camera = astra.devices["Camera"][row["device_name"]]
        alpaca_device_focuser = astra.devices["Focuser"][paired_devices["Focuser"]]
        alpaca_device_telescope = astra.devices["Telescope"][paired_devices["Telescope"]]

        astra_camera = AstraCamera(
            astra, alpaca_device_camera=alpaca_device_camera, row=row, folder=folder, hdr=hdr
        )
        astra_focuser = AstraFocuser(astra, alpaca_device_focuser=alpaca_device_focuser, row=row)
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

    def check_conditions(self):
        return self.astra.check_conditions(row=self.row)
