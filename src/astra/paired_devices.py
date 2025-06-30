import astra
from astra.alpaca_device_process import AlpacaDevice
from astra.config import ObservatoryConfig


class PairedDevices(dict[str, str]):
    """
    A class to manage paired devices with a camera in an observatory setup.

    Examples
    --------
    >>> from astra.paired_devices import PairedDevices
    >>> from astra.observatory import Observatory
    >>> paired_devices = PairedDevices.from_camera_name(camera_name="camera_observatoryname")
    """

    DEVICE_TYPES = [
        "Camera",
        "Dome",
        "FilterWheel",
        "Focuser",
        "Rotator",
        "Telescope",
    ]

    def __init__(
        self,
        paired_device_names: dict[str, str],
        devices: dict[str, dict[str, AlpacaDevice]] = {},
        *,
        observatory_config: ObservatoryConfig | None = None,
        camera_name: str | None = None,
    ):
        super().__init__(sorted(paired_device_names.items(), key=lambda item: item[0]))
        self.observatory_config = (
            ObservatoryConfig.from_config()
            if observatory_config is None
            else observatory_config
        )
        self.devices = devices
        if camera_name is not None:
            self["Camera"] = camera_name

    @classmethod
    def from_camera_name(
        cls,
        camera_name: str,
        devices: dict[str, dict[str, AlpacaDevice]] = {},
        *,
        observatory_config: ObservatoryConfig | None = None,
    ) -> "PairedDevices":
        """
        Creates a PairedDevices instance from a camera name and a dictionary of devices.
        """
        observatory_config = (
            ObservatoryConfig.from_config()
            if observatory_config is None
            else observatory_config
        )
        paired_device_names = observatory_config["Camera"][
            cls._get_camera_index(observatory_config, camera_name)
        ]["paired_devices"]
        paired_device_names["Camera"] = camera_name

        return cls(
            devices=devices,
            paired_device_names=paired_device_names,
            observatory_config=observatory_config,
        )

    @classmethod
    def from_observatory(
        cls,
        observatory: "astra.observatory.Observatory",
        *,
        camera_name: str | None = None,
        paired_device_names: dict[str, str] | None = None,
    ) -> "PairedDevices":
        if paired_device_names is not None:
            return cls(
                devices=observatory.devices,
                paired_device_names=paired_device_names,
                observatory_config=observatory.config,
                camera_name=camera_name,
            )
        elif camera_name is not None and paired_device_names is None:
            return cls.from_camera_name(
                camera_name=camera_name,
                devices=observatory.devices,
                observatory_config=observatory.config,
            )

        raise ValueError("Either camera_name or paired_device_names must be provided.")

    def get_device_config(self, device_type: str) -> dict:
        """
        Returns the observatory config for the device type if it is paired with the camera.
        """
        if device_type not in self.observatory_config:
            raise ValueError(
                f"Device type '{device_type}' not found in observatory config."
            )

        device_name = self.get(device_type)

        device_config_list = self.observatory_config[device_type]
        config_selection = [
            item
            for item in device_config_list
            if item.get("device_name") == device_name
        ]
        if not config_selection:
            return {}
        device_config = config_selection[0]
        return device_config

    def get_device(self, device_type: str) -> AlpacaDevice | None:
        """Return the device of the specified type if it is paired with the camera."""
        device_name = self[device_type]
        return self.devices.get(device_type, {}).get(device_name, None)

    @property
    def camera_name(self) -> str:
        """Return the name of the camera."""
        if "Camera" not in self:
            return "Unknown Camera"
        return self["Camera"]

    @property
    def camera(self) -> AlpacaDevice:
        """Return camera device."""
        self._raise_property_not_paired("Camera")
        return self.devices["Camera"][self["Camera"]]

    @property
    def dome(self) -> AlpacaDevice:
        """Return paired dome device."""
        self._raise_property_not_paired("Dome")
        return self.devices["Dome"][self["Dome"]]

    @property
    def focuser(self) -> AlpacaDevice:
        """Return paired focuser device."""
        self._raise_property_not_paired("Focuser")
        return self.devices["Focuser"][self["Focuser"]]

    @property
    def telescope(self) -> AlpacaDevice:
        """Return paired telescope device."""
        self._raise_property_not_paired("Telescope")
        return self.devices["Telescope"][self["Telescope"]]

    @property
    def guider(self) -> AlpacaDevice:
        """Return guider paired to telescope."""
        self._raise_property_not_paired("Telescope")
        return self.devices["Guider"][self["Telescope"]]

    @property
    def filter_wheel(self) -> AlpacaDevice:
        self._raise_property_not_paired("FilterWheel")
        return self.devices["FilterWheel"][self["FilterWheel"]]

    @property
    def rotator(self) -> AlpacaDevice:
        self._raise_property_not_paired("Rotator")
        return self.devices["Rotator"][self["Rotator"]]

    def _raise_property_not_paired(self, device_type: str):
        if device_type not in self:
            raise ValueError(
                f"{device_type} device is not paired with the camera '{self.camera_name}'."
            )

    @staticmethod
    def _get_camera_index(observatory_config, camera_name) -> int:
        camera_index = next(
            (
                idx
                for idx, device in enumerate(observatory_config["Camera"])
                if device["device_name"] == camera_name
            ),
            None,
        )
        if camera_index is None:
            raise ValueError(f"Camera '{camera_name}' not found in observatory config.")
        return camera_index

    def __getitem__(self, device_type: str) -> str:
        if device_type not in self:
            raise KeyError(
                f"{device_type} not not paired with the camera '{self.camera_name}'."
                f" with devices {list(self.keys())}."
            )
        return super().__getitem__(device_type)

    def __repr__(self) -> str:
        dict_repr = super().__repr__()
        return f"PairedDevices(paired_device_names={dict_repr})"
