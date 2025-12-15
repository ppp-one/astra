"""
Observatory device pairing management for coordinated astronomical operations.

This module provides the PairedDevices class for managing relationships between
observatory devices that need to work together. In astronomical observatories,
devices like cameras, telescopes, domes, focusers, and filter wheels are often
paired to ensure coordinated operation during observations.

The module handles:
    - Device pairing configuration from observatory settings
    - Device discovery and mapping from camera names
    - Property-based access to paired devices
    - Configuration retrieval for paired devices
    - Validation of device relationships

Device pairing is essential for:
    - Ensuring telescope and camera work together
    - Coordinating dome movement with telescope pointing
    - Managing filter wheel operations with camera exposures
    - Synchronizing focuser adjustments with observations

Typical Usage
-------------

.. code-block:: python

    # Create paired devices from camera name
    paired = PairedDevices.from_camera_name("main_camera")

    # Access paired devices
    telescope = paired.telescope
    dome = paired.dome

    # Check if device is paired
    if "FilterWheel" in paired:
        filter_wheel = paired.filter_wheel

Example:
    >>> from astra.paired_devices import PairedDevices
    >>> from astra.observatory import Observatory
    >>> paired_devices = PairedDevices.from_camera_name(camera_name="camera_observatoryname")
    >>> print(f"Camera: {paired_devices.camera_name}")
    >>> if "Telescope" in paired_devices:
    ...     telescope = paired_devices.telescope
"""

import astra
from astra.alpaca_device_process import AlpacaDevice
from astra.config import ObservatoryConfig


class PairedDevices(dict[str, str]):
    """
    A class to manage paired devices with a camera in an observatory setup.

    Extends dict to store device type to device name mappings for devices that
    are configured to work together in an observatory. Provides convenient access
    to paired devices and their configurations, ensuring that operations can be
    coordinated across multiple observatory components.

    The class maintains relationships between devices based on observatory
    configuration and provides property-based access to common device types.
    It also handles device validation and configuration retrieval.

    Attributes:
        DEVICE_TYPES (list): Supported device types for pairing.
        observatory_config (ObservatoryConfig): Configuration for the observatory.
        devices (dict): Dictionary of all available devices by type and name.

    Properties:
        camera_name: Name of the paired camera
        camera: AlpacaDevice instance for the paired camera
        dome: AlpacaDevice instance for the paired dome
        focuser: AlpacaDevice instance for the paired focuser
        telescope: AlpacaDevice instance for the paired telescope
        guider: AlpacaDevice instance for the guider (paired to telescope)
        filter_wheel: AlpacaDevice instance for the paired filter wheel
        rotator: AlpacaDevice instance for the paired rotator

    Example:
        >>> from astra.paired_devices import PairedDevices
        >>> from astra.observatory import Observatory
        >>> paired_devices = PairedDevices.from_camera_name(camera_name="camera_observatoryname")
        >>> print(f"Camera: {paired_devices.camera_name}")
        >>> if "Telescope" in paired_devices:
        ...     telescope = paired_devices.telescope
        ...     print(f"Paired telescope: {telescope}")
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
        devices: dict[str, dict[str, AlpacaDevice]] | None = None,
        *,
        observatory_config: ObservatoryConfig | None = None,
        camera_name: str | None = None,
    ):
        """
        Initialize PairedDevices with device mappings and configurations.

        Parameters:
            paired_device_names (dict[str, str]): Mapping of device types to device names.
                For example: {"Camera": "main_camera", "Telescope": "main_telescope"}
            devices (dict[str, dict[str, AlpacaDevice]], optional): Nested dictionary of
                available devices organized by type then name. Defaults to empty dict.
            observatory_config (ObservatoryConfig | None, optional): Observatory configuration
                object. If None, loads from default config. Defaults to None.
            camera_name (str | None, optional): Name of the camera to add to pairing.
                If provided, adds Camera entry to paired_device_names. Defaults to None.
        """
        super().__init__(sorted(paired_device_names.items(), key=lambda item: item[0]))
        self.observatory_config = (
            ObservatoryConfig.from_config()
            if observatory_config is None
            else observatory_config
        )
        self.devices = devices if devices is not None else {}
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
        Create a PairedDevices instance from a camera name using observatory configuration.

        Looks up the camera in the observatory configuration and retrieves its
        paired device mappings. This is the most common way to create PairedDevices
        instances when you know the camera name but need to discover its paired devices.

        Parameters:
            camera_name (str): Name of the camera to find paired devices for.
            devices (dict[str, dict[str, AlpacaDevice]], optional): Available device
                instances organized by type and name. Defaults to empty dict.
            observatory_config (ObservatoryConfig | None, optional): Observatory
                configuration to use. If None, loads from default config. Defaults to None.

        Returns:
            PairedDevices: Instance configured with devices paired to the specified camera.

        Raises:
            ValueError: If the camera name is not found in the observatory configuration.

        Example:
            >>> paired = PairedDevices.from_camera_name("main_camera")
            >>> print(f"Devices paired to {paired.camera_name}: {list(paired.keys())}")
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
        observatory: "astra.observatory.Observatory",  # type: ignore
        *,
        camera_name: str | None = None,
        paired_device_names: dict[str, str] | None = None,
    ) -> "PairedDevices":
        """
        Create a PairedDevices instance from an existing Observatory object.

        Provides two ways to create paired devices: either by specifying a camera name
        to look up its paired devices, or by directly providing the device name mappings.

        Parameters:
            observatory (astra.observatory.Observatory): Observatory instance containing
                device configurations and instances.
            camera_name (str | None, optional): Name of camera to find paired devices for.
                Must be provided if paired_device_names is None. Defaults to None.
            paired_device_names (dict[str, str] | None, optional): Direct mapping of
                device types to names. Must be provided if camera_name is None. Defaults to None.

        Returns:
            PairedDevices: Instance configured with the specified device pairing.

        Raises:
            ValueError: If neither camera_name nor paired_device_names is provided.

        Example:
            >>> from astra.observatory import Observatory
            >>> obs = Observatory()
            >>> # Method 1: From camera name
            >>> paired = PairedDevices.from_observatory(obs, camera_name="main_camera")
            >>> # Method 2: Direct device mapping
            >>> device_map = {"Camera": "cam1", "Telescope": "scope1"}
            >>> paired = PairedDevices.from_observatory(obs, paired_device_names=device_map)
        """
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
        Get the observatory configuration for a paired device type.

        Retrieves the full configuration dictionary for the specified device type
        if it is paired with the current camera. The configuration contains all
        settings and parameters needed to operate the device.

        Parameters:
            device_type (str): Type of device to get configuration for (e.g., "Telescope",
                "Dome", "FilterWheel"). Must be one of the supported device types.

        Returns:
            dict: Configuration dictionary for the device. Returns empty dict if the
                device is paired but no configuration is found.

        Raises:
            ValueError: If the device type is not found in the observatory configuration.

        Example:
            >>> paired = PairedDevices.from_camera_name("main_camera")
            >>> telescope_config = paired.get_device_config("Telescope")
            >>> print(f"Telescope model: {telescope_config.get('model', 'Unknown')}")
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
        """
        Get the device instance for a paired device type.

        Retrieves the actual AlpacaDevice instance for the specified device type
        if it is paired with the current camera and available in the devices dictionary.

        Parameters:
            device_type (str): Type of device to retrieve (e.g., "Telescope", "Dome").

        Returns:
            AlpacaDevice | None: The device instance if available, None if the device
                type is not paired or the device instance is not found.

        Example:
            >>> paired = PairedDevices.from_camera_name("main_camera")
            >>> telescope = paired.get_device("Telescope")
            >>> if telescope:
            ...     print(f"Telescope connected: {telescope.connected}")
        """
        device_name = self[device_type]
        return self.devices.get(device_type, {}).get(device_name, None)

    @property
    def camera_name(self) -> str:
        """
        Get the name of the paired camera.

        Returns:
            str: Name of the camera device. Returns "Unknown Camera" if no camera is paired.
        """
        if "Camera" not in self:
            return "Unknown Camera"
        return self["Camera"]

    @property
    def camera(self) -> AlpacaDevice:
        """
        Get the paired camera device instance.

        Returns:
            AlpacaDevice: The camera device instance.

        Raises:
            ValueError: If no camera is paired with this device set.
        """
        self._raise_property_not_paired("Camera")
        return self.devices["Camera"][self["Camera"]]

    @property
    def dome(self) -> AlpacaDevice:
        """
        Get the paired dome device instance.

        Returns:
            AlpacaDevice: The dome device instance for controlling observatory enclosure.

        Raises:
            ValueError: If no dome is paired with this camera.
        """
        self._raise_property_not_paired("Dome")
        return self.devices["Dome"][self["Dome"]]

    @property
    def focuser(self) -> AlpacaDevice:
        """
        Get the paired focuser device instance.

        Returns:
            AlpacaDevice: The focuser device instance for controlling telescope focus.

        Raises:
            ValueError: If no focuser is paired with this camera.
        """
        self._raise_property_not_paired("Focuser")
        return self.devices["Focuser"][self["Focuser"]]

    @property
    def telescope(self) -> AlpacaDevice:
        """
        Get the paired telescope device instance.

        Returns:
            AlpacaDevice: The telescope device instance for controlling telescope movement.

        Raises:
            ValueError: If no telescope is paired with this camera.
        """
        self._raise_property_not_paired("Telescope")
        return self.devices["Telescope"][self["Telescope"]]

    @property
    def guider(self) -> AlpacaDevice:
        """
        Get the guider device paired to the telescope.

        Note: The guider uses the same name as the paired telescope since they
        are typically associated together.

        Returns:
            AlpacaDevice: The guider device instance for autoguiding operations.

        Raises:
            ValueError: If no telescope (and thus no guider) is paired with this camera.
        """
        self._raise_property_not_paired("Telescope")
        return self.devices["Guider"][self["Telescope"]]

    @property
    def filter_wheel(self) -> AlpacaDevice:
        """
        Get the paired filter wheel device instance.

        Returns:
            AlpacaDevice: The filter wheel device instance for controlling optical filters.

        Raises:
            ValueError: If no filter wheel is paired with this camera.
        """
        self._raise_property_not_paired("FilterWheel")
        return self.devices["FilterWheel"][self["FilterWheel"]]

    @property
    def rotator(self) -> AlpacaDevice:
        """
        Get the paired rotator device instance.

        Returns:
            AlpacaDevice: The rotator device instance for controlling field rotation.

        Raises:
            ValueError: If no rotator is paired with this camera.
        """
        self._raise_property_not_paired("Rotator")
        return self.devices["Rotator"][self["Rotator"]]

    def _raise_property_not_paired(self, device_type: str):
        """
        Raise an error if the specified device type is not paired.

        Parameters:
            device_type (str): The device type to check for pairing.

        Raises:
            ValueError: If the device type is not paired with the current camera.
        """
        if device_type not in self:
            raise ValueError(
                f"{device_type} device is not paired with the camera '{self.camera_name}'."
            )

    @staticmethod
    def _get_camera_index(observatory_config, camera_name) -> int:
        """
        Find the index of a camera in the observatory configuration.

        Parameters:
            observatory_config (ObservatoryConfig): Observatory configuration object.
            camera_name (str): Name of the camera to find.

        Returns:
            int: Index of the camera in the configuration list.

        Raises:
            ValueError: If the camera name is not found in the configuration.
        """
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
        """
        Get the device name for a paired device type.

        Parameters:
            device_type (str): Type of device to get the name for.

        Returns:
            str: Name of the device of the specified type.

        Raises:
            KeyError: If the device type is not paired with the current camera.
        """
        if device_type not in self:
            raise KeyError(
                f"{device_type} not not paired with the camera '{self.camera_name}'."
                f" with devices {list(self.keys())}."
            )
        return super().__getitem__(device_type)

    def __repr__(self) -> str:
        """
        Return a string representation of the PairedDevices instance.

        Returns:
            str: String representation showing the device name mappings.
        """
        dict_repr = super().__repr__()
        return f"PairedDevices(paired_device_names={dict_repr})"
