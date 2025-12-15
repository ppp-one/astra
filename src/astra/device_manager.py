"""
Device management for Alpaca devices within the Astra framework.

Key capabilities:
    - Load and initialize Alpaca devices based on observatory configuration
    - Connect to devices and start polling for FITS header data
    - Pause and resume polling during critical operations
    - Monitor device responsiveness for watchdog functionality
    - Force immediate polling for specific device types
"""

import logging
import time

from astra.alpaca_device_process import (
    AlpacaDevice,
    AlpacaDeviceIPCError,
    RemoteDeviceError,
)
from astra.config import ObservatoryConfig
from astra.logger import ObservatoryLogger
from astra.queue_manager import QueueManager
from astra.thread_manager import ThreadManager


class DeviceManager:
    """
    Manages loading, connecting, polling, pausing/resuming,
    and monitoring all Alpaca devices for an observatory.

    This class handles the lifecycle of devices including:

    - Loading device configurations
    - Establishing connections
    - Starting/stopping polling for FITS header data
    - Pausing/resuming polls during critical operations
    - Checking device responsiveness for watchdog monitoring
    - Forcing immediate polls for specific device types

    It interacts with the ObservatoryConfig for device settings,
    uses the ObservatoryLogger for logging, and relies on
    QueueManager and ThreadManager for asynchronous operations.

    Attributes:
        observatory_config (ObservatoryConfig): Configuration for the observatory.
        logger (ObservatoryLogger): Logger for logging messages and errors.
        queue_manager (QueueManager): Manages the queue for inter-thread communication.
        thread_manager (ThreadManager): Manages threads for concurrent operations.
        devices (dict): Dictionary of loaded devices organized by type and name.
        device_task_monitor_queue (dict): Tracks tasks for monitoring device status.

    """

    def __init__(
        self,
        observatory_config: ObservatoryConfig,
        logger: ObservatoryLogger,
        queue_manager: QueueManager,
        thread_manager: ThreadManager,
    ):
        self._observatory_config = observatory_config
        self.logger = logger
        self.queue_manager = queue_manager
        self.thread_manager = thread_manager

        self.devices: dict[str, dict[str, AlpacaDevice]] = {}
        self.device_task_monitor_queue = {}

    @property
    def observatory_config(self) -> ObservatoryConfig:
        """
        Get the observatory configuration, reloading if the file has been modified.

        This property provides access to the observatory configuration and automatically
        reloads it if the underlying configuration file has been modified since the
        last access.

        Returns:
            ObservatoryConfig: The current observatory configuration object.

        Note:
            If the configuration is reloaded, devices may need to be restarted
            (TODO: implement automatic device restart).
        """
        if self._observatory_config.is_outdated():
            self.logger.info("Config file modified, reloading.")
            self._observatory_config.load()

            # TODO restart devices

        return self._observatory_config

    def load_devices(self):
        """Load and initialize Alpaca devices based on the observatory configuration."""
        self.logger.info("Loading devices")
        debug = self.logger.getEffectiveLevel() == logging.DEBUG
        devices: dict[str, dict[str, AlpacaDevice]] = {}
        for device_type in self.observatory_config:
            devices[device_type] = {}
            if device_type == "Misc":
                continue
            for d in self.observatory_config[device_type]:
                try:
                    devices[device_type][d["device_name"]] = AlpacaDevice(
                        ip=d["ip"],
                        device_type=device_type,
                        device_number=d["device_number"],
                        device_name=d["device_name"],
                        queue=self.queue_manager.queue,
                        debug=debug,
                        connectable=bool(d.get("connectable", True)),
                    )
                    devices[device_type][d["device_name"]].start()
                    self.device_task_monitor_queue[d["device_name"]] = {}
                    self.logger.debug(
                        f"Loaded {device_type} {d['device_name']} at {d['ip']}"
                    )
                except Exception as e:
                    self.logger.report_device_issue(
                        device_type=device_type,
                        device_name=d["device_name"],
                        message=f"Error loading {device_type} {d['device_name']}",
                        exception=e,
                    )
        self.devices = devices
        self.logger.info("Devices loaded")

        try:
            self.check_telescopes_in_domes()
        except Exception as e:
            self.logger.report_device_issue(
                device_type="DeviceManager",
                device_name="check_telescopes_in_domes",
                message="Error checking telescopes in domes after loading devices",
                exception=e,
            )

    def connect_all(self, fits_config):
        """
        Connect to all loaded devices and start polling for FITS header data.

        Establishes connections to all initialized devices and begins regular polling
        of device properties needed for FITS headers. Different polling intervals
        are used based on device criticality:

        - **Most devices**: 5-second intervals
        - **SafetyMonitor**: 1-second intervals for safety-critical data

        The method:
            1. Connects to all devices in the devices dictionary
            2. Starts polling threads for non-fixed FITS header properties
            3. Sets up special high-frequency polling for safety monitors
            4. Starts the watchdog process after all connections are established

        Raises:
            Exception: Device connection errors are logged and added to error_source,
                but do not prevent other devices from being connected.

        Note:
            - SPECULOOS observatories skip focuser connection due to compatibility issues
            - A 1-second delay is added after connections before starting the watchdog
              to ensure devices are ready
        """
        self.logger.info("Connecting to devices")
        successful_connections = []
        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                try:
                    if self.devices[device_type][device_name].connectable:
                        self.devices[device_type][device_name].set(
                            "Connected", True
                        )  ## slow?
                        self.logger.info(f"{device_type} {device_name} connected")
                        successful_connections.append(f"{device_type}_{device_name}")
                    else:
                        self.logger.warning(
                            f"{device_type} {device_name} skipping connection as marked not connectable"
                        )
                except RemoteDeviceError as e:
                    self.logger.report_device_issue(
                        device_type=device_type,
                        device_name=device_name,
                        message=f"{device_type} {device_name} not responding (network/HTTP error).",
                        exception=e,
                    )
                except AlpacaDeviceIPCError as e:
                    self.logger.report_device_issue(
                        device_type=device_type,
                        device_name=device_name,
                        message=f"IPC error with device process for {device_type} {device_name}.",
                        exception=e,
                    )
                except Exception as e:
                    self.logger.report_device_issue(
                        device_type=device_type,
                        device_name=device_name,
                        message=f"Error connecting to {device_type} {device_name}",
                        exception=e,
                    )

        if len(successful_connections) == 0:
            self.logger.critical(
                "No devices connected successfully. "
                "Check network and astra configuration. Exiting."
            )
            self.stop_all_devices()
            raise Exception("Some devices failed to connect.")
        elif len(successful_connections) < sum(
            self.devices[device_type][device_name].connectable
            for device_type in self.devices
            for device_name in self.devices[device_type]
        ):
            self.logger.error(
                f"Some devices failed to connect ({len(successful_connections)} successful). "
                "Check logs for details. "
            )

        self.logger.info("Starting polling non-fixed fits headers")
        for _, fits_row in fits_config.iterrows():
            if (
                fits_row["device_type"]
                not in ["astropy_default", "astra", "astra_fixed", ""]
            ) and fits_row["fixed"] is False:
                device_type = fits_row["device_type"]
                if device_type in self.devices:
                    for device_name in self.devices[device_type]:
                        device = self.devices[device_type][device_name]

                        # find polling delay in self.observatory_config
                        delay = next(
                            (
                                d.get("polling_interval", 5)
                                for d in self.observatory_config[device_type]
                                if d["device_name"] == device_name
                            ),
                            5,  # default fallback if device not found
                        )
                        try:
                            # 5 second polling
                            device.start_poll(fits_row["device_command"], delay)
                        except Exception as e:
                            self.logger.report_device_issue(
                                device_type,
                                device_name,
                                f"Error starting polling for {device_type} {device_name}",
                                exception=e,
                            )
        if "SafetyMonitor" in self.observatory_config:
            device_type = "SafetyMonitor"
            device_name = self.observatory_config[device_type][0]["device_name"]
            device = self.devices[device_type][device_name]
            try:
                # find polling delay in self.observatory_config
                delay = next(
                    (
                        d.get("polling_interval", 1)
                        for d in self.observatory_config[device_type]
                        if d["device_name"] == device_name
                    ),
                    1,  # default fallback if device not found
                )
                device.start_poll("IsSafe", delay)  # 1 second polling
            except Exception as e:
                self.logger.report_device_issue(
                    device_type=device_type,
                    device_name=device_name,
                    message=f"Error starting polling for {device_type} {device_name}",
                    exception=e,
                )
        self.logger.info("Connect all sequence complete")
        time.sleep(1)  # wait for devices to connect and start polling
        # TODO: check one device's latest polling is valid before starting watchdog

    def pause_polls(self, device_types=None):
        """
        Pause polling for specified device types or all devices.

        Temporarily stops the regular polling of device properties. This is useful
        during critical operations where device communication needs to be minimized
        or when devices need to be accessed exclusively by other processes.

        Parameters:
            device_types (list, optional): A list of device type strings to pause
                polling for (e.g., ['Telescope', 'Camera']). If None, pauses
                polling for all device types. Defaults to None.

        Note:
            - Only device types that exist in the devices dictionary will be affected
            - Polling can be resumed using the resume_polls() method
            - This is commonly used in SPECULOOS operations before critical commands
        """
        if device_types is not None:
            self.logger.debug(f"Pausing polls for {device_types} if present")
        else:
            self.logger.debug("Pausing polls for all devices")
            device_types = list(self.devices.keys())
        for device_type in device_types:
            if device_type in self.devices:
                for device_name in self.devices[device_type]:
                    try:
                        self.devices[device_type][device_name].pause_polls()
                    except Exception as e:
                        self.logger.report_device_issue(
                            device_type,
                            device_name,
                            f"{device_type} {device_name} could not pause polls",
                            exception=e,
                        )

    def resume_polls(self, device_types=None):
        """
        Resume polling for specified device types or all devices.

        Restarts the regular polling of device properties that was previously
        paused using pause_polls(). This restores normal device monitoring
        and data collection for FITS headers.

        Parameters:
            device_types (list, optional): A list of device type strings to resume
                polling for (e.g., ['Telescope', 'Camera']). If None, resumes
                polling for all device types. Defaults to None.

        Note:
            - Only device types that exist in the devices dictionary will be affected
            - This should be called after pause_polls() to restore normal operation
            - Errors during resume are logged but don't prevent other devices from resuming
        """

        if device_types is not None:
            self.logger.debug(f"Resuming polls for {device_types} if present")
        else:
            self.logger.debug("Resuming polls for all devices")
            device_types = list(self.devices.keys())

        for device_type in device_types:
            if device_type in self.devices:
                for device_name in self.devices[device_type]:
                    try:
                        self.devices[device_type][device_name].resume_polls()
                    except Exception as e:
                        self.logger.report_device_issue(
                            device_type,
                            device_name,
                            f"{device_type} {device_name} could not resume polls",
                            exception=e,
                        )

    def stop_all_devices(self):
        """Stop all devices and their associated processes."""
        self.logger.info("Stopping all devices")
        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                try:
                    self.devices[device_type][device_name].stop()
                except Exception as e:
                    self.logger.report_device_issue(
                        device_type,
                        device_name,
                        f"Error stopping {device_type} {device_name}",
                        exception=e,
                    )
        self.logger.info("All devices stopped")

    def check_devices_alive(self) -> bool:
        """
        Check if all connected devices are responsive and alive.

        Iterates through all loaded devices and tests their responsiveness by
        calling the is_alive() method. This helps detect communication failures,
        device crashes, or network issues that could affect observatory operations.

        Returns:
            bool: True if all devices are responsive, False if any device fails
                to respond or encounters an error.

        Side Effects:
            - Adds unresponsive devices to error_source list for monitoring
            - Logs error messages for each unresponsive device
            - Returns False immediately if any device fails

        Note:
            - Called regularly by the watchdog for continuous health monitoring
            - Critical for detecting device failures before they affect observations
            - Used to trigger error handling and recovery procedures
        """
        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                try:
                    r = self.devices[device_type][device_name].is_alive()
                    if r is False:
                        self.logger.report_device_issue(
                            device_type=device_type,
                            device_name=device_name,
                            message=f"{device_type} {device_name} unresponsive",
                        )
                        return False
                except Exception as e:
                    self.logger.report_device_issue(
                        device_type=device_type,
                        device_name=device_name,
                        message=f"{device_type} {device_name} unresponsive. ",
                        exception=e,
                    )
                    return False
        return True

    def force_poll_observing_conditions(self, fits_config):
        """Force an immediate poll of all ObservingConditions devices."""
        if "ObservingConditions" in self.devices:
            for _, device in self.devices["ObservingConditions"].items():
                for _, fits_row in fits_config.iterrows():
                    if (
                        fits_row["device_type"] == "ObservingConditions"
                        and not fits_row["fixed"]
                    ):
                        device.force_poll(fits_row["device_command"])

    def list_device_names(self, device_type: str, paired_devices=None) -> list:
        """List device names of a given type, optionally filtered by paired devices."""
        if paired_devices is not None:
            return (
                [] if device_type not in device_type else [paired_devices[device_type]]
            )

        return (
            []
            if device_type not in self.devices
            else list(self.devices[device_type].keys())
        )

    def check_telescopes_in_domes(self):
        """Check telescope assignments in domes.

        Warn if:
            - There are telescopes not assigned to any dome
            - There are domes assigned to telescopes that are not connected

        Raise error if:
            - Dome configuration is invalid
        """
        if "Telescope" not in self.devices:
            return

        assigned_telescopes = set()
        dome_issues: dict[str, dict] = {}

        for dome_conf in self.observatory_config.get("Dome", []):
            try:
                telescopes = dome_conf.get("telescopes", [])
                if not isinstance(telescopes, (list, tuple)):
                    dome_issues[dome_conf.get("device_name", "<unknown>")] = {
                        "telescopes_field_invalid": telescopes
                    }
                    self.logger.warning(
                        f"Dome {dome_conf.get('device_name', '<unknown>')} has an invalid 'telescopes' field: {telescopes!r}"
                    )
                    continue
                assigned_telescopes.update(telescopes)
                # detect references to telescopes that don't exist
                missing = [
                    t for t in telescopes if t not in self.devices.get("Telescope", {})
                ]
                if missing:
                    dome_issues.setdefault(
                        dome_conf.get("device_name", "<unknown>"), {}
                    )["missing_telescopes"] = missing
                    self.logger.warning(
                        f"Dome {dome_conf.get('device_name', '<unknown>')} references missing telescope(s): {', '.join(missing)}"
                    )
            except Exception as e:
                name = dome_conf.get("device_name", "<unknown>")
                dome_issues[name] = {"config_error": str(e)}
                self.logger.report_device_issue(
                    device_type="Dome",
                    device_name=name,
                    message=f"Error reading dome config for {name}",
                    exception=e,
                )

        unassigned_telescopes = (
            set(self.devices["Telescope"].keys()) - assigned_telescopes
        )
        if unassigned_telescopes:
            self.logger.warning(
                "The following telescope(s) are not assigned to any dome: "
                + ", ".join(unassigned_telescopes)
                + ". "
                "Domes may close without checking whether these telescopes are parked."
            )
            self.logger.debug(
                f"{list(self.devices['Telescope'].keys())} "
                + "are the connected telescopes."
                f" {assigned_telescopes} are the assigned telescopes."
            )
        else:
            self.logger.debug("All telescopes are assigned to domes.")

        # Find domes with telescopes not in devices and log a warning
        missing_telescopes = assigned_telescopes - set(self.devices["Telescope"].keys())
        if missing_telescopes:
            self.logger.warning(
                "The following telescope(s) assigned to domes are not connected: "
                + ", ".join(missing_telescopes)
                + "."
            )
