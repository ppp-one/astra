"""
Observatory Control System for Autonomous Astronomical Operations.

This module provides the core Observatory class for managing and controlling
astronomical observatories. It handles device coordination, safety monitoring,
automated observations, and data acquisition for professional astronomical
facilities.

The module integrates multiple subsystems including:
    - Alpaca-compatible device drivers for telescopes, cameras, and accessories
    - Real-time safety and weather monitoring systems
    - Automated observation schedule execution
    - Database logging and FITS header management
    - Thread-safe multiprocessing architecture

Key Components:
    - Observatory: Main control class for observatory operations
    - Device Management: Alpaca protocol device coordination
    - Safety Systems: Weather monitoring and error handling
    - Scheduling: Automated observation execution
    - Calibration: Automated flat, dark, and bias frame acquisition
    - Pointing & Guiding: Telescope pointing correction and guiding

Usage:
    This module is typically used as part of the ASTRA observatory automation
    framework. The Observatory class is instantiated with a configuration file
    and then manages all aspects of observatory operation.

Note:
    This software is designed for professional astronomical observatories.
    Proper configuration of safety systems is essential to prevent equipment
    damage and ensure personnel safety.
"""

import logging
import math
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import astropy.units as u
import numpy as np
import pandas as pd
import psutil
from astropy.coordinates import AltAz, EarthLocation, SkyCoord, get_body
from astropy.io import fits
from astropy.time import Time
from astropy.wcs.utils import WCS

from astra import Config, utils
from astra.alpaca_device_process import AlpacaDevice
from astra.autofocus import Autofocuser, Defocuser
from astra.calibrate_guiding import GuidingCalibrator
from astra.config import ObservatoryConfig
from astra.database_manager import DatabaseManager
from astra.device_manager import DeviceManager
from astra.guiding import GuiderManager
from astra.image_handler import HeaderManager, ImageHandler
from astra.logger import (
    ConsoleStreamHandler,
    DatabaseLoggingHandler,
    FileHandler,
    ObservatoryLogger,
)
from astra.paired_devices import PairedDevices
from astra.pointer import PointingCorrectionHandler
from astra.queue_manager import QueueManager
from astra.safety_monitor import SafetyMonitor
from astra.scheduler import Action, ScheduleManager
from astra.thread_manager import ThreadManager

logging.getLogger("sqlite3worker").setLevel(logging.INFO)


class Observatory:
    """
    Autonomous astronomical observatory control system.

    The Observatory class provides comprehensive control and automation for astronomical
    observatories, managing telescopes, cameras, filter wheels, focusers, domes, and
    other equipment. It coordinates complex observation sequences, safety monitoring,
    device management, and data acquisition for autonomous or semi-autonomous operation.

    Key Features:
        - Multi-device coordination and control via Alpaca protocol
        - Autonomous observation scheduling and execution
        - Real-time safety monitoring and weather assessment
        - Automated calibration sequences (flats, darks, bias frames)
        - Autoguiding and pointing correction capabilities
        - Comprehensive error handling and recovery
        - Database logging of all operations and device states
        - FITS header management and metadata completion
        - Thread-safe multiprocessing architecture

    Observatory Operations:
        - Schedule-driven autonomous observations
        - Real-time device monitoring and polling
        - Safety watchdog with weather integration
        - Automatic dome and telescope control
        - Image acquisition and processing pipelines
        - Pointing model generation and refinement
        - Focus maintenance and autofocus routines
        - Calibration frame acquisition

    Safety Systems:
        - Continuous weather monitoring integration
        - Device health checking and error detection
        - Automatic observatory closure on unsafe conditions

    Architecture:
        - Thread-based concurrent operations
        - Database-backed logging and state persistence
        - Queue-based multiprocessing communication
        - Configuration-driven device management
        - Modular sequence and action execution

    Usage:
        Typically instantiated with a configuration file that defines the observatory
        layout, device connections, safety parameters, and operational settings.
        The observatory can then be operated manually or through automated scheduling.

    Note:
        This class is designed for professional astronomical observatories and
        requires proper configuration of safety systems and device drivers.
        Improper use could result in equipment damage or safety hazards.
    """

    def __init__(
        self,
        config_filename: str,
        truncate_factor: float | None = None,
        custom_observatory: str = "",
        logging_level: int = logging.INFO,
    ):
        """
        Initialize the Observatory object.

        Sets up the observatory configuration, database, logging, device management,
        and scheduling systems. Creates a queue for multiprocessing and initializes
        all necessary attributes for observatory operations.

        Parameters:
            config_filename (str): Path to the configuration file for the observatory.
                The filename is used to derive the observatory name.
            truncate_factor (float | None, optional): If specified, the schedule is truncated by a
                factor and moved to the current time. Defaults to None.
            custom_observatory (str, optional): If specified, enables custom observatory
                logic and error handling. Defaults to None.

        Attributes:
            name (str): Observatory name derived from config filename.
            database_manager (DatabaseManager): Manages the observatory database.
            logger (logging.Logger): Logger instance for the observatory.
            _config (ObservatoryConfig): Observatory configuration object.
            fits_config (pd.DataFrame): FITS header configuration.
            thread_manager (ThreadManager): Manages threads for observatory operations.
            queue (Queue): Multiprocessing queue for communication.
            heartbeat (dict): System status information.
            error_free (bool): Flag indicating error-free operation.
            weather_safe (bool): Flag indicating weather safety status.
            schedule_running (bool): Flag indicating if schedule is running.
            robotic_switch (bool): Flag for robotic operation mode.
            devices (dict): Dictionary of connected devices.
            guider (dict): Dictionary of guiding objects per telescope.
        """

        # set observatory name
        self.name = Path(config_filename).stem.replace("_config", "")
        self._config = ObservatoryConfig.from_config(Config(observatory_name=self.name))

        # setup logger and database
        self.database_manager = DatabaseManager.from_observatory_config(self._config)
        self.database_manager.create_database()

        self.logger = ObservatoryLogger(self.name, level=logging_level)
        self.logger.addHandler(DatabaseLoggingHandler(self.database_manager))
        self.logger.addHandler(ConsoleStreamHandler())
        self.logger.addHandler(FileHandler(Config().paths.log_file))
        self.database_manager.logger = self.logger

        # log start up
        self.logger.debug("Database and DatabaseLoggingHandler initialized")
        self.logger.info("Astra starting up")

        # warn if debug mode
        if self.logger.getEffectiveLevel() == logging.DEBUG:
            self.logger.warning("Astra is running in debug mode")

        # read observatory config files
        self.fits_config = self._config.load_fits_config()

        # running threads list
        self.thread_manager = ThreadManager()

        # queue for multiprocessing
        self.queue_manager = QueueManager(
            logger=self.logger,
            database_manager=self.database_manager,
            thread_manager=self.thread_manager,
        )
        self.queue_manager.start_queue_thread()

        # heartbeat dictionary
        self.heartbeat = {}

        # custom logic flags
        self.speculoos = custom_observatory == "speculoos"

        # error and weather handling flags

        # watchdog/schedule running flags, robotic switch
        self.watchdog_running = False
        self.robotic_switch = False

        # schedule
        self.schedule_manager = ScheduleManager(
            schedule_path=Config().paths.schedules / f"{self.name}.jsonl",
            truncate_factor=truncate_factor,
            logger=self.logger,
        )

        # load devices
        self.device_manager = DeviceManager(
            observatory_config=self.config,
            logger=self.logger,
            queue_manager=self.queue_manager,
            thread_manager=self.thread_manager,
        )
        self.device_manager.load_devices()

        self._image_handlers: dict[str, ImageHandler] = {}

        # for each telescope, create a donuts guider
        self.guider_manager = GuiderManager.from_observatory(self)
        self.safety_monitor = SafetyMonitor(
            observatory_config=self.config,
            logger=self.logger,
            database_manager=self.database_manager,
            device_manager=self.device_manager,
        )

        self.logger.info("Astra initialized")

    @property
    def config(self) -> ObservatoryConfig:
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
        if self._config.is_outdated():
            self.logger.info("Config file modified, reloading.")
            self._config.load()

            # TODO restart devices

        return self._config

    @property
    def devices(self) -> dict[str, dict[str, AlpacaDevice]]:
        """Get the dictionary of connected devices."""
        return self.device_manager.devices

    @property
    def weather_safe(self) -> bool | None:
        return self.safety_monitor.weather_safe

    @property
    def time_to_safe(self) -> float:
        return self.safety_monitor.time_to_safe

    @property
    def image_handler(self) -> ImageHandler:
        """
        Get the ImageHandler instance for image processing and FITS header management.

        DEPRECATED for multi-camera observatories. Use get_image_handler(camera_name) instead.

        This property provides backward compatibility but will raise an error if multiple
        ImageHandlers are active (indicating multi-camera use). For new code, use
        get_image_handler(camera_name) to explicitly specify which camera's ImageHandler
        to retrieve.

        Returns:
            ImageHandler: The current ImageHandler instance.

        Raises:
            ValueError: If multiple ImageHandlers are active or if no ImageHandler is initialized.
        """
        if len(self._image_handlers) > 1:
            raise ValueError(
                "Multiple ImageHandlers active. Use get_image_handler(camera_name) instead."
            )
        if not self._image_handlers:
            raise ValueError(
                "ImageHandler not initialized. Call setup_image_handler first."
            )
        return list(self._image_handlers.values())[0]

    @image_handler.setter
    def image_handler(self, value: ImageHandler) -> None:
        """DEPRECATED: Setting image_handler directly is no longer supported."""
        raise DeprecationWarning(
            "Setting image_handler directly is deprecated. "
            "ImageHandlers are now managed per-camera via setup_image_handler()."
        )

    def get_image_handler(self, camera_name: str) -> ImageHandler:
        """
        Get the ImageHandler for a specific camera.

        Parameters:
            camera_name (str): The name of the camera device.

        Returns:
            ImageHandler: The ImageHandler instance for the specified camera.

        Raises:
            ValueError: If ImageHandler for the specified camera is not initialized.
        """
        if camera_name not in self._image_handlers:
            raise ValueError(
                f"ImageHandler for camera '{camera_name}' not initialized. "
                "Call setup_image_handler first."
            )
        return self._image_handlers[camera_name]

    def connect_all_devices(self):
        """
        Connect to all loaded devices and start polling for FITS header data.

        Establishes connections to all initialized devices and begins regular polling
        of device properties needed for FITS headers. Different polling intervals
        are used based on device criticality:
        - Most devices: 5-second intervals
        - SafetyMonitor: 1-second intervals for safety-critical data

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
        self.device_manager.connect_all(
            fits_config=self.fits_config,
            speculoos=self.speculoos,
        )
        self.start_watchdog()

    def start_watchdog(self) -> None:
        """
        Start the observatory watchdog monitoring thread.

        Initializes and starts a daemon thread that runs the watchdog() method
        to continuously monitor observatory safety, weather conditions, device
        health, and system status. The watchdog is essential for autonomous
        operation and safety.

        The method:
        1. Checks if watchdog is already running to prevent duplicates
        2. Creates a new daemon thread running the watchdog() method
        3. Adds the thread to the threads list for tracking

        Note:
            - If watchdog is already running, logs a warning and returns
            - The watchdog thread is marked as a daemon thread
            - The thread is automatically tracked in the observatory's thread list
        """

        if self.watchdog_running is True:
            self.logger.warning("Watchdog already running")
            return

        self.thread_manager.start_thread(
            target=self.watchdog,
            thread_type="watchdog",
            device_name="watchdog",
            thread_id="watchdog",
        )

    def watchdog(self) -> None:
        """
        Main observatory monitoring loop for safety and operational status.

        Continuously monitors critical observatory systems and takes appropriate
        actions to ensure safe and efficient operation. The watchdog is the
        central control system that coordinates all observatory activities.

        See class docstring for full behavior.
        """
        self.logger.info("Starting watchdog")
        self.watchdog_running = True

        while self.watchdog_running:
            self.device_manager.check_devices_alive()
            self.update_heartbeat()
            self._watchdog_step()
            self.database_manager.maybe_run_backup(self.thread_manager)
            time.sleep(0.5)

        # Stop watchdog with clean exit
        self.schedule_manager.running = False
        self.robotic_switch = False
        self.watchdog_running = False
        self.logger.warning("Watchdog stopped")

    def _watchdog_step(self):
        if self.logger.error_free:
            try:
                # Check schedule
                try:
                    reloaded = self.schedule_manager.reload_if_updated()
                    if reloaded and self.robotic_switch:
                        self.logger.warning("Robotic switch is on, starting schedule")
                        self.start_schedule()
                except Exception as e:
                    self.logger.report_device_issue(
                        device_type="Schedule",
                        device_name="schedule",
                        message="Error checking schedule",
                        exception=e,
                    )
                    return

                # Check safety monitor
                if not self.safety_monitor:
                    self.logger.warning("No safety monitor configured")
                    return

                if not self.safety_monitor.update_status():
                    self.close_observatory()
            except Exception as e:
                self.logger.report_device_issue(
                    device_type="Watchdog",
                    device_name="watchdog",
                    message="Error during watchdog check",
                    exception=e,
                )
        else:
            self._handle_watchdog_errors()
            self.watchdog_running = False
            return

    def _handle_watchdog_errors(self) -> None:
        """Handle system errors detected by the watchdog loop."""
        try:
            # stop schedule + automation
            self.schedule_manager.running = False
            self.robotic_switch = False

            # wait to see if multiple devices report errors
            self.logger.info(
                "Waiting 30 seconds to see if error is multi-device. Main watchdog thread exited."
            )
            time.sleep(30)

            if len(self.logger.error_source) == 0:
                self.logger.report_device_issue(
                    device_type="error_source",
                    device_name="error_source",
                    message="No error sources found in error_source",
                    level="warning",
                )

            df = pd.DataFrame(self.logger.error_source)
            device_types = df.device_type.unique()
            device_names = df.device_name.unique()

            # Multiple device errors → panic
            if len(device_names) > 1:
                self.logger.error("Multiple devices have errors. Panic.")
                for error_source in self.logger.error_source:
                    self.logger.error(
                        f"Device {error_source['device_type']} {error_source['device_name']} "
                        f"has error: {error_source['error']}"
                    )
                # TODO: Panic mode

            # Single device error
            elif len(device_names) == 1 and len(device_types) == 1:
                self.logger.warning(
                    f"Device {device_types[0]} {device_names[0]} has errors."
                )

            # Close observatory if telescope/dome are unaffected
            if (
                "Dome" not in device_types
                and "Telescope" not in device_types
                and ("Dome" in self.config or "Telescope" in self.config)
            ):
                self.logger.warning(
                    "Closing observatory due to no errors in Dome or Telescope"
                )
                self.close_observatory(error_sensitive=False)

            # Dome-specific closure logic
            elif (
                "Dome" not in device_types
                and "Telescope" in device_types
                and "Dome" in self.config
            ):
                for dome_config in self.config["Dome"]:
                    if dome_config.get("close_dome_on_error", False):
                        if self.speculoos:
                            self.speculoos_check_and_ack_error(close=True)

                        device_name = dome_config["device_name"]
                        self.logger.warning(
                            f"Closing Dome {device_name} due to errors."
                        )
                        self.execute_and_monitor_device_task(
                            "Dome",
                            "ShutterStatus",
                            1,
                            "CloseShutter",
                            device_name=device_name,
                            log_message=f"Closing Dome shutter of {device_name}",
                            weather_sensitive=False,
                            error_sensitive=False,
                        )

            # Final heartbeat update
            self.update_heartbeat()

        except Exception as e:
            self.logger.error(
                f"Error during error handling: {str(e)}",
                exc_info=True,
                stack_info=True,
            )
            # TODO: Panic mode

    def update_heartbeat(self) -> None:
        """
        Update the observatory heartbeat with current system status information.

        Creates a comprehensive status snapshot of the observatory including system
        health, device status, resource usage, and operational state. This heartbeat
        information is used for monitoring and debugging observatory operations.

        The heartbeat includes:
        - Current timestamp with millisecond precision
        - Error status and error source details
        - Weather safety status
        - Schedule execution status
        - System resource usage (CPU, memory, disk)
        - Active thread information
        - Device polling status for all connected devices
        - Monitor action queue status

        This information is typically used by:
        - External monitoring systems
        - Web interfaces for observatory status
        - Debugging and troubleshooting
        - Health check systems

        Note:
            - Called regularly by the watchdog to maintain current status
            - Provides real-time snapshot of observatory state
            - Essential for remote monitoring of autonomous operations
        """
        # update heartbeat
        self.heartbeat["datetime"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]
        self.heartbeat["error_free"] = self.logger.error_free
        self.heartbeat["error_source"] = self.logger.error_source
        self.heartbeat["weather_safe"] = self.weather_safe
        self.heartbeat["schedule_running"] = self.schedule_manager.running
        self.heartbeat["cpu_percent"] = psutil.cpu_percent()
        self.heartbeat["memory_percent"] = psutil.virtual_memory().percent
        self.heartbeat["disk_percent"] = psutil.disk_usage("/").percent
        self.heartbeat["threads"] = self.thread_manager.get_thread_summary()

        polled_list = {}

        for device_type in self.devices:
            polled_list[device_type] = {}

            for device_name in self.devices[device_type]:
                polled_list[device_type][device_name] = {}

                try:
                    polled = self.devices[device_type][device_name].poll_latest()
                except Exception as e:
                    self.logger.report_device_issue(
                        device_type=device_type,
                        device_name=device_name,
                        message=f"Error polling {device_type} {device_name}",
                        exception=e,
                    )
                    polled = None

                if polled is not None:  # not sure if correct to put this here, or later
                    polled_keys = polled.keys()
                    for k in polled_keys:
                        polled_list[device_type][device_name][k] = {}
                        polled_list[device_type][device_name][k]["value"] = polled[k][
                            "value"
                        ]
                        polled_list[device_type][device_name][k]["datetime"] = polled[
                            k
                        ]["datetime"]

        self.heartbeat["polling"] = polled_list
        self.heartbeat["monitor-action-queue"] = (
            self.device_manager.device_task_monitor_queue
        )

    def speculoos_check_and_ack_error(self, close=False) -> None:
        """
        Check for and acknowledge SPECULOOS AsTelOS telescope errors.

        SPECULOOS-specific method that monitors telescope error states and
        automatically acknowledges errors that can be safely cleared. This is
        essential for the autonomous operation of SPECULOOS telescopes which use
        the AsTelOS control system.

        Parameters:
            close (bool, optional): If True, checks for errors related to
                observatory closure operations. If False, checks for general
                operational errors. Defaults to False.

        The method:
        1. Iterates through all telescope devices
        2. Checks for AsTelOS-specific error conditions
        3. Attempts to acknowledge clearable errors automatically
        4. Logs error status and acknowledgment results

        Error Handling:
        - Only acknowledges errors that are safe to clear
        - Maintains error state for serious issues requiring manual intervention
        - Logs all error checking and acknowledgment activities

        Note:
            - Only used with SPECULOOS observatories
            - Critical for autonomous error recovery
            - Should be called before and after critical telescope operations
        """
        if "Telescope" in self.config:
            for telescope_name in self.devices["Telescope"]:
                telescope = self.devices["Telescope"][telescope_name]

                # check telescope status
                valid, all_errors, messages = utils.check_astelos_error(
                    telescope, close=close
                )

                if valid and len(all_errors) > 0:
                    self.logger.info(
                        f"Attempting to acknowledge AsTelOS errors for {telescope_name}: {messages}"
                    )
                    ack, messages = utils.ack_astelos_error(
                        telescope, valid, all_errors, messages, close=close
                    )

                    if ack:
                        self.logger.info(
                            f"AsTelOS errors successfully acknowledged for {telescope_name}: {messages}"
                        )
                    else:
                        self.logger.report_device_issue(
                            device_type="Telescope",
                            device_name=telescope_name,
                            message="AsTelOS errors not successfully acknowledged for"
                            + f" {telescope_name}: {messages}",
                        )

                if not valid:
                    self.logger.report_device_issue(
                        device_type="Telescope",
                        device_name=telescope_name,
                        message=f"AsTelOS errors invalid for {telescope_name}: {messages}",
                    )

    def open_observatory(self, paired_devices: dict | None = None) -> None:
        """
        Open the observatory for observations in a safe, controlled sequence.

        Performs the complete observatory opening sequence, ensuring safety at each step:
        1. Opens dome shutter (if present and weather is safe)
        2. Unparks telescope (if present and weather is safe)
        3. Handles SPECULOOS-specific error acknowledgment and polling management

        The sequence only proceeds if weather conditions are safe and no errors
        are present. For SPECULOOS observatories, special error handling and
        polling management is performed.

        Parameters:
            paired_devices (dict, optional): Dictionary specifying which specific
                devices to use for the opening sequence. If None, uses all
                available devices of each type. Defaults to None.

        Safety Checks:
            - Weather safety verification before each major operation
            - Error-free status confirmation
            - SPECULOOS-specific error acknowledgment and recovery

        Note:
            - SPECULOOS observatories pause polling during critical operations
            - Opening sequence is aborted if unsafe conditions develop
            - Telescope readiness is verified after unparking for SPECULOOS systems
        """

        if self.speculoos:
            # SPECULOOS EDIT
            self.device_manager.pause_polls(["Dome", "Telescope", "Focuser"])

            # SPECULOOS EDIT
            self.speculoos_check_and_ack_error()

        if "Dome" in self.config:
            if self.weather_safe and self.logger.error_free:
                # open dome shutter
                if paired_devices is not None:
                    self.execute_and_monitor_device_task(
                        "Dome",
                        "ShutterStatus",
                        0,
                        "OpenShutter",
                        device_name=paired_devices["Dome"],
                        log_message=f"Opening Dome shutter of {paired_devices['Dome']}",
                    )
                else:
                    for device_name in self.devices["Dome"]:
                        self.execute_and_monitor_device_task(
                            "Dome",
                            "ShutterStatus",
                            0,
                            "OpenShutter",
                            device_name=device_name,
                            log_message=f"Opening Dome shutter of {device_name}",
                        )

        if self.speculoos:
            # SPECULOOS EDIT
            self.speculoos_check_and_ack_error()

        if "Telescope" in self.config:
            if self.weather_safe and self.logger.error_free:
                # unpark telescope
                if paired_devices is not None:
                    self.execute_and_monitor_device_task(
                        "Telescope",
                        "AtPark",
                        False,
                        "Unpark",
                        device_name=paired_devices["Telescope"],
                        log_message=f"Unparking Telescope {paired_devices['Telescope']}",
                    )
                else:
                    for device_name in self.devices["Telescope"]:
                        self.execute_and_monitor_device_task(
                            "Telescope",
                            "AtPark",
                            False,
                            "Unpark",
                            device_name=device_name,
                            log_message=f"Unparking Telescope {device_name}",
                        )

        if self.speculoos:
            # SPECULOOS EDIT
            self.speculoos_check_and_ack_error()

            # SPECULOOS EDIT
            self.device_manager.resume_polls(["Dome", "Telescope", "Focuser"])

            # check if telescope(s) are ready
            start_time = time.time()
            if self.weather_safe and self.logger.error_free:
                for telescope_name in self.devices["Telescope"]:
                    telescope = self.devices["Telescope"][telescope_name]

                    r = telescope.get(
                        "CommandString", Command="TELESCOPE.READY_STATE", Raw=True
                    )

                    while float(r) != 1:
                        self.logger.info(f"Waiting for {telescope_name} to be ready")

                        time.sleep(1)

                        r = telescope.get(
                            "CommandString", Command="TELESCOPE.READY_STATE", Raw=True
                        )

                        # timeout
                        if time.time() - start_time > 120:
                            self.logger.report_device_issue(
                                device_type="Telescope",
                                device_name=telescope_name,
                                message="Timeout waiting for telescope "
                                + f"{telescope_name} to be ready",
                            )
                            break

                        if float(r) == 1:
                            self.logger.info(f"{telescope_name} is ready")
                        elif float(r) < 0:
                            self.logger.report_device_issue(
                                device_type="Telescope",
                                device_name=telescope_name,
                                message="Issue with telescope getting ready, "
                                + f"status: {r}",
                            )

    def close_observatory(
        self, paired_devices: PairedDevices | None = None, error_sensitive: bool = True
    ) -> bool:
        """
        Close the observatory in a safe, controlled sequence.

        Performs the complete observatory shutdown sequence to ensure equipment
        safety and protection from weather. The sequence follows this order:
        1. Stop any active guiding operations
        2. Stop telescope slewing and tracking
        3. Park the telescope to safe position
        4. Park the dome and close shutter (if dome present)

        For SPECULOOS observatories, includes special error handling and polling
        management during the closure sequence.

        Parameters:
            paired_devices (dict, optional): Dictionary specifying which specific
                devices to use for the closing sequence. Format:
                {'Telescope': 'TelescopeName', 'Dome': 'DomeName'}
                If None, uses all available devices. Defaults to None.
            error_sensitive (bool, optional): If True, the closure process is
                sensitive to system errors. If False, attempts closure even
                with errors present. Defaults to True.

        Returns:
            bool: True if the closure sequence completed successfully.

        Note:
            - SPECULOOS observatories pause polling during critical operations
            - Dome errors are acknowledged before attempting closure
            - Critical for protecting equipment during unsafe weather conditions
        """
        if self.speculoos:
            # SPECULOOS EDIT
            self.device_manager.pause_polls(["Dome", "Telescope", "Focuser"])

            # acknowledge errors if dome not closed, if any
            device_names = (
                [paired_devices["Dome"]] if paired_devices else self.devices["Dome"]
            )
            for device_name in device_names:
                dome = self.devices["Dome"][device_name]
                ShutterStatus = dome.get("ShutterStatus")
                if ShutterStatus == 0:  # open
                    self.speculoos_check_and_ack_error(close=True)

        if "Telescope" in self.config:
            # telescope guiding and slewing
            device_names = (
                [paired_devices["Telescope"]]
                if paired_devices
                else self.devices["Telescope"]
            )
            for device_name in device_names:
                try:
                    self.guider_manager.stop_guider(
                        device_name, thread_manager=self.thread_manager
                    )

                except Exception as e:
                    self.logger.report_device_issue(
                        device_type="Guider",
                        device_name=device_name,
                        message=f"Error stopping telescope {device_name} guiding",
                        exception=e,
                    )

                self.execute_and_monitor_device_task(
                    "Telescope",
                    "Slewing",
                    False,
                    "AbortSlew",
                    device_name=device_name,
                    log_message=f"Stopping telescope {device_name} slewing",
                    weather_sensitive=False,
                    error_sensitive=error_sensitive,
                )

                # stop telescope tracking
                self.execute_and_monitor_device_task(
                    "Telescope",
                    "Tracking",
                    False,
                    "Tracking",
                    device_name=device_name,
                    log_message=f"Stopping telescope {device_name} tracking",
                    weather_sensitive=False,
                    error_sensitive=error_sensitive,
                )

                # park telescope
                self.execute_and_monitor_device_task(
                    "Telescope",
                    "AtPark",
                    True,
                    "Park",
                    device_name=device_name,
                    log_message=f"Parking telescope {device_name}",
                    weather_sensitive=False,
                    error_sensitive=error_sensitive,
                )

        if "Dome" in self.config:
            # only proceed if telescope parked
            if (
                "Telescope" in self.config
                and self.config["Dome"][0].get("close_dome_on_error", False)
                is False  # TODO: assume one dome, but should check all domes or paired dome
            ):
                telescope_names = (
                    [paired_devices["Telescope"]]
                    if paired_devices
                    else self.devices["Telescope"]
                )

                for telescope_name in telescope_names:
                    telescope = self.devices["Telescope"][telescope_name]
                    at_park = telescope.get("AtPark")
                    if at_park is False:
                        self.logger.report_device_issue(
                            device_type="Telescope",
                            device_name=telescope_name,
                            message=f"Telescope {telescope_name} not parked, "
                            "cannot close dome during close_observatory method",
                        )
                        return False
            # park dome
            device_names = (
                [paired_devices["Dome"]] if paired_devices else self.devices["Dome"]
            )
            for device_name in device_names:
                self.execute_and_monitor_device_task(
                    "Dome",
                    "AtPark",
                    True,
                    "Park",
                    device_name=device_name,
                    log_message=f"Parking Dome {device_name}",
                    weather_sensitive=False,
                    error_sensitive=error_sensitive,
                )

                # close dome shutter
                self.execute_and_monitor_device_task(
                    "Dome",
                    "ShutterStatus",
                    1,
                    "CloseShutter",
                    device_name=device_name,
                    log_message=f"Closing Dome shutter of {device_name}",
                    weather_sensitive=False,
                    error_sensitive=error_sensitive,
                )

        if self.speculoos:
            # SPECULOOS EDIT
            self.device_manager.resume_polls(["Dome", "Telescope", "Focuser"])

        return True

    def toggle_robotic_switch(self) -> None:
        """
        Toggle the observatory's robotic operation mode on or off.

        Controls the robotic switch that enables or disables autonomous observatory
        operations. When enabled, the observatory can execute schedules automatically.
        When disabled, manual control is required for all operations.

        Behavior:
        - If robotic switch is currently ON: Turns it OFF and stops any running schedule
        - If robotic switch is currently OFF: Turns it ON and starts the schedule
          (if watchdog is running)

        Safety Features:
        - Requires watchdog to be running before enabling robotic mode
        - Automatically stops schedule execution when disabling robotic mode
        - Logs all state changes for monitoring and debugging

        Note:
            - Essential safety feature for autonomous operations
            - Provides manual override capability for emergency situations
            - Schedule execution is dependent on robotic switch being enabled
        """
        if self.robotic_switch:
            self.robotic_switch = False
            self.logger.info("Robotic switch turned off")
            # stop schedule if running
            self.schedule_manager.stop_schedule(self.thread_manager)
        else:
            if self.watchdog_running is False:
                self.logger.warning(
                    "Robotic switch cannot be turned on without watchdog running"
                )
                return

            self.robotic_switch = True
            self.logger.info("Robotic switch turned on")

            if self.schedule_manager.running:
                # stop schedule if running
                self.schedule_manager.stop_schedule(self.thread_manager)

            # start schedule if not running
            self.start_schedule()

    def start_schedule(self) -> None:
        """
        Start the observatory schedule execution in a new thread.

        Initializes and starts a dedicated daemon thread for executing the loaded
        schedule. Performs various safety and readiness checks before starting
        schedule execution to ensure safe autonomous operation.

        Pre-execution Checks:
        - Schedule must be loaded
        - Schedule must not already be running
        - Watchdog must be running for safety monitoring
        - Schedule end time must be in the future
        - No duplicate schedule threads allowed

        Thread Management:
        - Creates a daemon thread running run_schedule()
        - Resets the 'completed' flag on all schedule items
        - Adds thread to the observatory's thread tracking list
        - Thread ID 'schedule' for easy identification

        Safety Features:
        - Multiple validation checks prevent unsafe execution
        - Automatic thread cleanup if conditions not met
        - Logging of all start attempts and failures

        Note:
            - Schedule execution continues until completion or safety conditions fail
            - Essential component of autonomous observatory operations
            - Coordinates with watchdog for continuous safety monitoring
        """

        if self.schedule_manager.schedule is None:
            self.logger.warning("Schedule not loaded")
            return

        if self.schedule_manager.running:
            self.logger.warning("Schedule already running")
            return

        if self.watchdog_running is False:
            self.logger.warning("Schedule cannot be started without watchdog running")
            return

        if self.schedule_manager.schedule[-1].end_time < datetime.now(UTC):
            self.logger.warning("Schedule end time in the past")
            return

        # check schedule not in threads
        if self.thread_manager.is_thread_running("schedule"):
            self.logger.warning("Schedule currently running")
            return

        # reset completed column on new start
        self.schedule_manager.schedule.reset_completion()

        self.thread_manager.start_thread(
            target=self.run_schedule,
            device_name="Schedule",
            thread_type="run_schedule",
            thread_id="schedule",
        )

    def run_schedule(self) -> None:
        """
        Execute the observatory schedule while monitoring safety and operational conditions.

        Manages the execution of scheduled observatory activities in a continuous loop,
        ensuring safety conditions are met before starting each action. The scheduler
        coordinates multiple concurrent operations while maintaining system safety.

        Key features:
        - Waits for weather safety confirmation before starting
        - Iterates through schedule rows checking timing and conditions
        - Starts actions in separate threads for concurrent execution
        - Handles both weather-dependent and weather-independent operations
        - Manages thread lifecycle and cleanup
        - Performs final header completion after schedule ends

        Safety Management:
        - Monitors weather_safe, error_free, and watchdog_running status
        - Aborts operations if unsafe conditions develop
        - Times out if weather safety check takes longer than 2 minutes

        Thread Management:
        - Removes completed threads from tracking list
        - Prevents duplicate actions from starting
        - Ensures proper cleanup on schedule completion

        Note:
            - Schedule must be loaded before calling this method
            - Method runs until schedule completion or safety conditions fail
            - Automatically starts final header completion thread on exit
        """
        self.schedule_manager.running = True
        self.logger.info("Running schedule")

        t0 = time.time()
        while self.weather_safe is None and (time.time() - t0) < 120:
            self.logger.info("Waiting for safety conditions to be checked")
            time.sleep(1)

        if self.weather_safe is None:
            self.logger.report_device_issue(
                device_type="SafetyMonitor",
                device_name="",
                message="Weather safety check timed out",
            )
            return

        schedule = self.schedule_manager.get_schedule()
        while (
            self.schedule_manager.running
            and self.watchdog_running
            and self.logger.error_free
        ):
            self.thread_manager.remove_dead_threads()
            ids = self.thread_manager.get_thread_ids()

            # loop through schedule
            for i, action in enumerate(schedule):
                # if schedule item not running, start thread if conditions are met
                if (
                    (i not in ids)
                    and self.check_conditions(action)
                    and (not action.completed)
                ):
                    th = self.thread_manager.start_thread(
                        target=self.run_action,
                        device_name=action.device_name,
                        thread_type=action.action_type,
                        thread_id=i,
                        args=(action,),
                    )

                    if action.action_value.get("execute_parallel", False) is False:
                        # wait for thread to finish
                        th.join()  # TODO: join last parallel thread?

            # exit while loop if reached end of schedule
            if schedule[-1].end_time < datetime.now(UTC):
                break
            if self.schedule_manager.get_schedule().is_completed():
                self.logger.info("All scheduled actions completed. Ending schedule.")
                self.schedule_manager.running = False
                break

            time.sleep(1)

        # run headers completion
        self.thread_manager.start_thread(
            target=HeaderManager.final_headers,
            device_name="astra",
            thread_type="Headers",
            thread_id="complete_headers",
            args=(
                self.database_manager,
                self.logger,
                self.config,
                self.devices,
                self.fits_config,
            ),
        )

        self.logger.info(
            "Schedule stopped. "
            f"{self.schedule_manager.get_completed_percentage()}% of actions completed."
        )

    def run_action(self, action: Action) -> None:
        """
        Execute the action specified in the schedule (Action object).

        Parameters:
            action (Action): The action object to execute.

        Raises:
            Exception: Any unexpected error that occurs during execution.

        Notes:
            - For 'object', 'calibration', or 'flats' action types, specialized sequences are executed based on the action_type.
            - For 'open' action type, the function may turn on camera cooler, set temperature, and open the observatory dome.
            - For 'close' action type, the function may close the observatory dome.
            - For other action types, the function assumes it's an ASCOM command and attempts to execute it on the specified device.

        """
        self.logger.info(f"Starting {action.device_name} {action.action_type}")
        try:
            if action.device_name in self.devices["Camera"]:
                paired_devices = PairedDevices.from_observatory(
                    observatory=self,
                    camera_name=action.device_name,
                )
                camera_config = paired_devices.get_device_config("Camera")
                set_temperature = camera_config["temperature"]
                temperature_tolerance = camera_config.get("temperature_tolerance", 1)
                cooling_timeout = camera_config.get("cooling_timeout", 30)
                if action.action_type not in ["close", "open"]:
                    self.cool_camera(
                        action,
                        set_temperature,
                        temperature_tolerance,
                        cooling_timeout,
                    )
            else:
                self.robotic_switch = False
                self.schedule_manager.running = False
                self.logger.warning(
                    f"Camera {action.device_name} not found in observatory devices."
                )
                return
            if not self.check_conditions(action):
                return
            if "object" == action.action_type:
                self.image_sequence(action, paired_devices)
            elif "autofocus" == action.action_type:
                self.autofocus_sequence(action, paired_devices)
            elif "calibrate_guiding" == action.action_type:
                self.guiding_calibration_sequence(action, paired_devices)
            elif "calibration" == action.action_type:
                self.image_sequence(action, paired_devices)
            elif "flats" == action.action_type:
                self.flats_sequence(action, paired_devices)
            elif "pointing_model" == action.action_type:
                self.pointing_model_sequence(action, paired_devices)
            elif "open" == action.action_type:
                if "Camera" in self.config:
                    self.open_observatory(paired_devices)
                    self.cool_camera(
                        action,
                        set_temperature,
                        temperature_tolerance,
                        cooling_timeout,
                    )
                else:
                    self.open_observatory()
            elif "close" == action.action_type:
                if "Camera" in self.config:
                    self.close_observatory(paired_devices)
                    self.cool_camera(
                        action,
                        set_temperature,
                        temperature_tolerance,
                        cooling_timeout,
                    )
                else:
                    self.close_observatory()
            elif "cool_camera" == action.action_type:
                if "Camera" in self.config:
                    self.cool_camera(
                        action,
                        set_temperature,
                        temperature_tolerance,
                        cooling_timeout,
                    )
            elif "complete_headers" == action.action_type:
                HeaderManager.final_headers(
                    self.database_manager,
                    self.logger,
                    self.config,
                    self.devices,
                    self.fits_config,
                )
            else:
                self.logger.report_device_issue(
                    device_type="Schedule",
                    device_name=action.device_name,
                    message=(
                        f"Invalid action_type: {action.device_name} {action.action_type} "
                        f"with {action.action_value} is not a valid method or property for "
                        f"{action.device_type} {action.device_name}"
                    ),
                )
            if (
                self.logger.error_free
                and self.schedule_manager.running
                and self.watchdog_running
            ):
                if (
                    action.action_type
                    in ["calibration", "close", "cool_camera", "complete_headers"]
                ) or self.weather_safe:
                    action.completed = True
                    action.set_status("FINISHED")
            self.logger.info(
                f"{action.action_type} sequence ended for {action.device_name}"
            )
            self.logger.info(
                f"{action.action_type} sequence had a planned start time of {action.start_time} and end time of {action.end_time}"
            )
        except Exception as e:
            self.schedule_manager.running = False
            self.logger.report_device_issue(
                "Schedule", action.device_name, "Run action error.", exception=e
            )

    def cool_camera(
        self,
        action: Action,
        set_temperature: float,
        temperature_tolerance: float = 1,
        cooling_timeout: int = 30,
    ) -> None:
        """
        Cool a camera to the specified temperature.

        Activates the camera cooler and sets the target temperature with
        specified tolerance. This is typically done before imaging sequences
        to reduce thermal noise and ensure consistent camera performance.

        Parameters:
            action (dict): Schedule action containing camera device information,
                specifically the 'device_name' field.
            set_temperature (float): Target temperature in degrees Celsius
                for the camera CCD.
            temperature_tolerance (float, optional): Acceptable temperature
                deviation from target in degrees Celsius. Defaults to 1.
            cooling_timeout (int, optional): Time in minutes to wait for cooling
                before raising error. Defaults to 30.

        Process:
        1. Turns on the camera cooler
        2. Sets the target CCD temperature with specified tolerance
        3. Waits up to cooling_timeout for temperature stabilization

        Safety Features:
        - Not weather sensitive (can operate in unsafe weather)
        - Continuous monitoring until target temperature reached
        - Configurable timeout for temperature stabilization

        Note:
            - Essential for scientific imaging to reduce thermal noise
            - Temperature stabilization can take significant time
            - Used in camera cooling sequences and before observations
        """
        # turn camera cooler on
        self.execute_and_monitor_device_task(
            "Camera",
            "CoolerOn",
            True,
            "CoolerOn",
            device_name=action.device_name,
            log_message=f"Turning on camera cooler for {action.device_name}",
            weather_sensitive=False,
        )

        # set temperature
        self.execute_and_monitor_device_task(
            "Camera",
            "CCDTemperature",
            set_temperature,
            "SetCCDTemperature",
            device_name=action.device_name,
            run_command_type="set",
            abs_tol=temperature_tolerance,
            log_message=f"Setting camera {action.device_name} temperature to {set_temperature}C with tolerance of {temperature_tolerance}C",
            timeout=60 * cooling_timeout,
            weather_sensitive=False,
        )

    def pre_sequence(
        self, action: Action, paired_devices: dict | PairedDevices
    ) -> None:
        """
        Prepare the observatory and metadata for a sequence.

        This method is responsible for preparing the observatory and gathering necessary information
        before running a sequence. Depending on the parameters in the action value in the inputted row,
        it can move the telescope to specificed (ra, dec) coordinates, and the filter wheel to the specified
        filter. It also creates a directory for the sequence images and writes a header with relevant information.

        Parameters:
            action (Action): An Action object containing information about the action to be performed.
            paired_devices (dict): A list of paired devices required for the sequence.
        """
        if not isinstance(paired_devices, PairedDevices):
            paired_devices = PairedDevices.from_observatory(
                observatory=self,
                paired_device_names=paired_devices,
            )

        self.logger.debug(f"Running pre_sequence for {action.summary_string()}")

        # prepare observatory for sequence
        self.setup_observatory(paired_devices, action.action_value)

        # Create image handler
        self.setup_image_handler(action=action, paired_devices=paired_devices)

        self.logger.debug(f"Finished pre_sequence for {action.summary_string()}")

    def setup_image_handler(self, action, paired_devices):
        try:
            camera_name = action.device_name
            image_handler = ImageHandler.from_action(
                action=action,
                paired_devices=paired_devices,
                logger=self.logger,
                observatory_config=self.config,
                fits_config=self.fits_config,
            )
            self._image_handlers[camera_name] = image_handler
            self.logger.debug(f"Created image handler for camera '{camera_name}'")
        except Exception as e:
            self.logger.report_device_issue(
                device_type="ImageHandler",
                device_name=action.device_name,
                message="Error creating image handler",
                exception=e,
            )
            raise e

    def setup_observatory(
        self,
        paired_devices: dict | PairedDevices,
        action_value: dict,
        filter_list_index: int = 0,
    ) -> None:
        """
        Prepares the observatory for a sequence by performing necessary setup actions.

        Parameters:
            paired_devices (dict): A dictionary specifying paired devices for the sequence.
            action_value (dict): A dictionary containing information about the action to be performed.
            filter_list_index (int, optional): The index of the filter in the filter list (default is 0).

        This method prepares the observatory for a sequence by performing the following steps:

        If the action value contains 'ra' and 'dec' keys, it will:
            1. open_observatory(paired_devices)
            2. Set telescope tracking to true
            3. Slew telescope to the specified target coordinates.

        If the action value contains 'filter' key, it will:
            1. Setting the filter wheel to the specified filter position.

        Notes:
            - This method relies on certain conditions like weather safety, error-free operation, and no interruptions.
            - The 'paired_devices' dictionary should specify devices required for the sequence.

        """

        self.logger.debug(
            f"Running setup_observatory for {paired_devices} {action_value}"
        )

        if not isinstance(paired_devices, PairedDevices):
            paired_devices = PairedDevices.from_observatory(
                observatory=self,
                paired_device_names=paired_devices,
            )

        # unpark and slew to target
        if (
            (action_value.get("ra", None) is not None)
            and (action_value.get("dec", None) is not None)
            and (action_value.get("disable_telescope_movement", False) is False)
            and self.check_conditions()
        ):
            if "Telescope" in paired_devices:
                # open dome and unpark telescope -- this will open all domes if not in paired_devices...?
                self.open_observatory(paired_devices)

                telescope = paired_devices.telescope
                telescope_name = paired_devices["Telescope"]

                if self.check_conditions():
                    # set tracking to true
                    self.execute_and_monitor_device_task(
                        "Telescope",
                        "Tracking",
                        True,
                        "Tracking",
                        device_name=telescope_name,
                        log_message=f"Setting Telescope {telescope_name} tracking to True",
                    )

                    # slew to target
                    self.logger.info(
                        f"Slewing Telescope {telescope_name} to {action_value['ra']} {action_value['dec']}"
                    )
                    telescope.get(
                        "SlewToCoordinatesAsync",
                        RightAscension=24 * action_value["ra"] / 360,
                        Declination=action_value["dec"],
                    )

                    time.sleep(1)

                    # wait for slew to finish
                    self.wait_for_slew(paired_devices)

        # set filter
        if (
            (action_value.get("filter", None) is not None)
            and "FilterWheel" in paired_devices
            and self.logger.error_free
        ):
            # get filter name
            f = action_value["filter"]
            if isinstance(f, list):
                f = f[filter_list_index]

            filter_wheel = paired_devices.filter_wheel
            names = filter_wheel.get("Names")

            # find index of filter name
            if f in names:
                filter_index = [i for i, d in enumerate(names) if d == f][0]
            else:
                raise ValueError(f"Filter {f} not found in {names}")

            filter_wheel_name = paired_devices["FilterWheel"]
            # set filter
            self.execute_and_monitor_device_task(
                "FilterWheel",
                "Position",
                filter_index,
                "Position",
                device_name=filter_wheel_name,
                log_message=f"Setting FilterWheel {filter_wheel_name} to {f}",
                weather_sensitive=False,
            )
            filter_focus_shift = filter_wheel.get("FocusOffsets")[filter_index]
        else:
            filter_focus_shift = 0

        if (
            (
                (action_value.get("focus_shift", None) is not None)
                or (action_value.get("focus_position", None) is not None)
                or (filter_focus_shift is not None)
            )
            and ("Focuser" in paired_devices)
            and self.logger.error_free
        ):
            defocuser = Defocuser(
                observatory=self,
                paired_devices=paired_devices,
            )

            if action_value.get("focus_position", None) is not None:
                new_focus_position = action_value["focus_position"]
            elif action_value.get("focus_shift", None) is not None:
                new_focus_position = (
                    defocuser.best_focus_position + action_value["focus_shift"]
                )

            else:
                new_focus_position = defocuser.best_focus_position

            new_focus_position += filter_focus_shift

            defocuser.defocus(new_focus_position)
        elif "Focuser" in paired_devices:
            # Move focuser to best focus position
            defocuser = Defocuser(
                observatory=self,
                paired_devices=paired_devices,
            )
            defocuser.refocus()

        if "Camera" in paired_devices:
            camera = paired_devices.camera
            bin = action_value.get("bin", 1)

            self.logger.info(
                f"Setting Camera {paired_devices['Camera']} binning to {bin}x{bin}"
            )

            camera.set("BinX", bin)
            camera.set("BinY", bin)
            camera.set("NumX", camera.get("CameraXSize") // camera.get("BinX"))
            camera.set("NumY", camera.get("CameraYSize") // camera.get("BinY"))

    def wait_for_slew(self, paired_devices: PairedDevices) -> None:
        """
        Wait for telescope slewing operation to complete.

        Monitors the telescope's slewing status and blocks until the slew
        operation is finished. Includes safety condition checking and
        timeout protection to prevent infinite waiting.

        Parameters:
            paired_devices (PairedDevices): Object containing the telescope
                device to monitor for slewing completion.

        Safety Features:
        - Continuous condition checking during wait (weather, errors, schedule)
        - Automatic timeout protection (prevents infinite loops)
        - 1-second settle time after slew completion

        Process:
        1. Checks initial slewing status
        2. Logs slewing start if telescope is moving
        3. Polls slewing status with safety condition checks
        4. Waits for slewing to complete
        5. Adds settle time for mechanical stabilization

        Note:
            - Critical for ensuring telescope positioning accuracy
            - Safety conditions are checked continuously during wait
            - Settle time allows for mechanical vibrations to dampen
        """

        telescope = paired_devices.telescope

        # wait for slew to finish
        start_time = time.time()

        slewing = telescope.get("Slewing")

        if slewing is True:
            self.logger.info(f"Telescope {paired_devices['Telescope']} slewing...")

        while slewing is True and self.check_conditions():
            if time.time() - start_time > 120:  # 2 minutes hardcoded limit
                raise TimeoutError("Slew timeout")

            time.sleep(0.1)

            slewing = telescope.get("Slewing")

        # slew settle time (guess)
        time.sleep(1)

    def check_conditions(self, action: Action | None = None) -> bool:
        """
        Check if current conditions allow safe execution of observatory operations.

        Evaluates multiple safety and operational conditions to determine if
        an action or sequence can proceed safely. Different action types have
        different condition requirements based on their weather sensitivity.

        Parameters:
            action (Action, optional): The action object containing action_type,
                start_time, and end_time. If None, only base conditions are checked.
                Defaults to None.

        Returns:
            bool: True if all required conditions are met for the operation,
                False if any condition fails.

        Base Conditions (always checked):
        - error_free: No system errors present
        - schedule_running: Schedule execution is active
        - watchdog_running: Safety monitoring is active

        Action-Specific Conditions:
        - Weather-sensitive actions (open, object, flats, autofocus, calibrate_guiding,
          pointing_model): Also require weather_safe
        - Weather-independent actions (calibration, close, cool_camera,
          complete_headers): Only require base conditions
        - Time-sensitive actions: Must be within scheduled start/end time window

        Note:
            - Used throughout the system for safety checks
            - Different actions have different safety requirements
            - Critical for preventing unsafe operations during bad weather
        """
        if self.weather_safe is None:
            self.logger.error(
                "Weather safety not yet determined, cannot proceed with action."
                "This should not happen."
            )
            return False

        base_conditions = (
            self.logger.error_free
            and self.schedule_manager.running
            and self.watchdog_running
        )

        if action is None:
            return base_conditions and self.weather_safe

        time_conditions = action.start_time <= datetime.now(UTC) <= action.end_time

        if action.action_type in [
            "open",
            "object",
            "flats",
            "autofocus",
            "calibrate_guiding",
            "pointing_model",
        ]:
            return base_conditions and time_conditions and self.weather_safe
        elif action.action_type in [
            "calibration",
            "close",
            "cool_camera",
            "complete_headers",
        ]:
            return base_conditions and time_conditions
        else:
            return False

    def perform_exposure(
        self,
        camera: AlpacaDevice,
        exptime,
        maxadu,
        action: Action,
        use_light=True,
        log_option=None,
        maximal_sleep_time=0.1,
        sequence_counter: int = 0,
        wcs=None,
    ) -> tuple[bool, Path | None]:
        """
        Execute a camera exposure and handle image acquisition.

        Performs a complete camera exposure sequence including exposure start,
        monitoring, and image saving. Handles both light and dark frames with
        appropriate header information and file management.

        Parameters:
            camera (AlpacaDevice): The camera device to use for exposure.
            exptime (float): Exposure time in seconds.
            maxadu (int): Maximum ADU value for the camera.
            action (Action): The action object containing action_type and device_name.
            use_light (bool, optional): Whether to use light during exposure.
                False for dark frames. Defaults to True.
            log_option (str, optional): Additional text for logging messages.
                Defaults to None.
            maximal_sleep_time (float, optional): Maximum sleep interval during
                image ready polling. Defaults to 0.1 seconds.
            sequence_counter (int, optional): Sequence number for the image
                in a series. Defaults to 0.
            wcs (WCS, optional): World Coordinate System solution for the image.
                Defaults to None.

        Returns:
            tuple: A tuple containing:
                - bool: True if exposure was successful, False otherwise
                - Path or None: Path to saved image file, None if failed

        Note:
            - Monitors conditions continuously during exposure
            - Handles exposure timing and image readiness polling
            - Automatically saves image with appropriate filename and headers
            - Sets proper IMAGETYP header based on action type and use_light
        """
        # TODO consider waiting dynamically
        # def wait_for_image_ready(exptime):
        # """"
        # Dynamical alternative to time.sleep(min(maximal_sleep_time, exptime / 10))
        # """"
        #     start_time_waiting = time.time()

        #     while not camera.get('ImageReady') and self.check_conditions(row):
        #         elapsed_time = time.time() - start_time_waiting

        #         if elapsed_time/exptime > 0.9:
        #             time.sleep(0.01)
        #         else:
        #             time.sleep(min(0.5, exptime*0.9/2))

        # Yield to other threads
        time.sleep(0)

        # Get the image handler for this specific camera
        camera_name = camera.device_name
        image_handler = self.get_image_handler(camera_name)

        image_handler.header["EXPTIME"] = exptime
        use_light = image_handler.header.set_imagetype(
            action_type=action.action_type, use_light=use_light
        )
        image_handler.header.set_action_type(action)

        # Log information about the exposure
        log_option_tmp = "" if log_option is None else f"{log_option} "
        self.logger.info(
            f"Exposing {log_option_tmp}{action.device_name} "
            f"{image_handler.header['IMAGETYP']} "
            f"for exposure time {image_handler.header['EXPTIME']:.3f} s "
            f"from {image_handler.header['ASTRATYP']} sequence."
        )

        # Start exposure
        exposure_start_time = time.time()
        exposure_end_time = time.time()
        camera.get("StartExposure", Duration=exptime, Light=use_light)

        # Wait for the image to be ready
        exposure_successful = True

        while not camera.get("ImageReady"):
            if not self.check_conditions(action):
                exposure_successful = False
                break

            try:
                if (exposure_end_time - exposure_start_time) > 3 * exptime + 180:
                    exposure_successful = False
                    raise TimeoutError("Exposure timeout")
            except TimeoutError as e:
                self.logger.report_device_issue(
                    device_type="Camera",
                    device_name=action.device_name,
                    message=f"Exposure timed out after 3*{exptime:.3f} + 180 seconds "
                    + f"for {action.device_name}.",
                    exception=e,
                )

            time.sleep(min(maximal_sleep_time, exptime / 10))
            exposure_end_time = time.time()

        if not exposure_successful:
            self.logger.warning("Last exposure was not completed successfully.")
            filepath = None
            # if error_free is True, abort exposure
            if self.logger.error_free:
                camera.get("AbortExposure")()  # check
        else:
            # get last exposure information
            exposure_start_datetime = pd.to_datetime(
                camera.get("LastExposureStartTime")
            )

            # get image array and info
            image = camera.get("ImageArray")
            image_info = camera.get("ImageArrayInfo")

            filepath = image_handler.save_image(
                image=image,
                image_info=image_info,
                maxadu=maxadu,
                device_name=camera.device_name,
                exposure_start_datetime=exposure_start_datetime,
                wcs=wcs,
                sequence_counter=sequence_counter,
            )

            self.logger.info(
                f"Image saved as {os.path.basename(filepath)}. "
                f"Acquired in {(time.time() - exposure_end_time):.3f} s after ImageReady was True, "
                f"and {(time.time() - exposure_start_time - exptime):.3f} s after exposure integration should have ended."
            )

            self.database_manager.execute(
                f"INSERT INTO images VALUES ('{filepath}', '{camera.device_name}', "
                f"'{0}', '{exposure_start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}')"
            )

        return exposure_successful, filepath

    def image_sequence(self, action: Action, paired_devices: PairedDevices) -> None:
        """
        Execute a sequence of astronomical images with a camera.

        Runs a complete imaging sequence including observatory setup, multiple
        exposures with various exposure times, pointing correction, and optional
        guiding. Handles both object imaging and calibration sequences.

        Parameters:
            row (dict): Schedule row containing sequence information including:
                - device_name: Camera device to use
                - action_type: Type of sequence ('object' or 'calibration')
                - start_time/end_time: Sequence timing constraints
                - action_value: Sequence parameters (exposure times, filters, etc.)
            paired_devices (PairedDevices): Object containing all devices needed
                for the sequence (camera, telescope, filter wheel, etc.)

        Sequence Features:
        - Pre-sequence setup (telescope pointing, filter selection)
        - Multiple exposure time support
        - Automatic pointing correction for object sequences
        - Optional autoguiding activation and management
        - Continuous condition monitoring throughout sequence

        Process Flow:
        1. Pre-sequence setup (telescope pointing, filters, headers)
        2. Iterate through exposure time list
        3. Perform pointing correction (object sequences only)
        4. Start guiding if configured
        5. Execute exposures with safety monitoring
        6. Stop guiding and telescope tracking at completion

        Note:
            - Supports both single and multiple exposure times
            - Automatically handles different sequence types
            - Coordinates telescope, camera, and filter wheel operations
            - Essential for all astronomical imaging operations
        """

        self.logger.info(
            f"Running {action.action_type} sequence for {action.device_name}, "
            f"starting {action.start_time} and ending {action.end_time}"
        )

        self.pre_sequence(action, paired_devices)
        action_value = action.action_value

        camera = paired_devices.camera
        maxadu = camera.get("MaxADU")

        if action.action_type == "calibration":
            exptime_list = action_value["exptime"]
            n_exposures_list = action_value["n"]
        else:
            exptime_list = [action_value["exptime"]]
            if action_value.get("n", None):
                n_exposures_list = [int(action_value["n"])]
            else:
                n_exposures_list = [
                    int(1e6)
                ]  # hacky  # TODO make this part of action_config defaults

        pointing_complete = False
        pointing_attempts = 0
        guiding = False
        wcs_solve = None

        for i, exptime in enumerate(exptime_list):
            if not self.check_conditions(action):
                break

            n_exposures = n_exposures_list[i]

            for exposure in range(n_exposures):
                if action_value.get("n", None):
                    log_option = f"{exposure + 1}/{n_exposures}"
                else:
                    log_option = None

                if not self.check_conditions(action):
                    break

                success, filepath = self.perform_exposure(
                    camera,
                    exptime=exptime,
                    maxadu=maxadu,
                    action=action,
                    log_option=log_option,
                    wcs=wcs_solve,
                    sequence_counter=i,
                )

                if not success:
                    break

                # pointing correction if not already done
                if action_value.get("pointing") and pointing_complete is False:
                    if filepath is None:
                        self.logger.error(
                            "No image file path returned from exposure, "
                            "cannot do pointing correction"
                        )
                        break
                    pointing_complete, wcs_solve = self.pointing_correction(
                        action,
                        filepath,
                        paired_devices,
                        sync=False,
                        slew=True,
                    )

                    if self.speculoos:
                        time.sleep(exptime * 3)  # for spirit

                    pointing_attempts += 1

                    if wcs_solve is not None:
                        with fits.open(filepath, mode="update") as hdul:
                            hdul[0].header.update(wcs_solve.to_header())  # type: ignore
                            hdul.flush()

                    if pointing_complete is False:
                        wcs_solve = (
                            None  # to not contaminate the next image if pointing fails
                        )

                    if pointing_attempts > 3 and pointing_complete is False:
                        self.logger.warning(
                            f"Pointing correction for {action_value['object']} with "
                            f"{action.device_name} failed after {pointing_attempts} attempts"
                        )
                        pointing_complete = True
                else:
                    pointing_complete = True

                # initialise guiding once pointing correction is complete
                if (
                    action_value.get("guiding")
                    and guiding is False
                    and pointing_complete is True
                ):
                    guiding = self.guider_manager.start_guider(
                        image_handler=self.get_image_handler(camera.device_name),
                        paired_devices=paired_devices,
                        thread_manager=self.thread_manager,
                    )

        # stop guiding at end of sequence
        if action_value.get("guiding", False):
            self.guider_manager.stop_guider(
                paired_devices["Telescope"], thread_manager=self.thread_manager
            )

        # stop telescope tracking at end of sequence
        if "Telescope" in paired_devices:
            self.execute_and_monitor_device_task(
                "Telescope",
                "Tracking",
                False,
                "Tracking",
                device_name=paired_devices["Telescope"],
                log_message=f"Stopping telescope {paired_devices['Telescope']} tracking",
            )

        # Clean up the image handler for this camera
        camera_name = camera.device_name
        if camera_name in self._image_handlers:
            del self._image_handlers[camera_name]
            self.logger.debug(f"Cleaned up image handler for camera '{camera_name}'")

    def pointing_model_sequence(
        self, action: Action, paired_devices: PairedDevices
    ) -> None:
        """
        Execute a pointing model sequence to improve telescope pointing accuracy.

        Generates a systematic series of sky positions and captures images at each
        location to build or refine a telescope pointing model. The sequence creates
        a spiral pattern of points from zenith down to a specified altitude, avoiding
        positions too close to the Moon.

        Parameters:
            action (Action): Schedule action containing sequence information.
            paired_devices (dict): Dictionary of paired devices for the sequence,
                including telescope and camera.

        Action Value Parameters (from row['action_value']):
            - 'n' (int, optional): Number of pointing positions. Defaults to 20.
            - 'exptime' (float, optional): Exposure time in seconds. Defaults to 1.
            - Additional standard action parameters (ra, dec, etc.)

        Process:
        1. Creates pointing_model directory for image storage
        2. Generates spiral pattern of sky coordinates from zenith
        3. For each position (if not too close to Moon):
           - Slews telescope to target coordinates
           - Takes exposure with specified parameters
           - Performs pointing correction to measure error
           - Updates FITS header with correction information
        4. Continues until all positions are captured or conditions change

        Safety Features:
        - Continuous condition checking during sequence
        - Moon avoidance for accurate measurements
        - Error handling for individual pointing failures

        Note:
            - Critical for maintaining high telescope pointing accuracy
            - Results improve automated observation precision
            - Typically run during commissioning or maintenance periods
        """

        self.logger.info(action.summary_string(verbose=True))

        self.pre_sequence(action, paired_devices)
        action_value = action.action_value

        # number of points
        N = action_value.get("n", 100)

        # set exptime to 1 if not specified
        exptime = action_value.get("exptime", 1)  # default to 1 second

        # get camera
        camera = self.devices["Camera"][action.device_name]
        maxadu = camera.get("MaxADU")

        # Get the image handler for this camera
        image_handler = self.get_image_handler(camera.device_name)

        # find dark frame
        dark_frame = None
        if action_value.get("dark_subtraction", False):
            self.logger.info(
                f"Dark subtraction enabled. Looking for matching dark frame for {action.device_name}"
                f" with exposure time {exptime} s in {image_handler.image_directory}"
            )
            # TODO is this also just the last image?
            darks = list(
                Path(image_handler.image_directory).glob(
                    f"*Dark Frame_{exptime:.3f}*.fits"
                )
            )

            if len(darks) > 0:
                dark_frame = darks[-1]
                self.logger.info(f"Using dark frame {dark_frame}")
            else:
                self.logger.warning(
                    f"No dark frame found in {image_handler.image_directory}. Dark subtraction will not be applied."
                )

        # get location
        obs_location = image_handler.get_observatory_location()
        MOON_LIMIT = u.Quantity(20, u.deg)  # pointing distance to the moon in degrees

        # create pointing_model folder
        date_str = (
            action.start_time + timedelta(hours=obs_location.lon.deg / 15)
        ).strftime("%Y%m%d")
        folder = Config().paths.images / "pointing_model" / date_str
        folder.mkdir(exist_ok=True)
        self.logger.info(f"{folder} created for pointing model images")

        # Generate points (spiral from zenith to 30 deg above horizon)
        num_turns = np.sqrt(N / 2)
        t_linear = np.linspace(0, 1, N)  # Generate base points
        ts = t_linear**0.5  # increase spacing towards zenith
        t_shift = 0

        # update header
        image_handler.header["IMAGETYP"] = "Light Frame"
        image_handler.header["OBJECT"] = "Pointing Model"
        action_value["object"] = "Pointing Model"

        # open dome and unpark telescope
        self.open_observatory(paired_devices)

        counter = 0
        while counter < N and self.check_conditions(action):
            t = ts[counter] + t_shift
            # 30 degree horizon limit, and 85 degree zenith limit (telescope weird at exactly 90)
            alt = 85 - (55 * t)
            if alt < 30:
                alt = 30
            az = (360 * num_turns * t) % 360

            # Get current moon position
            observing_time = Time(datetime.now(UTC))
            moon_altaz = get_body(
                "moon", observing_time
            ).transform_to(  # Alternative method
                AltAz(obstime=observing_time, location=obs_location)
            )

            # Convert coordinates to equatorial
            target_altaz = SkyCoord(
                alt=u.Quantity(alt, u.deg),
                az=u.Quantity(az, u.deg),
                frame=AltAz(obstime=observing_time, location=obs_location),
            )
            target_radec = target_altaz.transform_to("icrs")

            # Calculate separation from moon
            moon_separation = moon_altaz.separation(target_altaz)

            if moon_separation <= MOON_LIMIT:
                # iteratively shift point if too close to moon
                t_shift += 0.01
                continue
            else:
                t_shift = 0

            self.logger.info(f"Running pointing model point {counter + 1}/{N}")

            # move telescope to target
            action_value["ra"] = target_radec.ra.deg  # type: ignore
            action_value["dec"] = target_radec.dec.deg  # type: ignore
            self.setup_observatory(paired_devices, action_value)

            if self.speculoos:
                # wait for telescope to settle
                time.sleep(exptime * 3)  # for spirit

            # perform exposure
            success, filepath = self.perform_exposure(
                camera,
                exptime,
                maxadu,
                action,
                sequence_counter=counter,
                log_option=None,
            )

            if not success:
                break

            # pointing correction, sync and no slew
            pointing_complete, wcs_solve = self.pointing_correction(
                action,
                filepath,
                paired_devices,
                dark_frame=dark_frame,
                sync=True,
                slew=False,
            )

            # update header with wcs
            if wcs_solve is not None:
                with fits.open(filepath, mode="update") as hdul:
                    hdul[0].header.update(wcs_solve.to_header())  #  type: ignore
                    hdul.flush()

            wcs_solve = None

            counter += 1

        # Clean up the image handler for this camera
        camera_name = camera.device_name
        if camera_name in self._image_handlers:
            del self._image_handlers[camera_name]
            self.logger.debug(f"Cleaned up image handler for camera '{camera_name}'")

    def pointing_correction(
        self,
        action: Action,
        filepath: str | Path | None,
        paired_devices: PairedDevices,
        dark_frame: str | Path | None = None,
        sync: bool = False,
        slew: bool = True,
    ) -> tuple[bool, WCS | None]:
        """
        Perform telescope pointing correction based on an acquired image.

        Uses plate solving to determine the actual pointing position from a captured
        image and corrects the telescope pointing if the error exceeds the configured
        threshold. Supports both sync-only and slew corrections.

        Parameters:
            action (Action): The action object containing action_type,
                start_time, end_time, and action_value with target coordinates.
            filepath (str | Path): Path to the FITS image file for plate solving.
            paired_devices (PairedDevices): Object containing telescope and other
                devices for the correction.
            dark_frame (str | Path | None, optional): Path to a dark frame for calibration.
                Defaults to None (no dark subtraction).
            sync (bool, optional): If True, only sync the telescope without slewing.
                Defaults to False.
            slew (bool, optional): If True, allows slewing to correct large errors.
                If False, sets pointing threshold to 0. Defaults to True.

        Returns:
            tuple: A tuple containing:
                - bool: True if pointing correction completed successfully
                - WCS or None: World Coordinate System object if plate solve succeeded,
                  None if failed

        Process:
        1. Performs plate solving on the provided image
        2. Calculates pointing error relative to target coordinates
        3. Compares error to configured pointing threshold
        4. Executes sync or slew correction based on error magnitude
        5. Logs correction details and results

        Note:
            - Uses PointingCorrectionHandler for plate solving and correction calculations
            - Pointing threshold is configurable per telescope in the configuration
            - Falls back gracefully if plate solving fails
        """
        action_value = action.action_value
        self.logger.info(
            f"Running pointing correction for {action_value['object']} with {action.device_name}"
        )
        try:
            pointing_corrector_handler = PointingCorrectionHandler.from_fits_file(
                filepath,
                dark_frame=dark_frame,
                target_ra=action_value["ra"],
                target_dec=action_value["dec"],
            )
            pointing_correction = pointing_corrector_handler.pointing_correction

            number_of_stars_to_use = min(
                len(pointing_corrector_handler.image_star_mapping.stars_in_image), 16
            )

            self.logger.info(
                f"Plate solve succeeded using {number_of_stars_to_use} identified stars and "
                f"{len(pointing_corrector_handler.image_star_mapping.gaia_stars_in_image)} catalog stars. "
            )

        except Exception as e:
            self.logger.warning(
                f"Failed running pointing correction for {action_value['object']}"
                f" with {action.device_name}: {str(e)}",
                exc_info=True,
            )
            pointing_complete = True
            return (pointing_complete, None)

        # get telescope index
        # convert to degrees
        pointing_threshold = (
            paired_devices.get_device_config("Telescope")["pointing_threshold"] / 60
        )

        if slew is False:
            pointing_threshold = 0

        angular_separation = pointing_correction.angular_separation
        if abs(angular_separation) < pointing_threshold:
            self.logger.info(
                f"No further pointing correction required. "
                f"Correction of {angular_separation * 60:.2f}' "
                f"within threshold of {pointing_threshold * 60:.2f}'"
            )
            pointing_complete = True

            return (
                pointing_complete,
                pointing_corrector_handler.image_star_mapping.wcs,
            )

        self.logger.info(
            f"Pointing correction of {angular_separation * 60:.2f}' "
            f"required as it is outside threshold of {pointing_threshold * 60:.2f}'"
        )
        self.logger.info(f"RA shift: {pointing_correction.offset_ra * 60:.2f}'")
        self.logger.info(f"DEC shift: {pointing_correction.offset_dec * 60:.2f}'")

        pointing_complete = False

        # telescope
        telescope = paired_devices.telescope

        if sync:
            telescope.get(
                "SyncToCoordinates",
                RightAscension=24
                * (action_value["ra"] + pointing_correction.offset_ra)
                / 360,
                Declination=action_value["dec"] + pointing_correction.offset_dec,
            )

            if slew:
                # re-slew to target
                self.setup_observatory(paired_devices, action_value)
        else:
            # new_ra = action_value["ra"] - (real_center.ra - action_value["ra"])
            new_ra = pointing_correction.proxy_ra
            new_dec = pointing_correction.proxy_dec

            if slew:
                # slew to target
                self.logger.info(
                    f"Slewing Telescope {paired_devices['Telescope']} to corrected position: {new_ra} {new_dec}"
                )
                telescope.get(
                    "SlewToCoordinatesAsync",
                    RightAscension=24 * new_ra / 360,
                    Declination=new_dec,
                )

                time.sleep(1)

                # wait for slew to finish
                self.wait_for_slew(paired_devices)

        return (pointing_complete, pointing_corrector_handler.image_star_mapping.wcs)

    def guiding_calibration_sequence(
        self, action: Action, paired_devices: PairedDevices
    ) -> bool:
        """
        Perform autoguiding calibration to establish guide star movements.

        Executes a calibration sequence that maps the relationship between guide
        commands and resulting star movements on the guide camera. This calibration
        is essential for accurate autoguiding performance.

        Parameters:
            action (Action): Schedule action containing guiding calibration information.
            paired_devices (PairedDevices): Object containing telescope, guide camera,
                and other devices required for calibration.

        Returns:
            bool: True if calibration completed successfully, False if failed
                or conditions became unsafe.

        Process:
        1. Prepares observatory and creates calibration metadata
        2. Checks safety conditions before starting
        3. Executes guiding calibration using GuidingCalibrator
        4. Measures guide star response to directional commands
        5. Calculates calibration parameters for future guiding
        6. Returns success status

        Safety Features:
        - Continuous condition checking during calibration
        - Graceful handling of calibration failures
        - Proper error logging and reporting

        Note:
            - Required before autoguiding can be used effectively
            - Calibration parameters are device and mount specific
            - Should be performed when guiding setup changes
        """
        self.logger.info(f"Running guiding calibration for {action.device_name}")
        try:
            self.pre_sequence(action, paired_devices)
            if not self.check_conditions(action=action):
                return False

            guiding_calibrator = GuidingCalibrator(
                astra_observatory=self,
                action=action,
                paired_devices=paired_devices,
                image_handler=self.get_image_handler(action.device_name),
            )
            guiding_calibrator.slew_telescope_one_hour_east_of_sidereal_meridian()
            guiding_calibrator.perform_calibration_cycles()
            guiding_calibrator.complete_calibration_config()
            guiding_calibrator.save_calibration_config()
            guiding_calibrator.update_observatory_config()

            self.logger.info(f"Guiding calibration for {action.device_name} completed")
            success = True

        except Exception as e:
            success = False
            self.logger.report_device_issue(
                device_type="Camera",
                device_name=action.device_name,
                message=f"Error running guiding calibration for {action.device_name}",
                exception=e,
            )
        finally:
            # Clean up the image handler for this camera
            camera_name = action.device_name
            if camera_name in self._image_handlers:
                del self._image_handlers[camera_name]
                self.logger.debug(
                    f"Cleaned up image handler for camera '{camera_name}'"
                )

        return success

    def autofocus_sequence(self, action: Action, paired_devices: PairedDevices) -> bool:
        """
        Perform autofocus sequence to achieve optimal telescope focus.

        Executes an automated focusing routine that systematically tests different
        focus positions to find the optimal focus setting. Uses star analysis
        to measure focus quality and determine the best focus position.

        Parameters:
            action (Action): Schedule action containing autofocus sequence information.
            paired_devices (PairedDevices): Object containing telescope, camera,
                focuser, and other devices required for autofocus.

        Returns:
            bool: True if autofocus completed successfully and achieved good focus,
                False if failed or conditions became unsafe.

        Process:
        1. Prepares observatory and creates autofocus metadata
        2. Checks safety conditions before starting
        3. Executes autofocus routine using appropriate algorithm
        4. Takes test exposures at different focus positions
        5. Analyzes star quality metrics (FWHM, HFD, etc.)
        6. Determines and sets optimal focus position
        7. Returns success status

        Focus Methods:
        - Uses Autofocuser or Defocuser classes for focus optimization
        - Supports different focus algorithms (curve fitting, star analysis)
        - Handles both coarse and fine focus adjustments

        Safety Features:
        - Continuous condition checking during focus sequence
        - Graceful handling of focus failures
        - Proper error logging and reporting

        Note:
            - Critical for achieving optimal image quality
            - Should be performed regularly or when focus changes
            - Temperature changes often require refocusing
        """
        self.logger.info(f"Running autofocus for {action.device_name}")
        try:
            self.pre_sequence(action, paired_devices)
            if not self.check_conditions(action=action):
                return False

            autofocuser = Autofocuser(
                observatory=self,
                action=action,
                paired_devices=paired_devices,
            )
            autofocuser.determine_autofocus_calibration_field()
            autofocuser.slew_to_calibration_field()
            autofocuser.setup()

            success = autofocuser.run()
            if success:
                autofocuser.make_summary_plot()
                autofocuser.create_result_file()
                autofocuser.save_best_focus_position()

        except Exception as e:
            self.logger.report_device_issue(
                device_type="Camera",
                device_name=action.device_name,
                message=f"Error running autofocus for {action.device_name}",
                exception=e,
            )
        finally:
            # Clean up the image handler for this camera
            camera_name = action.device_name
            if camera_name in self._image_handlers:
                del self._image_handlers[camera_name]
                self.logger.debug(
                    f"Cleaned up image handler for camera '{camera_name}'"
                )

        return success

    def flats_sequence(self, action: Action, paired_devices: PairedDevices) -> None:
        """
        Execute a flat field calibration sequence during twilight.

        Captures flat field images during astronomical twilight when the sky
        provides near-uniform illumination. Automatically manages telescope positioning,
        exposure timing, and filter changes to create comprehensive flat field
        libraries for image calibration.

        Parameters:
            action (Action): Schedule action containing flats sequence information.
            paired_devices (PairedDevices): Object containing all devices needed
                for the sequence (camera, telescope, filter wheel, etc.)


        Process:
        1. Monitors sun altitude for optimal flat field conditions
        2. Positions telescope for uniform sky illumination
        3. Calculates optimal exposure times for target ADU levels
        4. Captures flat frames with consistent brightness
        5. Iterates through multiple filters if specified
        6. Handles exposure time adjustments as sky brightness changes

        Timing Considerations:
        - Only operates during narrow twilight windows
        - Monitors sun elevation for optimal conditions
        - Automatically adjusts for changing sky brightness
        - Stops when conditions become unsuitable

        Safety Features:
        - Continuous sky brightness monitoring
        - Automatic exposure time calculation
        - Weather and condition checking
        - Graceful handling of changing conditions

        Note:
            - Critical for high-quality photometric calibration
            - Timing is crucial - operates only during twilight
            - Results improve scientific data quality significantly
        """
        # TODO make action logging more uniform
        self.logger.info(
            f"Running flats sequence for {action.device_name}, "
            f"starting {action.start_time} and ending {action.end_time}"
        )

        # creates folder for images, writes base header, and sets filter to first filter in list
        self.pre_sequence(action, paired_devices)

        # target adu and camera offset needed for flat exposure time calculation
        camera_config = paired_devices.get_device_config("Camera")

        config_target_adu = camera_config["flats"]["target_adu"]
        config_target_adu_tolerance = camera_config["flats"].get(
            "target_adu_tolerance", config_target_adu * 0.2
        )
        target_adu = [config_target_adu, config_target_adu_tolerance]
        offset = camera_config["flats"]["bias_offset"]
        lower_exptime_limit = camera_config["flats"]["lower_exptime_limit"]
        upper_exptime_limit = camera_config["flats"]["upper_exptime_limit"]

        # get location to determine if sun is up
        obs_location = self.get_image_handler(
            action.device_name
        ).get_observatory_location()

        # wait for sun to be in right position
        sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)
        self.logger.info(
            f"Sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
        )

        if self.check_conditions(action) and (take_flats is False):
            self.logger.info(
                f"Not the right time to take flats for {action.device_name}, sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
            )

            # calculate time until sun is in right position of between -1 and -12 degrees altitude
            if sun_rising:
                # angle between sun_altaz.alt.degree and -12
                angle = -12 - sun_altaz.alt.degree
            else:
                # angle between sun_altaz.alt.degree and -1
                angle = sun_altaz.alt.degree + 1

            # time until sun is in right position
            time_to_wait = angle / 0.25  # 0.25 degrees per minute

            if time_to_wait < 0:
                time_to_wait = 24 * 60 + time_to_wait

            self.logger.info(
                f"Waiting min. {time_to_wait:.2f} minutes for sun to be "
                f"in right position for {action.device_name}"
            )

        while self.check_conditions(action) and (take_flats is False):
            sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

            if take_flats is False:
                time.sleep(1)

        camera = paired_devices.camera
        maxadu = camera.get("MaxADU")

        # camera orignal framing  # TODO delete?
        # numx = camera.get("NumX")
        # numy = camera.get("NumY")
        # startx = camera.get("StartX")
        # starty = camera.get("StartY")

        # start taking flats
        for i, filter_name in enumerate(action.action_value["filter"]):
            count = 0
            sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

            if self.check_conditions(action) and take_flats:
                ## initial setup + exposure setting
                # sets filter (and focus, soon...)
                self.setup_observatory(
                    paired_devices, action.action_value, filter_list_index=i
                )

                # opens dome and move telescope to flat position
                self.flats_position(obs_location, paired_devices, action)

                # establishing initial exposure time
                exptime = self.flats_exptime(
                    obs_location,
                    paired_devices,
                    action,
                    # numx,
                    # numy,
                    # startx,
                    # starty,
                    target_adu,
                    offset,
                    lower_exptime_limit,
                    upper_exptime_limit,
                )

                if exptime < lower_exptime_limit or exptime > upper_exptime_limit:
                    self.logger.info("Moving on...")
                    continue

                # Get the image handler for this camera
                image_handler = self.get_image_handler(camera.device_name)
                image_handler.header["EXPTIME"] = exptime
                image_handler.header["FILTER"] = filter_name

                while self.check_conditions(action) and (
                    count < action.action_value["n"][i]
                ):
                    log_option = f"{count + 1}/{action.action_value['n'][i]}"

                    success, filepath = self.perform_exposure(
                        camera,
                        exptime,
                        maxadu,
                        action=action,
                        sequence_counter=count,
                        log_option=log_option,
                    )

                    if not success:
                        break
                    else:
                        # move telescope to flat position
                        self.flats_position(obs_location, paired_devices, action)

                        with fits.open(filepath) as hdul:
                            data = hdul[0].data  # type: ignore
                            median_adu = np.nanmedian(data)
                            fraction = (median_adu - offset) / (target_adu[0] - offset)

                            if (
                                math.isclose(
                                    target_adu[0],
                                    median_adu,
                                    rel_tol=0,
                                    abs_tol=target_adu[1],
                                )
                                is False
                            ):
                                exptime = exptime / fraction

                                if (
                                    exptime < lower_exptime_limit
                                    or exptime > upper_exptime_limit
                                ):
                                    self.logger.warning(
                                        f"Exposure time of {exptime:.3f} s out of user "
                                        f"defined range of {lower_exptime_limit} s "
                                        f"to {upper_exptime_limit} s"
                                    )
                                    break
                                else:
                                    self.logger.info(
                                        f"Setting new exposure time to {exptime:.3f} s "
                                        f"as median ADU of {median_adu} is not within "
                                        f"{target_adu[1]} of {target_adu[0]}"
                                    )

                        image_handler.header["EXPTIME"] = exptime

                        count += 1

            else:
                if take_flats is False:
                    self.logger.info(
                        f"Not the right time to take flats for {action.device_name}, "
                        f"sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
                    )

                self.logger.info("Moving on...")
                break

        # Clean up the image handler for this camera
        camera_name = camera.device_name
        if camera_name in self._image_handlers:
            del self._image_handlers[camera_name]
            self.logger.debug(f"Cleaned up image handler for camera '{camera_name}'")

    def flats_position(
        self, obs_location: EarthLocation, paired_devices: dict, action: Action
    ) -> None:
        """
        Position telescope to optimal sky location for flat field imaging.

        Calculates and moves the telescope to an optimal sky position for capturing
        uniform flat field images. The position is determined based on sun location,
        telescope constraints, and sky brightness uniformity requirements.

        Parameters:
            obs_location (EarthLocation): Observatory geographical location for
                astronomical calculations.
            paired_devices (dict): Dictionary of paired devices including telescope
                and dome for positioning operations.
            action (Action): The action object containing action_type,
                start_time, end_time, and action_value with target coordinates.

        Process:
        1. Calculates optimal sky position based on current conditions
        2. Considers sun position and twilight geometry
        3. Ensures position provides uniform illumination
        4. Commands telescope to slew to calculated position
        5. Updates action_value with target coordinates

        Positioning Strategy:
        - Avoids regions near sun or moon for uniform illumination
        - Selects high altitude positions when possible
        - Considers dome constraints and telescope limits
        - Optimizes for sky brightness uniformity

        Note:
            - Critical for obtaining high-quality flat field calibrations
            - Position affects uniformity and quality of flat frames
            - Coordinates with flats_exptime for complete flat acquisition
        """

        if action.action_value.get("disable_telescope_movement", False) is True:
            return

        if "Telescope" in paired_devices:
            # check if ready to take flats
            take_flats = False
            while self.check_conditions(action) and (take_flats is False):
                sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

                if take_flats is False:
                    time.sleep(1)

            if self.check_conditions(action) and take_flats:
                target_altaz = SkyCoord(
                    alt=u.Quantity(75, u.deg),
                    az=sun_altaz.az + u.Quantity(180, u.degree),
                    frame=AltAz(obstime=Time.now(), location=obs_location),
                )

                target_radec = target_altaz.transform_to("icrs")

                action_value = {}
                action_value["ra"] = target_radec.ra.deg  # type: ignore
                action_value["dec"] = target_radec.dec.deg  # type: ignore

                # move telescope to target
                self.setup_observatory(paired_devices, action_value)

    def flats_exptime(
        self,
        obs_location: EarthLocation,
        paired_devices: dict,
        action: Action,
        # numx: int,
        # numy: int,
        # startx: int,
        # starty: int,
        target_adu: list,
        offset: float,
        lower_exptime_limit: float,
        upper_exptime_limit: float,
        exptime: float | None = None,
    ) -> float:
        """
        Set the exposure time for flat field calibration images.

        This function adjusts the exposure time for flat field calibration images captured with a camera device
        to achieve a specific target median ADU (Analog-to-Digital Units) level, considering user-defined limits. It uses 64x64
        pixel subframes to speed up the process.

        Parameters:
            obs_location (EarthLocation): The location of the observatory.
            paired_devices (dict): A dictionary specifying paired devices, including 'Camera' for the camera device.
            action (Action): The action object containing action_type, start_time, end_time, and action_value.
            numx (int): The original number of pixels in the X-axis of the camera sensor.  # TODO delete?
            numy (int): The original number of pixels in the Y-axis of the camera sensor.
            startx (int): The original starting pixel position in the X-axis for the camera sensor.
            starty (int): The original starting pixel position in the Y-axis for the camera sensor.
            target_adu (list): A list containing the target ADU level and tolerance as [target_level, tolerance].
            offset (float): The offset ADU level to be considered when adjusting the exposure time.
            lower_exptime_limit (float): The lower limit for the exposure time in seconds.
            upper_exptime_limit (float): The upper limit for the exposure time in seconds.
            exptime (float, optional): The initial exposure time guess. If not provided, it is calculated as the
                midpoint between lower_exptime_limit and upper_exptime_limit.

        Returns:
            exptime (float): The adjusted exposure time in seconds that meets the target ADU level within the specified limits.

        """

        sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

        # initial exposure time guess
        if exptime is None:
            exptime = upper_exptime_limit if sun_rising else lower_exptime_limit
        assert exptime is not None, "exptime should not be None here"  # for mypy

        if (
            ("Camera" in paired_devices)
            and self.check_conditions(action)
            and take_flats
        ):
            camera = self.devices["Camera"][paired_devices["Camera"]]

            # TODO delete?
            # set camera to view small area to speed up read times, such to determine right exposure time (assuming detector is bigger than 64x64)
            # self.execute_and_monitor_device_task('Camera', 'NumX', 64, 'NumX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumX to 64")
            # time.sleep(1)
            # self.execute_and_monitor_device_task('Camera', 'NumY', 64, 'NumY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumY to 64")
            # time.sleep(1)
            # self.execute_and_monitor_device_task('Camera', 'StartX', int(numx/2 - 32), 'StartX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartX to {int(numx/2 - 32)}")
            # time.sleep(1)
            # self.execute_and_monitor_device_task('Camera', 'StartY', int(numy/2 - 32), 'StartY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartY to {int(numy/2 - 32)}")

            time.sleep(1)  # wait for camera to settle

            self.logger.info(
                f"Exposing full frame of {paired_devices['Camera']} for exposure time {exptime} s"
            )
            camera.get("StartExposure", Duration=exptime, Light=True)

            getting_exptime = True
            while self.check_conditions(action) and getting_exptime:
                r = camera.get("ImageReady")
                time.sleep(
                    0.1
                )  # add 0.1 s sleep to avoid spamming the camera and high cpu usage
                time.sleep(0)  # yield to other threads
                if r is True:
                    arr = camera.get("ImageArray")
                    median_adu = np.nanmedian(arr)
                    fraction = (median_adu - offset) / (target_adu[0] - offset)

                    sun_rising, take_flats, sun_altaz = utils.is_sun_rising(
                        obs_location
                    )

                    if (
                        math.isclose(
                            target_adu[0], median_adu, rel_tol=0, abs_tol=target_adu[1]
                        )
                        is False
                        and take_flats is True
                    ):
                        exptime = exptime / fraction

                        if exptime > upper_exptime_limit:  # type: ignore
                            self.logger.warning(
                                f"Exposure time of {exptime:.3f}s needed for next flat is greater than user defined limit of {upper_exptime_limit}s"
                            )
                            if sun_rising is True:
                                self.logger.info(
                                    f"Sun is rising, waiting 10s to try again. Sun is at {sun_altaz.alt.degree:.2f} degrees."
                                )
                                time.sleep(10)
                                exptime = upper_exptime_limit
                                self.logger.info(
                                    f"Exposing full frame of {paired_devices['Camera']} for exposure time {exptime}s"
                                )
                                camera.get(
                                    "StartExposure", Duration=exptime, Light=True
                                )
                            else:
                                self.logger.info(
                                    f"Sun is setting. Sun at {sun_altaz.alt.degree:.2f} degrees."
                                )
                                getting_exptime = False

                        elif exptime < lower_exptime_limit:  # type: ignore
                            self.logger.warning(
                                f"Exposure time of {exptime:.3f}s needed for next flat is lower than user defined limit of {lower_exptime_limit}s"
                            )

                            if sun_rising is False:
                                self.logger.info(
                                    f"Sun is setting, waiting 10s to try again. Sun is at {sun_altaz.alt.degree:.2f} degrees."
                                )
                                time.sleep(10)
                                exptime = lower_exptime_limit
                                self.logger.info(
                                    f"Exposing full frame of {paired_devices['Camera']} for exposure time {exptime}s"
                                )
                                camera.get(
                                    "StartExposure", Duration=exptime, Light=True
                                )
                            else:
                                self.logger.info(
                                    f"Sun is rising. Sun at {sun_altaz.alt.degree:.2f} degrees."
                                )
                                getting_exptime = False

                        else:
                            self.logger.info(
                                f"Exposure time of {exptime:.3f}s needed for next flat is within user defined tolerance"
                            )
                            getting_exptime = False

                    else:
                        if take_flats is True:
                            self.logger.info(
                                f"Exposure time of {exptime:.3f}s needed for next flat is within user defined tolerance"
                            )
                        getting_exptime = False

            # TODO delete?
            # set camera back to original framing
            # self.execute_and_monitor_device_task('Camera', 'StartX', startx, 'StartX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartX to {startx}")
            # time.sleep(1)
            # self.execute_and_monitor_device_task('Camera', 'StartY', starty, 'StartY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartY to {starty}")
            # time.sleep(1)
            # self.execute_and_monitor_device_task('Camera', 'NumX', numx, 'NumX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumX to {numx}")
            # time.sleep(1)
            # self.execute_and_monitor_device_task('Camera', 'NumY', numy, 'NumY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumY to {numy}")

            time.sleep(1)  # wait for camera to settle
        assert exptime is not None, "exptime should not be None here"  # for mypy

        return exptime

    def execute_and_monitor_device_task(
        self,
        device_type: str,
        monitor_command: str,
        desired_condition: Any,
        run_command: str,
        device_name: str,
        run_command_type: str = "",
        abs_tol: float = 0,
        log_message: str = "",
        timeout: float = 120,
        error_sensitive: bool = True,
        weather_sensitive: bool = True,
    ) -> None:
        """
        Monitor a device property and execute commands to achieve desired conditions.

        Continuously monitors a device property and executes a command if the current
        value doesn't match the desired condition. Provides robust error handling,
        timeout management, and safety checks during execution.

        This is a fundamental method for observatory automation, handling everything
        from telescope movements to camera settings with appropriate safety checks.

        Parameters:
            device_type (str): Type of device to monitor (e.g., 'Telescope', 'Camera').
            monitor_command (str): Property to monitor on the device.
            desired_condition (any): Target value or condition to achieve.
            run_command (str): Command to execute if condition not met.
            device_name (str): Specific device name to operate on.
            run_command_type (str, optional): Command type ('set' or 'get').
                Defaults to empty string for simple commands.
            abs_tol (float, optional): Absolute tolerance for numerical comparisons.
                Defaults to 0 for exact matches.
            log_message (str, optional): Custom message logged when action starts.
                Defaults to empty string.
            timeout (float, optional): Maximum time to wait in seconds.
                Defaults to 120 seconds.
            error_sensitive (bool, optional): Whether to abort on system errors.
                Defaults to True.
            weather_sensitive (bool, optional): Whether to abort on unsafe weather.
                Defaults to True.

        Safety Features:
        - Continuous monitoring of weather and error conditions
        - Timeout protection prevents infinite loops
        - Queue management prevents conflicting operations
        - Detailed error logging and reporting

        Note:
            - Uses queue system to prevent overlapping operations on same device
            - Automatically handles different data types and comparison methods
            - Critical for all automated observatory operations
        """

        def check_safe():
            """
            Check if the current weather and error conditions are safe for operation.
            """
            return (not weather_sensitive or self.weather_safe) and (
                not error_sensitive or self.logger.error_free
            )

        start_time = time.time()
        self.logger.debug(
            f"Monitor action: Starting {device_type} {device_name} {monitor_command} {desired_condition} {run_command} {run_command_type} {abs_tol} {log_message} {timeout}"
        )

        # create unique key for monitor action and add to queue for device_name
        unique_key = f"{device_type}{monitor_command}{desired_condition}{run_command}{run_command_type}"
        self.device_manager.device_task_monitor_queue[device_name][unique_key] = (
            start_time
        )

        try:
            # Wait for turn
            while any(
                value
                < self.device_manager.device_task_monitor_queue[device_name][unique_key]
                for value in self.device_manager.device_task_monitor_queue[
                    device_name
                ].values()
            ):
                if not check_safe():
                    return
                time.sleep(0.5)
                self.logger.debug(
                    f"Monitor action: Waiting {device_type} {device_name} {monitor_command}"
                )
                if time.time() - start_time > 3 * timeout:
                    raise TimeoutError(
                        f"Monitor run action queue timeout: {device_type} {monitor_command} {desired_condition} {run_command}"
                    )

            ## Execute monitor action
            device = self.devices[device_type][device_name]

            # define run command type
            if monitor_command == run_command and run_command_type == "":
                run_command_type = "set"
            elif run_command_type == "":
                run_command_type = "get"

            ran = False
            while True:
                monitor_status = device.get(monitor_command)
                isclose = math.isclose(
                    monitor_status, desired_condition, rel_tol=0, abs_tol=abs_tol
                )

                if not check_safe():
                    return

                if time.time() - start_time > timeout:
                    raise TimeoutError(
                        f"Monitor-action for {device_name} timeout: Timeout for reaching desired condition of {desired_condition} "
                        f"when monitoring {monitor_command}, currently at {monitor_status} on {device_type}"
                    )

                if isclose is False and ran is False:
                    self.logger.info(
                        f"Monitor-action for {device_name}: Desired condition of {desired_condition} does not "
                        f"match {monitor_status} when monitoring {monitor_command}, running {run_command} on {device_type}"
                    )
                    if log_message:
                        self.logger.info(log_message)

                    if run_command_type == "get":
                        device.get(run_command, no_kwargs=True)
                    elif run_command_type == "set":
                        device.set(run_command, desired_condition)

                    ran = True

                elif isclose and ran:
                    self.logger.info(
                        f"Monitor-action for {device_name} complete: Desired condition of {desired_condition} "
                        f"for {monitor_command} met, after running "
                        f"{run_command}{'=' + str(desired_condition) if run_command_type == 'set' else ''} on {device_type}"
                    )
                    return

                if isclose and not ran:
                    return

                time.sleep(0.5)

        except Exception as e:
            self.logger.report_device_issue(
                device_type=device_type,
                device_name=device_name,
                message=(
                    f"Monitor-action error: Device Type: {device_type}, "
                    f"Device Name: {device_name}, Monitor Command: {monitor_command}, "
                    f"Desired Condition: {desired_condition}, "
                    f"Run Command: {run_command}, "
                    f"Run Command Type: {run_command_type}, "
                    f"Absolute Tolerance: {abs_tol}, Log Message: {log_message}, "
                    f"Timeout: {timeout}."
                ),
                exception=e,
            )

        finally:
            if (
                device_name in self.device_manager.device_task_monitor_queue
                and unique_key
                in self.device_manager.device_task_monitor_queue[device_name]
            ):
                del self.device_manager.device_task_monitor_queue[device_name][
                    unique_key
                ]
