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
from multiprocessing import Manager
from pathlib import Path
from threading import Thread

import astropy.units as u
import numpy as np
import pandas as pd
import psutil
import sqlite3
import traceback

from astropy.coordinates import AltAz, EarthLocation, SkyCoord, get_body
from astropy.io import fits
from astropy.time import Time
from astropy.wcs.utils import WCS
from sqlite3worker import Sqlite3Worker

from astra import ASTRA_VER, Config, utils
from astra.alpaca_device_process import AlpacaDevice
from astra.autofocus import Autofocuser, Defocuser
from astra.calibrate_guiding import GuidingCalibrator
from astra.config import ObservatoryConfig
from astra.guiding import Guider
from astra.image_handler import create_image_dir, save_image
from astra.logging_handler import LoggingHandler
from astra.paired_devices import PairedDevices
from astra.pointer import PointingCorrectionHandler
from astra.schedule import process_schedule

logging.getLogger("sqlite3worker").setLevel(logging.INFO)
CONFIG = Config()


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
        custom_observatory: str = None,
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
            cursor (Sqlite3Worker): Database cursor for logging and data storage.
            logger (logging.Logger): Logger instance for the observatory.
            _config (ObservatoryConfig): Observatory configuration object.
            fits_config (pd.DataFrame): FITS header configuration.
            threads (list): List of running threads.
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

        # create database
        self.cursor = self.create_db()

        # set up logger
        self.logger = logging.getLogger(self.name)
        self.logger.addHandler(LoggingHandler(self))
        # self.logger.propagate = False # prevent double logging?

        # log start up
        self.logger.info("Astra starting up")

        # warn if debug mode
        if self.logger.getEffectiveLevel() == logging.DEBUG:
            self.logger.warning("Astra is running in debug mode")

        # read observatory config files
        # self._config = self.read_config(config_filename)
        self._config = ObservatoryConfig.from_config(CONFIG)
        self.fits_config = pd.read_csv(
            CONFIG.paths.observatory_config / f"{self.name}_fits_header_config.csv"
        )

        # running threads list
        self.threads = []

        # queue for multiprocessing
        self.queue = Manager().Queue()
        self.queue_running = True

        th = Thread(target=self.queue_get, daemon=True)
        th.start()

        self.threads.append(
            {"type": "queue", "device_name": "queue", "thread": th, "id": "queue"}
        )

        # heartbeat dictionary
        self.heartbeat = {}

        # custom logic flags
        self.speculoos = custom_observatory == "speculoos"
        self.truncate_factor = truncate_factor

        # log+polling backup flags
        self.run_backup = True
        self.backup_time = datetime.strptime(
            self.config["Misc"]["backup_time"], "%H:%M"
        )

        # optional configuration for pattern used to write filename.
        # Test if pattern formatting fails here to validate the config to avoid it failing later during acquisition
        def check_filename_pattern(filename_pattern, pattern_name, obj_name, device_name, image_type,
                                   filter_name, filter_name_clean, exptime, timestamp,
                                   timestamp_date, timestamp_time, image_sequence_n, duskdawn="*"):
            # Check the format
            try:
                self.logger.info(f"Config {pattern_name} filename pattern: [{filename_pattern}] ({type(filename_pattern)})")

                filename = filename_pattern.format(device=device_name,
                                                   filter_orig=filter_name,
                                                   filter_clean=filter_name_clean,
                                                   object_name=obj_name,
                                                   exptime=exptime,
                                                   imagetype=image_type,
                                                   timestamp=timestamp,
                                                   timestamp_date=timestamp_date,
                                                   timestamp_time=timestamp_time,
                                                   sequence=image_sequence_n,
                                                   duskdawn=duskdawn)
                self.logger.info(f"Config filename test: {filename}")
            except Exception as e:
                self.logger.error("Can't parse {pattern_name} filename pattern")
                self.logger.error(traceback.format_exception(e))

        self.save_config = {}
        if "raw_filename_pattern" in self.config["Misc"]: 
            filename_pattern = self.config["Misc"]["raw_filename_pattern"]

            # run a check on it
            dt = datetime.now()
            timestamp = dt.strftime('%Y%m%d_%H%M%S.%f')[:-3]
            timestamp_date = dt.strftime('%Y%m%d')
            timestamp_time = dt.strftime('%H%M%S')
            check_filename_pattern(filename_pattern, "raw", "TestObj", "TestDevice", "Light Frame", "F'", "F", 30.0,
                                   timestamp, timestamp_date, timestamp_time, 42)

            self.save_config["raw_filename_pattern"] = filename_pattern

        if "raw_filename_pattern_glob" in self.config["Misc"]:
            filename_pattern = self.config["Misc"]["raw_filename_pattern_glob"]

            dt = datetime.now()
            timestamp = dt.strftime('%Y%m%d_%H%M%S.%f')[:-3]
            timestamp_date = dt.strftime('%Y%m%d')
            timestamp_time = dt.strftime('%H%M%S')
            check_filename_pattern(filename_pattern, "raw glob", "*", "TestDevice", "Light Frame", "*", "*", "*", "*", "*", "*", "*")

            self.save_config["raw_filename_pattern_glob"] = filename_pattern

        if "bias_filename_pattern" in self.config["Misc"]:
            filename_pattern = self.config["Misc"]["bias_filename_pattern"]

            # run a check on it
            dt = datetime.now()
            timestamp = dt.strftime('%Y%m%d_%H%M%S.%f')[:-3]
            timestamp_date = dt.strftime('%Y%m%d')
            timestamp_time = dt.strftime('%H%M%S')
            check_filename_pattern(filename_pattern, "bias", "TestObj", "TestDevice", "Light Frame", "F'", "F", 30.0,
                                   timestamp, timestamp_date, timestamp_time, 42)

            self.save_config["bias_filename_pattern"] = filename_pattern

        if "dark_filename_pattern" in self.config["Misc"]:
            filename_pattern = self.config["Misc"]["dark_filename_pattern"]

            # run a check on it
            dt = datetime.now()
            timestamp = dt.strftime('%Y%m%d_%H%M%S.%f')[:-3]
            timestamp_date = dt.strftime('%Y%m%d')
            timestamp_time = dt.strftime('%H%M%S')
            check_filename_pattern(filename_pattern, "dark", "TestObj", "TestDevice", "Light Frame", "F'", "F", 30.0,
                                   timestamp, timestamp_date, timestamp_time, 42)

            self.save_config["dark_filename_pattern"] = filename_pattern

        if "flat_filename_pattern" in self.config["Misc"]:
            filename_pattern = self.config["Misc"]["flat_filename_pattern"]

            # run a check on it
            dt = datetime.now()
            timestamp = dt.strftime('%Y%m%d_%H%M%S.%f')[:-3]
            timestamp_date = dt.strftime('%Y%m%d')
            timestamp_time = dt.strftime('%H%M%S')
            check_filename_pattern(filename_pattern, "flat", "TestObj", "TestDevice", "Light Frame", "F'", "F", 30.0,
                                   timestamp, timestamp_date, timestamp_time, 42, duskdawn="Dawn")

            self.save_config["flat_filename_pattern"] = filename_pattern

        if "utc_offset_hours" in self.config["Misc"]:
            self.save_config["utc_offset_hours"] = self.config["Misc"]["utc_offset_hours"]

        if (("flat_filename_pattern" in self.config["Misc"])
            and ("{duskdawn}" in self.config["Misc"]["flat_filename_pattern"])
            and ("utc_offset_hours" not in self.config["Misc"])):
            self.logger.error("Missing `utc_offset_hours` in observatory config for {duskdawn} in filename pattern")

        # error and weather handling flags
        self.error_free = True
        self.error_source = []
        self.weather_safe = None
        self.time_to_safe = 0

        # watchdog/schedule running flags, robotic switch
        self.watchdog_running = False
        self.schedule_running = False
        self.robotic_switch = False

        # schedule
        self.schedule_path = CONFIG.paths.schedules / f"{self.name}.jsonl"
        self.schedule_mtime = self.get_schedule_mtime()
        self.schedule = None
        if self.schedule_mtime != 0:
            self.schedule = self.read_schedule()

        # load devices
        self.monitor_action_queue = (
            {}
        )  # queue for monitoring/running actions per device_name
        self.devices = self.load_devices()
        self.last_image = None

        # for each telescope, create a donuts guider
        self.guider: dict[str, Guider] = {}
        if "Telescope" in self.config:
            for device_name in self.devices["Telescope"]:
                telescope = self.devices["Telescope"][device_name]
                telescope_index = [
                    i
                    for i, d in enumerate(self.config["Telescope"])
                    if d["device_name"] == device_name
                ][0]
                if "guider" in self.config["Telescope"][telescope_index]:
                    guider_params = self.config["Telescope"][telescope_index]["guider"]
                    self.guider[device_name] = Guider(telescope, self, guider_params)

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

    def create_db(self) -> Sqlite3Worker:
        """
        Create and initialize the observatory database.

        Creates a SQLite database for storing observatory data including device polling
        information, image metadata, and log entries. The database includes three main
        tables: polling (device status data), images (image file information), and
        log (system log messages).

        Returns:
            Sqlite3Worker: The database cursor object for executing queries and managing
            the database connection with a maximum queue size of 200.

        Note:
            The database file is created in the logs directory using the observatory
            name as the filename with a .db extension.
        """

        db_name = CONFIG.paths.logs / f"{self.name}.db"
        cursor = Sqlite3Worker(db_name, max_queue_size=2000)

        # Enable WAL mode - critical for concurrent access
        cursor.execute("PRAGMA journal_mode=WAL")

        # Optimize for high write throughput
        cursor.execute("PRAGMA synchronous=NORMAL")  # Faster than FULL, safer than OFF
        cursor.execute("PRAGMA cache_size=10000")  # Increase cache (10MB for large DB)
        cursor.execute("PRAGMA temp_store=memory")  # Use memory for temp operations

        db_command_0 = """CREATE TABLE IF NOT EXISTS polling (
                device_type   TEXT,
                device_name TEXT,
                device_command TEXT,
                device_value TEXT,
                datetime TEXT)"""

        cursor.execute(db_command_0)

        db_command_1 = """CREATE TABLE IF NOT EXISTS images (
                filename   TEXT,
                camera_name TEXT,
                complete_hdr INTEGER,
                date_obs TEXT)"""

        cursor.execute(db_command_1)

        db_command_2 = """CREATE TABLE IF NOT EXISTS log (
                datetime TEXT,
                level TEXT,
                message TEXT)"""

        cursor.execute(db_command_2)

        return cursor

    def backup(self) -> None:
        """
        Back up database tables from the previous 24 hours to CSV files.

        Creates timestamped CSV backups of the main database tables (polling, log,
        autoguider_log, autoguider_info_log) and stores them in an archive directory.
        Also monitors disk usage and logs a warning if disk usage exceeds 90%.

        The backup process:
            1. Checks available disk space and warns if usage > 90%
            2. Creates an archive directory if it doesn't exist
            3. Exports specified database tables to timestamped CSV files
            4. Logs the backup completion or any errors encountered

        Raises:
            Exception: Any errors during the backup process are logged and added
                to the error_source list for monitoring.
        """

        try:
            self.run_backup = False
            self.logger.info("Backing up database")

            # check disk space
            disk_usage = psutil.disk_usage("/")
            if disk_usage.percent > 90:
                self.logger.warning(f"Disk usage {disk_usage.percent}% is high")

            db_path = CONFIG.paths.logs / f"{self.name}.db"

            # create backup directory if not exists
            archive_path = CONFIG.paths.logs / "archive"
            archive_path.mkdir(exist_ok=True)

            tables = ["polling", "log", "autoguider_log", "autoguider_info_log"]
            # 'images', 'autoguider_ref'

            db = sqlite3.connect(db_path)
            for table in tables:
                # backup table
                df = pd.read_sql_query(
                    f"SELECT * FROM {table} WHERE datetime > datetime('now', '-1 days')",
                    db,
                )
                dt_str = (
                    df["datetime"]
                    .iloc[0]
                    .replace(":", "")
                    .replace("-", "")
                    .replace(" ", "_")
                    .split(".")[0]
                )
                df.to_csv(
                    os.path.join(
                        CONFIG.paths.logs,
                        "archive",
                        f"{self.name}_{table}_{dt_str}.csv",
                    ),
                    index=False,
                )

            for table in tables:
                # once back up complete, delete rows older than 3 days ago from database
                # to minimize database size for speed
                self.cursor.execute(
                    f"DELETE FROM {table} WHERE datetime < datetime('now', '-3 days')"
                )

            db.close()

            self.logger.info("Database backed up")

        except Exception as e:
            self.error_source.append(
                {"device_type": "Backup", "device_name": "backup", "error": str(e)}
            )
            self.logger.error(f"Error backing up database: {str(e)}")

    def load_devices(self) -> dict[str, dict[str, AlpacaDevice]]:
        """
        Load and initialize all Alpaca devices from the observatory configuration.

        Iterates through the observatory configuration, creating AlpacaDevice objects
        for each defined device. Devices are categorized by type (Telescope, Camera,
        FilterWheel, etc.) and stored in a nested dictionary structure.

        Returns:
            dict[str, dict[str, AlpacaDevice]]: A nested dictionary where the outer
            keys are device types and inner keys are device names, with values
            being the initialized AlpacaDevice objects.

        Note:
            - Debug mode affects device initialization parameters
            - The 'Misc' configuration section is skipped as it contains
              non-device settings
            - Each device is initialized with its specific configuration parameters
              including IP address, port, and device number
        """

        self.logger.info("Loading devices")

        debug = self.logger.getEffectiveLevel() == logging.DEBUG

        devices = {}
        for device_type in self.config:
            devices[device_type] = {}
            if device_type != "Misc":
                for d in self.config[device_type]:
                    try:
                        devices[device_type][d["device_name"]] = AlpacaDevice(
                            d["ip"],
                            device_type,
                            d["device_number"],
                            d["device_name"],
                            self.queue,
                            debug,
                        )

                        devices[device_type][d["device_name"]].start()

                        self.monitor_action_queue[d["device_name"]] = {}
                    except Exception as e:
                        self.error_source.append(
                            {
                                "device_type": device_type,
                                "device_name": d["device_name"],
                                "error": str(e),
                            }
                        )
                        self.logger.error(
                            f"Error loading {device_type} {d['device_name']}: {str(e)}"
                        )

        self.logger.info("Devices loaded")

        return devices

    def connect_all(self) -> None:
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

        self.logger.info("Connecting to devices")

        # connect to all devices
        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                try:
                    # SPECULOOS EDIT
                    if device_type == "Focuser" and self.speculoos:
                        self.logger.warning(
                            f"{device_type} {device_name} skipping connecting, because method not valid. - SPECULOOS specific"
                        )
                    else:
                        self.devices[device_type][device_name].set(
                            "Connected", True
                        )  ## slow?
                        self.logger.info(f"{device_type} {device_name} connected")
                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": device_type,
                            "device_name": device_name,
                            "error": str(e),
                        }
                    )
                    self.logger.error(
                        f"Error connecting to {device_type} {device_name}: {str(e)}"
                    )

        self.logger.info("Starting polling non-fixed fits headers")

        # start polling non-fixed fits headers
        for i, row in self.fits_config.iterrows():
            if (
                row["device_type"]
                not in ["astropy_default", "astra", "astra_fixed", ""]
            ) and row["fixed"] is False:
                device_type = row["device_type"]
                if device_type in self.devices:
                    for device_name in self.devices[device_type]:
                        device = self.devices[device_type][device_name]

                        # find polling delay in self.config
                        delay = next(
                            (
                                d.get("polling_interval", 5)
                                for d in self.config[device_type]
                                if d["device_name"] == device_name
                            ),
                            5,  # default fallback if device not found
                        )

                        try:
                            device.start_poll(
                                row["device_command"], delay
                            )  # 5 second polling
                        except Exception as e:
                            self.error_source.append(
                                {
                                    "device_type": device_type,
                                    "device_name": device_name,
                                    "error": str(e),
                                }
                            )
                            self.logger.error(
                                f"Error starting polling for {device_type} {device_name}: {str(e)}"
                            )

        if "SafetyMonitor" in self.config:
            device_type = "SafetyMonitor"
            device_name = self.config[device_type][0]["device_name"]

            device = self.devices[device_type][device_name]
            try:
                # find polling delay in self.config
                delay = next(
                    (
                        d.get("polling_interval", 1)
                        for d in self.config[device_type]
                        if d["device_name"] == device_name
                    ),
                    1,  # default fallback if device not found
                )
                device.start_poll("IsSafe", delay)  # 1 second polling
            except Exception as e:
                self.error_source.append(
                    {
                        "device_type": device_type,
                        "device_name": device_name,
                        "error": str(e),
                    }
                )
                self.logger.error(
                    f"Error starting polling for {device_type} {device_name}: {str(e)}"
                )

        self.logger.info("Connect all sequence complete")
        # run can<> ascom commands, needed for other commands to work? Else, alternatives needed.

        # start watchdog once all devices connected
        time.sleep(
            1
        )  # wait for devices to connect and start polling TODO: check one device's latest polling is valid before starting watchdog
        self.start_watchdog()

    def pause_polls(self, device_types: list = None) -> None:
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

            device_types = self.devices.keys()

        for device_type in device_types:
            if device_type in self.devices:
                for device_name in self.devices[device_type]:
                    try:
                        self.devices[device_type][device_name].pause_polls()
                    except Exception as e:
                        self.error_source.append(
                            {
                                "device_type": device_type,
                                "device_name": device_name,
                                "error": str(e),
                            }
                        )
                        self.logger.error(
                            f"{device_type} {device_name} could not pause polls: {str(e)}"
                        )

    def resume_polls(self, device_types: list = None) -> None:
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

            device_types = self.devices.keys()

        for device_type in device_types:
            if device_type in self.devices:
                for device_name in self.devices[device_type]:
                    try:
                        self.devices[device_type][device_name].resume_polls()
                    except Exception as e:
                        self.error_source.append(
                            {
                                "device_type": device_type,
                                "device_name": device_name,
                                "error": str(e),
                            }
                        )
                        self.logger.error(
                            f"{device_type} {device_name} could not pause polls: {str(e)}"
                        )

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

        th = Thread(target=self.watchdog, daemon=True)
        th.start()

        self.threads.append(
            {
                "type": "watchdog",
                "device_name": "watchdog",
                "thread": th,
                "id": "watchdog",
            }
        )

    def watchdog(self) -> None:
        """
        Main observatory monitoring loop for safety and operational status.

        Continuously monitors critical observatory systems and takes appropriate
        actions to ensure safe and efficient operation. The watchdog is the
        central control system that coordinates all observatory activities.

        Key monitoring functions:
        - Safety monitor status and weather conditions
        - Device health and responsiveness
        - System errors and error recovery
        - Schedule execution and timing
        - System resource usage (CPU, memory, disk)
        - Telescope altitude limits and safety boundaries

        Automated actions taken:
        - Close observatory when unsafe conditions detected
        - Start/stop scheduling based on conditions
        - Handle system errors and device failures
        - Perform daily database backups
        - Update system heartbeat for external monitoring

        The watchdog runs in a continuous loop with 0.5-second intervals and
        coordinates with the schedule runner to ensure safe autonomous operation.

        Note:
            - Exits when watchdog_running flag is set to False
            - Sets schedule_running and robotic_switch to False on exit
            - Handles both weather-dependent and weather-independent operations
        """

        self.logger.info("Starting watchdog")

        self.watchdog_running = True

        # initial safety monitor check
        if "SafetyMonitor" in self.config:
            self.logger.info("Safety monitor found")

            try:
                max_safe_duration = self.config["SafetyMonitor"][0]["max_safe_duration"]
            except KeyError:
                max_safe_duration = 30
                self.logger.warning(
                    f"No max_safe_duration in user config, defaulting to {max_safe_duration} minutes."
                )

            device_type = "SafetyMonitor"
            sm_name = self.config[device_type][0]["device_name"]
            safety_monitor = self.devices[device_type][sm_name]
        else:
            max_safe_duration = 0
            self.logger.warning("No safety monitor found")

        # observatory weather_log_warning flag, used to prevent multiple logging of weather unsafe
        weather_log_warning = False

        while self.watchdog_running:
            # check if any devices unresponsive - hopefully never happens
            self.check_devices_alive()

            # update heartbeat dictionary
            self.update_heartbeat()

            # if no errors, proceed with remaining watchdog checks/actions
            if self.error_free is True:
                try:
                    # check if schedule file updated
                    try:
                        schedule_mtime = self.get_schedule_mtime()

                        if (schedule_mtime > self.schedule_mtime) and (
                            self.schedule_running is False
                        ):
                            self.logger.warning("Schedule updated")
                            self.schedule = self.read_schedule()
                            if self.robotic_switch is True:
                                self.logger.warning(
                                    "Robotic switch is on, starting schedule"
                                )
                                self.start_schedule()
                    except Exception as e:
                        self.error_source.append(
                            {
                                "device_type": "Schedule",
                                "device_name": "schedule",
                                "error": str(e),
                            }
                        )
                        self.logger.error(f"Error checking schedule: {str(e)}")
                        continue

                    # check safety monitor
                    if "SafetyMonitor" in self.config:
                        sm_poll = safety_monitor.poll_latest()

                        # check if stale
                        last_update = (
                            datetime.now(UTC) - sm_poll["IsSafe"]["datetime"]
                        ).total_seconds()

                        if last_update > 3 and last_update < 30:
                            self.logger.warning(f"Safety monitor {last_update}s stale")
                        elif last_update > 30:
                            self.error_source.append(
                                {
                                    "device_type": "SafetyMonitor",
                                    "device_name": sm_name,
                                    "error": f"Stale data {last_update}s",
                                }
                            )
                            self.logger.error(f"Safety monitor {last_update}s stale")
                            continue

                        # action if weather unsafe
                        if sm_poll["IsSafe"]["value"] is False:
                            self.weather_safe = False

                            # log message saying weather unsafe
                            if weather_log_warning is False:
                                self.logger.warning("Weather unsafe from SafetyMonitor")

                        # check weather history for weather unsafe
                        rows = self.cursor.execute(
                            f"SELECT COUNT(*), MAX(datetime) FROM polling WHERE device_type = 'SafetyMonitor' AND device_value = 'False' AND datetime > datetime('now', '-{max_safe_duration} minutes')"
                        )
                    else:
                        self.logger.warning("No safety monitor found")
                        rows = [(0, None)]

                    # check internal safety monitor
                    (
                        internal_safety,
                        internal_time_to_safe,
                        internal_max_safe_duration,
                    ) = self.internal_safety_weather_monitor()

                    # if internal safety monitor is False, act on it
                    if internal_safety is False:
                        self.weather_safe = False

                        # log message saying weather unsafe
                        if weather_log_warning is False:
                            self.logger.warning(
                                "Weather unsafe from internal safety monitor"
                            )

                    # set time_to_safe if weather unsafe
                    if rows[0][0] > 0 or internal_time_to_safe > 0:
                        if rows[0][1] is not None:
                            time_since_last_unsafe = pd.to_datetime(
                                datetime.now(UTC)
                            ) - pd.to_datetime(rows[0][1], utc=True)
                        else:
                            time_since_last_unsafe = pd.to_timedelta(0)

                        current_time_to_safe = (
                            max_safe_duration
                            - time_since_last_unsafe.total_seconds() / 60
                        )

                        if rows[0][0] == 0:
                            self.time_to_safe = internal_time_to_safe
                        else:
                            self.time_to_safe = max(
                                current_time_to_safe, internal_time_to_safe
                            )
                    else:
                        self.time_to_safe = 0

                    self.logger.debug(
                        f"Watchdog: {rows} instances of weather unsafe found in last {max(max_safe_duration, internal_max_safe_duration)} minutes"
                    )

                    # if no weather unsafe in last max_safe_duration minutes, weather is "safe"
                    if (rows[0][0] == 0) and internal_safety:
                        self.weather_safe = True
                        if weather_log_warning:
                            self.logger.info(
                                f"Weather safe for the last {max(max_safe_duration, internal_max_safe_duration)} minutes"
                            )
                            # reset weather_log_warning flag
                            weather_log_warning = False
                    elif (
                        rows[0][0] > 0 and internal_safety
                    ):  # just in case isSafe value switches during realtime check
                        self.weather_safe = False

                        # log message saying weather unsafe
                        if weather_log_warning is False:
                            self.logger.warning(
                                "Weather unsafe from SafetyMonitor IsSafe history, internal safety monitor is True. "
                                "Are the internal safety monitor limits higher than SafetyMonitor values?"
                            )
                            weather_log_warning = True
                    else:
                        weather_log_warning = True

                    if self.weather_safe is False:
                        self.close_observatory()

                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": "Watchdog",
                            "device_name": "watchdog",
                            "error": str(e),
                        }
                    )
                    self.logger.error(
                        f"Error during watchdog check: {str(e)}",
                        exc_info=True,
                        stack_info=True,
                    )

            else:
                try:
                    # stop schedule
                    self.schedule_running = False
                    self.robotic_switch = False

                    # wait a bit to see if it's a multi-device error?
                    self.logger.info(
                        "Waiting 30 seconds to see if error is multi-device. Main watchdog thread exited."
                    )
                    time.sleep(30)

                    if len(self.error_source) == 0:
                        self.logger.warning("No error sources found in error_source.")
                        self.error_source.append(
                            {
                                "device_type": "error_source",
                                "device_name": "error_source",
                                "error": "No error sources found in error_source",
                            }
                        )

                    # make pandas dataframe of error_source
                    df = pd.DataFrame(self.error_source)

                    device_types = df.device_type.unique()
                    device_names = df.device_name.unique()

                    # multiple devices have errors
                    if len(device_names) > 1:
                        self.logger.error("Multiple devices have errors. Panic.")
                        for error_source in self.error_source:
                            self.logger.error(
                                f"Device {error_source['device_type']} {error_source['device_name']} has error: {error_source['error']}"
                            )
                        # TODO: Panic mode
                    elif len(device_names) == 1 and len(device_types) == 1:
                        self.logger.warning(
                            f"Device {device_types[0]} {device_names[0]} has errors."
                        )

                    # if no error in dome or telescope, park
                    if (
                        "Dome" not in device_types
                        and "Telescope" not in device_types
                        and ("Dome" in self.config or "Telescope" in self.config)
                    ):
                        self.logger.warning(
                            f"Closing observatory due to no errors in Dome or Telescope"
                        )
                        self.close_observatory(error_sensitive=False)

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
                                self.monitor_action(
                                    "Dome",
                                    "ShutterStatus",
                                    1,
                                    "CloseShutter",
                                    device_name=device_name,
                                    log_message=f"Closing Dome shutter of {device_name}",
                                    weather_sensitive=False,
                                    error_sensitive=False,
                                )

                    # update heartbeat dictionary for last status
                    self.update_heartbeat()

                except Exception as e:
                    self.logger.error(
                        f"Error during error handling: {str(e)}",
                        exc_info=True,
                        stack_info=True,
                    )
                    # TODO: Panic mode

                break  # exit watchdog loop

            # run backup once a day
            if (
                datetime.now(UTC).hour == self.backup_time.hour
                and datetime.now(UTC).minute == self.backup_time.minute
            ):
                if self.run_backup is True:
                    # run backup in separate thread
                    th = Thread(target=self.backup, daemon=True)
                    th.start()

                    self.threads.append(
                        {
                            "type": "Backup",
                            "device_name": "backup",
                            "thread": th,
                            "id": "backup",
                        }
                    )

            else:
                self.run_backup = True

            time.sleep(0.5)  # twice the safety monitor polling time

        self.schedule_running = False  # stop schedule if watchdog stopped
        self.robotic_switch = False
        self.watchdog_running = False
        self.logger.warning("Watchdog stopped")

    def internal_safety_weather_monitor(self) -> tuple[bool, float, float]:
        """
        Monitor internal safety systems and weather conditions.

        Evaluates weather conditions against configured safety limits and determines
        if operations can continue safely. Checks observing conditions parameters
        against their defined closing limits and calculates time to safe operation.

        Returns:
            tuple: A tuple containing:
                - bool: True if weather conditions are safe for operation
                - float: Time in seconds until conditions become safe (0 if already safe)
                - float: Maximum safe duration in seconds for current conditions

        The method examines each parameter in the closing_limits configuration:
        - Compares current values against upper and lower thresholds
        - Calculates time until conditions improve if currently unsafe
        - Determines maximum safe operating duration under current conditions

        Note:
            - Returns (True, 0, 0) if no ObservingConditions devices are configured
            - Used by the watchdog to make decisions about observatory operations
            - Critical for autonomous safety management
        """

        longest_time_to_safe = 0
        longest_max_safe_duration = 0

        if "ObservingConditions" not in self.config:
            return True, 0, 0

        if "closing_limits" not in self.config["ObservingConditions"][0]:
            return True, 0, 0

        closing_limits = self.config["ObservingConditions"][0]["closing_limits"]
        # find largest max_safe_duration
        max_safe_duration = max(
            limit.get("max_safe_duration", 0)
            for limits in closing_limits.values()
            for limit in limits
        )

        device_type = "ObservingConditions"
        q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > datetime('now', '-{max_safe_duration*1.1} minutes')"""

        rows = self.cursor.execute(q)

        # Convert to DataFrame
        columns = [
            "device_type",
            "device_name",
            "device_command",
            "device_value",
            "datetime",
        ]

        df = pd.DataFrame(rows, columns=columns)

        if df.shape[0] == 0:
            self.logger.warning("No data found for internal safety weather monitor")
            return True, 0, 0

        # Pivot: datetime as index, device_command as columns
        df = df.pivot(index="datetime", columns="device_command", values="device_value")

        # Ensure datetime index and numeric values
        df.index = pd.to_datetime(df.index, utc=True)
        df = df.sort_index()
        df = df.apply(pd.to_numeric, errors="coerce")

        # interpolate
        df = df.interpolate(method="time")

        if "SkyTemperature" in df.columns and "Temperature" in df.columns:
            df["RelativeSkyTemp"] = df["SkyTemperature"] - df["Temperature"]

        # Check each parameter and its limits
        for parameter, limits in closing_limits.items():
            if parameter not in df.columns:
                self.logger.warning(f"Parameter '{parameter}' not found in DataFrame")
                continue

            for limit in limits:
                max_safe_duration = limit.get("max_safe_duration", 0)
                lower_limit = limit.get("lower")
                upper_limit = limit.get("upper")

                if lower_limit is not None and upper_limit is not None:
                    condition = (df[parameter] < lower_limit) | (
                        df[parameter] > upper_limit
                    )
                elif lower_limit is not None:
                    condition = df[parameter] < lower_limit
                elif upper_limit is not None:
                    condition = df[parameter] > upper_limit
                else:
                    continue  # no limits defined

                # Apply the condition to the DataFrame
                _df = df[
                    condition
                    & (
                        df.index
                        > (
                            pd.Timestamp.now(tz="UTC")
                            - pd.Timedelta(minutes=max_safe_duration)
                        )
                    )
                ]

                count = _df.shape[0]
                if count == 0:
                    max_datetime = None
                else:
                    max_datetime = _df.index.max()
                if count > 0:
                    time_since_last_unsafe = pd.to_datetime(
                        datetime.now(UTC)
                    ) - pd.to_datetime(max_datetime, utc=True)

                    current_time_to_safe = (
                        max_safe_duration - time_since_last_unsafe.total_seconds() / 60
                    )

                    if current_time_to_safe > longest_time_to_safe:
                        longest_time_to_safe = current_time_to_safe

                    if max_safe_duration > longest_max_safe_duration:
                        longest_max_safe_duration = max_safe_duration

        return (
            longest_time_to_safe == 0,
            longest_time_to_safe,
            longest_max_safe_duration,
        )

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
                        self.error_source.append(
                            {
                                "device_type": device_type,
                                "device_name": device_name,
                                "error": "Device unresponsive",
                            }
                        )
                        self.logger.error(f"{device_type} {device_name} unresponsive")
                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": device_type,
                            "device_name": device_name,
                            "error": str(e),
                        }
                    )
                    self.logger.error(f"{device_type} {device_name} unresponsive")
                    return False

        return True

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
        self.heartbeat["error_free"] = self.error_free
        self.heartbeat["error_source"] = self.error_source
        self.heartbeat["weather_safe"] = self.weather_safe
        self.heartbeat["schedule_running"] = self.schedule_running
        self.heartbeat["cpu_percent"] = psutil.cpu_percent()
        self.heartbeat["memory_percent"] = psutil.virtual_memory().percent
        self.heartbeat["disk_percent"] = psutil.disk_usage("/").percent
        self.heartbeat["threads"] = [
            {"type": i["type"], "device_name": i["device_name"], "id": i["id"]}
            for i in self.threads
        ]

        polled_list = {}

        for device_type in self.devices:
            polled_list[device_type] = {}

            for device_name in self.devices[device_type]:
                polled_list[device_type][device_name] = {}

                try:
                    polled = self.devices[device_type][device_name].poll_latest()
                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": device_type,
                            "device_name": device_name,
                            "error": str(e),
                        }
                    )
                    self.logger.error(
                        f"Error polling {device_type} {device_name}: {str(e)}"
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
        self.heartbeat["monitor-action-queue"] = self.monitor_action_queue

    def speculoos_check_and_ack_error(self, close=False) -> None:
        """
        Check for and acknowledge SPECULOOS AsTelOS telescope errors.

        SPECULOOS-specific method that monitors telescope error states and
        automatically acknowledges errors that can be safely cleared. This is
        essential for autonomous operation of SPECULOOS telescopes which use
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
                        self.error_source.append(
                            {
                                "device_type": "Telescope",
                                "device_name": telescope_name,
                                "error": "AsTelOS errors not successfully acknowledged",
                            }
                        )
                        self.logger.error(
                            f"AsTelOS errors not successfully acknowledged for {telescope_name}: {messages}"
                        )

                if not valid:
                    self.error_source.append(
                        {
                            "device_type": "Telescope",
                            "device_name": telescope_name,
                            "error": "AsTelOS errors not valid",
                        }
                    )
                    self.logger.error(
                        f"AsTelOS errors invalid for {telescope_name}: {messages}"
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
            self.pause_polls(["Dome", "Telescope", "Focuser"])

            # SPECULOOS EDIT
            self.speculoos_check_and_ack_error()

        if "Dome" in self.config:
            if self.weather_safe and self.error_free:
                # open dome shutter
                if paired_devices is not None:
                    self.monitor_action(
                        "Dome",
                        "ShutterStatus",
                        0,
                        "OpenShutter",
                        device_name=paired_devices["Dome"],
                        log_message=f"Opening Dome shutter of {paired_devices['Dome']}",
                    )
                else:
                    for device_name in self.devices["Dome"]:
                        self.monitor_action(
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
            if self.weather_safe and self.error_free:
                # unpark telescope
                if paired_devices is not None:
                    self.monitor_action(
                        "Telescope",
                        "AtPark",
                        False,
                        "Unpark",
                        device_name=paired_devices["Telescope"],
                        log_message=f"Unparking Telescope {paired_devices['Telescope']}",
                    )
                else:
                    for device_name in self.devices["Telescope"]:
                        self.monitor_action(
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
            self.resume_polls(["Dome", "Telescope", "Focuser"])

            # check if telescope(s) are ready
            start_time = time.time()
            if self.weather_safe and self.error_free:
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
                            self.error_source.append(
                                {
                                    "device_type": "Telescope",
                                    "device_name": telescope_name,
                                    "error": "Timeout waiting for telescope to be ready",
                                }
                            )
                            self.logger.error(
                                f"Timeout waiting for {telescope_name} to be ready"
                            )
                            break

                        if float(r) == 1:
                            self.logger.info(f"{telescope_name} is ready")
                        elif float(r) < 0:
                            self.error_source.append(
                                {
                                    "device_type": "Telescope",
                                    "device_name": telescope_name,
                                    "error": f"Issue with telescope getting ready, status: {r}",
                                }
                            )
                            self.logger.error(
                                f"Issue with telescope getting ready, status: {r}"
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
            self.pause_polls(["Dome", "Telescope", "Focuser"])

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
                    self.stop_guider(device_name)

                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": "Guider",
                            "device_name": device_name,
                            "error": str(e),
                        }
                    )
                    self.logger.error(
                        f"Error stopping telescope {device_name} guiding: {str(e)}"
                    )

                self.monitor_action(
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
                self.monitor_action(
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
                self.monitor_action(
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
                        self.error_source.append(
                            {
                                "device_type": "Telescope",
                                "device_name": telescope_name,
                                "error": "Telescope not parked, cannot close dome during close_observatory method",
                            }
                        )
                        self.logger.error(
                            f"Telescope {telescope_name} not parked, cannot close dome during close_observatory method"
                        )
                        return False
            # park dome
            device_names = (
                [paired_devices["Dome"]] if paired_devices else self.devices["Dome"]
            )
            for device_name in device_names:
                self.monitor_action(
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
                self.monitor_action(
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
            self.resume_polls(["Dome", "Telescope", "Focuser"])

        return True

    def read_schedule(self) -> pd.DataFrame | None:
        """
        Read and process the observatory schedule from CSV file.

        Loads the schedule CSV file and converts it to a pandas DataFrame with
        proper datetime parsing. Automatically reloads the schedule if the file
        has been modified since the last read. Supports schedule truncation for
        development and testing purposes.

        Returns:
            pd.DataFrame or None: A DataFrame containing the schedule data with
                properly parsed 'start_time' and 'end_time' columns, or None if
                an error occurs during reading.

        Features:
        - Automatic file modification detection and reload
        - Datetime parsing for start_time and end_time columns
        - Optional schedule truncation for development (via truncate_factor)
        - Error handling with logging and error source tracking

        File Format:
        - CSV file with columns including start_time, end_time, device_name,
          action_type, and action_value
        - Datetime columns should be in ISO format compatible with pandas

        Note:
            - Schedule is sorted by start_time after loading
            - Truncation moves schedule to current time for testing
            - File modification time is tracked to enable automatic reloading
        """
        # TODO: schedule validity checker

        try:
            schedule_mtime = self.get_schedule_mtime()

            if (schedule_mtime > self.schedule_mtime) or (self.schedule is None):
                if self.schedule_running is True:
                    self.logger.warning(
                        "Schedule updating while the previous schedule is running. This will not take effect until the new schedule is run."
                    )

                self.logger.info("Reading schedule")
                self.schedule_mtime = schedule_mtime

                try:
                    if self.truncate_factor:
                        self.logger.warning(
                            f"Truncating schedule by factor: {self.truncate_factor}"
                        )
                    schedule = process_schedule(
                        self.schedule_path,
                        truncate_factor=self.truncate_factor,
                    )

                    # dump text of schedule to log by reading raw file
                    with open(self.schedule_path, "r") as f:
                        schedule_text = f.read()
                        self.logger.info(f"Schedule read: {schedule_text}")

                    return schedule
                except Exception as e:
                    self.logger.warning(
                        f"Warning: Issue processing schedule: {e}, please try again"
                    )
                    return None
            else:
                return self.schedule

        except Exception as e:
            self.error_source.append(
                {
                    "device_type": "Schedule",
                    "device_name": "",
                    "error": f"Error reading schedule: {e}",
                }
            )
            self.logger.error(f"Error reading schedule: {e}")

    def get_schedule_mtime(self) -> float:
        """
        Get the modification timestamp of the schedule file.

        Retrieves the last modification time of the schedule CSV file to enable
        automatic detection of schedule updates. Returns 0 if the file doesn't
        exist, which can be used to detect when no schedule is available.

        Returns:
            float: The Unix timestamp of the schedule file's last modification
                time, or 0.0 if the file does not exist.

        Note:
            - Used by read_schedule() to detect file changes
            - Enables automatic schedule reloading during operation
            - Returns 0 for non-existent files to simplify logic
        """
        if not self.schedule_path.exists():
            return 0
        else:
            return os.path.getmtime(self.schedule_path)

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
            self.stop_schedule()
        else:
            if self.watchdog_running is False:
                self.logger.warning(
                    "Robotic switch cannot be turned on without watchdog running"
                )
                return

            self.robotic_switch = True
            self.logger.info("Robotic switch turned on")

            if self.schedule_running:
                # stop schedule if running
                self.stop_schedule()

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

        if self.schedule is None:
            self.logger.warning("Schedule not loaded")
            return

        if self.schedule_running:
            self.logger.warning("Schedule already running")
            return

        if self.watchdog_running is False:
            self.logger.warning("Schedule cannot be started without watchdog running")
            return

        if self.schedule.iloc[-1]["end_time"] < datetime.now(UTC):
            self.logger.warning("Schedule end time in the past")
            return

        # check schedule not in threads
        for th in self.threads:
            if th["type"] == "run_schedule":
                self.logger.warning("Schedule currently running")
                return

        # reset completed column on new start
        self.schedule["completed"] = False

        th = Thread(target=self.run_schedule, daemon=True)
        th.start()
        self.threads.append(
            {
                "type": "run_schedule",
                "device_name": "Schedule",
                "thread": th,
                "id": "schedule",
            }
        )

    def stop_schedule(self) -> None:
        """
        Stop the currently running schedule execution thread.

        Safely stops the schedule execution by setting the schedule_running flag
        to False and waiting for the schedule thread to complete. This ensures
        that any ongoing actions can finish cleanly before the schedule stops.

        Process:
        1. Sets schedule_running flag to False (signals thread to stop)
        2. Finds the schedule thread in the threads list
        3. Waits for the thread to complete using join()
        4. Logs the stopping action

        Thread Safety:
        - Uses thread.join() to ensure clean shutdown
        - Schedule thread checks schedule_running flag regularly
        - Ongoing actions are allowed to complete before stopping

        Note:
            - If no schedule is running, logs a warning and returns
            - Essential for emergency stops and robotic switch operations
            - Used when weather becomes unsafe or errors occur
        """

        if self.schedule_running:
            self.schedule_running = False
            self.logger.info("Stopping schedule")
            for th in self.threads:
                if th["type"] == "run_schedule":
                    th["thread"].join()
                    break
        else:
            self.logger.warning("Schedule not running")

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
        self.schedule_running = True
        self.logger.info("Running schedule")

        t0 = time.time()
        while self.weather_safe is None and (time.time() - t0) < 120:
            self.logger.info("Waiting for safety conditions to be checked")
            time.sleep(1)

        if self.weather_safe is None:
            self.error_source.append(
                {
                    "device_type": "SafetyMonitor",
                    "device_name": "",
                    "error": "Weather safety check timed out",
                }
            )
            self.logger.error("Weather safety check timed out")
            return

        while self.schedule_running and self.watchdog_running and self.error_free:
            # loop through self.threads and remove the ones that are dead
            self.threads = [i for i in self.threads if i["thread"].is_alive()]

            # create list of running thread ids
            ids = [k["id"] for k in self.threads]

            # loop through schedule
            for i, row in self.schedule.iterrows():
                # if schedule item not running, start thread if conditions are met
                if (
                    (i not in ids)
                    and self.check_conditions(row)
                    and (row["completed"] is False)
                ):
                    th = Thread(target=self.run_action, args=(row,), daemon=True)
                    th.start()

                    self.threads.append(
                        {
                            "type": row["action_type"],
                            "device_name": row["device_name"],
                            "thread": th,
                            "id": i,
                        }
                    )

                    # wait for thread to finish
                    th.join()

            # exit while loop if reached end of schedule
            if self.schedule.iloc[-1]["end_time"] < datetime.now(UTC):
                break

            time.sleep(1)

        # run headers completion
        th = Thread(target=self.final_headers, daemon=True)
        th.start()
        self.threads.append(
            {
                "type": "Headers",
                "device_name": "astra",
                "thread": th,
                "id": "complete_headers",
            }
        )

        self.schedule_running = False
        self.logger.info("Schedule stopped")

    def run_action(self, row: dict) -> None:
        """
        Execute the action specified in the schedule.

        Parameters:
            row (dict): A dictionary representing the action to be executed, including device and action details.

        Raises:
            ValueError: If the provided action_type is not valid for the specified device.
            Exception: Any unexpected error that occurs during execution.

        Notes:
            - For 'object', 'calibration', or 'flats' action types, specialized sequences are executed based on the action_type.
            - For 'open' action type, the function may turn on camera cooler, set temperature, and open the observatory dome.
            - For 'close' action type, the function may close the observatory dome.
            - For other action types, the function assumes it's an ASCOM command and attempts to execute it on the specified device.

        """

        self.logger.info(f"Starting {row['device_name']} {row['action_type']}")

        try:
            if row["device_name"] in self.devices["Camera"]:
                paired_devices = PairedDevices.from_observatory(
                    observatory=self,
                    camera_name=row["device_name"],
                )
                camera_config = paired_devices.get_device_config("Camera")
                set_temperature = camera_config["temperature"]
                temperature_tolerance = camera_config.get("temperature_tolerance", 1)
                cooling_timeout = camera_config.get("cooling_timeout", 30)

                if row["action_type"] not in ["close", "open"]:
                    self.cool_camera(
                        row, set_temperature, temperature_tolerance, cooling_timeout
                    )

            if not self.check_conditions(row):
                return

            if "object" == row["action_type"]:
                self.image_sequence(row, paired_devices)

            elif "autofocus" == row["action_type"]:
                self.autofocus_sequence(row, paired_devices)

            elif "calibrate_guiding" == row["action_type"]:
                self.guiding_calibration_sequence(row, paired_devices)

            elif "calibration" == row["action_type"]:
                self.image_sequence(row, paired_devices)

            elif "flats" == row["action_type"]:
                self.flats_sequence(row, paired_devices)

            elif "pointing_model" == row["action_type"]:
                self.pointing_model_sequence(row, paired_devices)

            elif "open" == row["action_type"]:
                if "Camera" in self.config:
                    # open dome and unpark telescope
                    self.open_observatory(paired_devices)
                    self.cool_camera(
                        row, set_temperature, temperature_tolerance, cooling_timeout
                    )
                else:
                    # open all dome(s) and unpark telescope(s)
                    self.open_observatory()

            elif "close" == row["action_type"]:
                if "Camera" in self.config:
                    # close dome and park telescope
                    self.close_observatory(paired_devices)
                    self.cool_camera(
                        row, set_temperature, temperature_tolerance, cooling_timeout
                    )
                else:
                    # close all dome(s) and park telescope(s)
                    self.close_observatory()

            elif "cool_camera" == row["action_type"]:
                if "Camera" in self.config:
                    self.cool_camera(
                        row, set_temperature, temperature_tolerance, cooling_timeout
                    )

            elif "complete_headers" == row["action_type"]:
                self.final_headers()

            else:
                self.error_source.append(
                    {
                        "device_type": "Schedule",
                        "device_name": row["device_name"],
                        "error": f"Invalid action_type: {row['device_name']} {row['action_type']} with {row['action_value']} is not a valid method or property for {row['device_type']} {row['device_name']}",
                    }
                )
                self.logger.error(
                    f"Invalid action_type: {row['device_name']} {row['action_type']} with {row['action_value']} is not a valid method or property for {row['device_type']} {row['device_name']}"
                )

            # set 'completed' flag to True if ended under normal conditions
            if self.error_free and self.schedule_running and self.watchdog_running:
                if (
                    row["action_type"]
                    in ["calibration", "close", "cool_camera", "complete_headers"]
                ) or self.weather_safe:
                    self.schedule.loc[row.name, "completed"] = True

            self.logger.info(
                f"{row['action_type']} sequence ended for {row['device_name']}"
            )
            self.logger.info(
                f"{row['action_type']} sequence had a planned start time of {row['start_time']} and end time of {row['end_time']}"
            )

        except Exception as e:
            self.schedule_running = False
            self.error_source.append(
                {
                    "device_type": "Schedule",
                    "device_name": row["device_name"],
                    "error": str(e),
                }
            )
            self.logger.error(
                f"Run action error: {str(e)}", exc_info=True, stack_info=True
            )

    def cool_camera(
        self,
        row: dict,
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
            row (dict): Schedule row containing camera device information,
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
        self.monitor_action(
            "Camera",
            "CoolerOn",
            True,
            "CoolerOn",
            device_name=row["device_name"],
            log_message=f"Turning on camera cooler for {row['device_name']}",
            weather_sensitive=False,
        )

        # set temperature
        self.monitor_action(
            "Camera",
            "CCDTemperature",
            set_temperature,
            "SetCCDTemperature",
            device_name=row["device_name"],
            run_command_type="set",
            abs_tol=temperature_tolerance,
            log_message=f"Setting camera {row['device_name']} temperature to {set_temperature}C with tolerance of {temperature_tolerance}C",
            timeout=60 * cooling_timeout,
            weather_sensitive=False,
        )

    def pre_sequence(
        self, row: dict, paired_devices: dict, create_folder: bool = True
    ) -> tuple[dict, str, dict]:
        """
        Prepare the observatory and metadata for a sequence.

        This method is responsible for preparing the observatory and gathering necessary information
        before running a sequence. Depending on the parameters in the action value in the inputted row,
        it can move the telescope to specificed (ra, dec) coordinates, and the filter wheel to the specified
        filter. It also creates a directory for the sequence images and writes a header with relevant information.

        Parameters:
            row (dict): A dictionary containing information about the sequence action:

                - 'device_name': The name of the device.
                - 'action_type': The type of action (e.g., 'object').
                - 'action_value': The action's value (e.g., a command or parameter).

            paired_devices (dict): A list of paired devices required for the sequence.

        Returns:
            tuple: A tuple containing the following elements:

                - action_value: The evaluated action value.
                - folder (str): The path to the directory where images will be stored.
                - hdr (dict): A header dictionary with relevant information for the sequence.
        """
        if not isinstance(paired_devices, PairedDevices):
            paired_devices = PairedDevices.from_observatory(
                observatory=self,
                paired_device_names=paired_devices,
            )

        self.logger.debug(
            f"Running pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}"
        )

        action_value: dict = row["action_value"]

        # prepare observatory for sequence
        self.setup_observatory(paired_devices, action_value)

        # write base header
        hdr = self.base_header(paired_devices, action_value)

        # create image directory
        if create_folder:
            folder = create_image_dir(
                self.schedule.iloc[0]["start_time"],
                hdr.get("LONG-OBS"),
                action_value.get("dir"),
            )
        else:
            folder = None

        if "object" == row["action_type"]:
            hdr["IMAGETYP"] = "Light Frame"
        elif "flats" == row["action_type"]:
            if self.speculoos:
                hdr["IMAGETYP"] = "FLAT"
            else:
                hdr["IMAGETYP"] = "Flat Frame"

        self.logger.debug(
            f"Finished pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}"
        )

        return action_value, folder, hdr

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
            ("ra" in action_value)
            and ("dec" in action_value)
            and self.check_conditions()
        ):
            if "Telescope" in paired_devices:
                # open dome and unpark telescope -- this will open all domes if not in paired_devices...?
                self.open_observatory(paired_devices)

                telescope = paired_devices.telescope
                telescope_name = paired_devices["Telescope"]

                if self.check_conditions():
                    # set tracking to true
                    self.monitor_action(
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
            "filter" in action_value
            and "FilterWheel" in paired_devices
            and self.error_free
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
            self.monitor_action(
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
                "focus_shift" in action_value
                or "focus_position" in action_value
                or filter_focus_shift
            )
            and ("Focuser" in paired_devices)
            and self.error_free
        ):
            self.logger.info(
                f"Defocusing Focuser {paired_devices['Focuser']} with {action_value}."
            )

            defocuser = Defocuser(
                astra=self,
                paired_devices=paired_devices,
            )

            if "focus_position" in action_value:
                new_focus_position = action_value["focus_position"]
            elif "focus_shift" in action_value:
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
                astra=self,
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

    def check_conditions(self, row: dict | None = None) -> bool:
        """
        Check if current conditions allow safe execution of observatory operations.

        Evaluates multiple safety and operational conditions to determine if
        an action or sequence can proceed safely. Different action types have
        different condition requirements based on their weather sensitivity.

        Parameters:
            row (dict, optional): Schedule row containing action information.
                If None, checks only base conditions (error_free, schedule_running,
                watchdog_running). Defaults to None.

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

        base_conditions = (
            self.error_free and self.schedule_running and self.watchdog_running
        )

        if row is None:
            return base_conditions and self.weather_safe

        time_conditions = row["start_time"] <= datetime.now(UTC) <= row["end_time"]

        if row["action_type"] in [
            "open",
            "object",
            "flats",
            "autofocus",
            "calibrate_guiding",
            "pointing_model",
        ]:
            return base_conditions and time_conditions and self.weather_safe
        elif row["action_type"] in [
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
        sequence_idx,
        maxadu,
        row,
        hdr,
        folder,
        use_light=True,
        log_option=None,
        maximal_sleep_time=0.1,
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
            row (dict): Schedule row containing action information.
            hdr (fits.Header): FITS header dictionary for the image.
            folder (Path): Directory path for saving the image.
            use_light (bool, optional): Whether to use light during exposure.
                False for dark frames. Defaults to True.
            log_option (str, optional): Additional text for logging messages.
                Defaults to None.
            maximal_sleep_time (float, optional): Maximum sleep interval during
                image ready polling. Defaults to 0.1 seconds.
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

        # fill header parameters
        hdr["EXPTIME"] = exptime

        if row["action_type"] == "calibration":
            if exptime == 0:
                hdr["IMAGETYP"] = "Bias Frame"
            else:
                hdr["IMAGETYP"] = "Dark Frame"

            use_light = False

        elif row["action_type"] == "object":
            hdr["IMAGETYP"] = "Light Frame"

        # Log information about the exposure
        log_option_tmp = "" if log_option is None else f"{log_option} "
        self.logger.info(
            f"Exposing {log_option_tmp}{row['device_name']} {hdr['IMAGETYP']} for exposure time {hdr['EXPTIME']:.3f} s"
        )

        # Start exposure
        exposure_start_time = time.time()
        exposure_end_time = time.time()
        camera.get("StartExposure", Duration=exptime, Light=use_light)

        # Wait for the image to be ready
        exposure_successful = True

        while not camera.get("ImageReady"):
            if not self.check_conditions(row):
                exposure_successful = False
                break

            if (exposure_end_time - exposure_start_time) > 3 * exptime + 180:
                self.logger.error(
                    f"Exposure timed out after 3*{exptime:.3f} + 180 seconds for {row['device_name']}."
                )
                self.error_source.append(
                    {
                        "device_type": "Camera",
                        "device_name": row["device_name"],
                        "error": f"Exposure timed out after 3*{exptime:.3f} + 180 seconds",
                    }
                )
                exposure_successful = False

            time.sleep(min(maximal_sleep_time, exptime / 10))
            exposure_end_time = time.time()

        if not exposure_successful:
            self.logger.warning("Last exposure was not completed successfully.")
            filepath = None
            # if error_free is True, abort exposure
            if self.error_free:
                camera.get("AbortExposure")()  # check
        else:
            # get last exposure information
            last_exposure_start_time = camera.get("LastExposureStartTime")
            dateobs = pd.to_datetime(last_exposure_start_time)

            # get image array and info
            image = camera.get("ImageArray")
            image_info = camera.get("ImageArrayInfo")

            # save image
            filepath = save_image(
                image,
                image_info,
                maxadu,
                hdr,
                camera.device_name,
                dateobs,
                sequence_idx,
                folder,
                self.save_config,
                wcs,
            )

            self.logger.info(
                f"Image saved as {os.path.basename(filepath)}. "
                f"Acquired in {(time.time() - exposure_end_time):.3f} s after ImageReady was True, "
                f"and {(time.time() - exposure_start_time - exptime):.3f} s after exposure integration should have ended."
            )

            self.last_image = filepath

            ## add to database
            dt = dateobs.strftime("%Y-%m-%d %H:%M:%S.%f")
            self.cursor.execute(
                f"INSERT INTO images VALUES ('{filepath}', '{camera.device_name}', '{0}', '{dt}')"
            )

        return exposure_successful, filepath

    def image_sequence(self, row: dict, paired_devices: PairedDevices) -> None:
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
            f"Running {row['action_type']} sequence for {row['device_name']}, "
            f"starting {row['start_time']} and ending {row['end_time']}"
        )

        action_value, folder, hdr = self.pre_sequence(row, paired_devices)

        camera = paired_devices.camera
        maxadu = camera.get("MaxADU")

        if row["action_type"] == "calibration":
            exptime_list = action_value["exptime"]
            n_exposures_list = action_value["n"]
        else:
            exptime_list = [action_value["exptime"]]
            if "n" in action_value:
                n_exposures_list = [int(action_value["n"])]
            else:
                n_exposures_list = [int(1e6)]  # hacky

        pointing_complete = False
        pointing_attempts = 0
        guiding = False
        wcs_solve = None

        for i, exptime in enumerate(exptime_list):
            if not self.check_conditions(row):
                break

            n_exposures = n_exposures_list[i]

            for exposure in range(n_exposures):
                if "n" in action_value:
                    log_option = f"{exposure + 1}/{n_exposures}"
                else:
                    log_option = None

                if not self.check_conditions(row):
                    break

                success, filepath = self.perform_exposure(
                    camera,
                    exptime,
                    exposure,
                    maxadu,
                    row,
                    hdr,
                    folder,
                    log_option=log_option,
                    wcs=wcs_solve,
                )

                if not success:
                    break

                # pointing correction if not already done
                if action_value.get("pointing") and pointing_complete is False:
                    pointing_complete, wcs_solve = self.pointing_correction(
                        row,
                        action_value,
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
                            hdul[0].header.update(wcs_solve.to_header())
                            hdul.flush()

                    if pointing_complete is False:
                        wcs_solve = (
                            None  # to not contaminate the next image if pointing fails
                        )

                    if pointing_attempts > 3 and pointing_complete is False:
                        self.logger.warning(
                            f"Pointing correction for {action_value['object']} with "
                            f"{row['device_name']} failed after {pointing_attempts} attempts"
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
                    guiding = self.start_guider(
                        row, action_value, folder, paired_devices
                    )

        # stop guiding at end of sequence
        if action_value.get("guiding", False):
            self.stop_guider(paired_devices["Telescope"])

        # stop telescope tracking at end of sequence
        if "Telescope" in paired_devices:
            self.monitor_action(
                "Telescope",
                "Tracking",
                False,
                "Tracking",
                device_name=paired_devices["Telescope"],
                log_message=f"Stopping telescope {paired_devices['Telescope']} tracking",
            )

    def pointing_model_sequence(self, row: dict, paired_devices: dict) -> None:
        """
        Execute a pointing model sequence to improve telescope pointing accuracy.

        Generates a systematic series of sky positions and captures images at each
        location to build or refine a telescope pointing model. The sequence creates
        a spiral pattern of points from zenith down to a specified altitude, avoiding
        positions too close to the Moon.

        Parameters:
            row (dict): Schedule row containing sequence information:
                - 'device_name': Name of the camera device
                - 'action_type': Should be 'pointing_model'
                - 'start_time', 'end_time': Sequence timing
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

        self.logger.info(
            f"Running {row['action_type']} sequence for {row['device_name']}, "
            f"starting {row['start_time']} and ending {row['end_time']}"
        )

        action_value, folder, hdr = self.pre_sequence(
            row, paired_devices, create_folder=True
        )

        # number of points
        N = action_value.get("n", 100)

        # set exptime to 1 if not specified
        exptime = action_value.get("exptime", 1)  # default to 1 second

        # get camera
        camera = self.devices["Camera"][row["device_name"]]
        maxadu = camera.get("MaxADU")

        # find dark frame
        dark_frame = None
        if action_value.get("dark_subtraction", False):

            self.logger.info(
                f"Dark subtraction enabled. Looking for matching dark frame for {row['device_name']}"
                f" with exposure time {exptime} s in {folder}"
            )

            darks = list(Path(folder).glob(f"*Dark Frame_{exptime:.3f}*.fits"))

            if len(darks) > 0:
                dark_frame = darks[-1]
                self.logger.info(f"Using dark frame {dark_frame}")
            else:
                self.logger.warning(
                    f"No dark frame found in {folder}. Dark subtraction will not be applied."
                )

        # get location
        obs_lat = hdr["LAT-OBS"]
        obs_lon = hdr["LONG-OBS"]
        obs_alt = hdr["ALT-OBS"]
        obs_location = EarthLocation(
            lat=obs_lat * u.deg, lon=obs_lon * u.deg, height=obs_alt * u.m
        )
        MOON_LIMIT = 20 * u.deg  # pointing distance to the moon in degrees

        # create pointing_model folder
        date_str = (row["start_time"] + timedelta(hours=obs_lon / 15)).strftime(
            "%Y%m%d"
        )
        folder = CONFIG.paths.images / "pointing_model" / date_str
        folder.mkdir(exist_ok=True)
        self.logger.info(f"{folder} created for pointing model images")

        # Generate points (spiral from zenith to 30 deg above horizon)
        num_turns = np.sqrt(N / 2)
        t_linear = np.linspace(0, 1, N)  # Generate base points
        ts = t_linear**0.5  # increase spacing towards zenith
        t_shift = 0

        # update header
        hdr["IMAGETYP"] = "Light Frame"
        hdr["OBJECT"] = "Pointing Model"
        action_value["object"] = "Pointing Model"

        # open dome and unpark telescope
        self.open_observatory(paired_devices)

        counter = 0
        while counter < N and self.check_conditions(row):
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
                alt=alt * u.deg,
                az=az * u.deg,
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
            action_value["ra"] = target_radec.ra.deg
            action_value["dec"] = target_radec.dec.deg
            self.setup_observatory(paired_devices, action_value)

            if self.speculoos:
                # wait for telescope to settle
                time.sleep(exptime * 3)  # for spirit

            # perform exposure
            success, filepath = self.perform_exposure(
                camera,
                exptime,
                counter,
                maxadu,
                row,
                hdr,
                folder,
                log_option=None,
            )

            if not success:
                break

            # pointing correction, sync and no slew
            pointing_complete, wcs_solve = self.pointing_correction(
                row,
                action_value,
                filepath,
                paired_devices,
                dark_frame=dark_frame,
                sync=True,
                slew=False,
            )

            # update header with wcs
            if wcs_solve is not None:
                with fits.open(filepath, mode="update") as hdul:
                    hdul[0].header.update(wcs_solve.to_header())
                    hdul.flush()

            wcs_solve = None

            counter += 1

    def pointing_correction(
        self,
        row: dict,
        action_value: dict,
        filepath: str,
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
            row (dict): Schedule row containing action information and device details.
            action_value (dict): Action parameters including target coordinates:
                - 'ra': Target right ascension in degrees
                - 'dec': Target declination in degrees
                - 'object': Object name for logging
            filepath (str): Path to the FITS image file for plate solving.
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
        self.logger.info(
            f"Running pointing correction for {action_value['object']} with {row['device_name']}"
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
                f"{pointing_corrector_handler.image_star_mapping.number_of_matched_stars()} stars matched, "
                f"meeting the Astra requirement of at least {np.floor(number_of_stars_to_use * 0.8):.0f} stars (80% of {number_of_stars_to_use})."
            )

        except Exception as e:
            self.logger.warning(
                f"Failed running pointing correction for {action_value['object']}"
                f" with {row['device_name']}: {str(e)}"
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
        self.logger.info(f"RA shift: {pointing_correction.offset_ra}")
        self.logger.info(f"DEC shift: {pointing_correction.offset_dec}")

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

    def start_guider(
        self, row: dict, action_value: dict, folder: str, paired_devices: PairedDevices
    ) -> bool:
        """
        Start the autoguiding system for telescope tracking.

        Initializes and starts the guiding system to maintain accurate telescope
        tracking during long exposures. Creates a separate thread for guiding
        operations to run concurrently with image acquisition.

        Parameters:
            row (dict): Schedule row containing action information and device details.
            action_value (dict): Action parameters including:
                - 'filter': Filter name for guiding (single quotes are removed)
                - Other guiding configuration parameters
            folder (str): Directory path for saving guiding-related files.
            paired_devices (PairedDevices): Object containing telescope and guide
                camera devices for the guiding system.

        Returns:
            bool: True if guider was started successfully, False otherwise.

        Process:
        1. Logs guiding start for the specified telescope
        2. Extracts and cleans filter name from action parameters
        3. Creates guider thread with appropriate parameters
        4. Starts the guiding thread in background
        5. Adds thread to observatory's thread tracking list

        Note:
            - Guiding runs in a separate thread to avoid blocking main operations
            - Thread is tracked in self.threads for proper cleanup
            - Filter name formatting removes single quotes for compatibility
        """
        self.logger.info(f"Starting guiding for {paired_devices['Telescope']}")


        # build filename from pattern
        if 'raw_filename_pattern_glob' in self.save_config:
            # read filename config from config file
            filename_pattern = self.save_config['raw_filename_pattern_glob']
        else:
            # use default filename
            filename_pattern = '{device}_{filter_clean}_{exptime:.3f}_{timestamp}.fits'

        # get values to insert into pattern
        filter_name_clean = action_value['filter'].replace("'", "")
        filter_name = action_value['filter']
        exptime = action_value['exptime']
        obj_name = action_value['object']
        device_name = row['device_name']

        # fill in pattern
        # this is used as a glob to find image files, so fill in with "*"
        filename = filename_pattern.format(device=device_name,
                                           filter_orig=filter_name,
                                           filter_clean=filter_name_clean,
                                           object_name=obj_name,
                                           exptime=exptime,
                                           imagetype="*",
                                           timestamp="*",
                                           timestamp_date="*",
                                           timestamp_time="*",
                                           sequence="*")
        # create path
        filepath = Path('..') / 'images' / folder / filename

        # get glob as string
        glob_str = str(filepath)

        binning = paired_devices.camera.get("BinX")

        th = Thread(
            target=self.guider[paired_devices["Telescope"]].guider_loop,
            args=(
                paired_devices["Camera"],
                glob_str,
                action_value["exptime"] * 2,
                binning,
            ),
            daemon=True,
        )
        th.start()

        self.threads.append(
            {
                "type": "guider",
                "device_name": paired_devices["Telescope"],
                "thread": th,
                "id": "guider",
            }
        )

        return True

    def stop_guider(self, device_name: str) -> bool:
        """
        Stop the autoguiding system for a specific telescope.

        This function finds the correct guider thread using the telescope's
        device name, sets its running flag to False, and then waits for the
        thread to terminate.

        Parameters:
            device_name (str): The device name of the telescope whose guider
                            should be stopped.

        Returns:
            bool: True if the guider was stopped successfully, False otherwise.
        """

        # Find the guider thread in the list of running threads
        for i, thread_info in enumerate(self.threads):
            if (
                thread_info["type"] == "guider"
                and thread_info["device_name"] == device_name
            ):
                # Get the guider instance and set its running flag to False
                guider_instance = self.guider[device_name]

                if guider_instance.running:
                    self.logger.info(f"Stopping guiding for {device_name}")
                    guider_instance.running = False

                    # Wait for the thread to finish
                    thread_info["thread"].join()

                    # Remove the thread from the list
                    self.logger.info(f"Guiding for {device_name} stopped successfully.")
                    return True

        return False

    def guiding_calibration_sequence(self, row, paired_devices: PairedDevices) -> bool:
        """
        Perform autoguiding calibration to establish guide star movements.

        Executes a calibration sequence that maps the relationship between guide
        commands and resulting star movements on the guide camera. This calibration
        is essential for accurate autoguiding performance.

        Parameters:
            row (dict): Schedule row containing calibration sequence information:
                - 'device_name': Name of the camera device
                - 'action_type': Should be 'calibrate_guiding'
                - 'action_value': Calibration parameters and settings
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
        self.logger.info(f"Running guiding calibration for {row['device_name']}")
        try:
            action_value, _, hdr = self.pre_sequence(row, paired_devices)
            if not self.check_conditions(row=row):
                return False

            guiding_calibrator = GuidingCalibrator(
                astra_observatory=self,
                row=row,
                paired_devices=paired_devices,
                action_value=action_value,
                hdr=hdr,
            )
            guiding_calibrator.slew_telescope_one_hour_east_of_sidereal_meridian()
            guiding_calibrator.perform_calibration_cycles()
            guiding_calibrator.complete_calibration_config()
            guiding_calibrator.save_calibration_config()
            guiding_calibrator.update_observatory_config()

            self.logger.info(f"Guiding calibration for {row['device_name']} completed")
            success = True

        except Exception as e:
            success = False
            self.logger.exception(
                f"Error running guiding calibration for {row['device_name']}. Exception {str(e)}"
            )
            self.error_source.append(
                {
                    "device_type": "Camera",
                    "device_name": row["device_name"],
                    "error": f"Error running guiding calibration for {row['device_name']}",
                }
            )

        return success

    def autofocus_sequence(self, row, paired_devices: PairedDevices) -> bool:
        """
        Perform autofocus sequence to achieve optimal telescope focus.

        Executes an automated focusing routine that systematically tests different
        focus positions to find the optimal focus setting. Uses star analysis
        to measure focus quality and determine the best focus position.

        Parameters:
            row (dict): Schedule row containing autofocus sequence information:
                - 'device_name': Name of the camera device
                - 'action_type': Should be 'autofocus'
                - 'action_value': Autofocus parameters and settings
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
        self.logger.info(f"Running autofocus for {row['device_name']}")
        try:
            action_value, _, hdr = self.pre_sequence(row, paired_devices)
            if not self.check_conditions(row=row):
                return False

            autofocuser = Autofocuser(
                astra=self,
                row=row,
                paired_devices=paired_devices,
                action_value=action_value,
                hdr=hdr,
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
            success = False
            self.logger.exception(
                f"Error running autofocus for {row['device_name']}. Exception {str(e)}"
            )
            self.error_source.append(
                {
                    "device_type": "Camera",
                    "device_name": row["device_name"],
                    "error": f"Error running autofocus for {row['device_name']}",
                }
            )

        return success

    def flats_sequence(self, row: dict, paired_devices: dict) -> None:
        """
        Execute a flat field calibration sequence during twilight.

        Captures flat field images during astronomical twilight when the sky
        provides near-uniform illumination. Automatically manages telescope positioning,
        exposure timing, and filter changes to create comprehensive flat field
        libraries for image calibration.

        Parameters:
            row (dict): Schedule row containing flat sequence information:
                - 'device_name': Name of the camera device
                - 'action_type': Should be 'flats'
                - 'action_value': Flat sequence parameters including filters and targets
                - 'start_time', 'end_time': Sequence timing window
            paired_devices (dict): Dictionary of paired devices including telescope,
                camera, filter wheel, and dome for the sequence.


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

        self.logger.info(
            f"Running flats sequence for {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}"
        )

        # creates folder for images, writes base header, and sets filter to first filter in list
        action_value, folder, hdr = self.pre_sequence(row, paired_devices)

        # camera device
        camera = self.devices["Camera"][row["device_name"]]

        # target adu and camera offset needed for flat exposure time calculation
        cam_index = self.get_cam_index(row["device_name"])
        config_target_adu = self.config["Camera"][cam_index]["flats"]["target_adu"]
        config_target_adu_tolerance = self.config["Camera"][cam_index]["flats"].get(
            "target_adu_tolerance", config_target_adu * 0.2
        )
        target_adu = [config_target_adu, config_target_adu_tolerance]
        offset = self.config["Camera"][cam_index]["flats"]["bias_offset"]
        lower_exptime_limit = self.config["Camera"][cam_index]["flats"][
            "lower_exptime_limit"
        ]
        upper_exptime_limit = self.config["Camera"][cam_index]["flats"][
            "upper_exptime_limit"
        ]

        # camera max adu
        maxadu = camera.get("MaxADU")

        # camera orignal framing
        numx = camera.get("NumX")
        numy = camera.get("NumY")
        startx = camera.get("StartX")
        starty = camera.get("StartY")

        # get location to determine if sun is up
        obs_lat = hdr["LAT-OBS"]
        obs_lon = hdr["LONG-OBS"]
        obs_alt = hdr["ALT-OBS"]
        obs_location = EarthLocation(
            lat=obs_lat * u.deg, lon=obs_lon * u.deg, height=obs_alt * u.m
        )

        # wait for sun to be in right position
        sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)
        self.logger.info(
            f"Sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
        )

        if self.check_conditions(row) and (take_flats is False):
            self.logger.info(
                f"Not the right time to take flats for {row['device_name']}, sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
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
                f"Waiting min. {time_to_wait:.2f} minutes for sun to be in right position for {row['device_name']}"
            )

        while self.check_conditions(row) and (take_flats is False):
            sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

            if take_flats is False:
                time.sleep(1)

        # start taking flats
        for i, filter_name in enumerate(action_value["filter"]):
            count = 0
            sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

            if self.check_conditions(row) and take_flats:
                ## initial setup + exposure setting
                # sets filter (and focus, soon...)
                self.setup_observatory(
                    paired_devices, action_value, filter_list_index=i
                )

                # opens dome and move telescope to flat position
                self.flats_position(obs_location, paired_devices, row)

                # establishing initial exposure time
                exptime = self.flats_exptime(
                    obs_location,
                    paired_devices,
                    row,
                    numx,
                    numy,
                    startx,
                    starty,
                    target_adu,
                    offset,
                    lower_exptime_limit,
                    upper_exptime_limit,
                )

                if exptime < lower_exptime_limit or exptime > upper_exptime_limit:
                    self.logger.info("Moving on...")
                    continue

                hdr["EXPTIME"] = exptime
                hdr["FILTER"] = filter_name

                while self.check_conditions(row) and (count < action_value["n"][i]):
                    log_option = f"{count + 1}/{action_value['n'][i]}"

                    success, filepath = self.perform_exposure(
                        camera,
                        exptime,
                        count, 
                        maxadu,
                        row,
                        hdr,
                        folder,
                        log_option=log_option,
                    )

                    if not success:
                        break
                    else:
                        # move telescope to flat position
                        self.flats_position(obs_location, paired_devices, row)

                        with fits.open(filepath) as hdul:
                            data = hdul[0].data
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
                                        f"Exposure time of {exptime:.3f} s out of user defined range of {lower_exptime_limit} s to {upper_exptime_limit} s"
                                    )
                                    break
                                else:
                                    self.logger.info(
                                        f"Setting new exposure time to {exptime:.3f} s as median ADU of {median_adu} is not within {target_adu[1]} of {target_adu[0]}"
                                    )

                        hdr["EXPTIME"] = exptime

                        count += 1

            else:
                if take_flats is False:
                    self.logger.info(
                        f"Not the right time to take flats for {row['device_name']}, sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
                    )

                self.logger.info("Moving on...")
                break

    def flats_position(
        self, obs_location: EarthLocation, paired_devices: dict, row: dict
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
            row (dict): Schedule row containing sequence information:
                - 'action_value': Flat sequence parameters
                - 'device_name': Name of the camera device
                - Other timing and configuration parameters

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

        if "Telescope" in paired_devices:
            # check if ready to take flats
            take_flats = False
            while self.check_conditions(row) and (take_flats is False):
                sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

                if take_flats is False:
                    time.sleep(1)

            if self.check_conditions(row) and take_flats:
                target_altaz = SkyCoord(
                    alt=75 * u.deg,
                    az=sun_altaz.az + 180 * u.degree,
                    frame=AltAz(obstime=Time.now(), location=obs_location),
                )

                target_radec = target_altaz.transform_to("icrs")

                action_value = {}
                action_value["ra"] = target_radec.ra.deg
                action_value["dec"] = target_radec.dec.deg

                # move telescope to target
                self.setup_observatory(paired_devices, action_value)

    def flats_exptime(
        self,
        obs_location: EarthLocation,
        paired_devices: dict,
        row: dict,
        numx: int,
        numy: int,
        startx: int,
        starty: int,
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
            row (dict): A dictionary containing timing information for the flat field calibration.
            numx (int): The original number of pixels in the X-axis of the camera sensor.
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
        if exptime is None and sun_rising is False:
            exptime = lower_exptime_limit
        elif exptime is None and sun_rising is True:
            exptime = upper_exptime_limit

        if ("Camera" in paired_devices) and self.check_conditions(row) and take_flats:
            camera = self.devices["Camera"][paired_devices["Camera"]]

            # set camera to view small area to speed up read times, such to determine right exposure time (assuming detector is bigger than 64x64)
            # self.monitor_action('Camera', 'NumX', 64, 'NumX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumX to 64")
            # time.sleep(1)
            # self.monitor_action('Camera', 'NumY', 64, 'NumY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumY to 64")
            # time.sleep(1)
            # self.monitor_action('Camera', 'StartX', int(numx/2 - 32), 'StartX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartX to {int(numx/2 - 32)}")
            # time.sleep(1)
            # self.monitor_action('Camera', 'StartY', int(numy/2 - 32), 'StartY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartY to {int(numy/2 - 32)}")

            time.sleep(1)  # wait for camera to settle

            self.logger.info(
                f"Exposing full frame of {paired_devices['Camera']} for exposure time {exptime} s"
            )
            camera.get("StartExposure", Duration=exptime, Light=True)

            getting_exptime = True
            while self.check_conditions(row) and getting_exptime:
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

                        if exptime > upper_exptime_limit:
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

                        elif exptime < lower_exptime_limit:
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

            # set camera back to original framing
            # self.monitor_action('Camera', 'StartX', startx, 'StartX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartX to {startx}")
            # time.sleep(1)
            # self.monitor_action('Camera', 'StartY', starty, 'StartY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} StartY to {starty}")
            # time.sleep(1)
            # self.monitor_action('Camera', 'NumX', numx, 'NumX',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumX to {numx}")
            # time.sleep(1)
            # self.monitor_action('Camera', 'NumY', numy, 'NumY',
            #                     device_name = paired_devices['Camera'],
            #                     log_message = f"Setting Camera {paired_devices['Camera']} NumY to {numy}")

            time.sleep(1)  # wait for camera to settle

        return exptime

    def get_cam_index(self, cam: str) -> int:
        """
        Get the index of a camera device in the configuration file.

        Parameters:
            cam (str): The name of the camera device.

        Returns:
            int: The index of the camera device in the configuration file.

        """
        try:
            cam_index = [
                i
                for i, d in enumerate(self.config["Camera"])
                if d["device_name"] == cam
            ][0]
        except IndexError:
            cam_index = 0
            self.logger.error(
                f"Camera {cam} read from schedule could not be found in the observatory config."
            )
        return cam_index

    def base_header(
        self, paired_devices: PairedDevices, action_value: dict
    ) -> fits.Header:
        """
        Create a base FITS header with observatory and observation information.

        Constructs a comprehensive FITS header containing fixed observatory parameters,
        current device status, astronomical coordinates, and observation metadata.
        The header is built from the FITS configuration file and current system state.

        Parameters:
            paired_devices (PairedDevices): Object containing the devices being used
                for the current observation sequence.
            action_value (dict): Dictionary containing observation parameters from
                the schedule, including target coordinates, filters, and settings.

        Returns:
            fits.Header: A complete FITS header object containing:
                - Fixed observatory information (location, instrument details)
                - Current astronomical conditions (coordinates, time)
                - Device-specific parameters (telescope pointing, filter position)
                - Observation metadata (object name, exposure settings)

        Header Categories:
            - astra: Observatory and software version information
            - astropy_default: Standard astronomical coordinate systems
            - Device-specific: Current status from telescopes, cameras, etc.

        Note:
            - Some header values are populated from real-time device polling
            - Coordinate transformations are performed for various reference frames
            - Observatory location and timing information is automatically included
        """

        self.logger.info("Creating base header")

        hdr = fits.Header()
        for i, row in self.fits_config.iterrows():
            if row["device_type"] == "astra" and row["fixed"] is True:
                # custom headers
                if row["header"] == "FILTER":
                    device = paired_devices.filter_wheel
                    pos = device.get("Position")
                    names = device.get("Names")
                    hdr[row["header"]] = (names[pos], row["comment"])
                elif row["header"] == "XPIXSZ":
                    device = paired_devices.camera
                    binx = device.get("BinX")
                    xpixsize = device.get("PixelSizeX")
                    hdr[row["header"]] = (binx * xpixsize, row["comment"])
                elif row["header"] == "YPIXSZ":
                    device = paired_devices.camera
                    biny = device.get("BinY")
                    ypixsize = device.get("PixelSizeY")
                    hdr[row["header"]] = (biny * ypixsize, row["comment"])
                elif row["header"] == "APTAREA":
                    device = paired_devices.telescope
                    val = device.get("ApertureArea") * 1e6
                    hdr[row["header"]] = (val, row["comment"])
                elif row["header"] == "APTDIA":
                    device = paired_devices.telescope
                    val = device.get("ApertureDiameter") * 1e3
                    hdr[row["header"]] = (val, row["comment"])
                elif row["header"] == "FOCALLEN":
                    device = paired_devices.telescope
                    val = device.get("FocalLength") * 1e3
                    hdr[row["header"]] = (val, row["comment"])
                elif row["header"] == "OBJECT":
                    if row["header"].lower() in action_value:
                        hdr[row["header"]] = (
                            action_value[row["header"].lower()],
                            row["comment"],
                        )
                elif row["header"] in ["EXPTIME", "IMAGETYP"]:
                    hdr[row["header"]] = (None, row["comment"])
                elif row["header"] == "ASTRA":
                    hdr[row["header"]] = (ASTRA_VER, row["comment"])
                else:
                    self.logger.warning(f"Unknown header: {row['header']}")

            elif (
                row["device_type"]
                not in ["astropy_default", "astra", "astra_fixed", ""]
            ) and row["fixed"] is True:
                # direct ascom command headers
                device_type = row["device_type"]

                if device_type in self.devices:
                    device_name = paired_devices[device_type]
                    device = self.devices[device_type][device_name]

                    val = device.get(row["device_command"])

                    hdr[row["header"]] = (val, row["comment"])

            elif row["device_type"] == "astra_fixed":
                # fixed headers, ensure datatype
                try:
                    if row["dtype"] == "float":
                        hdr[row["header"]] = (
                            float(row["device_command"]),
                            row["comment"],
                        )
                    elif row["dtype"] == "int":
                        hdr[row["header"]] = (
                            int(row["device_command"]),
                            row["comment"],
                        )
                    elif row["dtype"] == "str":
                        hdr[row["header"]] = (
                            str(row["device_command"]),
                            row["comment"],
                        )
                    elif row["dtype"] == "bool":
                        hdr[row["header"]] = (
                            bool(row["device_command"]),
                            row["comment"],
                        )
                    else:
                        hdr[row["header"]] = (row["device_command"], row["comment"])
                        self.logger.error(f"Unknown data type: {row['dtype']}")
                except ValueError:
                    self.error_source.append(
                        {
                            "device_type": "Headers",
                            "device_name": "",
                            "error": "ValueError",
                        }
                    )
                    self.logger.error(f"Invalid value for data type: {row}")

        self.logger.info("Base header created")

        return hdr

    def final_headers(self) -> None:
        """
        Complete FITS headers with interpolated device data.

        Post-processes captured images by adding dynamic header information that
        wasn't available at exposure time. Uses polled device data to interpolate
        accurate values for each image timestamp, ensuring complete and accurate
        FITS headers for scientific analysis.

        The process:
        1. Retrieves incomplete images from the database
        2. Groups images by camera for efficient processing
        3. Queries polled device data around image timestamps
        4. Interpolates device values to exact exposure times
        5. Updates FITS files with complete headers
        6. Marks images as header-complete in database

        Key Features:
        - Time-interpolated device values for precise timestamps
        - Handles multiple cameras and device types simultaneously
        - Preserves original headers while adding missing information
        - Robust error handling with detailed logging

        Data Sources:
        - Device polling data from database
        - FITS configuration file for header mapping
        - Original image FITS headers for timing information

        Error Handling:
        - Individual image failures don't stop batch processing
        - All errors are logged and added to error_source
        - Database consistency maintained even with partial failures

        Note:
            - Typically run after observation sequences complete
            - Critical for ensuring complete scientific metadata
            - May take significant time for large image sets
        """

        try:
            self.logger.info("Completing headers")
            # get images from sql
            rows = self.cursor.execute("SELECT * FROM images WHERE complete_hdr = 0;")
            df_images = pd.DataFrame(
                rows, columns=["filepath", "camera_name", "complete_hdr", "date_obs"]
            )

            if df_images.empty:
                self.logger.info("No headers to complete, as there are no images.")
                return

            # loop through cameras (usually just one)
            for cam in df_images["camera_name"].unique():
                # filter image dataframe by camera
                df_images_filt = df_images[df_images["camera_name"] == cam]

                # get paired devices for camera
                cam_index = self.get_cam_index(cam)
                paired_devices = self.config["Camera"][cam_index]["paired_devices"]
                paired_devices["Camera"] = cam

                # convert date_obs to datetime type, sort by date_obs, and convert to jd
                df_images_filt["date_obs"] = pd.to_datetime(
                    df_images_filt["date_obs"], format="%Y-%m-%d %H:%M:%S.%f"
                )
                df_images_filt = df_images_filt.sort_values(by="date_obs").reset_index(
                    drop=True
                )
                df_images_filt["jd_obs"] = (
                    df_images_filt["date_obs"].apply(utils.to_jd).sort_values()
                )

                # add small time increment to avoid duplicate jd, this adds 0.0864 ms to each image that has duplicate jd_obs
                while df_images_filt["jd_obs"].duplicated().sum() > 0:
                    df_images_filt["jd_obs"] = df_images_filt["jd_obs"].mask(
                        df_images_filt["jd_obs"].duplicated(),
                        df_images_filt["jd_obs"] + 1e-9,
                    )

                df_images_filt = df_images_filt.sort_values(by="jd_obs").reset_index()

                # get polled data from ascom devices +- 10 seconds of first and last image
                t0 = pd.to_datetime(df_images_filt["date_obs"].iloc[0]) - pd.Timedelta(
                    "10 sec"
                )
                t1 = pd.to_datetime(df_images_filt["date_obs"].iloc[-1]) + pd.Timedelta(
                    "10 sec"
                )

                q = f"""SELECT * FROM polling WHERE datetime BETWEEN "{str(t0)}" AND "{str(t1)}";"""
                rows = self.cursor.execute(q)
                df_poll = pd.DataFrame(
                    rows,
                    columns=[
                        "device_type",
                        "device_name",
                        "device_command",
                        "device_value",
                        "datetime",
                    ],
                )
                df_poll["jd"] = pd.to_datetime(
                    df_poll["datetime"], format="%Y-%m-%d %H:%M:%S.%f"
                ).apply(utils.to_jd)

                # find unique headers in polled commands
                df_poll_unique = df_poll[
                    ["device_type", "device_name", "device_command"]
                ].drop_duplicates()

                # drop row that have device_type and device_command that are not in fits_config to avoid errors later
                df_poll_unique = df_poll_unique[
                    df_poll_unique.apply(
                        lambda x: (
                            x["device_type"] in self.fits_config["device_type"].values
                        )
                        and (
                            x["device_command"]
                            in self.fits_config["device_command"].values
                        ),
                        axis=1,
                    )
                ]

                # get header and comment from fits_config
                df_poll_unique["header"] = df_poll_unique.apply(
                    lambda x: (
                        self.fits_config[
                            (self.fits_config["device_type"] == x["device_type"])
                            & (
                                self.fits_config["device_command"]
                                == x["device_command"]
                            )
                        ]["header"].values[0]
                    ),
                    axis=1,
                )
                df_poll_unique["comment"] = df_poll_unique.apply(
                    lambda x: (
                        self.fits_config[
                            (self.fits_config["device_type"] == x["device_type"])
                            & (
                                self.fits_config["device_command"]
                                == x["device_command"]
                            )
                        ]["comment"].values[0]
                    ),
                    axis=1,
                )

                # keep rows that only have device_name in paired_devices
                df_poll_unique = df_poll_unique[
                    df_poll_unique["device_name"].isin(paired_devices.values())
                ]

                # form interpolated dataframe
                df_inp = pd.DataFrame(
                    columns=df_poll_unique["header"], index=df_images_filt["jd_obs"]
                )

                # interpolate polled data onto image times
                for i, row in df_poll_unique.iterrows():
                    df_poll_filtered = df_poll[
                        (df_poll["device_type"] == row["device_type"])
                        & (df_poll["device_name"] == row["device_name"])
                        & (df_poll["device_command"] == row["device_command"])
                    ]

                    df_poll_filtered = df_poll_filtered.sort_values(by="jd")
                    df_poll_filtered = df_poll_filtered.set_index("jd")

                    df_poll_filtered["device_value"] = (
                        df_poll_filtered["device_value"]
                        .replace({"True": 1.0, "False": 0.0})
                        .astype(float)
                    )

                    df_inp[row["header"]] = utils.interpolate_dfs(
                        df_images_filt["jd_obs"], df_poll_filtered["device_value"]
                    )["device_value"].fillna(0)

                # update files
                for i, row in df_images_filt.iterrows():
                    try:
                        with fits.open(row["filepath"], mode="update") as filehandle:
                            hdr = filehandle[0].header
                            for header in df_inp.columns:
                                hdr[header] = (
                                    df_inp.iloc[i][header],
                                    df_poll_unique[df_poll_unique["header"] == header][
                                        "comment"
                                    ].values[0],
                                )

                            hdr["RA"] = hdr["RA"] * (360 / 24)  # convert to degrees

                            location = EarthLocation(
                                lat=hdr["LAT-OBS"] * u.deg,
                                lon=hdr["LONG-OBS"] * u.deg,
                                height=hdr["ALT-OBS"] * u.m,
                            )
                            target = SkyCoord(
                                hdr["RA"], hdr["DEC"], unit=(u.deg, u.deg), frame="icrs"
                            )

                            utils.hdr_times(hdr, self.fits_config, location, target)
                            filehandle[0].add_checksum()

                            self.cursor.execute(
                                f'''UPDATE images SET complete_hdr = 1 WHERE filename="{row["filepath"]}"'''
                            )
                    except FileNotFoundError:
                        self.logger.warning(
                            f"Error completing headers: {row['filepath']}"
                        )
                    finally:
                        self.cursor.execute(
                            f'''UPDATE images SET complete_hdr = 1 WHERE filename="{row["filepath"]}"'''
                        )

            self.logger.info("Completing headers... Done.")

        except Exception as e:
            self.error_source.append(
                {"device_type": "Headers", "device_name": "", "error": str(e)}
            )
            self.logger.error(f"Error completing headers: {e}")

    def monitor_action(
        self,
        device_type: str,
        monitor_command: str,
        desired_condition: any,
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
                not error_sensitive or self.error_free
            )

        start_time = time.time()
        self.logger.debug(
            f"Monitor action: Starting {device_type} {device_name} {monitor_command} {desired_condition} {run_command} {run_command_type} {abs_tol} {log_message} {timeout}"
        )

        # create unique key for monitor action and add to queue for device_name
        unique_key = f"{device_type}{monitor_command}{desired_condition}{run_command}{run_command_type}"
        self.monitor_action_queue[device_name][unique_key] = start_time

        try:
            # Wait for turn
            while any(
                value < self.monitor_action_queue[device_name][unique_key]
                for value in self.monitor_action_queue[device_name].values()
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
            self.error_source.append(
                {
                    "device_type": device_type,
                    "device_name": "",
                    "error": str(e),
                }
            )
            self.logger.error(
                f"Monitor-action error: Device Type: {device_type}, Device Name: {device_name}, "
                f"Monitor Command: {monitor_command}, Desired Condition: {desired_condition}, "
                f"Run Command: {run_command}, Run Command Type: {run_command_type}, "
                f"Absolute Tolerance: {abs_tol}, Log Message: {log_message}, Timeout: {timeout}, "
                f"Error: {e}"
            )

        finally:
            if (
                device_name in self.monitor_action_queue
                and unique_key in self.monitor_action_queue[device_name]
            ):
                del self.monitor_action_queue[device_name][unique_key]

    def queue_get(self) -> None:
        """
        Process multiprocessing queue messages for database operations and logging.

        Continuously monitors the multiprocessing queue for database queries and
        log messages from device processes. Handles different message types and
        maintains system operation by processing database operations and managing
        thread cleanup.

        Message Types Handled:
        - 'query': Executes SQL database queries from device processes
        - 'log': Processes log messages with different severity levels
          - 'info', 'warning', 'error', 'debug' log levels supported
          - Error messages are added to error_source for monitoring

        Background Operations:
        - Cleans up completed threads from the threads list
        - Maintains database consistency across multiprocessing boundaries
        - Ensures proper error propagation from device processes

        Error Handling:
        - Catches and logs queue processing errors
        - Adds queue errors to error_source for monitoring
        - Stops queue processing on fatal errors

        Note:
            - Runs continuously until queue_running flag is False
            - Essential for multiprocessing communication with devices
            - Handles both synchronous database operations and asynchronous logging
            - Thread cleanup prevents memory leaks in long-running operations
        """

        while self.queue_running:
            try:
                metadata, r = self.queue.get()

                if r["type"] == "query":
                    self.cursor.execute(r["data"])
                elif r["type"] == "log":
                    if r["data"][0] == "info":
                        self.logger.info(r["data"][1])
                    elif r["data"][0] == "warning":
                        self.logger.warning(r["data"][1])
                    elif r["data"][0] == "error":
                        self.logger.error(r["data"][1])
                        self.error_source.append(
                            {
                                "device_type": metadata["device_type"],
                                "device_name": metadata["device_name"],
                                "error": r["data"][1],
                            }
                        )
                    elif r["data"][0] == "debug":
                        self.logger.debug(r["data"][1])

                # pick up work of watchdog
                # cleanup dead threads
                self.threads = [i for i in self.threads if i["thread"].is_alive()]

            except Exception as e:
                error_text = f"{type(e).__name__}: {e}"
                self.error_source.append(
                    {
                        "device_type": "Queue",
                        "device_name": "queue_get",
                        "error": error_text,
                    }
                )
                self.logger.error(
                    f"Queue get error: {error_text}", exc_info=True, stack_info=True
                )
                self.queue_running = False
