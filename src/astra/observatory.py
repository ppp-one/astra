import logging
import math
import os
import sqlite3
import time
from datetime import UTC, datetime
from multiprocessing import Manager
from pathlib import Path
from threading import Thread
from typing import Tuple

import astropy.units as u
import numpy as np
import pandas as pd
import psutil
import yaml
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

# TODO (set 2024-07-20):
# - move schedule things to schedule.py
# - optimise image ack < -- save sequence
# - organise order of methods
# - bugs
# - internal safety monitor
# - add more logging
# - add more error handling
# - add more comments
# - add more docstrings
# - add more tests
# - add more type hints


class Observatory:
    def __init__(
        self,
        config_filename: str,
        truncate_schedule: bool = False,
        speculoos: bool = False,
    ):
        """
        Initialize the Astra object.

        Parameters:
            config_filename (str): path to the configuration file for the observatory.
            truncate_schedule (bool): if True, the schedule is truncated by a factor of 100 and moved to the current time.

        Attributes:

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

        self.threads.append({"type": "queue", "device_name": "queue", "thread": th, "id": "queue"})

        # heartbeat dictionary
        self.heartbeat = {}

        # custom logic flags
        self.speculoos = speculoos
        self.truncate_schedule = truncate_schedule

        # log+polling backup flags
        self.run_backup = True
        self.backup_time = datetime.strptime(self.config["Misc"]["backup_time"], "%H:%M")

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
        self.schedule_path = CONFIG.paths.schedules / f"{self.name}.csv"
        self.schedule_mtime = self.get_schedule_mtime()
        self.schedule = None
        if self.schedule_mtime != 0:
            self.schedule = self.read_schedule()

        # load devices
        self.monitor_action_queue = {}  # queue for monitoring/running actions per device_name
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
                    self.guider[device_name] = Guider(
                        telescope, self.cursor, self.logger, guider_params
                    )

        self.logger.info("Astra initialized")

    @property
    def config(self) -> ObservatoryConfig:
        if self._config.is_outdated():
            self.logger.info("Config file modified, reloading.")
            self._config.load()

            # TODO restart devices

        return self._config

    def create_db(self) -> Sqlite3Worker:
        """
        Creates a new database with the given configuration file name.

        Returns:
            cursor (Sqlite3Worker): The cursor object for the newly created database.
        """

        db_name = CONFIG.paths.logs / f"{self.name}.db"
        cursor = Sqlite3Worker(db_name)

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
        Backs up the database tables of previous 24 hours into csv files.

        Checks if disk drive is filling up
        """

        try:
            self.run_backup = False
            self.logger.info("Backing up database")

            # check disk space
            disk_usage = psutil.disk_usage("/")
            if disk_usage.percent > 90:
                self.logger.warning(f"Disk usage {disk_usage.percent}% is high")

                # TODO: action

            dt_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
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
                df.to_csv(
                    os.path.join(
                        CONFIG.paths.logs,
                        "archive",
                        f"{self.name}_{table}_{dt_str}.csv",
                    ),
                    index=False,
                )

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

    def read_config(self, yaml_filename: str) -> dict:
        """
        Reads a YAML configuration file and returns a dictionary containing its contents.

        Parameters:
            config_filename (str): The path to the YAML configuration file.

        Returns:
            dict: A dictionary containing the contents of the YAML configuration file.
        """

        self.logger.info("Reading config file")

        observatory = {}
        with open(yaml_filename, "r") as stream:
            try:
                observatory = yaml.safe_load(stream)
                self.logger.info(f"Config file {yaml_filename} read")
            except yaml.YAMLError as exc:
                self.logger.error(f"Error reading config file {yaml_filename}: {exc}")

        return observatory

    def load_devices(self) -> dict[str, dict[str, AlpacaDevice]]:
        """
        This method iterates through the observatory configuration, creating and starting
        device objects for each defined device.

        Returns:
            devices (dict): A dictionary containing initialized device objects, categorized
            by device type.
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
        Connects to all loaded devices and starts polling at specific intervals
        to retrieve non-fixed FITS headers. The polling interval is 5 seconds for most
        devices and 1 second for the SafetyMonitor.

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
                        self.devices[device_type][device_name].set("Connected", True)  ## slow?
                        self.logger.info(f"{device_type} {device_name} connected")
                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": device_type,
                            "device_name": device_name,
                            "error": str(e),
                        }
                    )
                    self.logger.error(f"Error connecting to {device_type} {device_name}: {str(e)}")

        self.logger.info("Starting polling non-fixed fits headers")

        delay = 5  # seconds
        # start polling non-fixed fits headers
        for i, row in self.fits_config.iterrows():
            if (row["device_type"] not in ["astropy_default", "astra", "astra_fixed", ""]) and row[
                "fixed"
            ] is False:
                device_type = row["device_type"]
                if device_type in self.devices:
                    for device_name in self.devices[device_type]:
                        device = self.devices[device_type][device_name]
                        try:
                            device.start_poll(row["device_command"], delay)  # 5 second polling
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

        delay = 1  # seconds
        if "SafetyMonitor" in self.config:
            device_type = "SafetyMonitor"
            device_name = self.config[device_type][0]["device_name"]

            device = self.devices[device_type][device_name]
            try:
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
        This method pauses the polling of all devices, or a subset of devices if specified.

        Parameters:
            device_types (list, optional): A list of device types to pause polling. Defaults to None.

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
        This method resumes the polling of all devices, or a subset of devices if specified.

        Parameters:
            device_types (list, optional): A list of device types to resume polling. Defaults to None.

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
        Start the watchdog thread if it is not already running.

        This method initializes and starts a new thread responsible for monitoring
        certain aspects of the system. If the watchdog thread is already running,
        it logs a warning and takes no action.

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
        Periodically monitors various aspects of the observatory's operation and takes appropriate actions in case of issues.

        This function performs the following checks and actions:

        - Periodically checks the SafetyMonitor's status, telescope altitude, system errors, and device responsiveness.
        - If the SafetyMonitor indicates unsafe conditions, it closes the observatory.
        - Starts the schedule independently of weather conditions, running only calibration sequences when weather is unsafe.
        - Monitors for system errors and handles them as necessary.

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
                                self.logger.warning("Robotic switch is on, starting schedule")
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

                            self.close_observatory()  # checks if already closed and closes if not

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
                            self.logger.warning("Weather unsafe from internal safety monitor")

                        self.close_observatory()  # checks if already closed and closes if not

                    # set time_to_safe if weather unsafe
                    if rows[0][0] > 0 or internal_time_to_safe > 0:
                        if rows[0][1] is not None:
                            time_since_last_unsafe = pd.to_datetime(
                                datetime.now(UTC)
                            ) - pd.to_datetime(rows[0][1], utc=True)
                        else:
                            time_since_last_unsafe = pd.to_timedelta(0)

                        current_time_to_safe = (
                            max_safe_duration - time_since_last_unsafe.total_seconds() / 60
                        )

                        if rows[0][0] == 0:
                            self.time_to_safe = internal_time_to_safe
                        else:
                            self.time_to_safe = max(current_time_to_safe, internal_time_to_safe)
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
                            weather_log_warning = False  # reset weather_log_warning flag
                    else:
                        self.weather_safe = False  # set here too just in case watchdog started after weather unsafe?
                        weather_log_warning = True

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
                        # only one device has errors
                        # match device_types[0]:
                        #     case "SafetyMonitor":
                        #         pass
                        #     case "ObservingConditions":
                        #         pass
                        #     case "Telescope":
                        #         pass
                        #     case "Dome":
                        #         pass
                        #     case "Guider":
                        #         pass
                        #     case "Camera":
                        #         pass
                        #     case "FilterWheel":
                        #         pass
                        #     case "Focuser":
                        #         pass
                        #     case "Rotator":
                        #         pass
                        #     case "CoverCalibrator":
                        #         pass
                        #     case "Switch":
                        #         pass
                        #     case "Schedule":
                        #         pass
                        #     case "Queue":
                        #         # restart queue?
                        #         pass
                        #     case "Headers":
                        #         pass
                        #     case "Watchdog":
                        #         pass
                        #     case "Backup":
                        #         pass
                        #     case _:
                        #         pass

                    if self.speculoos:
                        # if not dome or telescope, park
                        if "Dome" not in device_types and "Telescope" not in device_types:
                            self.logger.warning(
                                f"(SPECULOOS EDIT): Closing observatory due to no errors in Dome or Telescope"
                            )
                            self.close_observatory(error_sensitive=False)

                        elif "Dome" not in device_types and "Telescope" in device_types:
                            self.logger.warning(
                                f"(SPECULOOS EDIT): Closing Dome due to no errors in Dome, but errors in Telescope"
                            )
                            for device_name in self.devices["Dome"]:
                                self.speculoos_check_and_ack_error(close=True)
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

    def internal_safety_weather_monitor(self) -> float:
        """
        This method monitors the internal safety of the observatory and the weather conditions.

        """

        longest_time_to_safe = 0
        longest_max_safe_duration = 0
        if "ObservingConditions" in self.config:
            if "closing_limits" in self.config["ObservingConditions"][0]:
                closing_limits = self.config["ObservingConditions"][0]["closing_limits"]

                for parameter in closing_limits:
                    limits = closing_limits[parameter]
                    for limit in limits:
                        max_safe_duration = limit.get("max_safe_duration", 0)
                        lower_limit = limit.get("lower", None)
                        upper_limit = limit.get("upper", None)

                        if lower_limit is not None and upper_limit is not None:
                            q = f"""
                            SELECT COUNT(*), MAX(datetime) FROM polling 
                            WHERE device_type = 'ObservingConditions' 
                            AND device_command = '{parameter}' 
                            AND (CAST(device_value AS FLOAT) < {lower_limit} OR CAST(device_value AS FLOAT) > {upper_limit})
                            AND datetime > datetime('now', '-{max_safe_duration} minutes')
                            """
                        elif lower_limit is not None:
                            q = f"""
                            SELECT COUNT(*), MAX(datetime) FROM polling 
                            WHERE device_type = 'ObservingConditions' 
                            AND device_command = '{parameter}' 
                            AND CAST(device_value AS FLOAT) < {lower_limit}
                            AND datetime > datetime('now', '-{max_safe_duration} minutes')
                            """
                        elif upper_limit is not None:
                            q = f"""
                            SELECT COUNT(*), MAX(datetime) FROM polling 
                            WHERE device_type = 'ObservingConditions' 
                            AND device_command = '{parameter}' 
                            AND CAST(device_value AS FLOAT) > {upper_limit}
                            AND datetime > datetime('now', '-{max_safe_duration} minutes')
                            """

                        rows = self.cursor.execute(q)

                        if rows[0][0] > 0:
                            time_since_last_unsafe = pd.to_datetime(
                                datetime.now(UTC)
                            ) - pd.to_datetime(rows[0][1], utc=True)

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
        # update heartbeat
        self.heartbeat["datetime"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
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
                    self.logger.error(f"Error polling {device_type} {device_name}: {str(e)}")
                    polled = None

                if polled is not None:  # not sure if correct to put this here, or later
                    polled_keys = polled.keys()
                    for k in polled_keys:
                        polled_list[device_type][device_name][k] = {}
                        polled_list[device_type][device_name][k]["value"] = polled[k]["value"]
                        polled_list[device_type][device_name][k]["datetime"] = polled[k]["datetime"]

        self.heartbeat["polling"] = polled_list
        self.heartbeat["monitor-action-queue"] = self.monitor_action_queue

    def speculoos_check_and_ack_error(self, close=False) -> None:
        if "Telescope" in self.config:
            for telescope_name in self.devices["Telescope"]:
                telescope = self.devices["Telescope"][telescope_name]

                # check telescope status
                valid, all_errors, messages = utils.check_astelos_error(telescope, close=close)

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
                    self.logger.error(f"AsTelOS errors invalid for {telescope_name}: {messages}")

    def open_observatory(self, paired_devices: dict | None = None) -> None:
        """
        Opens the observatory in a controlled sequence: first, it opens the dome shutter if available,
        and then it unparks the telescope if available and weather safe.

        Parameters:
            paired_devices (dict): A dictionary of paired devices. Defaults to None.

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

                    r = telescope.get("CommandString", Command="TELESCOPE.READY_STATE", Raw=True)

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
                            self.logger.error(f"Timeout waiting for {telescope_name} to be ready")
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
                            self.logger.error(f"Issue with telescope getting ready, status: {r}")

    def close_observatory(
        self, paired_devices: dict | None = None, error_sensitive: bool = True
    ) -> bool:
        """
        Close the observatory operations in the following order:

        1. Stop telescope slewing and tracking.

        2. Park the telescope.

        3. Stop guiding if applicable.

        4. Park the dome and close its shutter (if observatory has a dome).

        Parameters:
            paired_devices (dict, optional): A dictionary of paired devices to specify the target devices.
                Example: {'Telescope': 'TelescopeName', 'Dome': 'DomeName'}

        """
        if self.speculoos:
            # SPECULOOS EDIT
            self.pause_polls(["Dome", "Telescope", "Focuser"])

            # acknowledge errors if dome not closed, if any
            device_names = [paired_devices["Dome"]] if paired_devices else self.devices["Dome"]
            for device_name in device_names:
                dome = self.devices["Dome"][device_name]
                ShutterStatus = dome.get("ShutterStatus")
                if ShutterStatus == 0:  # open
                    self.speculoos_check_and_ack_error(close=True)

        if "Telescope" in self.config:
            # telescope guiding and slewing
            device_names = (
                [paired_devices["Telescope"]] if paired_devices else self.devices["Telescope"]
            )
            for device_name in device_names:
                try:
                    if self.guider[device_name].running:
                        self.guider[device_name].running = False
                except Exception as e:
                    self.error_source.append(
                        {
                            "device_type": "Guider",
                            "device_name": device_name,
                            "error": str(e),
                        }
                    )
                    self.logger.error(f"Error stopping telescope {device_name} guiding: {str(e)}")

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
            # park dome
            device_names = [paired_devices["Dome"]] if paired_devices else self.devices["Dome"]
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
        Read the schedule CSV file and return it as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the schedule data, with columns 'start_time' and 'end_time'.

        Raises:
            Exception: If an error occurs during reading.

        Notes:
            - If the schedule CSV file is not found, a FileNotFoundError is raised.
            - The schedule DataFrame is sorted by the 'start_time' column.
            - If self.truncate_schedule is True, the schedule is truncated for development purposes using the update_times function.
        """
        # TODO: schedule validity checker, add schedule as string to log?

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
                    schedule = process_schedule(
                        self.schedule_path,
                        truncate=self.truncate_schedule,
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
        Get the timestamp of the schedule file. If the file does not exist, return 0.

        Returns:
            float: The timestamp of the schedule file.

        """
        if not self.schedule_path.exists():
            return 0
        else:
            return os.path.getmtime(self.schedule_path)

    def toggle_robotic_switch(self) -> None:
        """
        Toggle the robotic switch on or off.

        This method is used to control the robotic switch, which is responsible for managing
        the observatory's robotic operations. It can be used to enable or disable the robotic
        functionality as needed.

        """
        if self.robotic_switch:
            self.robotic_switch = False
            self.logger.info("Robotic switch turned off")
            # stop schedule if running
            self.stop_schedule()
        else:
            if self.watchdog_running is False:
                self.logger.warning("Robotic switch cannot be turned on without watchdog running")
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
        Start the schedule thread if it is not already running.

        This method initializes and starts a new thread responsible for executing the schedule.

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
        Stop the schedule thread if it is running.

        This method sets the schedule_running flag to False, indicating that the schedule should stop executing.

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
        Run the schedule while monitoring safety conditions and executing scheduled actions.

        This method manages the execution of a schedule, considering safety checks, weather conditions,
        and action types. It iterates through schedule rows, starts threads for actions if conditions are met.
        If the action type is to open or close, it ensures that the actions are completed before proceeding
        to the next item in the schedule.

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
                if (i not in ids) and self.check_conditions(row) and (row["completed"] is False):
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
            if row["device_type"] == "Camera":
                paired_devices = PairedDevices.from_observatory(
                    observatory=self,
                    camera_name=row["device_name"],
                )
                camera_config = paired_devices.get_device_config("Camera")
                set_temperature = camera_config["temperature"]
                temperature_tolerance = camera_config["temperature_tolerance"]

                if row["action_type"] not in ["close", "open"]:
                    self.cool_camera(row, set_temperature, temperature_tolerance)

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
                    self.cool_camera(row, set_temperature, temperature_tolerance)
                else:
                    # open all dome(s) and unpark telescope(s)
                    self.open_observatory()

            elif "close" == row["action_type"]:
                if "Camera" in self.config:
                    # close dome and park telescope
                    self.close_observatory(paired_devices)
                    self.cool_camera(row, set_temperature, temperature_tolerance)
                else:
                    # close all dome(s) and park telescope(s)
                    self.close_observatory()

            elif "cool_camera" == row["action_type"]:
                if "Camera" in self.config:
                    self.cool_camera(row, set_temperature, temperature_tolerance)

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
                if (row["action_type"] in ["calibration", "close"]) or self.weather_safe:
                    self.schedule.loc[row.name, "completed"] = True

            self.logger.info(f"{row['action_type']} sequence ended for {row['device_name']}")
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
            self.logger.error(f"Run action error: {str(e)}", exc_info=True, stack_info=True)

    def cool_camera(
        self, row: dict, set_temperature: float, temperature_tolerance: float = 1
    ) -> None:
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
            timeout=60 * 30,
            weather_sensitive=False,
        )  # 30 minutes

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

        action_value: dict = eval(row["action_value"])

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
            hdr["IMAGETYP"] = "FLAT"  # TODO: change to Flat Frame?

        self.logger.debug(
            f"Finished pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}"
        )

        return action_value, folder, hdr

    def setup_observatory(
        self, paired_devices: dict | PairedDevices, action_value: dict, filter_list_index: int = 0
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

        self.logger.debug(f"Running setup_observatory for {paired_devices} {action_value}")
        if not isinstance(paired_devices, PairedDevices):
            paired_devices = PairedDevices.from_observatory(
                observatory=self,
                paired_device_names=paired_devices,
            )

        # unpark and slew to target
        if ("ra" in action_value) and ("dec" in action_value) and self.check_conditions():
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
        if "filter" in action_value and "FilterWheel" in paired_devices and self.error_free:
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
            self.logger.info(f"Defocusing Focuser {paired_devices['Focuser']} with {action_value}.")

            defocuser = Defocuser(
                astra=self,
                paired_devices=paired_devices,
            )

            if "focus_position" in action_value:
                new_focus_position = action_value["focus_position"]
            elif "focus_shift" in action_value:
                new_focus_position = defocuser.best_focus_position + action_value["focus_shift"]
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

        if "bin" in action_value:
            if "Camera" in paired_devices:
                camera = paired_devices.camera
                self.logger.info(
                    f"Setting Camera {paired_devices['Camera']} binning to {action_value['bin']}"
                )
                camera.set("BinX", action_value["bin"])
                camera.set("BinY", action_value["bin"])
                camera.set("NumX", camera.get("CameraXSize") // camera.get("BinX"))
                camera.set("NumY", camera.get("CameraYSize") // camera.get("BinY"))

    def wait_for_slew(self, paired_devices: PairedDevices) -> None:
        """
        Wait for a telescope to complete its slewing operation.

        Parameters:
            paired_devices (dict): A dictionary containing paired devices, including the 'Telescope' device.

        Raises:
            TimeoutError: If the slewing operation takes longer than 2 minutes.

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
        Check the conditions for running a sequence or action.

        This method checks the conditions required to run a sequence or action, including weather safety,
        error-free operation, no interruptions, and running schedule and watchdog processes.

        Parameters:
            row (dict, optional): A dictionary containing information about the sequence or action.

        Returns:
            bool: True if all conditions are met, False otherwise.

        """

        base_conditions = self.error_free and self.schedule_running and self.watchdog_running

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
        maxadu,
        row,
        hdr,
        folder,
        use_light=True,
        log_option=None,
        maximal_sleep_time=0.1,
        wcs=None,
    ) -> Tuple[bool, Path | None]:
        """
        Perform camera exposure, log information, and wait for the image to be ready.

        Parameters:
            use_light (bool, optional): Whether to use light during the exposure (default is True).
            log_option (str or None, optional): Additional information for logging (default is None, adding nothing).
            maximal_sleep_time (float, optional): The maximum sleep time in seconds during the waiting process (default is 0.01).

        Returns:
            bool: True if the exposure was successful, False otherwise.
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
                folder,
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
        Run an image sequence for a specific camera.
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
                        row, action_value, filepath, paired_devices
                    )

                    if self.speculoos:
                        time.sleep(exptime * 3)  # for spirit

                    pointing_attempts += 1

                    if wcs_solve is not None:
                        with fits.open(filepath, mode="update") as hdul:
                            hdul[0].header.update(wcs_solve.to_header())
                            hdul.flush()

                    if pointing_complete is False:
                        wcs_solve = None  # to not contaminate the next image if pointing fails

                    if pointing_attempts > 3 and pointing_complete is False:
                        self.logger.warning(
                            f"Pointing correction for {action_value['object']} with "
                            f"{row['device_name']} failed after {pointing_attempts} attempts"
                        )
                        pointing_complete = True
                else:
                    pointing_complete = True

                # initialise guiding once pointing correction is complete
                if action_value.get("guiding") and guiding is False and pointing_complete is True:
                    guiding = self.start_guider(row, action_value, folder, paired_devices)

        # stop guiding at end of sequence
        if action_value.get("guiding"):
            if self.guider[paired_devices["Telescope"]].running:
                self.logger.info(f"Stopping telescope {paired_devices['Telescope']} guiding")
                self.guider[paired_devices["Telescope"]].running = False
                
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
        Run a pointing model sequence for a specific camera.

        The function generates a series of points in a spiral pattern from the zenith
        down to a specified altitude above the horizon (30 degrees by default).
        For each of these points (unless they are too close to the mooon), an exposure is performed
        and afterwards a pointing correction based on the captured image is performed, updating
        the header information of the exposure.
        """

        self.logger.info(
            f"Running {row['action_type']} sequence for {row['device_name']}, "
            f"starting {row['start_time']} and ending {row['end_time']}"
        )

        action_value, folder, hdr = self.pre_sequence(row, paired_devices, create_folder=False)


        action_value["object"] = "pointing_model"

        # create pointing_model folder
        folder = CONFIG.paths.images / "pointing_model"
        folder.mkdir(exist_ok=True)

        # number of points
        N = action_value.get("n", 20)

        # set exptime to 1 if not specified
        exptime = action_value.get("exptime", 1)  # default to 1 second

        # get camera
        camera = self.devices[row["device_type"]][row["device_name"]]
        maxadu = camera.get("MaxADU")

        # get location
        obs_lat = hdr["LAT-OBS"]
        obs_lon = hdr["LONG-OBS"]
        obs_alt = hdr["ALT-OBS"]
        obs_location = EarthLocation(lat=obs_lat * u.deg, lon=obs_lon * u.deg, height=obs_alt * u.m)
        MOON_LIMIT = 20 * u.deg  # pointing distance to the moon in degrees

        # Generate points (spiral from zenith to 30 deg above horizon)
        num_turns = np.sqrt(N / 2)
        t_linear = np.linspace(0, 1, N)  # Generate base points
        ts = t_linear**0.5  # increase spacing towards zenith
        t_shift = 0

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
            moon_altaz = get_body("moon", observing_time).transform_to(  # Alternative method
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

            # perform exposure
            success, filepath = self.perform_exposure(
                camera,
                exptime,
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
                row, action_value, filepath, paired_devices, sync=True, slew=False
            )

            if self.speculoos:
                time.sleep(exptime * 3)  # for spirit

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
        sync: bool = False,
        slew: bool = True,
    ) -> tuple[bool, WCS | None]:
        """Perform a pointing correction"""
        self.logger.info(
            f"Running pointing correction for {action_value['object']} with {row['device_name']}"
        )
        try:
            pointing_corrector_handler = PointingCorrectionHandler.from_fits_file(
                filepath, target_ra=action_value["ra"], target_dec=action_value["dec"]
            )
            pointing_correction = pointing_corrector_handler.pointing_correction
            # offset_ra, offset_dec, wcs_solve, angular_separation = utils.pointing(
            #     filepath, action_value["ra"], action_value["dec"]
            # )

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
                RightAscension=24 * (action_value["ra"] + pointing_correction.offset_ra) / 360,
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
        Start the guider for a telescope.
        """
        self.logger.info(f"Starting guiding for {paired_devices['Telescope']}")

        filter_name = action_value["filter"].replace("'", "")
        glob_str = os.path.join(
            "..",
            "images",
            folder,
            f"{row['device_name']}_{filter_name}_{action_value['object']}_{action_value['exptime']:.3f}_*.fits",
        )  # be careful with if custom naming is used

        th = Thread(
            target=self.guider[paired_devices["Telescope"]].guider_loop,
            args=(paired_devices["Camera"], glob_str, action_value["exptime"] * 2),
            daemon=True,
        )
        th.start()

        self.threads.append(
            {
                "type": "guider",
                "device_name": row["device_name"],
                "thread": th,
                "id": "guider",
            }
        )

        return True

    def guiding_calibration_sequence(self, row, paired_devices: PairedDevices) -> bool:
        """
        Perform guding calibration.

        Parameters:
            row (dict): A dictionary containing information about the sequence action:
                - 'device_name': The name of the device.
                - 'action_type': The type of action (e.g., 'object').
                - 'action_value': The action's value (e.g., a command or parameter).
            paired_devices (dict): A dictionary specifying paired devices for the sequence.

        Returns:
            bool: True if the guiding calibration was successful, False otherwise.
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
        Perform autofocus.

        Parameters:
            row (dict): A dictionary containing information about the sequence action:
                - 'device_name': The name of the device.
                - 'action_type': The type of action (e.g., 'object').
                - 'action_value': The action's value (e.g., a command or parameter).
            paired_devices (dict): A dictionary specifying paired devices for the sequence.

        Returns:
            bool: True if the autofocus was successful, False otherwise.
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
        Performs a flats sequence.

        A flats sequence is a series of exposures with a consistent brightness level, typically used for calibrating images.

        Parameters:
            row (dict): A dictionary containing information about the sequence and the device, including 'action_value', 'device_name', 'start_time', and 'end_time'.
            It should include keys like 'device_name', 'start_time', and 'end_time'.

            paired_devices (dict): A dictionary of paired devices required for the sequence.


        The function captures and saves flat field images, adjusting exposure times as necessary to reach the
        desired target ADU (Analog-to-Digital Unit) value, set in the config file.

        Reference:
            Wei, P., Shang, Z., Ma, B., Zhao, C., Hu, Y. and Liu, Q., 2014, August. Problems with twilight/supersky flat-field for wide-field robotic telescopes and the solution. In Observatory Operations: Strategies, Processes, and Systems V (Vol. 9149, pp. 877-883). SPIE.
            https://arxiv.org/pdf/1407.8283.pdf

        Notes:
            - The sequence will continue to run until one of the following conditions is met:
                - The current time exceeds 'end_time'.
                - Adverse weather conditions are detected.
                - An error occurs during execution.
                - The sequence is manually interrupted.
                - The schedule is stopped.
                - The watchdog process is terminated.

        """

        self.logger.info(
            f"Running flats sequence for {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}"
        )

        # creates folder for images, writes base header, and sets filter to first filter in list
        action_value, folder, hdr = self.pre_sequence(row, paired_devices)

        # camera device
        camera = self.devices[row["device_type"]][row["device_name"]]

        # target adu and camera offset needed for flat exposure time calculation
        cam_index = self.get_cam_index(row["device_name"])
        target_adu = self.config["Camera"][cam_index]["flats"]["target_adu"]
        offset = self.config["Camera"][cam_index]["flats"]["bias_offset"]
        lower_exptime_limit = self.config["Camera"][cam_index]["flats"]["lower_exptime_limit"]
        upper_exptime_limit = self.config["Camera"][cam_index]["flats"]["upper_exptime_limit"]

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
        obs_location = EarthLocation(lat=obs_lat * u.deg, lon=obs_lon * u.deg, height=obs_alt * u.m)

        # wait for sun to be in right position
        sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)
        self.logger.info(
            f"Sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
        )

        if self.check_conditions(row) and (take_flats is False):
            self.logger.info(
                f"Not the right time to take flats for {row['device_name']}, sun at {sun_altaz.alt.degree:.2f} degrees and {'rising' if sun_rising else 'setting'}"
            )

            # calculate time until sun is in right position of between -1 and -10 degrees altitude
            if sun_rising:
                # angle between sun_altaz.alt.degree and -10
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

            print(sun_rising, take_flats, obs_location.lat.degree, sun_altaz.alt.degree)
            if take_flats is False:
                time.sleep(1)

        # start taking flats
        for i, filter_name in enumerate(action_value["filter"]):
            count = 0
            sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

            if self.check_conditions(row) and take_flats:
                ## initial setup + exposure setting
                # sets filter (and focus, soon...)
                self.setup_observatory(paired_devices, action_value, filter_list_index=i)

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

                                if exptime < lower_exptime_limit or exptime > upper_exptime_limit:
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

    def flats_position(self, obs_location: EarthLocation, paired_devices: dict, row: dict) -> None:
        """
        Move a telescope to a optimal sky flat position for capturing flat frames.

        Parameters:
            obs_location (EarthLocation): The location of the observatory.
            paired_devices (dict): A dictionary of paired devices required for the sequence.
            row (dict): A dictionary containing information about the sequence and the device, including 'action_value', 'device_name', 'start_time', and 'end_time'.

        Notes:
            - The sequence will continue to run until one of the following conditions is met:
                - The current time exceeds 'end_time'.
                - Adverse weather conditions are detected.
                - An error occurs during execution.
                - The sequence is manually interrupted.
                - The schedule is stopped.
                - The watchdog process is terminated.

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
                time.sleep(0.1)  # add 0.1 s sleep to avoid spamming the camera and high cpu usage
                time.sleep(0)  # yield to other threads
                if r is True:
                    arr = camera.get("ImageArray")
                    median_adu = np.nanmedian(arr)
                    fraction = (median_adu - offset) / (target_adu[0] - offset)

                    sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

                    if (
                        math.isclose(target_adu[0], median_adu, rel_tol=0, abs_tol=target_adu[1])
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
                                camera.get("StartExposure", Duration=exptime, Light=True)
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
                                camera.get("StartExposure", Duration=exptime, Light=True)
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
            cam_index = [i for i, d in enumerate(self.config["Camera"]) if d["device_name"] == cam][
                0
            ]
        except IndexError:
            cam_index = 0
            self.logger.error(
                f"Camera {cam} read from schedule could not be found in the observatory config."
            )
        return cam_index

    def base_header(self, paired_devices: PairedDevices, action_value: dict) -> fits.Header:
        """
        This function creates a base header for the fits file.

        Parameters:
            paired_devices (dict): A dictionary specifying paired devices, such as Telescope.
            action_value (dict): A dictionary containing action values from schedule.

        Returns:
            fits.Header: The FITS header containing the specified header entries.

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
                row["device_type"] not in ["astropy_default", "astra", "astra_fixed", ""]
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
        Add final headers to fits file.

        This method retrieves the captured image paths from the sqlite.db, and adds the missing headers
        using the polled data from each device (see 'connect_all' method). The polled data is then interpolated onto
        the same time series using the same dateobs from the fits file. The final headers are then written to the fits file.

        The process involves the following steps:
            1. Fetch images from the sqlite database that have not yet received final headers.
            2. Sort and process images by camera.
            3. Retrieve polled data from ASCOM devices within a time window around the first and last image timestamps.
            4. Extract unique headers and comments from a fits_config dictionary.
            5. Interpolate and populate headers for each image using the polled data.
            6. Update the FITS files with the final headers.
            7. Mark processed images as complete in the database.

        Raises:
            Exception: If any error occurs during the header completion process, it is logged and added to 'error_source'.

        Returns:
            None

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
                df_images_filt = df_images_filt.sort_values(by="date_obs").reset_index(drop=True)
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
                t0 = pd.to_datetime(df_images_filt["date_obs"].iloc[0]) - pd.Timedelta("10 sec")
                t1 = pd.to_datetime(df_images_filt["date_obs"].iloc[-1]) + pd.Timedelta("10 sec")

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
                        lambda x: (x["device_type"] in self.fits_config["device_type"].values)
                        and (x["device_command"] in self.fits_config["device_command"].values),
                        axis=1,
                    )
                ]

                # get header and comment from fits_config
                df_poll_unique["header"] = df_poll_unique.apply(
                    lambda x: (
                        self.fits_config[
                            (self.fits_config["device_type"] == x["device_type"])
                            & (self.fits_config["device_command"] == x["device_command"])
                        ]["header"].values[0]
                    ),
                    axis=1,
                )
                df_poll_unique["comment"] = df_poll_unique.apply(
                    lambda x: (
                        self.fits_config[
                            (self.fits_config["device_type"] == x["device_type"])
                            & (self.fits_config["device_command"] == x["device_command"])
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
                        self.logger.warning(f"Error completing headers: {row['filepath']}")
                    finally:
                        self.cursor.execute(
                            f'''UPDATE images SET complete_hdr = 1 WHERE filename="{row["filepath"]}"'''
                        )

            self.logger.info("Completing headers... Done.")

        except Exception as e:
            self.error_source.append({"device_type": "Headers", "device_name": "", "error": str(e)})
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
        Monitor device(s) of device_type for a given monitor_command and run_command if desired_condition is not met.

        Args:
            device_type (str): Type of the device(s) to monitor.
            monitor_command (str): The command to monitor on the device(s).
            desired_condition (any): The desired condition that should be met.
            run_command (str): The command to run if the desired_condition is not met.
            device_name (str): Name of the specific device to monitor.
            run_command_type (str, optional): Type of run command ('set' or 'get') (default '').
            abs_tol (float, optional): Absolute tolerance for comparing conditions (default 0).
            log_message (str, optional): Custom log message that runs if conditions not initially met (default '').
            timeout (float, optional): Maximum time to monitor before timing out (default 120 seconds).
            error_sensitive (bool, optional): If True, monitor action will be sensitive to errors (default True).
            weather_sensitive (bool, optional): If True, monitor action will be sensitive to weather conditions (default True).

        """

        def check_safe():
            return (not weather_sensitive or self.weather_safe) and (
                not error_sensitive or self.error_free
            )

        start_time = time.time()
        self.logger.debug(
            f"Monitor action: Starting {device_type} {device_name} {monitor_command} {desired_condition} {run_command} {run_command_type} {abs_tol} {log_message} {timeout}"
        )

        # create unique key for monitor action and add to queue for device_name
        unique_key = (
            f"{device_type}{monitor_command}{desired_condition}{run_command}{run_command_type}"
        )
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
        Retrieve and process items from the queue until it's stopped.

        This method continuously retrieves items from the queue and processes them based on their type.
        If the type is 'query', it executes the SQL query provided in the item's data.
        If the type is 'log', it logs the data and appends errors to the error_source if applicable.

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
                self.error_source.append(
                    {
                        "device_type": "Queue",
                        "device_name": "queue_get",
                        "error": str(e),
                    }
                )
                self.logger.error(f"Queue get error: {str(e)}")
                self.queue_running = False
