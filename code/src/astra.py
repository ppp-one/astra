import logging

import time
from datetime import datetime
from threading import Thread

import astropy.units as u
import numpy as np
import math
import pandas as pd
import utils
from guiding import Guider
import yaml
import os
import psutil
from alpaca_device_process import AlpacaDevice
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.io import fits
from astropy.time import Time
from sqlite3worker import Sqlite3Worker  # https://github.com/dashawn888/sqlite3worker

from multiprocessing import Manager

ASTRA_VER = '0.1.0'

def update_times(df, time_factor):
    '''
    Update the start and end times to present day factored by the time factor
    '''

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for i, row in df.iterrows():

        device_type, device_name, action_type, action_value, start_time, end_time = row
        
        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor
        
        
        new_start_time = datetime.utcnow()
        
        
        if prev_end_time:
            ss_time_diff = start_time - prev_start_time
            ss_time_diff = ss_time_diff / time_factor
            
            new_start_time = prev_new_start_time + ss_time_diff
            
        
        new_end_time = new_start_time + se_time_diff

        new_row = [device_type, device_name, action_type, action_value, new_start_time, new_end_time]
        new_rows.append(new_row)
        
        prev_start_time = start_time
        prev_end_time = end_time
        
        prev_new_start_time = new_start_time
    
    return pd.DataFrame(new_rows, columns=df.columns)


class Astra():
    def __init__(self, config_filename : str, debug : bool = False, truncate_schedule : bool = False):
        """
        Initialize the Astra object.

        Parameters:
            config_filename (str): path to the configuration file for the observatory.
            debug (bool): if True, Astra runs in debug mode.
            truncate_schedule (bool): if True, the schedule is truncated by a factor of 100 and moved to the current time.

        Attributes:
            debug (bool): whether Astra is running in debug mode.
            truncate_schedule (bool): whether the schedule is truncated to the current time.
            heartbeat (dict): dictionary containing the heartbeat data of the astra process.
            threads (list): list of threads started by Astra.
            queue (Queue): a multiprocessing queue used to communicate between processes.
            queue_running (bool): whether the queue is running.
            cursor (Cursor): a cursor object used to execute SQL statements on the database.
            error_free (bool): whether Astra is error-free.
            error_source (list): list of error sources.
            weather_safe (None): whether the weather is safe for observing.
            watchdog_running (bool): whether the watchdog thread is running.
            schedule_running (bool): whether the schedule thread is running.
            interrupt (bool): whether Astra is interrupted.
            observatory (dict): dictionary containing the configuration of the observatory.
            observatory_name (str): name of the observatory.
            schedule_mtime (float): modification time of the schedule file.
            schedule (DataFrame): pandas DataFrame containing the schedule.
            fits_config (DataFrame): pandas DataFrame containing the FITS headers configuration.
            devices (dict): dictionary containing the devices used by Astra.
            last_image (None): last image taken by Astra.
            guider (dict): dictionary containing the guider objects for each telescope.
        """
        self.observatory_name = os.path.splitext(os.path.basename(config_filename))[0]  

        self.debug = debug
        self.truncate_schedule = truncate_schedule

        self.heartbeat = {}

        self.threads = []
        self.queue = Manager().Queue()
        self.queue_running = True

        th = Thread(target=self.queue_get, daemon=True)
        th.start()

        self.threads.append({'type': 'queue', 'device_name': 'queue', 'thread': th, 'id' : 'queue'})

        self.cursor = self.create_db(config_filename)

        if self.debug is True:
            self.__log('warning', 'Astra is running in debug mode, schedule start time moved to present time and truncated by factor of 100')

        self.__log('info', 'Astra starting up')

        self.error_free = True
        self.error_source = []
        self.weather_safe = None

        self.watchdog_running = False
        self.schedule_running = False
        self.interrupt = False
        
        self.observatory = self.read_config(config_filename)      

        self.schedule_path = os.path.join('..', 'schedule', f'{self.observatory_name}.csv')
        self.schedule_mtime = os.path.getmtime(self.schedule_path)
        self.schedule = None
        self.schedule = self.read_schedule()

        fits_config_path = os.path.join('..', 'config', f'{self.observatory_name}_fits_headers.csv')
        self.fits_config = pd.read_csv(fits_config_path)

        self.devices = self.load_devices()
        self.last_image = None

        # for each telescope, create a donuts guider
        self.guider = {}
        if 'Telescope' in self.observatory:
            for device_name in self.devices['Telescope']:
                telescope = self.devices['Telescope'][device_name]
                telescope_index = [i for i, d in enumerate(self.observatory['Telescope']) if d['device_name'] == device_name][0]
                if 'guider' in self.observatory['Telescope'][telescope_index]:
                    guider_params = self.observatory['Telescope'][telescope_index]['guider']
                    self.guider[device_name] = Guider(telescope, self.cursor, guider_params)

        self.__log('info', 'Astra initialized')
    
    def __log(self, level : str, message : str) -> None:
        '''
        Logs the message to the database using the specified log level.

        Parameters:
            level (str): The log level. Valid values are 'info', 'debug', 'warning', 'error', and 'critical'.
            message (str): The message to log.
        '''

        # make message safe for sql
        message = message.replace("'", "''")

        # logging
        if level == 'info':
            logging.info(message)
        elif level == 'debug' and self.debug is True:
            logging.debug(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            self.error_free = False
            logging.error(message, exc_info=True)
        elif level == 'critical':
            logging.critical(message)
        
        dt_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        if level == 'debug' and self.debug is True:
            self.cursor.execute(f"INSERT INTO log VALUES ('{dt_str}', '{level}', '{message}')")
        elif level != 'debug':
            self.cursor.execute(f"INSERT INTO log VALUES ('{dt_str}', '{level}', '{message}')")

    def create_db(self, config_filename : str) -> Sqlite3Worker:
        """
        Creates a new database with the given configuration file name.

        Parameters:
            config_filename (str): The name of the configuration file.

        Returns:
            cursor (Sqlite3Worker): The cursor object for the newly created database.
        """

        db_name = os.path.join('..', 'log', f'{self.observatory_name}.db')
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

    def read_config(self, config_filename : str) -> dict:
        """
        Reads a YAML configuration file and returns a dictionary containing its contents.

        Parameters:
            config_filename (str): The path to the YAML configuration file.

        Returns:
            dict: A dictionary containing the contents of the YAML configuration file.
        """

        self.__log('info', 'Reading config file')

        observatory = {}
        with open(config_filename, 'r') as stream:
            try:
                observatory = yaml.safe_load(stream)
                self.__log('info', f"Config file {config_filename} read")
            except yaml.YAMLError as exc:
                self.__log('error', f"Error reading config file {config_filename}: {exc}")

        return observatory
    
    def load_devices(self) -> dict:
        '''
        This method iterates through the observatory configuration, creating and starting
        device objects for each defined device.

        Returns:
            devices (dict): A dictionary containing initialized device objects, categorized
            by device type.
        '''

        self.__log('info', 'Loading devices')

        devices = {}
        for device_type in self.observatory:
            devices[device_type] = {}
            if device_type != 'Misc':
                for d in self.observatory[device_type]:
                    try:
                        devices[device_type][d['device_name']] = AlpacaDevice(d['ip'], 
                                                                              device_type, 
                                                                              d['device_number'], 
                                                                              d['device_name'],
                                                                              self.queue, 
                                                                              self.debug)
                        devices[device_type][d['device_name']].start()
                    except Exception as e:
                        self.error_source.append({'device_type': device_type, 'device_name': d['device_name'], 'error': str(e)})
                        self.__log('error', f"Error loading {device_type} {d['device_name']}: {str(e)}")
        
        self.__log('info', 'Devices loaded')

        return devices

    def connect_all(self) -> None:
        '''
        Connects to all loaded devices and starts polling at specific intervals
        to retrieve non-fixed FITS headers. The polling interval is 5 seconds for most
        devices and 1 second for the SafetyMonitor.

        '''

        self.__log('info', 'Connecting to devices')

        # connect to all devices
        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                try:
                    self.devices[device_type][device_name].set("Connected", True) ## slow?
                    self.__log('info', f"{device_type} {device_name} connected")
                except Exception as e:
                    self.error_source.append({'device_type': device_type, 'device_name': device_name, 'error': str(e)})
                    self.__log('error', f"Error connecting to {device_type} {device_name}: {str(e)}")

        self.__log('info', 'Starting polling non-fixed fits headers')

        delay = 5 # seconds
        # start polling non-fixed fits headers
        for i, row in self.fits_config.iterrows():
            if (row['device_type'] not in ['astropy_default', 'astra', 'astra_fixed', '']) and row['fixed'] is False:
                device_type = row['device_type']
                for device_name in self.devices[device_type]:
                    device = self.devices[device_type][device_name]
                    try:
                        device.start_poll(row['device_command'], delay) # 5 second polling
                    except Exception as e:
                        self.error_source.append({'device_type': device_type, 'device_name': device_name, 'error': str(e)})
                        self.__log('error', f"Error starting polling for {device_type} {device_name}: {str(e)}")
        
        delay = 1 # seconds
        if 'SafetyMonitor' in self.observatory:
            device_type = 'SafetyMonitor'
            device_name = self.observatory[device_type][0]['device_name']

            device = self.devices[device_type][device_name]
            try:
                device.start_poll('IsSafe', delay) # 1 second polling
            except Exception as e:
                self.error_source.append({'device_type': device_type, 'device_name': device_name, 'error': str(e)})
                self.__log('error', f"Error starting polling for {device_type} {device_name}: {str(e)}")

        self.__log('info', 'Connect all sequence complete')
        # run can<> ascom commands, needed for other commands to work? Else, alternatives needed.

    def unload_all(self) -> None:
        '''
        This method gracefully shuts down the various components of the system, including the watchdog,
        schedule, and polling mechanisms. It also disconnects and unloads all devices registered in the system,
        ensuring a clean shutdown. Finally, it closes the SQLite database.

        '''
        
        self.__log('info', 'Disconnecting from devices')

        # stop threads
        if self.watchdog_running is True:
            self.watchdog_running = False

        if self.schedule_running is True:
            self.schedule_running = False

        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                try:
                    self.devices[device_type][device_name].stop_poll()
                    
                    self.devices[device_type][device_name].set("Connected", False) ## slow?
                    self.__log('info', f"{device_type} {device_name} disconnected")

                    self.devices[device_type][device_name].stop() ## unloads device?
                except Exception as e:
                    self.error_source.append({'device_type': device_type, 'device_name': device_name, 'error': str(e)})
                    self.__log('error', f"{device_type} {device_name} not disconnected: {str(e)}")
                
        self.__log('info', 'Disconnect all sequence complete')

        self.cursor.close()

    def start_watchdog(self) -> None:
        '''
        Start the watchdog thread if it is not already running.

        This method initializes and starts a new thread responsible for monitoring
        certain aspects of the system. If the watchdog thread is already running,
        it logs a warning and takes no action.

        '''
        
        if self.watchdog_running is True:
            self.__log('warning', 'Watchdog already running')
            return
        
        th = Thread(target=self.watchdog, daemon = True)
        th.start()

        self.threads.append({'type': 'watchdog', 'device_name': 'watchdog', 'thread': th, 'id' : 'watchdog'})

    def watchdog(self) -> None:
        """
        Periodically monitors various aspects of the observatory's operation and takes appropriate actions in case of issues.

        This function performs the following checks and actions:

        - Periodically checks the SafetyMonitor's status, telescope altitude, system errors, and device responsiveness.
        - If the SafetyMonitor indicates unsafe conditions, it closes the observatory.
        - Starts the schedule independently of weather conditions, running only calibration sequences when weather is unsafe.
        - Monitors for system errors and handles them as necessary.

        """

        self.__log('info', 'Starting watchdog')

        self.watchdog_running = True

        # initial safety monitor check
        if 'SafetyMonitor' in self.observatory:

            self.__log('info', 'Safety monitor found')

            device_type = 'SafetyMonitor'
            sm_name = self.observatory[device_type][0]['device_name']

            safety_monitor = self.devices[device_type][sm_name]

            try:
                sm_poll = safety_monitor.poll_latest()

                while sm_poll['IsSafe']['value'] is None and self.error_free:
                    sm_poll = safety_monitor.poll_latest()
                    time.sleep(0.5)
            except Exception as e:
                self.error_source.append({'device_type': 'SafetyMonitor', 'device_name': sm_name, 'error': str(e)})
                self.__log('error', f"Error polling safety monitor {sm_name}: {str(e)}")
        
        else:
            self.__log('warning', 'No safety monitor found')

        # observatory closed flag, used to prevent multiple logging of weather unsafe and closing observatory
        closed = False
        
        while self.watchdog_running:

            # check if any devices unresponsive - hopefully never happens   
            for device_type in self.devices:
                for device_name in self.devices[device_type]:
                    try:
                        r = self.devices[device_type][device_name].is_alive()
                        if r is False:
                            self.error_source.append({'device_type': device_type, 'device_name': device_name, 'error': 'Device unresponsive'})
                            self.__log('error', f"{device_type} {device_name} unresponsive")
                    except Exception as e:
                        self.error_source.append({'device_type': device_type, 'device_name': device_name, 'error': str(e)})
                        self.__log('error', f"{device_type} {device_name} unresponsive")
            
            # update heartbeat
            self.heartbeat['datetime'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            self.heartbeat['error_free'] = self.error_free
            self.heartbeat['error_source'] = self.error_source
            self.heartbeat['weather_safe'] = self.weather_safe
            self.heartbeat['schedule_running'] = self.schedule_running
            self.heartbeat['interrupt'] = self.interrupt
            self.heartbeat['cpu_percent'] = psutil.cpu_percent()
            self.heartbeat['memory_percent'] = psutil.virtual_memory().percent


            if self.error_free is True:
                try:      
                    # cleanup dead threads
                    for i in self.threads:
                        if i['thread'].is_alive() is False:
                            self.threads.remove(i)

                    # check if schedule file updated
                    try:
                        schedule_mtime = os.path.getmtime(self.schedule_path)

                        if schedule_mtime > self.schedule_mtime:
                            self.__log('warning', 'Schedule updated')
                            self.schedule = self.read_schedule()
                    except Exception as e:
                        self.error_source.append({'device_type': 'Schedule', 'device_name': 'schedule', 'error': str(e)})
                        self.__log('error', f"Error checking schedule: {str(e)}")
                        continue

                    # check telescope(s) altitude
                    if 'Telescope' in self.observatory:
                        for telescope_name in self.devices['Telescope']:
                            telescope = self.devices['Telescope'][telescope_name]
                            telescope_index = [i for i, d in enumerate(self.observatory['Telescope']) if d['device_name'] == telescope_name][0]

                            if 'alt_limit' in self.observatory['Telescope'][telescope_index]:

                                alt_limit = self.observatory['Telescope'][telescope_index]['alt_limit']

                                t_poll = telescope.poll_latest()
                                if t_poll['Altitude']['value'] <= alt_limit:

                                    # TODO: BETTER LOGIC HERE NEEDED - CHECK IF SLEWING, GUIDING, TRACKING, ETC. before stopping
                                    self.__log('warning', f"Telescope {telescope_name} altitude {t_poll['Altitude']['value']} < {alt_limit}")

                                    # stop telescope slewing
                                    self.monitor_action('Telescope', 'Slewing', False, 'AbortSlew',
                                                        device_name = telescope_name,
                                                        log_message = f"Stopping telescope {telescope_name} slewing")
                                    

                                    # stop tracking
                                    self.monitor_action('Telescope', 'Tracking', False, 'Tracking',
                                                        device_name = telescope_name,
                                                        log_message = f"Stopping telescope {telescope_name} tracking")
        
                                    # stop guiding
                                    try:
                                        self.guider[telescope_name].running = False
                                    except Exception as e:
                                        self.error_source.append({'device_type': 'Guider', 'device_name': telescope_name, 'error': str(e)})
                                        self.__log('error', f"Error stopping telescope {telescope_name} guiding: {str(e)}")
                                        continue
                                    
                    
                    # check safety monitor
                    if 'SafetyMonitor' in self.observatory:

                        sm_poll = safety_monitor.poll_latest()

                        # check if stale
                        last_update = (datetime.utcnow() - sm_poll['IsSafe']['datetime']).total_seconds()

                        match last_update:
                            case _ if last_update > 3 and last_update < 30:
                                self.__log('warning', f"Safety monitor {last_update}s stale")
                            case _ if last_update > 30:
                                self.error_source.append({'device_type': 'SafetyMonitor', 'device_name': sm_name, 'error': f"Stale data {last_update}s"})
                                self.__log('error', f"Safety monitor {last_update}s stale")
                                continue
                        

                        if sm_poll['IsSafe']['value'] is False:

                            self.weather_safe = False
                            
                            # log message saying weather unsafe and close observatory
                            if closed is False:
                                self.__log('warning', 'Weather unsafe')
                                # self.__log('info', 'Closing observatory')

                            # may want to close dome before park telescope?
                            self.close_observatory() # checks if already closed and closes if not
                            closed = True

                            ## TODO: kill certain threads?


                    if self.schedule_running is False:
                        # TODO: Smarter logic
                        
                        if 'SafetyMonitor' in self.observatory:
                            if self.truncate_schedule is True:
                                rows = self.cursor.execute("SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND device_value = 'False' AND datetime > datetime('now', '-1 minutes')")
                            else:
                                rows = self.cursor.execute("SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND device_value = 'False' AND datetime > datetime('now', '-30 minutes')")

                        else:
                            rows = []

                        self.__log('debug', f"Watchdog: {len(rows)} instances of weather unsafe found in last 1 minutes")
                        
                        # if no weather unsafe in last 30 minutes, start schedule
                        if len(rows) == 0:
                            self.weather_safe = True
                            closed = False # reset closed flag, but should be smarter than this.
                            
                        # start schedule - if weather is not safe, only calibration sequences will run
                        if self.schedule.iloc[-1]['end_time'] > datetime.utcnow():
                            
                            if self.truncate_schedule is False:
                                self.schedule = self.read_schedule()

                            self.schedule_running = True
                            th = Thread(target=self.run_schedule, daemon = True)
                            th.start()
                            self.threads.append({'type': 'run_schedule', 'device_name': 'Schedule', 'thread': th, 'id' : 'schedule'})


                except Exception as e:
                    self.__log('error', f"Error during watchdog check: {str(e)}")

            else:
                try:
                    # stop watchdog
                    self.watchdog_running = False

                    # stop schedule
                    self.schedule_running = False  

                    # wait a bit to see if it's a multi-device error?
                    time.sleep(30)

                    # make pandas dataframe of error_source
                    df = pd.DataFrame(self.error_source)

                    device_types = df.device_type.unique()
                    device_names = df.device_name.unique()

                    # multiple devices have errors
                    if len(device_names) > 1:
                        self.__log('error', f"Multiple devices have errors: {device_names}. Panic.")
                        # TODO: Panic mode
                    elif len(device_names) == 1 and len(device_types) == 1:
                        # only one device has errors
                        match device_types[0]:
                            case 'SafetyMonitor':
                                self.close_observatory()
                            case 'ObservingConditions':
                                self.close_observatory()
                            case 'Telescope':
                                pass
                            case 'Dome':
                                pass
                            case 'Guider':
                                self.close_observatory()
                            case 'Camera':
                                self.close_observatory()
                            case 'FilterWheel':
                                self.close_observatory()
                            case 'Focuser':
                                self.close_observatory()
                            case 'Rotator':
                                pass
                            case 'CoverCalibrator':
                                pass
                            case 'Switch':
                                pass
                            case 'Schedule':
                                self.close_observatory()
                            case 'Queue':
                                # restart queue?
                                pass
                            case 'Headers':
                                pass
                            case _:
                                pass
                except Exception as e:
                    self.__log('error', f"Error during error handling: {str(e)}")
                    # TODO: Panic mode

            time.sleep(0.5) # twice the safety monitor polling time


        self.watchdog_running = False
        self.__log('warning', 'Watchdog stopped')
    
    def open_observatory(self, paired_devices : dict = None) -> None:
        """
        Opens the observatory in a controlled sequence: first, it opens the dome shutter if available, 
        and then it unparks the telescope if available and weather safe.

        Parameters:
            paired_devices (dict): A dictionary of paired devices. Defaults to None.

        """

        if 'Dome' in self.observatory:
            if self.weather_safe and self.error_free and (self.interrupt is False):
                # open dome shutter
                if paired_devices is not None:
                    self.monitor_action('Dome', 'ShutterStatus', 0, 'OpenShutter', 
                                        device_name = paired_devices['Dome'],
                                        log_message = f"Opening Dome shutter of {paired_devices['Dome']}")
                else:
                    self.monitor_action('Dome', 'ShutterStatus', 0, 'OpenShutter',
                                        log_message = "Opening Dome shutter(s)")

        if 'Telescope' in self.observatory:
            if self.weather_safe and self.error_free and (self.interrupt is False):
                # unpark telescope
                if paired_devices is not None:
                    self.monitor_action('Telescope', 'AtPark', False, 'Unpark', 
                                        device_name = paired_devices['Telescope'],
                                        log_message = f"Unparking Telescope {paired_devices['Telescope']}")
                else:
                    self.monitor_action('Telescope', 'AtPark', False, 'Unpark',
                                        log_message = "Unparking Telescope(s)")

    def close_observatory(self, paired_devices : dict = None) -> None:
        '''
        Close the observatory operations in the following order:

        1. Stop telescope slewing and tracking.

        2. Park the telescope.

        3. Stop guiding if applicable.

        4. Park the dome and close its shutter (if observatory has a dome).

        Parameters:
            paired_devices (dict, optional): A dictionary of paired devices to specify the target devices.
                Example: {'Telescope': 'TelescopeName', 'Dome': 'DomeName'}

        '''
        
        if 'Telescope' in self.observatory:

            # stop telescope slewing
            if paired_devices is not None:
                self.monitor_action('Telescope', 'Slewing', False, 'AbortSlew', 
                                        device_name = paired_devices['Telescope'], 
                                        log_message = f"Stopping telescope {paired_devices['Telescope']} slewing")
            else:
                self.monitor_action('Telescope', 'Slewing', False, 'AbortSlew',
                                        log_message = "Stopping Telescope(s) slewing")

            # stop telescope tracking
            if paired_devices is not None:
                self.monitor_action('Telescope', 'Tracking', False, 'Tracking',
                                        device_name = paired_devices['Telescope'],
                                        log_message = f"Stopping telescope {paired_devices['Telescope']} tracking")
            else:
                self.monitor_action('Telescope', 'Tracking', False, 'Tracking',
                                        log_message = "Stopping Telescope(s) tracking")
                
            # park telescope
            if paired_devices is not None:
                self.monitor_action('Telescope', 'AtPark', True, 'Park', 
                                        device_name = paired_devices['Telescope'],
                                        log_message = f"Parking telescope {paired_devices['Telescope']}")

            else:
                self.monitor_action('Telescope', 'AtPark', True, 'Park',
                                        log_message = "Parking Telescope(s)")
                
            # stop guiding
            for d in self.devices['Telescope']:
                self.guider[d].running = False
                try:
                    self.guider[d].running = False
                except Exception as e:
                    self.error_source.append({'device_type': 'Guider', 'device_name': d, 'error': str(e)})
                    self.__log('error', f"Error stopping telescope {d} guiding: {str(e)}")
                    continue

            
        if 'Dome' in self.observatory:

            # park dome
            if paired_devices is not None:
                self.monitor_action('Dome', 'AtPark', True, 'Park', 
                                        device_name = paired_devices['Dome'],
                                        log_message = f"Parking Dome {paired_devices['Dome']}")
            else:
                self.monitor_action('Dome', 'AtPark', True, 'Park',
                                        log_message = "Parking Dome(s)")
            
            # close dome shutter
            if paired_devices is not None:
                self.monitor_action('Dome', 'ShutterStatus', 1, 'CloseShutter', 
                                        device_name = paired_devices['Dome'],
                                        log_message = f"Closing Dome shutter of {paired_devices['Dome']}")
            else:
                self.monitor_action('Dome', 'ShutterStatus', 1, 'CloseShutter',
                                        log_message = "Closing Dome shutter(s)")

    def toggle_interrupt_thread(self) -> None:
        '''
        Starts a new thread to handle user interrupt.

        This function starts a new thread to handle user interrupt by toggling the interrupt flag and stopping various observatory actions.
        '''

        if self.interrupt is True:
            self.__log('warning', 'Observatory already interrupted')
            return

        th = Thread(target=self.toggle_interrupt, daemon=True)
        th.start()

        self.threads.append({'type': 'toggle_interrupt', 'device_name': 'all_devices', 'thread': th, 'id' : 'toggle_interrupt'})

    def toggle_interrupt(self) -> None:
        '''
        Handle user interrupt by toggling the interrupt flag and stopping various observatory actions.

        This function handles user interruptions by setting the interrupt flag to True, which signals the observatory
        to stop ongoing actions. It stops the watchdog, schedule, and performs the following actions depending on the
        observatory components:

        - Telescope: Abort slewing, stop tracking, and stop guiding.
        - Dome: Abort slewing.
        - Camera: Stop exposure.

        '''

        self.interrupt = True

        self.__log('warning', 'Observatory interrupted')

        # stop watchdog
        self.watchdog_running = False

        # stop schedule
        self.schedule_running = False        

        ## abort all actions
        if 'Telescope' in self.observatory:

            # stop telescope slewing
            th = Thread(target=self.monitor_action, args=('Telescope', 'Slewing', False, 'AbortSlew',), daemon=True)
            th.start()

            self.threads.append({'type': 'AbortSlew', 'device_name': 'all_telescopes', 'thread': th, 'id' : 'stop_slewing'})

            # stop telescope tracking
            th = Thread(target=self.monitor_action, args=('Telescope', 'Tracking', False, 'Tracking',), daemon=True)
            th.start()

            self.threads.append({'type': 'Tracking', 'device_name': 'all_telescopes', 'thread': th, 'id' : 'stop_tracking'})

            # stop guiding
            for d in self.devices['Telescope']:
                self.guider[d].running = False
                try:
                    self.guider[d].running = False
                except Exception as e:
                    self.error_source.append({'device_type': 'Guider', 'device_name': d, 'error': str(e)})
                    self.__log('error', f"Error stopping telescope {d} guiding: {str(e)}")
                    continue

        if 'Dome' in self.observatory:
            # stop dome slewing
            th = Thread(target=self.monitor_action, args=('Dome', 'Slewing', False, 'AbortSlew',), daemon=True)
            th.start()

            self.threads.append({'type': 'AbortSlew', 'device_name': 'all_domes', 'thread': th, 'id' : 'stop_dome_slewing'})

        if 'Camera' in self.observatory:
            # stop camera exposure -- sometimes misses if camera is idle already between exposures
            th = Thread(target=self.monitor_action, args=('Camera', 'CameraState', 0, 'AbortExposure',), daemon=True)
            th.start()

            self.threads.append({'type': 'AbortSlew', 'device_name': 'all_cameras', 'thread': th, 'id' : 'stop_camera'})


        # wait for all threads to finish
        for t in self.threads:
            if t['thread'].is_alive() is True:
                if t['id'] in ['stop_slewing', 'stop_tracking', 'stop_dome_slewing', 'stop_camera']:
                    time.sleep(1)
            else:
                self.threads.remove(t)

        time.sleep(5) # time for interrupt to be caught by other threads

        # reset interrupt
        self.interrupt = False
        
    def read_schedule(self) -> pd.DataFrame:
        """
        Read the schedule CSV file and return it as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the schedule data, with columns 'start_time' and 'end_time'.

        Raises:
            FileNotFoundError: If the schedule CSV file does not exist.
            Exception: If an error occurs during reading.

        Notes:
            - If the schedule CSV file is not found, a FileNotFoundError is raised.
            - The schedule DataFrame is sorted by the 'start_time' column.
            - If self.truncate_schedule is True, the schedule is truncated for development purposes using the update_times function.
        """
        # TODO: schedule validity checker, add schedule as string to log?

        try:
            schedule_mtime = os.path.getmtime(self.schedule_path)

            if (schedule_mtime > self.schedule_mtime) or (self.schedule is None):

                if self.schedule_running is True:
                    self.__log('warning', 'Schedule updating while the previous schedule is running. This will not take effect until the new schedule is run.')

                self.__log('info', 'Reading schedule')

                schedule = pd.read_csv(self.schedule_path)
                schedule['start_time'] = pd.to_datetime(schedule.start_time)
                schedule['end_time'] = pd.to_datetime(schedule.end_time)

                # Sort the schedule by start_time
                schedule = schedule.sort_values(by=['start_time'])

                # For development: Truncate the schedule if self.truncate_schedule is True
                if self.truncate_schedule is True:
                    schedule = update_times(schedule, 100)

                self.__log('info', 'Schedule read')

                self.schedule_mtime = schedule_mtime

                return schedule
            else:
                return self.schedule

        except FileNotFoundError:
            self.error_source.append({'device_type': 'Schedule', 'device_name': '', 'error': 'Schedule CSV file not found.'})
            self.__log('error', 'Schedule CSV file not found.')
            raise
        except Exception as e:
            self.error_source.append({'device_type': 'Schedule', 'device_name': '', 'error': f'Error reading schedule: {e}'})
            self.__log('error', f'Error reading schedule: {e}')
            raise

    def run_schedule(self) -> None:
        '''
        Run the schedule while monitoring safety conditions and executing scheduled actions.

        This method manages the execution of a schedule, considering safety checks, weather conditions,
        and action types. It iterates through schedule rows, starts threads for actions if conditions are met.
        If the action type is to open or close, it ensures that the actions are completed before proceeding 
        to the next item in the schedule.
 
        '''
        t0 = time.time()
        while self.weather_safe is None and (time.time() - t0) < 120:
            self.__log('info', 'Waiting for safety conditions to be checked')
            time.sleep(1)
        
        if self.weather_safe is None:
            self.error_source.append({'device_type': 'SafetyMonitor', 'device_name': '', 'error': 'Weather safety check timed out'})
            self.__log('error', 'Weather safety check timed out')
            return False

        self.__log('info', 'Running schedule')
        self.schedule_running = True
        for i, row in self.schedule.iterrows():

            # run if weather safe, or the action is calibration (bias, dark)
            if (self.weather_safe is True) or (row['action_type'] in ['calibration']):

                # loop through self.threads and remove the ones that are dead due to finishing or weather getting to them?
                for j in self.threads:
                    if j['thread'].is_alive() is False:
                        self.threads.remove(j)

                ids = [k['id'] for k in self.threads]

                # if not running, start thread
                if i not in ids:
                    run_row = True
                    while run_row and self.schedule_running and ((self.weather_safe is True) or (row['action_type'] in ['calibration'])) and self.error_free and (self.interrupt is False):
                        t = datetime.utcnow()
                        
                        if row['start_time'] >= t:
                            time.sleep(1)

                        elif (row['start_time'] <= t) and (row['end_time'] >= t):

                            th = Thread(target=self.run_action, args=(row,), daemon=True)
                            th.start()

                            self.threads.append({'type': row['action_type'], 'device_name': row['device_name'], 'thread': th, 'id' : i})
                            
                            # if open or close, wait for thread to finish before continuing
                            if row['action_type'] in ['open', 'close']:
                                # wait for thread to finish
                                while (th.is_alive() is True) and self.weather_safe and self.schedule_running and self.error_free and (self.interrupt is False):
                                    time.sleep(1)

                            run_row = False

                            # if last row, sleep until thread is finished to prevent from returning to start of schedule by watchdog
                            if i == self.schedule.index[-1]:
                                while (th.is_alive() is True) and (self.weather_safe or (row['action_type'] in ['calibration'])) and self.schedule_running and self.error_free and (self.interrupt is False):
                                    time.sleep(1)
                                self.__log('info', f"Waiting for last schedule item to reach endtime of {row['end_time']}: {row['device_name']} {row['action_type']}")
                                while ((self.weather_safe is True) or (row['action_type'] in ['calibration'])) and self.schedule_running and self.error_free and (self.interrupt is False):
                                    t_until_end = (row['end_time'] - datetime.utcnow()).total_seconds()
                                    if t_until_end > 0:
                                        time.sleep(1)
                                    else:
                                        break
                        else:
                            run_row = False

        # run headers completion
        if self.schedule_running and self.error_free and (self.interrupt is False):
            self.__log('info', 'Completing headers')
            th = Thread(target=self.final_headers, daemon=True)
            th.start()
            self.threads.append({'type': 'Headers', 'device_name': 'astra', 'thread': th, 'id' : "complete_headers"})

        self.schedule_running = False
        self.__log('info', 'Schedule stopped')
        
    def run_action(self, row : dict) -> None:
        '''
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

        '''

        self.__log('info', f"Starting {row['device_name']} {row['action_type']}")

        try:
            if row['device_type'] == 'Camera':
                cam_index = [i for i, d in enumerate(self.observatory['Camera']) if d['device_name'] == row['device_name']][0]
                paired_devices = self.observatory['Camera'][cam_index]['paired_devices']
                paired_devices['Camera'] = row['device_name']

            if 'object' == row['action_type']:
                self.object_sequence(row, paired_devices)
            elif 'calibration' == row['action_type']:
                self.calibration_sequence(row, paired_devices)
            elif 'flats' == row['action_type']:
                self.flats_sequence(row, paired_devices)
            elif 'open' == row['action_type']:
                if 'Camera' in self.observatory:
                    # turn camera cooler on
                    self.monitor_action('Camera', 'CoolerOn', True, 'CoolerOn',
                                        device_name = row['device_name'],
                                        log_message = f"Turning on camera cooler for {row['device_name']}")
                    
                    # set temperature
                    set_temperature = self.observatory['Camera'][cam_index]['temperature']
                    self.monitor_action('Camera', 'CCDTemperature', set_temperature, 'SetCCDTemperature',
                                        device_name = row['device_name'],
                                        run_command_type='set',
                                        abs_tol=0.5,
                                        log_message = f"Setting camera {row['device_name']} temperature to {set_temperature}",
                                        timeout = 60*30) # 30 minutes

                    # open dome and unpark telescope
                    self.open_observatory(paired_devices)
                else:
                    # open all dome(s) and unpark telescope(s)
                    self.open_observatory()

            elif 'close' == row['action_type']:
                if 'Camera' in self.observatory:
                    # close dome and park telescope
                    self.close_observatory(paired_devices)
                else:
                    # close all dome(s) and park telescope(s)
                    self.close_observatory()

            else: 
                # if not 'object' or 'calibration' or 'flats', assume it's an ASCOM command
                device = self.devices[row['device_type']][row['device_name']]

                if row['action_type'] in dir(device.device):
                    if isinstance(eval(row['action_value']), dict):
                        device.get(row['action_type'])(**eval(row['action_value']))
                        self.__log('info', f"Finished {row['device_name']} {row['action_type']} {row['action_value']}")
 
                    else:
                        device.set(row['action_type'], row['action_value'])
                        self.__log('info', f"Finished {row['device_name']} {row['action_type']} {row['action_value']}")
                        
                else:
                    raise ValueError(f"Invalid action_type: {row['device_name']} {row['action_type']} with {row['action_value']} is not a valid method or property for {row['device_type']} {row['device_name']}")
        except Exception as e:
            self.schedule_running = False
            self.__log('error', f"Run action error: {str(e)}")
        
    def pre_sequence(self, row : dict, paired_devices :dict) -> tuple:
        '''
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
        '''

        self.__log('debug', f"Running pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}")
        
        action_value = eval(row['action_value']) # TODO: put part of schedule check
        folder = utils.create_image_dir()
        
        # prepare observatory for sequence
        self.setup_observatory(paired_devices, action_value)

        # write base header
        hdr = self.base_header(paired_devices, action_value)
        
        if 'object' == row['action_type']:
            hdr['IMGTYPE'] = 'Light'

        self.__log('debug', f"Finished pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}")

        return action_value, folder, hdr
    
    def setup_observatory(self, paired_devices : dict, action_value : dict, filter_list_index : int = 0) -> None:
        '''
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

        '''

        self.__log('debug', f"Running setup_observatory for {paired_devices} {action_value}")

        # unpark and slew to target
        if ('ra' in action_value) and ('dec' in action_value) and self.weather_safe and self.error_free and (self.interrupt is False):

            if 'Telescope' in paired_devices:

                if 'Dome' not in paired_devices:
                    self.__log('warning', f"Telescope {paired_devices['Telescope']} has no paired Dome. Opening all available domes.")
                
                # open dome and unpark telescope -- this will open all domes if not in paired_devices...?
                self.open_observatory(paired_devices)

                telescope = self.devices['Telescope'][paired_devices['Telescope']]

                if self.weather_safe and self.error_free and (self.interrupt is False):
                
                    # set tracking to true
                    self.monitor_action('Telescope', 'Tracking', True, 'Tracking', 
                                        device_name = paired_devices['Telescope'],
                                        log_message = f"Setting Telescope {paired_devices['Telescope']} tracking to True")

                    # slew to target
                    self.__log('info', f"Slewing Telescope {paired_devices['Telescope']} to {action_value['ra']} {action_value['dec']}")
                    telescope.get('SlewToCoordinatesAsync')(RightAscension = 24*action_value['ra']/360, Declination = action_value['dec'])

                    # wait for slew to finish
                    self.wait_for_slew(paired_devices)

        # set filter
        if 'filter' in action_value and 'FilterWheel' in paired_devices and self.error_free and (self.interrupt is False):

            # get filter name
            f = action_value['filter']
            if isinstance(f, list):
                f = f[filter_list_index]

            filterwheel = self.devices['FilterWheel'][paired_devices['FilterWheel']]
            names = filterwheel.get('Names')

            # find index of filter name
            filter_index = [i for i, d in enumerate(names) if d == f][0]

            # set filter
            self.monitor_action('FilterWheel', 'Position', filter_index, 'Position', 
                                device_name = paired_devices['FilterWheel'],
                                log_message = f"Setting FilterWheel {paired_devices['FilterWheel']} to {f}")
            
    def wait_for_slew(self, paired_devices : dict) -> None:
        '''
        Wait for a telescope to complete its slewing operation.

        Parameters:
            paired_devices (dict): A dictionary containing paired devices, including the 'Telescope' device.

        Raises:
            TimeoutError: If the slewing operation takes longer than 2 minutes.

        '''

        telescope = self.devices['Telescope'][paired_devices['Telescope']]

        # wait for slew to finish
        start_time = time.time()
        
        slewing = telescope.get('Slewing')

        if slewing is True:
            self.__log('info', f"Telescope {paired_devices['Telescope']} slewing...")

        while slewing is True and self.weather_safe and self.error_free and (self.interrupt is False) and self.schedule_running and self.watchdog_running:

            if time.time() - start_time > 120: # 2 minutes hardcoded limit
                raise TimeoutError('Slew timeout')

            time.sleep(1)

            slewing = telescope.get('Slewing')
                
    def calibration_sequence(self, row : dict, paired_devices : dict) -> None:
        '''
        Run a bias/dark calibration sequence for a specific camera.

        This function performs a calibration sequence for a camera, capturing bias and dark frames.
        It operates based on the provided parameters and camera settings.

        Parameters:
            row (dict): A dictionary containing information about the camera and calibration settings.

                - 'device_name' (str): The name of the camera device.
                - 'start_time' (datetime): The start time for the calibration sequence.
                - 'end_time' (datetime): The end time for the calibration sequence.
                - 'device_type' (str): The type of the camera device.

            paired_devices (dict): A dictionary of paired devices used in the calibration sequence.

        Notes:
            - The function logs information about the calibration sequence's progress.
            - It captures bias and dark frames for different exposure times as specified in 'action_value'.
            - The sequence will continue to run until one of the following conditions is met:
                - The current time exceeds 'end_time'.
                - An error occurs during execution.
                - The sequence is manually interrupted.
                - The schedule is stopped.
                - The watchdog process is terminated.
        '''

        self.__log('info', f"Running calibration sequence for {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}")

        action_value, folder, hdr = self.pre_sequence(row, paired_devices)
        
        camera = self.devices[row['device_type']][row['device_name']]

        maxadu = camera.get('MaxADU')

        for i, exptime in enumerate(action_value['exptime']):

            count = 0
            if (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) \
                and self.error_free and (self.interrupt is False) \
                and self.schedule_running and self.watchdog_running:

                hdr['EXPTIME'] = exptime

                if exptime == 0:
                    hdr['IMGTYPE'] = 'Bias'
                else:
                    hdr['IMGTYPE'] = 'Dark'
                
                self.__log('info', f"Exposing {count + 1}/{action_value['n'][i]} {row['device_name']} {hdr['IMGTYPE']} for exposure time {hdr['EXPTIME']} s")
                camera.get('StartExposure')(Duration = exptime, Light = False)
            
                while (count < action_value['n'][i]) and (row['start_time'] <= datetime.utcnow()) \
                        and (row['end_time'] >= datetime.utcnow()) \
                        and self.error_free and (self.interrupt is False) and self.schedule_running \
                        and self.watchdog_running:

                    r = camera.get('ImageReady')
                    time.sleep(0) # yield to other threads
                    if r is True:
                        self.__log('debug', f"Image ready from {row['device_name']} to download.")

                        t0 = datetime.utcnow()

                        # get last exposure start time
                        r = camera.get('LastExposureStartTime')
                        self.__log('debug', f"LastExposureStartTime from {row['device_name']} was {r}")
                        dateobs = pd.to_datetime(r)
                        
                        # save image
                        self.__log('debug', f"Saving image from {row['device_name']}")
                        self.save_image(camera, hdr, dateobs, t0, maxadu, folder)

                        count += 1

                        if count < action_value['n'][i]:
                            # start next exposure
                            self.__log('info', f"Exposing {count + 1}/{action_value['n'][i]} {row['device_name']} {hdr['IMGTYPE']} for exposure time {hdr['EXPTIME']} s")
                            camera.get('StartExposure')(Duration = exptime, Light = False)

        self.__log('info', f"Calibration sequence ended for {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}")

    def object_sequence(self, row : dict, paired_devices : dict) -> None:
        '''
        Run an object sequence for a specified device.

        This method executes a sequence of actions for a given device, such as capturing images, pointing correction,
        and guiding if necessary, within a specified time frame and under specific conditions.

        Parameters:
            row (dict): A dictionary containing information about the sequence and the device, including 'action_value', 'device_name', 'start_time', and 'end_time'.
            paired_devices (dict): A dictionary specifying paired devices, such as a Telescope for guiding.

        Notes:
            - The 'row' dictionary should have the following keys:
                - 'action_value' (str): A JSON-encoded string containing configuration values for the sequence.
                - 'device_name' (str): The name of the device on which the sequence is to be executed.
                - 'start_time' (datetime): The start time for the sequence.
                - 'end_time' (datetime): The end time for the sequence.
            - The method will perform actions like capturing images, pointing correction, and guiding based on the 'action_value'.
            - The sequence will continue to run until one of the following conditions is met:
                - The current time exceeds 'end_time'.
                - Adverse weather conditions are detected.
                - An error occurs during execution.
                - The sequence is manually interrupted.
                - The schedule is stopped.
                - The watchdog process is terminated.
        '''

        self.__log('info', f"Running object sequence for {eval(row['action_value'])['object']} with {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}")

        action_value, folder, hdr = self.pre_sequence(row, paired_devices)

        hdr['EXPTIME'] = action_value['exptime']

        camera = self.devices[row['device_type']][row['device_name']]

        maxadu = camera.get('MaxADU')
        
        camera.get('StartExposure')(Duration = action_value['exptime'], Light = True)

        pointing_complete = False
        pointing_attempts = 0
        guiding = False

        while (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) \
                and self.weather_safe and self.error_free and (self.interrupt is False) \
                and self.schedule_running and self.watchdog_running:           
 
            r = camera.get('ImageReady')
            time.sleep(0) # yield to other threads
            if r is True:
            
                self.__log('debug', f"Image ready from {row['device_name']} to download.")

                t0 = datetime.utcnow()
                
                # get last exposure start time
                r = camera.get('LastExposureStartTime')
                self.__log('debug', f"LastExposureStartTime from {row['device_name']} was {r}")
                dateobs = pd.to_datetime(r)
                
                # save image
                self.__log('debug', f"Saving image from {row['device_name']}")
                filepath = self.save_image(camera, hdr, dateobs, t0, maxadu, folder)
                
                # pointing correction if not already done
                if 'pointing' in action_value and pointing_complete is False:
                    if action_value['pointing'] is True:
                        self.__log('info', f"Running pointing correction for {action_value['object']} with {row['device_name']}")

                        try:
                            offset_ra, offset_dec, wcs = utils.point_correction(filepath, action_value['ra'], action_value['dec'])
                        except Exception as e:
                            self.__log('warning', f"Error running pointing correction for {action_value['object']} with {row['device_name']}: {str(e)}")
                            pointing_complete = True

                        if pointing_complete is False:
                            tel_index = [i for i, d in enumerate(self.observatory['Telescope']) if d['device_name'] == paired_devices['Telescope']][0]
                            pointing_threshold = self.observatory['Telescope'][tel_index]['pointing_threshold'] / 60

                            if (abs(offset_ra) < pointing_threshold) or (abs(offset_dec) < pointing_threshold):
                                self.__log('info', f"No further pointing correction required. Correction of {offset_ra*60}\" {offset_dec*60}\" within threshold of {pointing_threshold*60}\"")
                                pointing_complete = True
                            else:
                                self.__log('info', f"Pointing correction of {offset_ra*60}\" {offset_dec*60}\" required")

                                # sync telescope to corrected coordinates, TODO: check if right +-
                                telescope = self.devices['Telescope'][paired_devices['Telescope']]
                                telescope.get('SyncToCoordinates')(RightAscension = 24*(action_value['ra'] + offset_ra)/360, Declination = action_value['dec'] + offset_dec)
                                
                                # re-slew to target
                                self.setup_observatory(paired_devices, action_value)
                        
                            pointing_attempts += 1

                            if pointing_attempts > 3:
                                self.__log('warning', f"Pointing correction for {action_value['object']} with {row['device_name']} failed after {pointing_attempts} attempts")
                                pointing_complete = True

                # initialise guiding once pointing correction is complete
                if 'guiding' in action_value and guiding is False and pointing_complete is True:
                    if action_value['guiding'] is True:
                        
                        self.__log('info', f"Starting guiding for {paired_devices['Telescope']}")

                        glob_str = os.path.join("..", "images", folder, 
                                                f"{row['device_name']}_{action_value['filter']}_{action_value['object']}_{action_value['exptime']}_*.fits")
                        
                        th = Thread(target=self.guider[paired_devices['Telescope']].guider_loop, args=(camera.device_name, glob_str,), daemon=True)
                        th.start()

                        self.threads.append({'type': 'guider', 'device_name': row['device_name'], 'thread': th, 'id' : 'guider'})

                        guiding = True

                # start next exposure
                self.__log('debug', f"Exposing {row['device_name']} again")
                camera.get('StartExposure')(Duration = action_value['exptime'], Light = True)

        # stop guiding at end of sequence
        if 'guiding' in action_value:
            if action_value['guiding'] is True:
                self.__log('info', f"Stopping guiding for {paired_devices['Telescope']}")
                try:
                    self.guider[paired_devices['Telescope']].running = False
                except Exception as e:
                    self.error_source.append({'device_type': 'Guider', 'device_name': paired_devices['Telescope'], 'error': str(e)})
                    self.__log('error', f"Error stopping telescope {paired_devices['Telescope']} guiding: {str(e)}")
            
        self.__log('info', f"Object sequence ended {eval(row['action_value'])['object']} with {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}")

    def flats_sequence(self, row : dict, paired_devices : dict) -> None:
        '''
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

        '''

        self.__log('info', f"Running flats sequence for {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}")

        action_value, folder, hdr = self.pre_sequence(row, paired_devices)

        # camera device (first filter set in pre_sequence by this point)
        camera = self.devices[row['device_type']][row['device_name']]

        # target adu and camera offset needed for flat exposure time calculation
        cam_index = [i for i, d in enumerate(self.observatory['Camera']) if d['device_name'] == row['device_name']][0]
        target_adu = self.observatory['Camera'][cam_index]['flats']['target_adu']
        offset = self.observatory['Camera'][cam_index]['flats']['bias_offset']
        lower_exptime_limit = self.observatory['Camera'][cam_index]['flats']['lower_exptime_limit']
        upper_exptime_limit = self.observatory['Camera'][cam_index]['flats']['upper_exptime_limit']

        # camera max adu
        maxadu = camera.get('MaxADU')

        # camera orignal framing
        numx = camera.get('NumX')
        numy = camera.get('NumY')
        startx = camera.get('StartX')
        starty = camera.get('StartY')
   
        # get location to determine if sun is up
        obs_lat = hdr['LAT-OBS']
        obs_lon = hdr['LONG-OBS']
        obs_alt = hdr['ALT-OBS']
        obs_location = EarthLocation(lat=obs_lat*u.deg, lon=obs_lon*u.deg, height=obs_alt*u.m)
        
        for i, filter_name in enumerate(action_value['filter']):
            count = 0
            if (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) \
                and self.weather_safe and self.error_free and (self.interrupt is False) \
                and self.schedule_running and self.watchdog_running:

                # sets filter (and focus, soon)
                self.setup_observatory(paired_devices, action_value, filter_list_index = i)

                # move telescope to flat position
                self.flats_position(obs_location, paired_devices, row)
                
                # establishing initial exposure time
                exptime = self.flats_exptime(paired_devices, row, numx, numy, startx, starty, target_adu, offset,
                                                        lower_exptime_limit, upper_exptime_limit)
                
                if exptime < lower_exptime_limit or exptime > upper_exptime_limit:
                    self.__log('info', "Moving on...")
                    break

                hdr['EXPTIME'] = exptime
                hdr['IMGTYPE'] = 'Flat'
                hdr['FILTER'] = filter_name

                camera.get('StartExposure')(Duration = exptime, Light = True)
                
                t_last_move = datetime.utcnow()
                while (count < action_value['n'][i]) and (row['start_time'] <= datetime.utcnow()) \
                        and (row['end_time'] >= datetime.utcnow()) and self.weather_safe \
                        and self.error_free and (self.interrupt is False) and self.schedule_running \
                        and self.watchdog_running:
                    
                    r = camera.get('ImageReady')
                    time.sleep(0) # yield to other threads
                    if r is True:
                            
                        self.__log('debug', f"Image ready from {row['device_name']} to download.")

                        t0 = datetime.utcnow()

                        # get last exposure start time
                        r = camera.get('LastExposureStartTime')
                        self.__log('debug', f"LastExposureStartTime from {row['device_name']} was {r}")
                        dateobs = pd.to_datetime(r)
                        
                        # save image
                        self.__log('debug', f"Saving image from {row['device_name']}")
                        filename = self.save_image(camera, hdr, dateobs, t0, maxadu, folder)

                        # if time passes 30s, move telescope
                        if (datetime.utcnow() - t_last_move).total_seconds() >= 30:

                            # move telescope to flat position
                            self.flats_position(obs_location, paired_devices)
                            
                            # get new exposure time since moved
                            exptime = self.flats_exptime(paired_devices, numx, numy, startx, starty, target_adu, offset,
                                                                    lower_exptime_limit, upper_exptime_limit, exposure_time=exptime)
                            t_last_move = datetime.utcnow()

                        else:
                            # check median ADU of image
                            with fits.open(filename) as hdul:
                                data = hdul[0].data
                                median_adu = np.nanmedian(data)
                                fraction = (median_adu - offset) / (target_adu[0] - offset)

                                if math.isclose(target_adu[0], median_adu, rel_tol=0, abs_tol=target_adu[1]) is False:
                                    exptime = exptime * fraction

                                    if exptime < lower_exptime_limit or exptime > upper_exptime_limit:
                                        self.__log('warning', f"Exposure time of {exptime} s out of user defined range of {lower_exptime_limit} s to {upper_exptime_limit} s")
                                        break
                                    else:
                                        self.__log('info', f"Setting new exposure time to {exptime} s as median ADU of {median_adu} is not within {target_adu[1]} of {target_adu[0]}")
     
                        hdr['EXPTIME'] = exptime

                        count += 1

                        if count < action_value['n'][i]:
                            # start next exposure
                            self.__log('debug', f"Exposing {row['device_name']} again")
                            camera.get('StartExposure')(Duration = exptime, Light = True)
        
        self.__log('info', f"Flat sequence ended for {row['device_name']}, starting {row['start_time']} and ending {row['end_time']}")

    def flats_position(self, obs_location : EarthLocation, paired_devices : dict, row : dict) -> None:
        '''
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

        '''

        if 'Telescope' in paired_devices:
            # check if ready to take flats
            take_flats = False
            while (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) \
                    and (take_flats is False) and self.error_free and (self.interrupt is False) \
                    and self.weather_safe and self.schedule_running and self.watchdog_running:
                
                sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

                if take_flats is False:
                    time.sleep(1)

            # move telescope to flat position
            telescope = self.devices['Telescope'][paired_devices['Telescope']]

            # flat position
            flat_position = SkyCoord(
                alt=75 * u.deg, az=sun_altaz.az + 180 * u.degree, obstime=Time.now(), location=obs_location, frame="altaz"
            )

            # set tracking to true
            self.monitor_action('Telescope', 'Tracking', True, 'Tracking',
                                    device_name = paired_devices['Telescope'],
                                    log_message = f"Setting Telescope {paired_devices['Telescope']} tracking to True")

            # slew
            telescope.get('SlewToAltAzAsync')(Azimuth=flat_position.az.deg, Altitude=flat_position.alt.deg)
            
            # wait for slew to finish
            self.wait_for_slew(paired_devices)

    def flats_exptime(self, paired_devices : dict, row : dict, numx : int, numy : int, startx : int, starty : int, target_adu : list,
                        offset : float, lower_exptime_limit : float, upper_exptime_limit : float, exptime : float = None) -> float:
        '''
        Set the exposure time for flat field calibration images.

        This function adjusts the exposure time for flat field calibration images captured with a camera device
        to achieve a specific target median ADU (Analog-to-Digital Units) level, considering user-defined limits. It uses 64x64 
        pixel subframes to speed up the process.

        Parameters:
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

        '''
        if 'Camera' in paired_devices:
            
            camera = self.devices['Camera'][paired_devices['Camera']]

            # set camera to view small area to speed up read times, such to determine right exposure time (assuming detector is bigger than 64x64)
            self.monitor_action('Camera', 'NumX', 64, 'NumX',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} NumX to 64")
            self.monitor_action('Camera', 'NumY', 64, 'NumY',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} NumY to 64")
            self.monitor_action('Camera', 'StartX', int(numx/2 - 32), 'StartX',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} StartX to {int(numx/2 - 32)}")
            self.monitor_action('Camera', 'StartY', int(numy/2 - 32), 'StartY',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} StartY to {int(numy/2 - 32)}")
            
            # initial exposure time guess
            if exptime is None:
                exptime = (lower_exptime_limit + upper_exptime_limit) / 2

            self.__log('info', f"Exposing subframe of {paired_devices['Camera']} for exposure time {exptime} s")
            camera.get('StartExposure')(Duration = exptime, Light = True)
            
            getting_exptime = True
            while (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) \
                    and getting_exptime and self.error_free and (self.interrupt is False) \
                    and self.weather_safe and self.schedule_running and self.watchdog_running:
                
                r = camera.get('ImageReady')
                time.sleep(0) # yield to other threads
                if r is True:

                    arr = camera.get('ImageArray')
                    median_adu = np.nanmedian(arr)
                    fraction = (median_adu - offset) / (target_adu[0] - offset)
  
                    if math.isclose(target_adu[0], median_adu, rel_tol=0, abs_tol=target_adu[1]) is False:
                        exptime = exptime * fraction

                        if exptime < lower_exptime_limit or exptime > upper_exptime_limit:
                            self.__log('warning', f"Exposure time of {exptime} s out of user defined range of {lower_exptime_limit} s to {upper_exptime_limit} s")
                            getting_exptime = False
                        else:
                            getting_exptime = False
                            # start next exposure to check if correct?
                            # self.__log('info', f"Exposing subframe of {paired_devices['Camera']} for exposure time {exptime} s")
                            # camera.get('StartExposure')(Duration = exptime, Light = True)

                    else:
                        getting_exptime = False

                
            # set camera back to original framing
            self.monitor_action('Camera', 'NumX', numx, 'NumX',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} NumX to {numx}")
            self.monitor_action('Camera', 'NumY', numy, 'NumY',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} NumY to {numy}")
            self.monitor_action('Camera', 'StartX', startx, 'StartX',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} StartX to {startx}")
            self.monitor_action('Camera', 'StartY', starty, 'StartY',
                                device_name = paired_devices['Camera'],
                                log_message = f"Setting Camera {paired_devices['Camera']} StartY to {starty}")
            
            return exptime
            
    def img_transform(self, device : AlpacaDevice, img : np.array, maxadu : int) -> np.array:
        '''
        This function takes in a device object, an image object, and a maximum ADU 
        value and returns a numpy array of the correct shape for astropy.io.fits.

        Parameters:
            device (AlpacaDevice): A device object that contains the ImageArrayInfo data.
            img (np.array): An image object that contains the image data.
            maxadu (int): The maximum ADU value.

        Returns:
            nda (np.array): A numpy array of the correct shape for astropy.io.fits.
        '''
        
        imginfo = device.get('ImageArrayInfo')

        # Determine the image data type
        if imginfo.ImageElementType == 0 or imginfo.ImageElementType == 1:
            imgDataType = np.uint16
        elif imginfo.ImageElementType == 2:
            if maxadu <= 65535:
                imgDataType = np.uint16 # Required for BZERO & BSCALE to be written
            else:
                imgDataType = np.int32
        elif imginfo.ImageElementType == 3:
            imgDataType = np.float64
        else:
            raise ValueError(f"Unknown ImageElementType: {imginfo.ImageElementType}")
        

        # Make a numpy array of he correct shape for astropy.io.fits
        if imginfo.Rank == 2:
            nda = np.array(img, dtype=imgDataType).transpose()
        else:
            nda = np.array(img, dtype=imgDataType).transpose(2,1,0)

        return nda
        
    def save_image(self, device : AlpacaDevice, hdr : fits.Header, dateobs : datetime, t0 : datetime, maxadu : int, folder : str) -> str:
        '''
        Save an image to disk.

        This function retrieves an image from an Alpaca device, transforms it, and saves it to disk in FITS format.
        The filename is generated based on device information and the image's characteristics.

        The FITS header is updated with the 'DATE-OBS' and 'DATE' keywords to record the exposure start time
        and the time when the file was written.

        After saving the image, it is logged, and its file path is returned.

        Parameters:
            device (AlpacaDevice): The camera from which to retrieve the image.
            hdr (fits.Header): The FITS header associated with the image.
            dateobs (datetime): The UTC date and time of exposure start.
            t0 (datetime): The starting time of the image acquisition.
            maxadu (int): The maximum analog-to-digital unit value for the image.
            folder (str): The folder where the image will be saved.

        Returns:
            str: The file path to the saved image.

        '''
        self.__log('debug', 'Getting image array')
        arr = device.get('ImageArray')

        self.__log('debug', 'Got image array, now loading to numpy array')
        img = np.array(arr)
        
        self.__log('debug', 'Loaded image array to numpy array, now transforming')
        
        nda = self.img_transform(device, img, maxadu) ## TODO: make more efficient?
        self.__log('debug', 'Image transformed, now saving to disk')

        hdr['DATE-OBS'] = (dateobs.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time of exposure start')  

        date = datetime.utcnow() 
        hdr['DATE'] = (date.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time when this file was written')  

        hdu = fits.PrimaryHDU(nda, header=hdr)

        if hdr['IMGTYPE'] == 'Light':
            filename = f"{device.device_name}_{hdr['FILTER']}_{hdr['OBJECT']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
        else:
            filename = f"{device.device_name}_{hdr['IMGTYPE']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"

        filepath = os.path.join('..', 'images', folder, filename)

        self.__log('debug', 'Writing to disk')
        hdu.writeto(filepath)
        self.__log('debug', 'Image written to disk')

        self.last_image = filepath

        ## add to database            
        dt = dateobs.strftime("%Y-%m-%d %H:%M:%S.%f")
        self.cursor.execute(f"INSERT INTO images VALUES ('{filepath}', '{device.device_name}', '{0}', '{dt}')")
        self.__log('info', f"Image saved as {os.path.basename(filepath)}")
        self.__log('info', f"Image acquired in {datetime.utcnow() - t0}")

        return filepath

    def base_header(self, paired_devices : dict, action_value : dict) -> fits.Header:
        '''
        This function creates a base header for the fits file.

        Parameters:
            paired_devices (dict): A dictionary specifying paired devices, such as Telescope.
            action_value (dict): A dictionary containing action values from schedule.

        Returns:
            fits.Header: The FITS header containing the specified header entries.

        '''

        self.__log('info', "Creating base header")

        hdr = fits.Header()
        for i, row in self.fits_config.iterrows():
            if row['device_type'] == 'astra' and row['fixed'] is True:
                # custom headers
                match row['header']:
                    case 'FILTER':
                        device = self.devices['FilterWheel'][paired_devices['FilterWheel']]
                        pos = device.get('Position')
                        names = device.get('Names')
                        hdr[row['header']] = (names[pos], row["comment"])
                    case 'XPIXSZ':
                        device = self.devices['Camera'][paired_devices['Camera']]
                        binx = device.get('BinX')
                        xpixsize = device.get('PixelSizeX')
                        hdr[row['header']] = (binx*xpixsize, row["comment"])
                    case 'YPIXSZ':
                        device = self.devices['Camera'][paired_devices['Camera']]
                        biny = device.get('BinY')
                        ypixsize = device.get('PixelSizeY')
                        hdr[row['header']] = (biny*ypixsize, row["comment"])
                    case 'APTAREA':
                        device = self.devices['Telescope'][paired_devices['Telescope']]
                        val = device.get('ApertureArea') * 1e6
                        hdr[row['header']] = (val, row["comment"])
                    case 'APTDIA':
                        device = self.devices['Telescope'][paired_devices['Telescope']]
                        val = device.get('ApertureDiameter') * 1e3
                        hdr[row['header']] = (val, row["comment"])
                    case 'FOCALLEN':
                        device = self.devices['Telescope'][paired_devices['Telescope']]
                        val = device.get('FocalLength') * 1e3
                        hdr[row['header']] = (val, row["comment"])
                    case 'OBJECT':
                        if row['header'].lower() in action_value:
                            hdr[row['header']] = (action_value[row['header'].lower()], row["comment"])
                    case 'EXPTIME' | 'IMAGETYP':
                        hdr[row['header']] = (None, row["comment"])
                    case 'ASTRA':
                        hdr[row['header']] = (ASTRA_VER, row["comment"])
                    case _:
                        self.__log('warning', f"Unknown header: {row['header']}")

            elif (row['device_type'] not in ['astropy_default', 'astra', 'astra_fixed', '']) and row['fixed'] is True:
                # direct ascom command headers
                device_type = row['device_type']
                device_name = paired_devices[device_type]
                device = self.devices[device_type][device_name]

                val = device.get(row['device_command'])

                hdr[row['header']] = (val, row["comment"])

            elif row['device_type'] == 'astra_fixed':
                # fixed headers, ensure datatype
                try:
                    match row['dtype']:
                        case 'float':
                            hdr[row['header']] = (float(row['device_command']), row["comment"])
                        case 'int':
                            hdr[row['header']] = (int(row['device_command']), row["comment"])
                        case 'str':
                            hdr[row['header']] = (str(row['device_command']), row["comment"])
                        case 'bool':
                            hdr[row['header']] = (bool(row['device_command']), row["comment"])
                        case _:
                            hdr[row['header']] = (row['device_command'], row["comment"])
                            self.__log('error', f"Unknown data type: {row['dtype']}")
                except ValueError:
                    self.__log('error', "Invalid value for data type")

        self.__log('info', "Base header created")

        return hdr
    
    def final_headers(self) -> None:
        '''
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

        '''

        try:
            # get images from sql
            rows = self.cursor.execute("SELECT * FROM images WHERE complete_hdr = 0;")
            df_images = pd.DataFrame(rows, columns=['filepath', 'camera_name', 'complete_hdr', 'date_obs'])

            if df_images.shape[0] > 0:
                # loop through cameras (usually just one)
                for cam in df_images['camera_name'].unique():

                    # filter image dataframe by camera
                    df_images_filt = df_images[df_images['camera_name'] == cam]

                    # get paired devices for camera
                    cam_index = [i for i, d in enumerate(self.observatory['Camera']) if d['device_name'] == cam][0]
                    paired_devices = self.observatory['Camera'][cam_index]['paired_devices']
                    paired_devices['Camera'] = cam

                    # convert date_obs to datetime type, sort by date_obs, and convert to jd
                    df_images_filt['date_obs'] = pd.to_datetime(df_images_filt['date_obs'], format='%Y-%m-%d %H:%M:%S.%f')
                    df_images_filt = df_images_filt.sort_values(by='date_obs').reset_index(drop=True)
                    df_images_filt['jd_obs'] = df_images_filt['date_obs'].apply(utils.to_jd).sort_values()

                    # add small time increment to avoid duplicate jd, this adds 0.0864 ms to each image that has duplicate jd_obs
                    while df_images_filt['jd_obs'].duplicated().sum() > 0:
                        df_images_filt['jd_obs'] = df_images_filt['jd_obs'].mask(df_images_filt['jd_obs'].duplicated(), df_images_filt['jd_obs'] + 1e-9)

                    df_images_filt = df_images_filt.sort_values(by='jd_obs').reset_index()

                    # get polled data from ascom devices +- 10 seconds of first and last image
                    t0 = pd.to_datetime(df_images_filt['date_obs'].iloc[0]) - pd.Timedelta('10 sec')
                    t1 = pd.to_datetime(df_images_filt['date_obs'].iloc[-1]) + pd.Timedelta('10 sec')
                                                                                                            
                    q = f"""SELECT * FROM polling WHERE datetime BETWEEN "{str(t0)}" AND "{str(t1)}";"""
                    rows = self.cursor.execute(q)
                    df_poll = pd.DataFrame(rows, columns=['device_type', 'device_name', 'device_command', 'device_value', 'datetime'])
                    df_poll['jd'] = pd.to_datetime(df_poll['datetime'], format='%Y-%m-%d %H:%M:%S.%f').apply(utils.to_jd)

                    # find unique headers in polled commands
                    df_poll_unique = df_poll[['device_type', 'device_name', 'device_command']].drop_duplicates()

                    # drop row that have device_type and device_command that are not in fits_config to avoid errors later
                    df_poll_unique = df_poll_unique[df_poll_unique.apply(lambda x : (x['device_type'] in self.fits_config['device_type'].values) and 
                                                                        (x['device_command'] in self.fits_config['device_command'].values), axis=1)]

                    # get header and comment from fits_config
                    df_poll_unique['header'] = df_poll_unique.apply(lambda x : (self.fits_config[(self.fits_config['device_type'] == x['device_type']) & 
                                                                        (self.fits_config['device_command'] == x['device_command'])]['header'].values[0]), axis=1)
                    df_poll_unique['comment'] = df_poll_unique.apply(lambda x : (self.fits_config[(self.fits_config['device_type'] == x['device_type']) & 
                                                                        (self.fits_config['device_command'] == x['device_command'])]['comment'].values[0]), axis=1)

                    # keep rows that only have device_name in paired_devices
                    df_poll_unique = df_poll_unique[df_poll_unique['device_name'].isin(paired_devices.values())]

                    # form interpolated dataframe
                    df_inp = pd.DataFrame(columns=df_poll_unique['header'], index=df_images_filt['jd_obs'])

                    # interpolate polled data onto image times
                    for i, row in df_poll_unique.iterrows():
                        df_poll_filtered = df_poll[(df_poll['device_type'] == row['device_type']) & (df_poll['device_name'] == row['device_name']) &
                                                    (df_poll['device_command'] == row['device_command'])]
                        
                        df_poll_filtered = df_poll_filtered.sort_values(by='jd')
                        df_poll_filtered = df_poll_filtered.set_index('jd')

                        df_poll_filtered['device_value'] = df_poll_filtered['device_value'].replace({'True': 1.0, 'False': 0.0}).astype(float)

                        df_inp[row['header']] = utils.interpolate_dfs(df_images_filt['jd_obs'], df_poll_filtered['device_value'])['device_value'].fillna(0)

                    # update files
                    for i, row in df_images_filt.iterrows():
                        with fits.open(row['filepath'], mode='update') as filehandle:
                            hdr = filehandle[0].header
                            for header in df_inp.columns:
                                hdr[header] = (df_inp.iloc[i][header], df_poll_unique[df_poll_unique['header'] == header]['comment'].values[0])
                                
                            location = EarthLocation(lat=hdr['LAT-OBS']*u.deg, lon=hdr['LONG-OBS']*u.deg, height=hdr['ALT-OBS']*u.m)
                            target = SkyCoord(hdr['RA'], hdr['DEC'], unit=(u.deg, u.deg), frame='icrs')
                            
                            utils.hdr_times(hdr, self.fits_config, location, target)
                            filehandle[0].add_checksum()

                            self.cursor.execute(f'''UPDATE images SET complete_hdr = 1 WHERE filename="{row[0]}"''')
            
            self.__log('info', 'Completing headers... Done.')
        
        except Exception as e:
            self.error_source.append({'device_type': 'Headers', 'device_name': '', 'error': str(e)})
            self.__log('error', f"Error completing headers: {e}")

    def monitor_action(self, device_type : str, monitor_command : str, desired_condition : any, run_command : str, 
                        device_name : str = '', run_command_type : str = '', abs_tol : float = 0, 
                        log_message : str = '', timeout : float = 120) -> None:
        '''
        Monitor device(s) of device_type for a given monitor_command and run_command if desired_condition is not met.
    
        Args:
            device_type (str): Type of the device(s) to monitor.
            monitor_command (str): The command to monitor on the device(s).
            desired_condition (any): The desired condition that should be met.
            run_command (str): The command to run if the desired_condition is not met.
            device_name (str, optional): Name of the specific device to monitor (default '').
            run_command_type (str, optional): Type of run command ('set' or 'get') (default '').
            abs_tol (float, optional): Absolute tolerance for comparing conditions (default 0).
            log_message (str, optional): Custom log message that runs if conditions not initially met (default '').
            timeout (float, optional): Maximum time to monitor before timing out (default 120 seconds).

        '''
        # TODO: improve logging
        # TODO: add weather_safe and error_free as optional check conditions?
        start_time = time.time()

        self.__log("debug", f"Monitor action: {device_type} {monitor_command} {desired_condition} {run_command} {run_command_type} {abs_tol} {log_message} {timeout}")

        if monitor_command == run_command and run_command_type == '':
            run_command_type = 'set'
        elif run_command_type == '':
            run_command_type = 'get'            

        self.__log("debug", f"run_command_type: {run_command_type}")

        if device_type in self.observatory:
            monitor_status = []
            self.__log("debug", f"device_name: {device_name}")
            if device_name == '':
                for d in self.devices[device_type]:
                    device = self.devices[device_type][d]
                    
                    # monitor
                    status = device.get(monitor_command)
                    monitor_status.append(status)

                    # run if desired_condition not met
                    if math.isclose(status, desired_condition, rel_tol=0, abs_tol=abs_tol) is False:
                        if run_command_type == 'get':
                            self.__log("debug", f"Running get {run_command} on {device_type} {d}")
                            device.get(run_command)()
                        elif run_command_type == 'set':
                            self.__log("debug", f"Running set {run_command} on {device_type} {d}")
                            device.set(run_command, desired_condition)

            else:
                device = self.devices[device_type][device_name]

                # monitor
                status = device.get(monitor_command)
                monitor_status.append(status)

                # run if desired_condition not met
                if math.isclose(status, desired_condition, rel_tol=0, abs_tol=abs_tol) is False:
                    if run_command_type == 'get':
                        self.__log("debug", f"Running get {run_command} on {device_type} {device_name}")
                        device.get(run_command)()
                    elif run_command_type == 'set':
                        self.__log("debug", f"Running set {run_command} on {device_type} {device_name}")
                        device.set(run_command, desired_condition)

            # check if desired_condition is met by all devices
            all_monitor_status = np.mean(monitor_status)

            # if not met, monitor until timeout
            if math.isclose(all_monitor_status, desired_condition, rel_tol=0, abs_tol=abs_tol) is False:
                if log_message != '':
                    self.__log("info", f"{log_message}")
                else:
                    self.__log("info", f"Monitor run action: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
                
                while math.isclose(all_monitor_status, desired_condition, rel_tol=0, abs_tol=abs_tol) is False:
                    monitor_status = []
                    if device_name == '':
                        for d in self.devices[device_type]:
                            device = self.devices[device_type][d]

                            # monitor
                            status = device.get(monitor_command)
                            monitor_status.append(status)

                    else:
                        device = self.devices[device_type][device_name]

                        # monitor
                        status = device.get(monitor_command)
                        monitor_status.append(status)

                    all_monitor_status = np.mean(monitor_status)

                    time.sleep(1)

                    if time.time() - start_time > timeout:
                        break
                
                if math.isclose(all_monitor_status, desired_condition, rel_tol=0, abs_tol=abs_tol) is True:
                    self.__log("info", f"Monitor run action complete: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
                else:
                    self.error_source.append({'device_type': device_type, 'device_name': '', 'error': 'Monitor run action timeout'})
                    self.__log("error", f"Monitor run action timeout: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
                    raise TimeoutError(f"Monitor run action timeout: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
        else:
            self.__log("error", f"{device_type} not found in observatory.")
            raise ValueError(f"{device_type} not found in observatory.")
    
    def queue_get(self) -> None:
        '''
        Retrieve and process items from the queue until it's stopped.

        This method continuously retrieves items from the queue and processes them based on their type.
        If the type is 'query', it executes the SQL query provided in the item's data.
        If the type is 'log', it logs the data and appends errors to the error_source if applicable.

        '''

        while self.queue_running:
            try:
                metadata, r = self.queue.get()
                
                if r['type'] == 'query':
                    self.cursor.execute(r['data'])
                elif r['type'] == 'log':
                    self.__log(r['data'][0], r['data'][1])
                    if r['data'][0] == 'error':
                        self.error_source.append({'device_type': metadata['device_type'], 'device_name': metadata['device_name'], 'error': r['data'][1]})

            except Exception as e:
                self.error_source.append({'device_type': 'Queue', 'device_name': 'queue_get', 'error': str(e)})
                self.__log("error", f"Queue get error: {str(e)}")
                self.queue_running = False
