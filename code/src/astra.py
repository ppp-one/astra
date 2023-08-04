import logging

import time
from datetime import datetime
from threading import Thread

import astropy.units as u
import numpy as np
import pandas as pd
import utils
from guiding import Guider
import yaml
import os
from alpaca_device_process import AlpacaDevice
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.io import fits
from astropy.time import Time
from sqlite3worker import Sqlite3Worker  # https://github.com/dashawn888/sqlite3worker

from multiprocessing import Manager

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
        # TODO: 
        # move to process?
        # add better logging
        # add better error handling
        # add types?
        # improve observatory safety logic

        self.debug = debug
        self.truncate_schedule = truncate_schedule

        self.threads = []
        self.queue = Manager().Queue()

        th = Thread(target=self.queue_get, daemon=True)
        th.start()

        self.threads.append({'type': 'queue', 'device_name': 'queue', 'thread': th, 'id' : 'queue'})

        self.db_name, self.cursor = self.create_db(config_filename)

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
        self.observatory_name = config_filename.split('/')[-1].split('.')[0]

        self.schedule_mtime = os.path.getmtime(f'../schedule/{self.observatory_name}.csv')
        self.schedule = None
        self.schedule = self.read_schedule()

        self.fits_config = pd.read_csv(f'../config/{self.observatory_name}_fits_headers.csv')

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
    
    def __log(self, level : str, message : str):
        '''
        Log a message to the database

        log levels: info, warning, error, critical
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

    def create_db(self, config_filename : str):
        '''
        Create a database for the observatory
        '''

        db_name = "../log/" + config_filename.split('/')[-1].split('.')[0] + '.db'
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


        return db_name, cursor

    def read_config(self, config_filename : str):
        '''
        Read the config yaml file and create a dictionary of observatory's setup.
        '''

        self.__log('info', 'Reading config file')

        observatory = {}
        with open(config_filename, 'r') as stream:
            try:
                observatory = yaml.safe_load(stream)
                self.__log('info', f"Config file {config_filename} read")
            except yaml.YAMLError as exc:
                self.__log('error', f"Error reading config file {config_filename}: {exc}")

        return observatory
    
    def load_devices(self):
        '''
        Read observatory config and create a dictionary of devices
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
                        self.__log('error', f"Error loading {device_type} {d['device_name']}: {str(e)}")
        
        self.__log('info', 'Devices loaded')

        return devices

    def connect_all(self):
        '''
        Connect to all the devices and start polling
        '''

        self.__log('info', 'Connecting to devices')

        # connect to all devices
        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                r = self.devices[device_type][device_name].set("Connected", True) ## slow?
                if r['status'] == 'success':
                    self.__log('info', f"{device_type} {device_name} connected")
                else:
                    self.__log('error', f"{device_type} {device_name} not connected")

        self.__log('info', 'Starting polling non-fixed fits headers')

        delay = 5 # seconds
        # start polling non-fixed fits headers
        for i, row in self.fits_config.iterrows():
            if (row['device_type'] not in ['astropy_default', 'astra', 'astra_fixed', '']) and row['fixed'] is False:
                device_type = row['device_type']
                for device_name in self.devices[device_type]:
                    device = self.devices[device_type][device_name]
                    device.start_poll(row['device_command'], delay) # 5 second polling
        
        delay = 1 # seconds
        if 'SafetyMonitor' in self.observatory:
            device_type = 'SafetyMonitor'
            device_name = self.observatory[device_type][0]['device_name']

            device = self.devices[device_type][device_name]
            device.start_poll('IsSafe', delay) # 1 second polling, could move to watchdog thread, but nice to have in db
            
        self.__log('info', 'Connect all sequence complete')
        # run can<> ascom commands, needed for other commands to work? Else, alternatives needed.

    def disconnect_all(self):
        '''
        Stop polling and disconnect from all the devices
        '''
        
        self.__log('info', 'Disconnecting from devices')

        # stop threads
        if self.watchdog_running is True:
            self.watchdog_running = False

        if self.schedule_running is True:
            self.schedule_running = False

        for device_type in self.devices:
            for device_name in self.devices[device_type]:
                self.devices[device_type][device_name].stop_poll()
                r = self.devices[device_type][device_name].set("Connected", False) ## slow?
                if r['status'] == 'success':
                    self.__log('info', f"{device_type} {device_name} disconnected")
                else:
                    self.__log('error', f"{device_type} {device_name} not disconnected")
                
                self.devices[device_type][device_name].stop()

        self.__log('info', 'Disconnect all sequence complete')

        self.cursor.close()

    def start_watchdog(self):
        '''
        Start the watchdog thread
        '''
        
        if self.watchdog_running is True:
            self.__log('warning', 'Watchdog already running')
            return
        
        th = Thread(target=self.watchdog, daemon = True)
        th.start()

        self.threads.append({'type': 'watchdog', 'device_name': 'watchdog', 'thread': th, 'id' : -1})

    def watchdog(self):
        '''
        Check the observatory is safe.
        Presently assumes only one safety monitor.
        '''

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
            except Exception as e:
                self.error_source.append({'type': 'SafetyMonitor', 'device_name': sm_name, 'error': str(e)})
                self.__log('error', f"Error polling safety monitor: {str(e)}")

            while sm_poll['data']['IsSafe']['value'] is None and self.error_free:

                try:
                    sm_poll = safety_monitor.poll_latest()
                except Exception as e:
                    self.error_source.append({'type': 'SafetyMonitor', 'device_name': sm_name, 'error': str(e)})
                    self.__log('error', f"Error polling safety monitor: {str(e)}")

                time.sleep(0.5)

        # observatory closing message flag
        closing_messsage = True
        
        while self.watchdog_running:
                
            if self.error_free is True:
                                    
                # cleanup dead threads
                for i in self.threads:
                    if i['thread'].is_alive() is False:
                        self.threads.remove(i)

                # check if schedule file updated
                try:
                    schedule_mtime = os.path.getmtime(f'../schedule/{self.observatory_name}.csv')

                    if schedule_mtime > self.schedule_mtime:
                        self.__log('warning', 'Schedule updated')
                        self.schedule = self.read_schedule()
                except Exception as e:
                    self.error_source.append({'type': 'Schedule', 'device_name': 'schedule', 'error': str(e)})
                    self.__log('error', f"Error checking schedule: {str(e)}")

                # check telescope(s) altitude
                if 'Telescope' in self.observatory:
                    for telescope_name in self.devices['Telescope']:
                        telescope = self.devices['Telescope'][telescope_name]
                        telescope_index = [i for i, d in enumerate(self.observatory['Telescope']) if d['device_name'] == telescope_name][0]

                        if 'alt_limit' in self.observatory['Telescope'][telescope_index]:
                            alt_limit = self.observatory['Telescope'][telescope_index]['alt_limit']

                            try:
                                t_poll = telescope.poll_latest()
                            except Exception as e:
                                self.error_source.append({'type': 'Telescope', 'device_name': telescope_name, 'error': str(e)})
                                self.__log('error', f"Error polling telescope: {str(e)}")
                                
                            if t_poll['data']['Altitude']['value'] <= alt_limit and self.error_free:
                                # TODO: BETTER LOGIC HERE NEEDED - CHECK IF SLEWING, GUIDING, TRACKING, ETC. before stopping
                                self.__log('warning', f"Telescope {telescope_name} altitude {t_poll['data']['Altitude']['value']} < {alt_limit}")

                                try:
                                    # stop telescope slewing
                                    telescope.get('AbortSlew')['data']()
                                except Exception as e:
                                    self.error_source.append({'type': 'Telescope', 'device_name': telescope_name, 'error': str(e)})
                                    self.__log('error', f"Error stopping telescope {telescope_name} slewing: {str(e)}")

                                try:
                                    # stop tracking
                                    telescope.set('Tracking', False)
                                except Exception as e:
                                    self.error_source.append({'type': 'Telescope', 'device_name': telescope_name, 'error': str(e)})
                                    self.__log('error', f"Error stopping telescope {telescope_name} tracking: {str(e)}")

                                try:
                                    # stop guiding
                                    self.guider[telescope_name].running = False
                                except Exception as e:
                                    self.error_source.append({'type': 'Guider', 'device_name': telescope_name, 'error': str(e)})
                                    self.__log('error', f"Error stopping telescope {telescope_name} guiding: {str(e)}")
                
                # check safety monitor
                if 'SafetyMonitor' in self.observatory:

                    try:
                        sm_poll = safety_monitor.poll_latest()
                    except Exception as e:
                        self.error_source.append({'type': 'SafetyMonitor', 'device_name': sm_name, 'error': str(e)})
                        self.__log('error', f"Error polling safety monitor: {str(e)}")
                    
                    # check if stale
                    last_update = (datetime.utcnow() - sm_poll['data']['IsSafe']['datetime']).total_seconds()

                    match last_update:
                        case _ if last_update > 3 and last_update < 30:
                            self.__log('warning', f"Safety monitor {last_update}s stale")
                        case _ if last_update > 30:
                            self.__log('error', f"Safety monitor {last_update}s stale")
                            self.error_source.append({'type': 'SafetyMonitor', 'device_name': sm_name, 'error': f"Stale data {last_update}s"})
                            break # breaks while loop
                    

                    if sm_poll['data']['IsSafe']['value'] is False:

                        self.weather_safe = False
                        
                        if closing_messsage is True:
                            self.__log('warning', 'Weather unsafe')
                            self.__log('info', 'Closing observatory')
                            closing_messsage = False

                        # may want to close dome before park telescope?
                        self.close_observatory() # checks if already closed and closes if not

                    else:

                        self.weather_safe = True

                    if self.schedule_running is False and self.error_free is True:
                        # TODO: Smarter logic
                        # if past 30 mins have been consecutive safe, start schedule
                        # if not, wait.

                        if self.truncate_schedule is True:
                            rows = self.cursor.execute("SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND device_value = 'False' AND datetime > datetime('now', '-1 minutes')")
                        else:
                            rows = self.cursor.execute("SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND device_value = 'False' AND datetime > datetime('now', '-30 minutes')")

                        if self.schedule.iloc[-1]['end_time'] > datetime.utcnow():

                            self.__log('debug', f"Watchdog: {len(rows)} instances of weather unsafe found in last 1 minutes")

                            # start schedule
                            if len(rows) == 0:
                                
                                if self.truncate_schedule is False:
                                    self.schedule = self.read_schedule()

                                self.schedule_running = True
                                th = Thread(target=self.run_schedule, daemon = True)
                                th.start()
                                self.threads.append({'type': 'run_schedule', 'device_name': 'Schedule', 'thread': th, 'id' : -2})

                                closing_messsage = True # reset closing message, but should be smarter than this.

                # check if any devices unresponsive, timeout needed?            
                for device_type in self.devices:
                    for device_name in self.devices[device_type]:
                        try:
                            self.devices[device_type][device_name].poll_latest()
                        except Exception as e:
                            self.error_source.append({'type': device_type, 'device_name': device_name, 'error': str(e)})
                            self.__log('error', f"Error polling {device_type} {device_name} during watchdog check: {str(e)}")
            
            else:

                # stop watchdog
                self.watchdog_running = False

                # stop schedule
                self.schedule_running = False  

                # wait a bit to see if it's a multi-device error?
                time.sleep(30)

                # make pandas dataframe of error_source
                df = pd.DataFrame(self.error_source)

                device_types = df.type.unique()
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
                            pass
                        case _:
                            pass

            time.sleep(0.5) # twice the safety monitor polling time


        self.watchdog_running = False
        self.__log('warning', 'Watchdog stopped')
    
    def close_observatory(self):
        '''
        Close observatory, park telescope, close dome, close dome shutter
        '''
        
        if 'Telescope' in self.observatory:
            # stop telescope tracking
            for device_name in self.devices['Telescope']:
                telescope = self.devices['Telescope'][device_name]

                try:
                    telescope.set('Tracking', False)
                except Exception as e:
                    self.__log('error', f"Error stopping telescope {device_name} tracking: {str(e)}")
                    self.error_source.append({'type': 'Telescope', 'device_name': device_name, 'error': f"Error stopping telescope {device_name} tracking: {str(e)}"})
                    return False

            # stop telescope slewing
            r = self.monitor_action('Telescope', 'Slewing', False, 'AbortSlew')
            if r['status'] != 'success':
                self.__log('error', 'Error stopping telescope(s) slewing')
                self.error_source.append({'type': 'Telescope', 'device_name': '', 'error': 'Error stopping telescope(s) slewing'})
                return False
            
            # park telescope
            r = self.monitor_action('Telescope', 'AtPark', True, 'Park')
            if r['status'] != 'success':
                self.__log('error', 'Error parking telescope(s)')
                self.error_source.append({'type': 'Telescope', 'device_name': '', 'error': 'Error parking telescope(s)'})
                return False
            
        if 'Dome' in self.observatory:
            # park dome                
            r = self.monitor_action('Dome', 'AtPark', True, 'Park')
            if r['status'] != 'success':
                self.__log('error', 'Error parking dome(s)')
                self.error_source.append({'type': 'Dome', 'device_name': '', 'error': 'Error parking dome(s)'})
                return False
            
            # close dome shutter
            r = self.monitor_action('Dome', 'ShutterStatus', 1, 'CloseShutter')
            if r['status'] != 'success':
                self.__log('error', 'Error closing dome shutter(s)')
                self.error_source.append({'type': 'Dome', 'device_name': '', 'error': 'Error closing dome shutter(s)'})
                return False

        return True

    def toggle_interrupt(self):
        '''
        Handle interrupt
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

            self.threads.append({'type': 'AbortSlew', 'device_name': 'all_telescopes', 'thread': th, 'id' : -3})

            for d in self.devices['Telescope']:
                device = self.devices['Telescope'][d]
                device.set('Tracking', False)

                self.guider[d].running = False

        if 'Dome' in self.observatory:
            # stop dome slewing
            th = Thread(target=self.monitor_action, args=('Dome', 'Slewing', False, 'AbortSlew',), daemon=True)
            th.start()

            self.threads.append({'type': 'AbortSlew', 'device_name': 'all_domes', 'thread': th, 'id' : -4})

        if 'Camera' in self.observatory:
            # stop camera exposure
            th = Thread(target=self.monitor_action, args=('Camera', 'CameraState', 0, 'AbortExposure',), daemon=True)
            th.start()

            self.threads.append({'type': 'AbortSlew', 'device_name': 'all_cameras', 'thread': th, 'id' : -5})


        # wait for all threads to finish
        for t in self.threads:
            if t['thread'].is_alive() is True:
                if t['id'] in [-3, -4, -5]:
                    time.sleep(1)
            else:
                self.threads.remove(t)

        # reset interrupt
        self.interrupt = False

    def read_schedule(self):
        '''
        Read the schedule, check for errors (e.g. device not in list/connected), return pandas dataframe
        Focus offset for changing filter + specified focus offset.
        If no end_time, set to end time of next item?
        order by start_time

        columns: device_type, device_name, action_type, action_value, start_time, end_time, target, complete

        valid action_type per device_type
        '''
        # TODO: error handling, add schedule as string to log.db?

        try:

            schedule_mtime = os.path.getmtime(f'../schedule/{self.observatory_name}.csv')

            if (schedule_mtime > self.schedule_mtime) or (self.schedule is None):

                if self.schedule_running is True:
                    self.__log('warning', 'Schedule updating while previous schedule is running. This will not take effect until the new schedule is run.')

                self.__log('info', 'Reading schedule')

                schedule = pd.read_csv(f'../schedule/{self.observatory_name}.csv')
                schedule['start_time'] =  pd.to_datetime(schedule.start_time)
                schedule['end_time'] = pd.to_datetime(schedule.end_time)

                # sort by start_time
                schedule = schedule.sort_values(by=['start_time'])

                # for development
                if self.truncate_schedule is True:
                    schedule = update_times(schedule, 100)

                # make new column called repeatable, set to True, TODO: Make useable.
                schedule['repeatable'] = True
                
                self.__log('info', 'Schedule read')

                self.schedule_mtime = schedule_mtime

                return schedule
            else:
                return self.schedule
            
        except Exception as e:
            self.__log('error', f'Error reading schedule: {e}')
            self.error_source.append({'type': 'Schedule', 'device_name': '', 'error': f'Error reading schedule: {e}'})
            return None

    def run_schedule(self):
        '''
        Run the schedule
        Interruptible by user and weather conditions
        while loop?
        run in thread
        '''

        while self.weather_safe is None:
            self.__log('info', 'Waiting for safety conditions to be checked')
            time.sleep(1)

        self.__log('info', 'Running schedule')
        self.schedule_running = True
        for i, row in self.schedule.iterrows():

            # safety check
            # run if weather safe, or the action is calibration (bias, dark)
            if (self.weather_safe is True) or (row['action_type'] in ['calibration']):

                # loop through self.threads and remove the ones that are dead due to finishing or weather getting to them?
                for j in self.threads:
                    if j['thread'].is_alive() is False:
                        self.threads.remove(j)

                ids = [k['id'] for k in self.threads] # or add an if statement in here..?

                # if not running, start thread
                if i not in ids:
                    run_row = True
                    while run_row and self.schedule_running and self.weather_safe and self.error_free:
                        t = datetime.utcnow()
                        
                        if row['start_time'] >= t:
                            # print((row['start_time'] - t).total_seconds(), 'seconds until')
                            # see last row in log
                            # rows = self.cursor.execute("SELECT * FROM log ORDER BY id DESC LIMIT 1")
                            # if rows[0]['message'] != f"Waiting for {row['device_name']} {row['action_type']} to start":
                            #     self.__log('info', f"Waiting for {row['device_name']} {row['action_type']} to start")
                            time.sleep(1)

                        elif (row['start_time'] <= t) and (row['end_time'] >= t) and (row['repeatable'] is True):

                            th = Thread(target=self.run_action, args=(row,), daemon=True)
                            th.start()

                            self.threads.append({'type': row['action_type'], 'device_name': row['device_name'], 'thread': th, 'id' : i})
                            
                            run_row = False

                            # if last row, sleep until thread is finished to prevent from returning to start of schedule by watchdog
                            if i == self.schedule.index[-1]:
                                while (th.is_alive() is True) and self.weather_safe and self.schedule_running and self.error_free and (self.interrupt is False):
                                    time.sleep(1)
                                self.__log('info', f"Waiting for last schedule item to reach endtime of {row['end_time']}: {row['device_name']} {row['action_type']}")
                                while self.weather_safe and self.schedule_running and self.error_free and (self.interrupt is False):
                                    t_until_end = (row['end_time'] - datetime.utcnow()).total_seconds()
                                    if t_until_end > 0:
                                        print(t_until_end, 'seconds until last action expected to finish')
                                        time.sleep(1)
                                    else:
                                        break
                        else:
                            run_row = False

        # run headers completion
        # self.__log('info', 'Completing headers')
        # th = Thread(target=self.final_headers, daemon=True)
        # th.start()
        # self.threads.append({'type': 'final_headers', 'device_name': 'astra', 'thread': th, 'id' : -6})

        self.schedule_running = False
        self.__log('warning', 'Schedule stopped')
        
    def run_action(self, row):
        '''
        Run the action specified in the schedule
        '''

        self.__log('info', f"Starting {row['device_name']} {row['action_type']}")

        try:
            if 'object' == row['action_type']:
                self.object_sequence(row)
            elif 'calibration' == row['action_type']:
                self.calibration_sequence(row)
            elif 'flats' == row['action_type']:
                self.flats_sequence(row)
            else: 
                # if not 'object' or 'calibration' or 'flats', assume it's an ASCOM command
                device = self.devices[row['device_type']][row['device_name']]

                if row['action_type'] in dir(device.device):
                    if isinstance(eval(row['action_value']), dict):
                        r = device.get(row['action_type'])
                        if r['status'] == "success":
                            data = r['data'](**eval(row['action_value']))
                            self.__log('info', f"Finished {row['device_name']} {row['action_type']} {row['action_value']} with {data}")
                        else:
                            raise ValueError(r)
                    else:
                        r = device.set(row['action_type'], row['action_value'])
                        if r['status'] == "success":
                            data = r['data']
                            self.__log('info', f"Finished {row['device_name']} {row['action_type']} {row['action_value']} with {data}")
                        else:
                            raise ValueError(r)
                else:
                    raise ValueError(f"Invalid action_type: {row['device_name']} {row['action_type']} with {row['action_value']} is not a valid method or property for {row['device_type']} {row['device_name']}")
        except Exception as e:
            self.__log('error', f"Run action error: {str(e)}")
            self.schedule_running = False
        
    def pre_sequence(self, row):
        '''
        Preparation for sequence
        '''

        self.__log('info', f"Running pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}")
        
        action_value = eval(row['action_value'])
        folder = utils.create_image_dir()

        cam_index = [i for i, d in enumerate(self.observatory['Camera']) if d['device_name'] == row['device_name']][0]
        paired_devices = self.observatory['Camera'][cam_index]['paired_devices']
        paired_devices['Camera'] = row['device_name']
        
        # prepare observatory for sequence
        self.move_telescope_filter(paired_devices, action_value)

        # write base header
        hdr = self.base_header(paired_devices, action_value)
        
        if 'object' == row['action_type']:
            hdr['IMGTYPE'] = 'Light'

        self.__log('info', f"Finished pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}")

        return action_value, folder, hdr, paired_devices
    
    def move_telescope_filter(self, paired_devices, action_value, filter_list_index = 0):
        '''
        Prepares the observatory for the sequence
        '''

        self.__log('info', f"Running move_telescope_filter for {paired_devices} {action_value}")

        # unpark and slew to target
        if ('ra' in action_value) and ('dec' in action_value) and self.weather_safe and self.error_free and (self.interrupt is False):
            if 'Dome' in paired_devices:
                dome = self.devices['Dome'][paired_devices['Dome']]

                # setting dome to slave - need to be part of config?
                self.__log('info', f"Setting Dome {paired_devices['Dome']} to slave.")
                r = dome.set('Slaved', True)
                if r['status'] != "success":
                    self.__log('warning', f"Error setting Dome {paired_devices['Dome']} to slave.")
                    # TODO: need to move dome manually???

                if self.weather_safe and self.error_free and (self.interrupt is False):
                    # check dome open
                    self.__log('info', f"Checking Dome {paired_devices['Dome']} is open.")
                    r = self.monitor_action('Dome', 'ShutterStatus', 0, 'OpenShutter')
                    if r['status'] != 'success':
                        raise ValueError(r)

            if 'Telescope' in paired_devices:
                telescope = self.devices['Telescope'][paired_devices['Telescope']]

                if self.weather_safe and self.error_free and (self.interrupt is False):
                    # unpark telescope
                    self.__log('info', f"Unparking Telescope {paired_devices['Telescope']}")
                    r = telescope.get('Unpark')

                    if r['status'] == "success":
                        r['data']()
                    else:
                        raise ValueError(r)
                
                    # set tracking to true
                    self.__log('info', f"Setting Telescope {paired_devices['Telescope']} tracking to True")
                    r = telescope.set('Tracking', True)

                    if r['status'] != "success":
                        raise ValueError(r)

                    # slew to target
                    self.__log('info', f"Slewing Telescope {paired_devices['Telescope']} to {action_value['ra']} {action_value['dec']}")
                    r = telescope.get('SlewToCoordinatesAsync')

                    if r['status'] == "success":
                        r['data'](RightAscension = 24*action_value['ra']/360, Declination = action_value['dec'])
                    else:
                        raise ValueError(r)

                    # wait for slew to finish
                    self.__log('info', f"Waiting for Telescope {paired_devices['Telescope']} to finish slewing")
                    start_time = time.time()

                    r = telescope.get('Slewing')
                    if r['status'] == "success":
                        slewing = r['data']
                    else:
                        raise ValueError(r)

                    while slewing is True and self.weather_safe and self.error_free and (self.interrupt is False) and self.schedule_running and self.watchdog_running:
                        self.__log('info', f"Telescope {paired_devices['Telescope']} slewing...")

                        if time.time() - start_time > 120: # 2 minutes hardcoded limit
                            raise TimeoutError('Slew timeout')

                        time.sleep(1)

                        r = telescope.get('Slewing')
                        if r['status'] == "success":
                            slewing = r['data']
                        else:
                            raise ValueError(r)

        # set filter
        if 'filter' in action_value and 'FilterWheel' in paired_devices and self.error_free and (self.interrupt is False):

            f = action_value['filter']
            if type(f) == list:
                f = f[filter_list_index]

            self.__log('info', f"Setting FilterWheel {paired_devices['FilterWheel']} to {f}")

            filterwheel = self.devices['FilterWheel'][paired_devices['FilterWheel']]
            r = filterwheel.get('Names')
            if r['status'] != "success":
                raise ValueError(r)
            else:
                names = r['data']

            # find index of filter name
            filter_index = [i for i, d in enumerate(names) if d == f][0]

            r = filterwheel.set('Position', filter_index)
            if r['status'] != "success":
                raise ValueError(r)
            
            # wait for filter to change
            # TODO: add better error handling here
            self.__log('info', f"Waiting for FilterWheel {paired_devices['FilterWheel']} to change to {f}")
            start_time = time.time()
            while filterwheel.get('Position')['data'] != filter_index:
                self.__log('info', f"FilterWheel {paired_devices['FilterWheel']} changing to {f}...")
                
                if time.time() - start_time > 120: # 2 minutes hardcoded limit
                    raise TimeoutError('Filter change timeout')
                    
                time.sleep(1)

            # TODO: focus offset? I don't like changing focus position often
            # how do I return focus to original position if weather stops sequence?
            # temp comp?
            # manual offset?
            # what if not absolute focus?
            # if 'Focuser' in paired_devices:
            #     focus_offset = filterwheel.get('FocusOffsets')['data'][filter_index]
            #     focuser = self.devices['Focuser'][paired_devices['Focuser']]
                
            #     fos_index = [i for i, d in enumerate(self.observatory['Focuser']) if d['device_name'] == paired_devices['Focuser']][0]
            #     focus_pos = self.observatory['Focuser'][fos_index]['focus_pos']

            #     focuser.get('Move')['data'](focus_pos + focus_offset)
            #     while focuser.get('IsMoving')['data'] == True:
            #         print('moving focuser')
            #         time.sleep(1)

    def calibration_sequence(self, row):
        '''
        Bias, darks
        '''

        # TODO: add count to log messages

        self.__log('info', f"Running calibration_sequence for {row['device_name']} {row['action_type']} {row['action_value']} {row['start_time']} {row['end_time']}")

        action_value, folder, hdr, paired_devices = self.pre_sequence(row)
        
        camera = self.devices[row['device_type']][row['device_name']]

        r = camera.get('MaxADU')
        if r['status'] != "success":
            raise ValueError(r)
        maxadu = r['data']


        for i, exptime in enumerate(action_value['exptime']):

            if (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) and self.weather_safe and self.error_free and (self.interrupt is False):

                hdr['EXPTIME'] = exptime

                if exptime == 0:
                    hdr['IMGTYPE'] = 'Bias'
                else:
                    hdr['IMGTYPE'] = 'Dark'


                r = camera.get('StartExposure')

                if r['status'] == "success":
                    r['data'](Duration = exptime, Light = False)
                    self.__log('info', f"Exposing {row['device_name']} {hdr['IMGTYPE']} for exposure time {hdr['EXPTIME']} s")
                else:
                    raise ValueError(r)
            
                count = 0
                while (count < action_value['n'][i]) and (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) and self.weather_safe and self.error_free and (self.interrupt is False):
                    r = camera.get('ImageReady')
                    time.sleep(0) # yield to other threads
                    if r['status'] == "success":
                        if r['data'] is True:
                            
                            self.__log('debug', f"Image ready from {row['device_name']} to download.")

                            t0 = datetime.utcnow()

                            # get last exposure start time
                            r = camera.get('LastExposureStartTime')
                            if r['status'] != "success":
                                raise ValueError(r)
                            
                            self.__log('debug', f"LastExposureStartTime from {row['device_name']} was {r['data']}")


                            dateobs = pd.to_datetime(r['data'])
                            
                            # save image
                            self.__log('debug', f"Saving image from {row['device_name']}")
                            self.save_image(camera, hdr, dateobs, t0, maxadu, folder)

                            count += 1

                            if count < action_value['n'][i]:
                                # start next exposure
                                self.__log('debug', f"Exposing {row['device_name']} again")
                                r = camera.get('StartExposure')

                                if r['status'] == "success":
                                    r['data'](Duration = exptime, Light = False)
                                    self.__log('info', f"Exposing {row['device_name']} {hdr['IMGTYPE']} for exposure time {hdr['EXPTIME']} s")
                                else:
                                    raise ValueError(r)

                    else:
                        raise ValueError(r)
        
        
        # if all images were taken, set repeatable to False

        # change row status in schedule to complete
        # self.__log('info', f"Updating schedule status to complete for {row['device_name']} {row['action_type']} {row['action_value']} {row['start_time']} {row['end_time']}")
   
        # self.schedule.loc[(self.schedule.index == row.name), 'repeatable'] = False
        

        self.__log('info', f"Calibration_sequence ended for {row['device_name']} {row['action_type']} {row['action_value']} {row['start_time']} {row['end_time']}")

    def object_sequence(self, row):
        '''
        Object sequence
        '''
        self.__log('info', f"Running object_sequence for {row['device_name']} {row['action_type']} {row['action_value']} {row['start_time']} {row['end_time']}")

        action_value, folder, hdr, paired_devices = self.pre_sequence(row)

        hdr['EXPTIME'] = action_value['exptime']
        camera = self.devices[row['device_type']][row['device_name']]

        r = camera.get('MaxADU')
        if r['status'] != "success":
            raise ValueError(r)
        maxadu = r['data']

        r = camera.get('StartExposure')

        if r['status'] == "success" and self.weather_safe and self.error_free and (self.interrupt is False):
            r['data'](Duration = action_value['exptime'], Light = True)
            self.__log('info', f"Exposing {row['device_name']} {hdr['IMGTYPE']} for exposure time {hdr['EXPTIME']} s")
        else:
            raise ValueError(r)


        pointing_complete = False
        guiding = False

        while (row['start_time'] <= datetime.utcnow()) and (row['end_time'] >= datetime.utcnow()) and self.weather_safe and self.error_free and (self.interrupt is False):            
            
            r = camera.get('ImageReady') # have a timeout in else part?
            time.sleep(0) # yield to other threads
            if r['status'] == "success":
                if r['data'] is True:
                    self.__log('debug', f"Image ready from {row['device_name']} to download.")

                    t0 = datetime.utcnow()
                    
                    # get last exposure start time
                    r = camera.get('LastExposureStartTime')
                    if r['status'] != "success":
                        raise ValueError(r)
                    
                    self.__log('debug', f"LastExposureStartTime from {row['device_name']} was {r['data']}")

                    dateobs = pd.to_datetime(r['data'])

                    # save image
                    self.__log('debug', f"Saving image from {row['device_name']}")
                    filepath = self.save_image(camera, hdr, dateobs, t0, maxadu, folder)

                    # pointing correction if not already done
                    if 'pointing' in action_value and pointing_complete is False:
                        if action_value['pointing'] is True:
                            self.__log('info', f"Running pointing correction for {action_value['object']} with {row['device_name']}")

                            offset_ra, offset_dec, wcs = utils.point_correction(filepath, action_value['ra'], action_value['dec'])

                            pointing_threshold = 0.1 / 60 # 0.1 arcmin
                            if (abs(offset_ra) < pointing_threshold) or (abs(offset_dec) < pointing_threshold):
                                self.__log('info', f"No further pointing correction required. Correction of {offset_ra*60}\" {offset_dec*60}\" within threshold of {pointing_threshold*60}\"")
                                pointing_complete = True
                            else:
                                self.__log('info', f"Pointing correction of {offset_ra*60}\" {offset_dec*60}\" required")
                                # sync telescope to corrected coordinates
                                telescope = self.devices['Telescope'][paired_devices['Telescope']]
                                r = telescope.get('SyncToCoordinates')
                                if r['status'] == "success":
                                    r['data'](RightAscension = 24*(action_value['ra'] + offset_ra)/360, Declination = action_value['dec'] + offset_dec)
                                else:
                                    raise ValueError(r)
                                
                                # re-slew to target
                                self.move_telescope_filter(paired_devices, action_value)
                    else:
                        pointing_complete = True

                    # initialise guiding once pointing correction is complete
                    if 'guiding' in action_value and guiding is False and pointing_complete is True:
                        if action_value['guiding'] is True:

                            glob_str = f"../images/{folder}/{row['device_name']}_{action_value['filter']}_{action_value['object']}_{action_value['exptime']}_*.fits"

                            th = Thread(target=self.guider[paired_devices['Telescope']].guider_loop, args=(camera.device_name, glob_str,), daemon=True)
                            th.start()

                            self.threads.append({'type': 'guider', 'device_name': row['device_name'], 'thread': th, 'id' : 'guider'})

                            guiding = True

                    # start next exposure
                    self.__log('debug', f"Exposing {row['device_name']} again")
                    r = camera.get('StartExposure')

                    if r['status'] == "success":
                        r['data'](Duration = action_value['exptime'], Light = True)
                        self.__log('info', f"Exposing {row['device_name']} {hdr['IMGTYPE']} for exposure time {hdr['EXPTIME']} s")
                    else:
                        raise ValueError(r)

                else:
                    pass
                    # https://ascom-standards.org/alpyca/alpaca.camera.html#alpaca.camera.Camera.PercentCompleted
                    # print("waiting for image")
                    # TODO: time.sleep(0.01) # change dynamically wrt when image last came in?
            else:
                raise ValueError(r)
            
        # TODO: better way to start/stop guiding?
        if 'guiding' in action_value:
            if action_value['guiding'] is True:
                self.guider[paired_devices['Telescope']].running = False
            
        self.__log('info', f"Object_sequence ended for {row['device_name']} {row['action_type']} {row['action_value']} {row['start_time']} {row['end_time']}")

    def flats_sequence(self, row):
        '''
        Flat sequence

        TODO: To be finished...
        '''
        # https://arxiv.org/pdf/1407.8283.pdf
        # https://iopscience.iop.org/article/10.1086/133817/pdf?casa_token=ogWaY-ZHTqYAAAAA:XvO7oL5ZGqnsCIRyF3zqQJJeLWpuxmnHiBU7ubMuGL5ipJhYXey6fix4HoTbOcYTFta6CnDqYQ
        # https://docs.pyobs.org/en/latest/api/utils/skyflats.html#pyobs.utils.skyflats.FlatFielder
        # https://github.com/pyobs/pyobs-core/blob/master/pyobs/utils/skyflats/pointing/static.py

        self.__log('info', f"Running flats_sequence for {row['device_name']} {row['action_type']} {row['action_value']} {row['start_time']} {row['end_time']}")

        action_value, folder, hdr, paired_devices = self.pre_sequence(row)

        # camera device (first filter set in pre_sequence)
        camera = self.devices[row['device_type']][row['device_name']]

        # target adu for flats
        cam_index = [i for i, d in enumerate(self.observatory['Camera']) if d['device_name'] == row['device_name']][0]
        target_adu = self.observatory['Camera'][cam_index]['target_adu']
        target_adu_max = target_adu[0] + target_adu[1]
        target_adu_min = target_adu[0] - target_adu[1]

        # camera orignal framing
        maxadu = camera.get('MaxADU')
        if maxadu['status'] == "success":
            maxadu = maxadu['data']
        else:
            raise ValueError(maxadu)

        numx = camera.get('NumX')
        if numx['status'] == "success":
            numx = numx['data']
        else:
            raise ValueError(numx)
        
        numy = camera.get('NumY')
        if numy['status'] == "success":
            numy = numy['data']
        else:
            raise ValueError(numy)
        
        startx = camera.get('StartX')
        if startx['status'] == "success":
            startx = startx['data']
        else:
            raise ValueError(startx)
        
        starty = camera.get('StartY')
        if starty['status'] == "success":
            starty = starty['data']
        else:
            raise ValueError(starty)
        
        # get location to determine if sun is up
        obs_lat = hdr['LAT-OBS']
        obs_lon = hdr['LONG-OBS']
        obs_alt = hdr['ALT-OBS']
        obs_location = EarthLocation(lat=obs_lat*u.deg, lon=obs_lon*u.deg, height=obs_alt*u.m)

        # check if ready to take flats
        take_flats = False
        while take_flats is False and self.error_free is True and self.interrupt is False and self.weather_safe is True:
            sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)

            if sun_altaz.alt.degree > 0:
                # check dome closed
                r = self.monitor_action('Dome', 'AtPark', True, 'Park')
                if r['status'] != 'success':
                    self.__log('error', 'Error parking dome')

            if take_flats is False:
                time.sleep(60)

        # move telescope to flat position
        telescope = self.devices['Telescope'][paired_devices['Telescope']]
        # get sweet spot for flat-fielding
        altaz = SkyCoord(
            alt=75 * u.deg, az=sun_altaz.az + 180 * u.degree, obstime=Time.now(), location=obs_location, frame="altaz"
        )
        r = telescope.set('Tracking', True)
        if r['status'] != 'success':
            raise ValueError(r)
        
        r = telescope.get('SlewToAltAzAsync')
        if r['status'] == 'success':
            r['data'](Azimuth=altaz.az.deg, Altitude=altaz.alt.deg)
        else:
            raise ValueError(r)
        

        # wait for telescope to reach position
        while telescope.get('Slewing')['data'] is True:
            time.sleep(1)

        # initial exposure time guess
        exposure_time = 1
                    
        for i, filter_name in enumerate(action_value['filter']):

            self.move_telescope_filter(paired_devices, action_value, filter_list_index = i) # sets filter/focus

            # MOVE TO FUNCTION?
            # establishing exposure time
            # set camera to view small area to speed up read times, such to determine right exposure time (assuming detector is bigger than 64x64)
            camera.set('NumX', 64)
            camera.set('NumY', 64)
            camera.set('StartX', int(numx/2 - 32))
            camera.set('StartY', int(numy/2 - 32))
            
            r = camera.get('StartExposure')['data'](Duration = exposure_time, Light = True)

            getting_exposure_time = True
            while getting_exposure_time:
                r = camera.get('ImageReady') # have a timeout in else part?
                time.sleep(0) # yield to other threads
                if r['status'] == "success":
                    if r['data'] is True:
                        r = camera.get('ImageArray')
                        
                        count_adu = np.nanmedian(r['data'])
                        fraction = count_adu / target_adu[0]

                        if count_adu < target_adu_min and count_adu > target_adu_max:
                            exposure_time = exposure_time * fraction
                            r = camera.get('StartExposure')['data'](Duration = exposure_time, Light = True)
                        else:
                            getting_exposure_time = False
            
            camera.set('NumX', numx)
            camera.set('NumY', numy)
            camera.set('StartX', startx)
            camera.set('StartY', starty)

            print(i, exposure_time)
            hdr['EXPTIME'] = exposure_time
            hdr['IMGTYPE'] = 'Flat'
            hdr['FILTER'] = filter_name

            r = camera.get('StartExposure')['data'](Duration = exposure_time, Light = True)
            
            count = 0
            t0 = datetime.utcnow()
            while count < action_value['n'][i]:

                sun_rising, take_flats, sun_altaz = utils.is_sun_rising(obs_location)
                if take_flats is False:
                    break

                r = camera.get('ImageReady') # have a timeout in else part?
                time.sleep(0) # yield to other threads
                if r['status'] == "success":
                    if r['data'] is True:
                        r = camera.get('ImageArray')
                        dateobs = pd.to_datetime(camera.get('LastExposureStartTime')['data']) ## need to have general case, change to pd.to_datetime?
                        
                        self.save_image(camera, hdr, dateobs, t0, maxadu, folder)

                        # if time passes 30s, move telescope
                        if (datetime.utcnow() - t0).total_seconds() >= 30:
                            # get sweet spot for flat-fielding
                            altaz = SkyCoord(
                                alt=75 * u.deg, az=sun_altaz.az + 180 * u.degree, obstime=Time.now(), location=obs_location, frame="altaz"
                            )
                            telescope.set('Tracking', True)
                            telescope.get('SlewToAltAzAsync')['data'](Azimuth=altaz.az.deg, Altitude=altaz.alt.deg)
                            # wait for telescope to reach position
                            while telescope.get('Slewing')['data'] is True:
                                time.sleep(1)

                            t0 = datetime.utcnow()

                        # update exposure time -- when to do this???
                        # calc with formula from paper?
                        # Flux = 10**(0.415*altsun+5.926)
                        


                        count += 1
                        print(count)

                        r = camera.get('StartExposure')['data'](Duration = exposure_time, Light = True)
                else:
                    print(r)
                    break

                if row['end_time'] <= datetime.utcnow():
                    break

    def img_transform(self, device, img, maxadu : int):
        '''
        This function takes in a device object, an image object, and a maximum ADU 
        value and returns a numpy array of the correct shape for astropy.io.fits.

        Parameters:
        - device: A device object that contains the ImageArrayInfo data.
        - img: An image object that contains the image data.
        - maxadu: The maximum ADU value.

        Returns:
        - nda: A numpy array of the correct shape for astropy.io.fits.
        '''
        
        r = device.get('ImageArrayInfo')

        if r['status'] != "success":
            raise ValueError(f"ImageArrayInfo failed: {r}")
        
        imginfo = r['data']

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
        
    def save_image(self, device, hdr, dateobs, t0, maxadu, folder):
        '''
        Save image to disk
        '''
        self.__log('debug', 'Getting image array')
        r = device.get('ImageArray')

        if r['status'] == "success":
            self.__log('debug', 'Got image array, now loading to numpy array')

            img = np.array(r['data'])
            self.__log('debug', 'Loaded image array to numpy array, now transforming')
            
            nda = self.img_transform(device, img, maxadu) ## TODO: make more efficient?
            self.__log('debug', 'Image transformed, now saving to disk')

            hdr['DATE-OBS'] = (dateobs.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time of exposure start')  

            date = datetime.utcnow() 
            hdr['DATE'] = (date.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time when this file was written')  

            hdu = fits.PrimaryHDU(nda, header=hdr)

            if hdr['IMGTYPE'] == 'Light':
                filepath = f"../images/{folder}/{device.device_name}_{hdr['FILTER']}_{hdr['OBJECT']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"
            else:
                filepath = f"../images/{folder}/{device.device_name}_{hdr['IMGTYPE']}_{hdr['EXPTIME']}_{date.strftime('%Y%m%d_%H%M%S.%f')[:-3]}.fits"

            self.__log('debug', 'Writing to disk')
            hdu.writeto(filepath)
            self.__log('debug', 'Image written to disk')

            self.last_image = filepath

            ## add to database            
            dt = dateobs.strftime("%Y-%m-%d %H:%M:%S.%f")
            self.cursor.execute(f"INSERT INTO images VALUES ('{filepath}', '{device.device_name}', '{0}', '{dt}')")
            self.__log('info', f"Image saved as {filepath.split('/')[-1]}")
            self.__log('info', f"Image acquired in {datetime.utcnow() - t0}")

            return filepath
        else:
            raise ValueError(r)
    
    def base_header(self, paired_devices, action_value):
        '''
        This function creates a base header for the fits file.

        Parameters:
        - paired_devices: A dictionary of paired devices.
        - action_value: A dictionary of action values.

        Returns:
        - hdr: A base header for the fits file.
        '''
        
        # TODO: error handling

        self.__log('info', "Creating base header")

        hdr = fits.Header()
        # need to add if statement for target/darks/bias
        for i, row in self.fits_config.iterrows():
            if row['device_type'] == 'astra' and row['fixed'] is True:
                match row['header']:
                    case 'FILTER':
                        device = self.devices['FilterWheel'][paired_devices['FilterWheel']]
                        pos = device.get('Position')['data']
                        names = device.get('Names')['data']
                        hdr[row['header']] = (names[pos], row["comment"])
                    case 'XPIXSZ':
                        device = self.devices['Camera'][paired_devices['Camera']]
                        binx = device.get('BinX')['data']
                        xpixsize = device.get('PixelSizeX')['data']
                        hdr[row['header']] = (binx*xpixsize, row["comment"])
                    case 'YPIXSZ':
                        device = self.devices['Camera'][paired_devices['Camera']]
                        biny = device.get('BinY')['data']
                        ypixsize = device.get('PixelSizeY')['data']
                        hdr[row['header']] = (biny*ypixsize, row["comment"])
                    case 'APTAREA':
                        device = self.devices['Telescope'][paired_devices['Telescope']]
                        val = device.get('ApertureArea')['data'] * 1e6
                        hdr[row['header']] = (val, row["comment"])
                    case 'APTDIA':
                        device = self.devices['Telescope'][paired_devices['Telescope']]
                        val = device.get('ApertureDiameter')['data'] * 1e3
                        hdr[row['header']] = (val, row["comment"])
                    case 'FOCALLEN':
                        device = self.devices['Telescope'][paired_devices['Telescope']]
                        val = device.get('FocalLength')['data'] * 1e3
                        hdr[row['header']] = (val, row["comment"])
                    case 'OBJECT':
                        if row['header'].lower() in action_value:
                            hdr[row['header']] = (action_value[row['header'].lower()], row["comment"])
                    case 'EXPTIME' | 'IMAGETYP':
                        hdr[row['header']] = (None, row["comment"])
                    case _:
                        self.__log('warning', f"Unknown header: {row['header']}")

            elif (row['device_type'] not in ['astropy_default', 'astra', 'astra_fixed', '']) and row['fixed'] is True:
                device_type = row['device_type']
                device_name = paired_devices[device_type]
                device = self.devices[device_type][device_name]

                r = device.get(row['device_command'])  ## error handling

                if r['status'] == "error":
                    raise ValueError(r)

                hdr[row['header']] = (r['data'], row["comment"])

            elif row['device_type'] == 'astra_fixed':
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
    
    def final_headers(self):
        '''
        Add final headers to fits file

        '''
        # TODO: make sure interpolated onto right time
        # TODO: error handling...

        ## get images from sql
        rows = self.cursor.execute("SELECT * FROM images WHERE complete_hdr = 0;")
        df_images = pd.DataFrame(rows, columns=['filepath', 'camera_name', 'complete_hdr', 'date_obs'])

        if df_images.shape[0] > 0:
            for cam in df_images['camera_name'].unique():

                df_images_filt = df_images[df_images['camera_name'] == cam]

                ## get paired devices
                cam_index = [i for i, d in enumerate(self.observatory['Camera']) if d['device_name'] == cam][0]
                paired_devices = self.observatory['Camera'][cam_index]['paired_devices']
                paired_devices['Camera'] = cam

                df_images_filt['date_obs'] = pd.to_datetime(df_images_filt['date_obs'], format='%Y-%m-%d %H:%M:%S.%f')

                df_images_filt = df_images_filt.sort_values(by='date_obs').reset_index(drop=True)
                df_images_filt['jd_obs'] = df_images_filt['date_obs'].apply(utils.to_jd).sort_values()

                # add small time increment to avoid duplicate jd TODO: better fix needed
                df_images_filt['jd_obs'] = df_images_filt['jd_obs'].reset_index().apply(lambda x : x + x['index'] * 1e-9, axis=1).drop('index', axis=1)['jd_obs']

                ## get polled data from ascom devices
                t0 = pd.to_datetime(df_images_filt['date_obs'].iloc[0]) - pd.Timedelta('10 sec') # right to use 10 sec?
                t1 = pd.to_datetime(df_images_filt['date_obs'].iloc[-1]) + pd.Timedelta('10 sec')
                                                                                                        
                q = f"""SELECT * FROM polling WHERE datetime BETWEEN "{str(t0)}" AND "{str(t1)}";"""
                rows = self.cursor.execute(q)
                df_poll = pd.DataFrame(rows, columns=['device_type', 'device_name', 'device_command', 'device_value', 'datetime'])
                df_poll['jd'] = pd.to_datetime(df_poll['datetime'], format='%Y-%m-%d %H:%M:%S.%f').apply(utils.to_jd)

                ## find unique headers in polled commands
                df_poll_unique = df_poll[['device_type', 'device_name', 'device_command']].drop_duplicates()

                ## drop row that have device_type and device_command that are not in fits_config
                df_poll_unique = df_poll_unique[df_poll_unique.apply(lambda x : (x['device_type'] in self.fits_config['device_type'].values) and (x['device_command'] in self.fits_config['device_command'].values), axis=1)]

                df_poll_unique['header'] = df_poll_unique.apply(lambda x : (self.fits_config[(self.fits_config['device_type'] == x['device_type']) & (self.fits_config['device_command'] == x['device_command'])]['header'].values[0]), axis=1)
                df_poll_unique['comment'] = df_poll_unique.apply(lambda x : (self.fits_config[(self.fits_config['device_type'] == x['device_type']) & (self.fits_config['device_command'] == x['device_command'])]['comment'].values[0]), axis=1)

                ## keep rows that only have device_name in paired_devices
                df_poll_unique = df_poll_unique[df_poll_unique['device_name'].isin(paired_devices.values())]

                ## form interpolated dataframe
                df_inp = pd.DataFrame(columns=df_poll_unique['header'], index=df_images_filt['jd_obs'])

                for i, row in df_poll_unique.iterrows():
                    df_poll_filtered = df_poll[(df_poll['device_type'] == row['device_type']) & (df_poll['device_name'] == row['device_name']) & (df_poll['device_command'] == row['device_command'])]
                    
                    df_poll_filtered = df_poll_filtered.sort_values(by='jd')
                    df_poll_filtered = df_poll_filtered.set_index('jd')

                    df_poll_filtered['device_value'] = df_poll_filtered['device_value'].replace({'True': 1.0, 'False': 0.0}).astype(float)

                    df_inp[row['header']] = utils.interpolate_dfs(df_images_filt['jd_obs'], df_poll_filtered['device_value'])['device_value'].fillna(0)

                ## update files
                for i, row in df_images_filt.iterrows():
                    with fits.open(row[0], mode='update') as filehandle:
                        hdr = filehandle[0].header
                        for header in df_inp.columns:
                            hdr[header] = (df_inp.iloc[i][header], df_poll_unique[df_poll_unique['header'] == header]['comment'].values[0])
                            
                        location = EarthLocation(lat=hdr['LAT-OBS']*u.deg, lon=hdr['LONG-OBS']*u.deg, height=hdr['ALT-OBS']*u.m)
                        target = SkyCoord(hdr['RA'], hdr['DEC'], unit=(u.deg, u.deg), frame='icrs')
                        
                        utils.hdr_times(hdr, self.fits_config, location, target)
                        filehandle[0].add_checksum()

                        self.cursor.execute(f'''UPDATE images SET complete_hdr = 1 WHERE filename="{row[0]}"''')
        
        self.__log('info', 'Completing headers... Done.')

    def monitor_action(self, device_type : str, monitor_command : str, desired_condition : any, run_command : str, timeout : float = 120):
        '''
        Monitor all devices for a given monitor_command and run_command if desired_condition is not met.
        '''
        try:
            start_time = time.time()

            if device_type in self.observatory:
                monitor_status = []
                for d in self.devices[device_type]:
                    device = self.devices[device_type][d]
                    status = device.get(monitor_command)['data']
                    monitor_status.append(status)

                    if status != desired_condition:
                        device.get(run_command)['data']()

                all_monitor_status = np.mean(monitor_status)

                if all_monitor_status != desired_condition:
                    self.__log("info", f"Monitor run action: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
                    while all_monitor_status != desired_condition:
                        monitor_status = []
                        for d in self.devices[device_type]:
                            device = self.devices[device_type][d]
                            status = device.get(monitor_command)['data']
                            monitor_status.append(status)

                        all_monitor_status = np.mean(monitor_status)

                        time.sleep(1)

                        if time.time() - start_time > timeout:
                            break
                        
                    
                    if all_monitor_status == desired_condition:
                        self.__log("info", f"Monitor run action complete: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
                        return {'status': 'success', 'data': True, 'message': f"Monitor run action complete: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}"}
                    else:
                        self.error_source.append({'type': device_type, 'device_name': '', 'error': 'Monitor run action timeout'})
                        self.__log("error", f"Monitor run action timeout: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}")
                        return {'status': 'error', 'data': False, 'message': f"Monitor run action timeout: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}"}
                else:
                    return {'status': 'success', 'data': True, 'message': f"Monitor run action complete: {device_type} {monitor_command} {desired_condition} {run_command} {all_monitor_status}"}
            else:
                self.__log("error", f"{device_type} not found in observatory.")
                return {'status': 'error', 'data': False, 'message': f"{device_type} not found in observatory."}
        except Exception as e:
            self.error_source.append({'type': device_type, 'device_name': '', 'error': str(e)})
            self.__log("error", f"Monitor run action error: {str(e)}")
            return {'status': 'error', 'data': False, 'message': f"Monitor run action error: {str(e)}"}
    
    def queue_get(self):

        while True:
            try:
                metadata, r = self.queue.get()
                
                if r['type'] == 'query':
                    self.cursor.execute(r['data'])
                elif r['type'] == 'log':
                    self.__log(r['data'][0], r['data'][1])
                    if r['data'][0] == 'error':
                        self.error_source.append(metadata)

            except Exception as e:
                self.error_source.append({'type': 'Queue', 'device_name': 'queue_get', 'error': str(e)})
                self.__log("error", f"Queue get error: {str(e)}")
