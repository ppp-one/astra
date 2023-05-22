import logging
import time
from datetime import datetime
from threading import Thread

from alpaca.camera import *
from alpaca.covercalibrator import *
from alpaca.dome import *
from alpaca.exceptions import *
from alpaca.filterwheel import *
from alpaca.focuser import *
from alpaca.observingconditions import *
from alpaca.rotator import *
from alpaca.safetymonitor import *
from alpaca.switch import *
from alpaca.telescope import *

logging.basicConfig(format='%(levelname)s,%(asctime)s.%(msecs)03d,%(process)d,%(name)s,(%(filename)s:%(lineno)d),%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging.Formatter.converter = time.gmtime


class AlpacaDevice():
    def __init__(self, ip, device_type, device_number, device_name, cursor):

        self.cursor = cursor

        if device_type in ["Telescope", "Camera", "CoverCalibrator", "Dome", "FilterWheel", "Focuser", "ObservingConditions", "Rotator", "SafetyMonitor", "Switch"]:
            self.device = globals()[device_type](ip, device_number)
        else:
            self.__log('error', f"{device_type} is not a valid device type")
            ## TODO: raise exception, does it kill the process?

        self.ip = ip
        self.device_number = device_number
        self.device_type = device_type
        self.device_name = device_name

        self._poll_list = []
        self._poll_latest = {}

        self.__log('info', f'{device_type} {device_name} loaded')

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
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message, exc_info=True)
        elif level == 'critical':
            logging.critical(message)
        else:
            logging.info(message)

        dt_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self.cursor.execute(f"INSERT INTO log VALUES ('{dt_str}', '{level}',  '{message}')")

    def get_can_states(self):
        methods = [o for o in dir(self.device) if o.startswith('Can')]

        can_states = {}
        for method in methods:
            can_states[method] = self.get(method)['data']

        return can_states

    def get(self, method):
        ## method getter
        try:
            data = getattr(self.device, method)
            return {"status" : "success", "data" : data, "message" : ""} # check if valid, need args?
        except Exception as e:
            self.__log('error', f'Get method error: {self.device_type}, {self.device_name}, {method}, {str(e)}')
            return {"status" : "error", "data" : "null", "message" : f"Get method error: {str(e)}"} # check if valid, need args?

    def set(self, method, value):
        ## property setter
        try:
            data = setattr(self.device, method, value)
            return {"status" : "success", "data" : data, "message" : ""} # check if valid, need args?
        except Exception as e:
            self.__log('error', f'Set method error: {self.device_type}, {self.device_name}, {method}, {str(e)}')
            return {"status" : "error", "data" : "null", "message" : f"Set method error: {str(e)}"} # check if valid, need args?

    def loop(self, method, delay):
        self._poll_list.append(method)
        self._poll_latest[method] = {}
        self._poll_latest[method]["value"] = None
        self._poll_latest[method]["datetime"] = None
        try:
            while method in self._poll_list:

                get = self.get(method)
                if get["status"] == "success":
                    val = get["data"]
                else:
                    ## try again, just in case...
                    get = self.get(method)
                    if get["status"] == "success":
                        val = get["data"]
                    else:
                        raise ValueError(get)

                dt = datetime.utcnow()
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                
                self.cursor.execute(f"INSERT INTO polling VALUES ('{self.device_type}', '{self.device_name}',  '{method}', '{val}', '{dt_str}')")

                self._poll_latest[method]["value"] = val
                self._poll_latest[method]["datetime"] = dt
                
                time.sleep(delay)
        except Exception as e:
            dt = datetime.utcnow()
            self._poll_latest[method]["datetime"] = dt
            self._poll_latest[method]["value"] = "null"
            self.__log('error', f'Loop error: {self.device_type}, {self.device_name}, {method}, {str(e)}')           
        
        
    def start_poll(self, method, delay): 
        if method not in self._poll_list:
            Thread(target=self.loop, args=(method, delay)).start()
            self.__log('info', f'{self.device_type}, {self.device_name}, {method} poll started with {delay} second cadence')
    
    def stop_poll(self, method=None):
        if method is None:
            self._poll_list = []
            self._poll_latest = {}
            self.__log('info', f'{self.device_type}, {self.device_name}, all polls stopped')
        elif method in self._poll_list:
            self._poll_list = list(filter((method).__ne__, self._poll_list))
            del self._poll_latest[method]
            self.__log('info', f'{self.device_type}, {self.device_name}, {method} poll stopped. {self._poll_list} left in poll list, and {self._poll_latest} left in poll dict')
        else:
            self.__log('error', f'Stop poll error: {self.device_type}, {self.device_name}, {method} not in poll list.')

    def poll_list(self):
        try:
            return {"status" : "success", "data" : self._poll_list, "message" : ""}
        except Exception as e:    
            self.__log('error', f'poll_list error: {self.device_type}, {self.device_name}, {str(e)}')
            return {"status" : "error", "data" : "null", "message" : f"poll_list error: {str(e)}"}

    def poll_latest(self):
        try:
            return {"status" : "success", "data" : self._poll_latest, "message" : ""}
        except Exception as e:
            self.__log('error', f'poll_latest error: {self.device_type}, {self.device_name}, {str(e)}')
            return {"status" : "error", "data" : "null", "message" : f"poll_latest error: {str(e)}"}