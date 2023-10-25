import time
from datetime import datetime

from threading import Thread
from multiprocessing import Process, Pipe, Lock

import win32com.client

import os
import signal

# https://medium.com/@sampsa.riikonen/doing-python-multiprocessing-the-right-way-a54c1880e300
# https://stackoverflow.com/questions/27435284/multiprocessing-vs-multithreading-vs-asyncio

class AscomDevice(Process):
    def __init__(self, device_type, device_name, queue, debug=False):
        super().__init__()
        self.front_pipe, self.back_pipe = Pipe()
        self.lock = Lock()
        self.queue = queue
        self.debug = debug

        if device_type in ["Telescope", "Camera", "CoverCalibrator", "Dome", "FilterWheel", "Focuser", "ObservingConditions", "Rotator", "SafetyMonitor", "Switch"]:
            self.device = win32com.client.Dispatch(device_name)
        else:
            print(f"{device_type} is not a valid device type")
            ## TODO: raise exception, does it kill the process?

        self.device_type = device_type
        self.device_name = device_name
        self.metadata = {"device_type" : device_type, "device_name" : device_name}

        self._poll_list = []
        self._poll_latest = {}

        self.queue.put((self.metadata, {"type" : "log", "data" : ("info", f'{device_type} {device_name} loaded')}))

    ## FRONTEND METHODS
    def __getstate__(self):
        state = self.__dict__.copy()
        # Don't pickle device
        del state["device"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Add device back since it doesn't exist in the pickle
        self.device = win32com.client.Dispatch(self.device_name)

    def get(self, method):
        ## method getter
        with self.lock:
            self.front_pipe.send(["get", {"method" : method}])
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise msg
            else:
                return msg

    def set(self, method, value):
        ## property setter
        with self.lock:
            self.front_pipe.send(["set", {"method" : method, "value" : value}])
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise msg
            else:
                return msg

    def start_poll(self, method, delay):
        with self.lock:
            self.front_pipe.send(["start_poll", {"method" : method, "delay": delay}])

    def stop_poll(self, method=None):
        with self.lock:
            self.front_pipe.send(["stop_poll", {"method" : method}])
    
    def poll_list(self):
        with self.lock:
            self.front_pipe.send("poll_list")
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise msg
            else:
                return msg

    def poll_latest(self):
        with self.lock:
            self.front_pipe.send("poll_latest")
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise msg
            else:
                return msg

    def stop(self):
        with self.lock:
            print(f"AlpacaDevice {self.device_type} stopping")
            self.front_pipe.send("stop")
            self.join()

    ## BACKEND CORE

    def run(self):
        print(f"AlpacaDevice {self.device_type} started with pid [{os.getpid()}]")
        self.active = True

        signal.signal(signal.SIGINT, self.stop__)
        signal.signal(signal.SIGTERM, self.stop__)
        
        while self.active:
            self.active = self.listenFront__()

        print(f"AlpacaDevice {self.device_type} stopped")

    def listenFront__(self):
        try:
            r = self.back_pipe.recv()
            message = r[0] if len(r) == 2 else r

            if message == "get":
                self.get__(**r[1])
                return True
            elif message == "set":
                self.set__(**r[1])
                return True
            elif message == "start_poll":
                self.start_poll__(**r[1])
                return True
            elif message == "stop_poll":
                self.stop_poll__(**r[1])
                return True
            elif message == "poll_list":
                self.poll_list__()
                return True
            elif message == "poll_latest":
                self.poll_latest__()
                return True
            elif message == "stop":
                return False
            else:
                print("listenFront__ : unknown message", message)
                return True
        except OSError:
            return False

    ## BACKEND METHODS

    def get__(self, method, pipe=True):
        ## method getter
        try:
            # permit 3 attempts
            data = None
            if self.debug:
                self.queue.put((self.metadata, {"type" : "log", "data" : ("debug", f'Getting method: {self.device_type}, {self.device_name}, {method}')}))

            for i in range(2):
                try:
                    if data is None:
                        data = getattr(self.device, method)
                        if self.debug:
                            self.queue.put((self.metadata, {"type" : "log", "data" : ("debug", f'Get method success: {self.device_type}, {self.device_name}, {method}')}))
                except Exception as e:
                    time.sleep(0)
                    self.queue.put((self.metadata, {"type" : "log", "data" : ("warning", f'Get method failed with data {str(data)}: {self.device_type}, {self.device_name}, {method}, {str(e)}, trying again...')}))
                    time.sleep(1)
                    continue
                time.sleep(0)

            if data is None:
                data = getattr(self.device, method)
                if self.debug:
                    self.queue.put((self.metadata, {"type" : "log", "data" : ("debug", f'Get method success: {self.device_type}, {self.device_name}, {method}')}))

            time.sleep(0)

            if pipe:
                self.back_pipe.send(data) # check if valid, need args?
            else:
                return {"status" : "success", "data" : data, "message" : ""}
        except Exception as e:
            if pipe:
                self.queue.put((self.metadata, {"type" : "log", "data" : ('error', f'Get method error with data {str(data)}: {self.device_type}, {self.device_name}, {method}, {str(e)}')}))
                self.back_pipe.send(e)
            else:
                return {"status" : "error", "data" : "null", "message" : f"Get method error: {str(e)}"}

    def set__(self, method, value):
        ## property setter
        try:
            data = setattr(self.device, method, value)
            self.back_pipe.send(data) # check if valid, need args?
        except Exception as e:
            self.queue.put((self.metadata, {"type" : "log", "data" : ('error', f'Set method error: {self.device_type}, {self.device_name}, {method}, {str(e)}')}))
            self.back_pipe.send(e) # check if valid, need args?

    def loop__(self, method, delay):
        self._poll_list.append(method)
        self._poll_latest[method] = {}
        self._poll_latest[method]["value"] = None
        self._poll_latest[method]["datetime"] = None
        try:
            while method in self._poll_list:

                get = self.get__(method, pipe=False)
                if get["status"] == "success":
                    val = get["data"]
                else:
                    time.sleep(0)
                    ## try again, just in case...
                    get = self.get__(method, pipe=False)
                    if get["status"] == "success":
                        val = get["data"]
                    else:
                        raise ValueError(get)
                time.sleep(0)

                dt = datetime.utcnow()
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

                self.queue.put((self.metadata, {"type" : "query", "data" : f"INSERT INTO polling VALUES ('{self.device_type}', '{self.device_name}',  '{method}', '{val}', '{dt_str}')"}))
                
                self._poll_latest[method]["value"] = val
                self._poll_latest[method]["datetime"] = dt
                
                time.sleep(delay)
        except Exception as e:
            dt = datetime.utcnow()
            self._poll_latest[method]["datetime"] = dt
            self._poll_latest[method]["value"] = "null"
            self.queue.put((self.metadata, {"type" : "log", "data" : ('error', f'Loop error: {self.device_type}, {self.device_name}, {method}, {str(e)}')}))
        
    def start_poll__(self, method, delay):
        if method not in self._poll_list:
            Thread(target=self.loop__, args=(method, delay), daemon=True).start()
            self.queue.put((self.metadata, {"type" : "log", "data" : ('info', f'{self.device_type}, {self.device_name}, {method} poll started with {delay} second cadence')}))
    
    def stop_poll__(self, method=None):
        if method is None:
            self._poll_list = []
            self._poll_latest = {}
            self.queue.put((self.metadata, {"type" : "log", "data" : ('info', f'{self.device_type}, {self.device_name}, all polls stopped')}))
        elif method in self._poll_list:
            self._poll_list = list(filter((method).__ne__, self._poll_list))
            del self._poll_latest[method]
            self.queue.put((self.metadata, {"type" : "log", "data" : ('info', f'{self.device_type}, {self.device_name}, {method} poll stopped. {self._poll_list} left in poll list, and {self._poll_latest} left in poll dict')}))       
        else:
            self.queue.put((self.metadata, {"type" : "log", "data" : ('warning', f'Stop poll error: {self.device_type}, {self.device_name}, {method} not in poll list.')}))

    def poll_list__(self):
        try:
            self.back_pipe.send(self._poll_list)
        except Exception as e:    
            self.queue.put((self.metadata, {"type" : "log", "data" : ('error', f'poll_list error: {self.device_type}, {self.device_name}, {str(e)}')}))
            self.back_pipe.send(e)

    def poll_latest__(self):
        try:
            self.back_pipe.send(self._poll_latest)
        except Exception as e:
            self.queue.put((self.metadata, {"type" : "log", "data" : ('error', f'poll_latest error: {self.device_type}, {self.device_name}, {str(e)}')}))
            self.back_pipe.send(e)

    def stop__(self, *args):
        self.active = False
        
        # close pipes
        self.front_pipe.close()
        self.back_pipe.close()

if __name__ == "__main__":

    from multiprocessing import Manager

    manager = Manager()
    queue = manager.Queue()

    d = AscomDevice("Telescope", "ASCOM.Simulator.Telescope", queue, debug=True)
    d.start()

    try:
        print(d.device)
        print(d.get("Connected"))
        time.sleep(1)
        print(d.set("Connected", True))
        print(d.get("Connected"))

        print(d.get("SiteElevation"))
        print(d.get("SiteElevation"))
        print(d.get("SiteElevation"))
        print(d.get("SiteElevation"))
        print(d.get("SiteElevation"))
        print(d.get("SiteElevation"))
        print(d.get("SiteElevation"))

        print(d.get("Connected"))
        print(d.set("Connected", False))
        time.sleep(1)
        print(d.get("Connected"))



    except Exception as e:
        print(e)

    d.stop()

    # d = win32com.client.Dispatch("ASCOM.Simulator.Telescope")
    # print(d.Connected)