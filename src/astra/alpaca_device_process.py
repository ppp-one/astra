"""ALPACA device multiprocessing wrapper for astronomical device control.

This module provides a multiprocessing-based wrapper for ALPACA astronomical
devices, enabling concurrent device polling, method execution, and data logging.
It implements a producer-consumer pattern with pipes for communication between
the main process and device-specific subprocesses.

Classes:
    AlpacaDevice: Multiprocessing wrapper for ALPACA astronomical devices

The module supports various ALPACA device types including telescopes, cameras,
filter wheels, focusers, domes, and environmental monitoring equipment.
"""

import os
import signal
import time
from datetime import UTC, datetime
from multiprocessing import Lock, Pipe, Process
from threading import Thread
from typing import Any, Dict, List, Optional, Union

import requests
from alpaca.camera import Camera
from alpaca.covercalibrator import CoverCalibrator
from alpaca.dome import Dome
from alpaca.filterwheel import FilterWheel
from alpaca.focuser import Focuser
from alpaca.observingconditions import ObservingConditions
from alpaca.rotator import Rotator
from alpaca.safetymonitor import SafetyMonitor
from alpaca.switch import Switch
from alpaca.telescope import Telescope

ALPACA_DEVICE_TYPES = {
    "Telescope": Telescope,
    "Camera": Camera,
    "CoverCalibrator": CoverCalibrator,
    "Dome": Dome,
    "FilterWheel": FilterWheel,
    "Focuser": Focuser,
    "ObservingConditions": ObservingConditions,
    "Rotator": Rotator,
    "SafetyMonitor": SafetyMonitor,
    "Switch": Switch,
}

# https://medium.com/@sampsa.riikonen/doing-python-multiprocessing-the-right-way-a54c1880e300
# https://stackoverflow.com/questions/27435284/multiprocessing-vs-multithreading-vs-asyncio


class AlpacaDeviceError(Exception):
    """Base error for AlpacaDevice failures (IPC or remote)."""

    @staticmethod
    def from_device(
        device: "AlpacaDevice",
        exc: Exception,
        method_name: str,
        method: str | None = None,
    ) -> "AlpacaDeviceError":
        """Create an appropriate AlpacaDeviceError subclass from a device and original exception."""
        if method is not None:
            method_name = f"{method}('{method_name}')"

        msg = f"{device.device_type} {device.device_name}: '{method_name}' failed: {str(exc)}"

        # classify the exception as remote/network if it looks like a requests/HTTP error
        is_remote = False
        try:
            if isinstance(exc, requests.RequestException):
                is_remote = True
        except Exception:
            # requests may not be available or exc may not be the same class object;
            # fall back to message heuristics
            pass

        if not is_remote:
            s = str(exc).lower()
            if (
                "connection refused" in s
                or "max retries exceeded" in s
                or "newconnectionerror" in s
            ):
                is_remote = True

        if is_remote:
            e = RemoteDeviceError(msg)
        else:
            e = AlpacaDeviceIPCError(msg)

        # preserve original exception as cause so callers can inspect it
        try:
            e.__cause__ = exc
        except Exception:
            pass

        return e


class AlpacaDeviceIPCError(AlpacaDeviceError):
    """Error communicating with the device subprocess (IPC)."""


class RemoteDeviceError(AlpacaDeviceError):
    """Remote/network error when the device subprocess fails to contact ALPACA HTTP server."""


class AlpacaDevice(Process):
    """Multiprocessing wrapper for ALPACA astronomical devices.

    Provides a process-based interface for ALPACA devices with concurrent
    polling capabilities, method execution, and inter-process communication
    via pipes. Supports automatic retry logic and comprehensive error handling.

    Args:
        ip (str): IP address of the ALPACA device server.
        device_type (str): Type of device (e.g., 'Telescope', 'Camera').
        device_number (int): Device number on the ALPACA server.
        device_name (str): User-friendly name for the device.
        queue: Multiprocessing queue for logging and data communication.
        connectable (bool): Whether to attempt initial connection to the device.
        debug (bool): Enable debug logging for device operations.

    Attributes:
        device: ALPACA device instance.
        metadata: Device identification information.
        front_pipe: Communication pipe for main process.
        back_pipe: Communication pipe for device subprocess.
        lock: Thread lock for pipe synchronization.
    """

    def __init__(
        self,
        ip: str,
        device_type: str,
        device_number: int,
        device_name: str,
        queue: Any,
        connectable: bool = True,
        debug: bool = False,
    ) -> None:
        super().__init__()
        self.front_pipe, self.back_pipe = Pipe()
        self.lock = Lock()
        self.queue = queue
        self.debug = debug

        if device_type in [
            "Telescope",
            "Camera",
            "CoverCalibrator",
            "Dome",
            "FilterWheel",
            "Focuser",
            "ObservingConditions",
            "Rotator",
            "SafetyMonitor",
            "Switch",
        ]:
            self.device = ALPACA_DEVICE_TYPES[device_type](ip, device_number)
        else:
            self.queue.put(
                (
                    {
                        "ip": ip,
                        "device_type": device_type,
                        "device_number": device_number,
                        "device_name": device_name,
                    },
                    {
                        "type": "log",
                        "data": (
                            "warning",
                            f"{device_type} is not a valid device type",
                        ),
                    },
                )
            )

        self.ip = ip
        self.device_number = device_number
        self.device_type = device_type
        self.device_name = device_name
        self.metadata = {
            "ip": ip,
            "device_type": device_type,
            "device_number": device_number,
            "device_name": device_name,
        }
        self.connectable = connectable

        self._poll_list = []
        self._poll_latest = {}
        self._poll_pause = False

        self.queue.put(
            (
                self.metadata,
                {
                    "type": "log",
                    "data": ("info", f"{device_type} {device_name} loaded"),
                },
            )
        )

    ## FRONTEND METHODS

    def get(self, method: str, **kwargs) -> Any:
        """Execute device method with automatic retry and error handling.

        Args:
            method (str): Name of the device method to execute.
            **kwargs: Keyword arguments to pass to the method.

        Returns:
            Any: Result from the device method execution.

        Raises:
            Exception: If the device method execution fails.
        """
        ## method getter
        with self.lock:
            self.front_pipe.send(["get", {"method": method, **kwargs}])
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise AlpacaDeviceError.from_device(
                    self, msg, method_name="get", method=method
                )
            else:
                return msg

    def set(self, method: str, value: Any) -> Any:
        """Set device property value with error handling.

        Args:
            method (str): Name of the device property to set.
            value (Any): Value to assign to the property.

        Returns:
            Any: Result from the property setter.

        Raises:
            Exception: If the property setting fails.
        """
        ## property setter
        with self.lock:
            self.front_pipe.send(["set", {"method": method, "value": value}])
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise AlpacaDeviceError.from_device(
                    self, msg, method_name="set", method=method
                )
            else:
                return msg

    def start_poll(self, method: str, delay: float) -> None:
        """Start continuous polling of a device method.

        Args:
            method (str): Name of the device method to poll.
            delay (float): Polling interval in seconds.
        """
        with self.lock:
            self.front_pipe.send(["start_poll", {"method": method, "delay": delay}])

    def stop_poll(self, method: Optional[str] = None) -> None:
        """Stop polling for a specific method or all methods.

        Args:
            method (Optional[str]): Method name to stop polling, or None for all.
        """
        with self.lock:
            self.front_pipe.send(["stop_poll", {"method": method}])

    def pause_polls(self) -> None:
        """Temporarily pause all active polling operations."""
        with self.lock:
            self.front_pipe.send("pause_polls")

    def resume_polls(self) -> None:
        """Resume all paused polling operations."""
        with self.lock:
            self.front_pipe.send("resume_polls")

    def poll_list(self) -> List[str]:
        """Get list of currently active polling methods.

        Returns:
            List[str]: List of method names being polled.

        Raises:
            Exception: If polling list retrieval fails.
        """
        with self.lock:
            self.front_pipe.send("poll_list")
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise AlpacaDeviceError.from_device(
                    device=self, exc=msg, method_name="poll_list"
                )
            else:
                return msg

    def poll_latest(self) -> Dict[str, Dict[str, Any]]:
        """Get latest polling results for all active methods.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping method names to
                                     their latest values and timestamps.

        Raises:
            Exception: If latest polling data retrieval fails.
        """
        with self.lock:
            self.front_pipe.send("poll_latest")
            msg = self.front_pipe.recv()
            if isinstance(msg, Exception):
                raise AlpacaDeviceError.from_device(
                    device=self, exc=msg, method_name="poll_latest"
                )
            else:
                return msg

    def stop(self) -> None:
        """Stop the device process and clean up resources."""
        with self.lock:
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "info",
                            f"AlpacaDevice {self.device_type} {self.device_number} stopping",
                        ),
                    },
                )
            )
            self.front_pipe.send("stop")
            self.join()

    ## BACKEND CORE

    def run(self) -> None:
        """Main process loop for handling device operations.

        Runs in the device subprocess to handle method calls, polling,
        and inter-process communication. Sets up signal handlers for
        graceful shutdown.
        """
        self.queue.put(
            (
                self.metadata,
                {
                    "type": "log",
                    "data": (
                        "info",
                        f"AlpacaDevice {self.device_type} {self.device_number} started "
                        f"with pid [{os.getpid()}]",
                    ),
                },
            )
        )
        self.active = True

        signal.signal(signal.SIGINT, self.stop_poll__)  # type: ignore
        signal.signal(signal.SIGTERM, self.stop_poll__)  # type: ignore

        while self.active:
            self.active = self.listenFront__()

        self.queue.put(
            (
                self.metadata,
                {
                    "type": "log",
                    "data": (
                        "info",
                        f"AlpacaDevice {self.device_type} {self.device_number} stopped",
                    ),
                },
            )
        )

    def listenFront__(self) -> bool:
        """Listen for and process messages from the main process.

        Handles various message types including method calls, polling
        control, and shutdown commands.

        Returns:
            bool: True to continue running, False to stop the process.
        """
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
            elif message == "pause_polls":
                self._poll_pause = True
                return True
            elif message == "resume_polls":
                self._poll_pause = False
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

    def get__(
        self, method: str, pipe: bool = True, **kwargs
    ) -> Union[Any, Dict[str, Any]]:
        """Backend method execution with retry logic and error handling.

        Args:
            method (str): Name of the device method to execute.
            pipe (bool): Whether to send result via pipe or return directly.
            **kwargs: Keyword arguments for the method.

        Returns:
            Union[Any, Dict[str, Any]]: Method result or status dictionary.
        """
        ## method getter
        try:
            # permit 3 attempts
            data = "not get"
            if self.debug:
                self.queue.put(
                    (
                        self.metadata,
                        {
                            "type": "log",
                            "data": (
                                "debug",
                                f"Getting method: {self.device_type}, {self.device_name}, {method}",
                            ),
                        },
                    )
                )

            for _ in range(2):
                try:
                    if data == "not get":
                        data = getattr(self.device, method)

                        # if kwargs, call method with kwargs
                        if kwargs:
                            if "no_kwargs" in kwargs:
                                data = data()
                            else:
                                data = data(**kwargs)

                        if self.debug:
                            self.queue.put(
                                (
                                    self.metadata,
                                    {
                                        "type": "log",
                                        "data": (
                                            "debug",
                                            "Get method success: "
                                            f"{self.device_type}, {self.device_name}, {method}",
                                        ),
                                    },
                                )
                            )
                except Exception as e:
                    time.sleep(0)
                    self.queue.put(
                        (
                            self.metadata,
                            {
                                "type": "log",
                                "data": (
                                    "warning",
                                    f"Get method failed with data {str(data)}: "
                                    f"{self.device_type}, {self.device_name}, {method}, "
                                    f"{str(e)}, trying again...",
                                ),
                            },
                        )
                    )
                    time.sleep(1)
                    continue

                time.sleep(0)

            # final run. If error, caught by try/except
            if data == "not get":
                data = getattr(self.device, method)

                # if kwargs, call method with kwargs
                if kwargs:
                    if "no_kwargs" in kwargs:
                        data = data()
                    else:
                        data = data(**kwargs)

                if self.debug:
                    self.queue.put(
                        (
                            self.metadata,
                            {
                                "type": "log",
                                "data": (
                                    "debug",
                                    "Get method success: "
                                    f"{self.device_type}, {self.device_name}, {method}",
                                ),
                            },
                        )
                    )

            time.sleep(0)

            if pipe:
                self.back_pipe.send(data)  # check if valid, need args?
            else:
                return {"status": "success", "data": data, "message": ""}
        except Exception as e:
            if pipe:
                self.queue.put(
                    (
                        self.metadata,
                        {
                            "type": "log",
                            "data": (
                                "error",
                                f"Get method error with data {str(data)}: "
                                f"{self.device_type}, {self.device_name}, {method}, {str(e)}",
                            ),
                        },
                    )
                )
                self.back_pipe.send(e)
            else:
                return {
                    "status": "error",
                    "data": "null",
                    "message": f"Get method error: {str(e)}",
                }

    def set__(self, method: str, value: Any) -> None:
        """Backend property setter with retry logic and error handling.

        Args:
            method (str): Name of the device property to set.
            value (Any): Value to assign to the property.
        """
        ## property setter
        try:
            # permit 3 attempts
            data = "not set"
            if self.debug:
                self.queue.put(
                    (
                        self.metadata,
                        {
                            "type": "log",
                            "data": (
                                "debug",
                                f"Setting method: {self.device_type}, {self.device_name}, {method}",
                            ),
                        },
                    )
                )

            for i in range(2):
                try:
                    if data == "not set":
                        data = setattr(self.device, method, value)

                        if self.debug:
                            self.queue.put(
                                (
                                    self.metadata,
                                    {
                                        "type": "log",
                                        "data": (
                                            "debug",
                                            f"Set method success: {self.device_type}, "
                                            f"{self.device_name}, {method} with data {str(data)}",
                                        ),
                                    },
                                )
                            )
                except Exception as e:
                    time.sleep(0)
                    self.queue.put(
                        (
                            self.metadata,
                            {
                                "type": "log",
                                "data": (
                                    "warning",
                                    f"Set method failed with data {str(data)}: "
                                    f"{self.device_type}, {self.device_name}, {method}, "
                                    f"{str(e)}, trying again...",
                                ),
                            },
                        )
                    )
                    time.sleep(1)
                    continue
                time.sleep(0)

            # final run. If error, caught by try/except
            if data == "not set":
                data = setattr(self.device, method, value)

                if self.debug:
                    self.queue.put(
                        (
                            self.metadata,
                            {
                                "type": "log",
                                "data": (
                                    "debug",
                                    f"Set method success: {self.device_type}, "
                                    f"{self.device_name}, {method}, with data {str(data)}",
                                ),
                            },
                        )
                    )

            time.sleep(0)
            self.back_pipe.send(data)  # check if valid, need args?
        except Exception as e:
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "error",
                            f"Set method error: {self.device_type}, {self.device_name}, "
                            f"{method}, {str(e)}",
                        ),
                    },
                )
            )
            self.back_pipe.send(e)  # check if valid, need args?

    def loop__(self, method: str, delay: float) -> None:
        """Continuous polling loop for a specific device method.

        Runs in a separate thread to continuously poll a device method
        at the specified interval, logging results to the database.

        Args:
            method (str): Name of the device method to poll.
            delay (float): Polling interval in seconds.
        """
        self._poll_list.append(method)
        self._poll_latest[method] = {}
        self._poll_latest[method]["value"] = None
        self._poll_latest[method]["datetime"] = None
        try:
            while method in self._poll_list:
                if not self._poll_pause:
                    get = self.get__(method, pipe=False)
                    if get["status"] == "success":
                        val = get["data"]
                    else:
                        time.sleep(1)
                        ## try again, just in case...
                        get = self.get__(method, pipe=False)
                        if get["status"] == "success":
                            val = get["data"]
                        else:
                            raise ValueError(get)
                    time.sleep(0)

                    dt = datetime.now(UTC)
                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

                    # Safely enqueue the polling result; if the queue is closed or
                    # otherwise unavailable, stop this poll thread gracefully instead
                    # of letting the exception crash the thread.
                    try:
                        self.queue.put(
                            (
                                self.metadata,
                                {
                                    "type": "query",
                                    "data": (
                                        f"INSERT INTO polling VALUES "
                                        f"('{self.device_type}', '{self.device_name}',  "
                                        f"'{method}', '{val}', '{dt_str}')"
                                    ),
                                },
                            )
                        )
                    except Exception as q_exc:
                        # Common failures include BrokenPipeError when the manager
                        # or parent process has exited. Record a minimal local
                        # failure state and stop polling this method.
                        dt = datetime.now(UTC)
                        self._poll_latest[method]["datetime"] = dt
                        self._poll_latest[method]["value"] = "null"
                        try:
                            print(
                                f"loop__: queue.put failed for {self.device_type} {self.device_name} {method}: {q_exc}"
                            )
                        except Exception:
                            # best effort to not raise from the exception handler
                            pass
                        # remove this method from the poll list to stop the loop
                        try:
                            if method in self._poll_list:
                                self._poll_list.remove(method)
                        except Exception:
                            pass
                        break

                    self._poll_latest[method]["value"] = val
                    self._poll_latest[method]["datetime"] = dt

                    time.sleep(delay)
                time.sleep(0)
        except Exception as e:
            dt = datetime.now(UTC)
            self._poll_latest[method]["datetime"] = dt
            self._poll_latest[method]["value"] = "null"

            # try to enqueue an error log; if the queue is gone, fallback to
            # printing and stop polling this method.
            try:
                self.queue.put(
                    (
                        self.metadata,
                        {
                            "type": "log",
                            "data": (
                                "error",
                                f"Loop error: {self.device_type}, {self.device_name}, "
                                f"{method}, {str(e)}",
                            ),
                        },
                    )
                )
            except Exception as q_exc:
                try:
                    print(
                        f"loop__: failed to queue loop error for {self.device_type} {self.device_name} {method}: {q_exc}"
                    )
                except Exception:
                    pass
            # ensure the poll is stopped
            try:
                if method in self._poll_list:
                    self._poll_list.remove(method)
            except Exception:
                pass

    def start_poll__(self, method: str, delay: float) -> None:
        """Start a new polling thread for the specified method.

        Args:
            method (str): Name of the device method to start polling.
            delay (float): Polling interval in seconds.
        """
        if method not in self._poll_list:
            Thread(target=self.loop__, args=(method, delay), daemon=True).start()
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "info",
                            f"{self.device_type}, {self.device_name}, {method} "
                            f"poll started with {delay} second cadence",
                        ),
                    },
                )
            )

    def stop_poll__(self, method: Optional[str] = None, *args) -> None:
        """Stop polling for a specific method or all methods.

        Args:
            method (Optional[str]): Method name to stop polling, or None for all.
            *args: Additional arguments (used for signal handlers).
        """
        if method is None:
            self._poll_list = []
            self._poll_latest = {}
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "info",
                            f"{self.device_type}, {self.device_name}, all polls stopped",
                        ),
                    },
                )
            )
        elif method in self._poll_list:
            self._poll_list = list(filter((method).__ne__, self._poll_list))
            del self._poll_latest[method]
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "info",
                            f"{self.device_type}, {self.device_name}, {method} poll stopped."
                            f"{self._poll_list} left in poll list, and {self._poll_latest} "
                            "left in poll dict",
                        ),
                    },
                )
            )
        else:
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "warning",
                            f"Stop poll error: {self.device_type}, {self.device_name}, "
                            f"{method} not in poll list.",
                        ),
                    },
                )
            )

    def poll_list__(self) -> None:
        """Send current polling list via pipe."""
        try:
            self.back_pipe.send(self._poll_list)
        except Exception as e:
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "error",
                            f"poll_list error: {self.device_type}, {self.device_name}, "
                            f"{str(e)}",
                        ),
                    },
                )
            )
            self.back_pipe.send(e)

    def poll_latest__(self) -> None:
        """Send latest polling results via pipe."""
        try:
            self.back_pipe.send(self._poll_latest)
        except Exception as e:
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "error",
                            f"poll_latest error: {self.device_type}, {self.device_name}, {str(e)}",
                        ),
                    },
                )
            )
            self.back_pipe.send(e)

    def stop__(self, *args) -> None:
        """Stop the device process and close communication pipes.

        Args:
            *args: Additional arguments (used for signal handlers).
        """
        self.active = False

        # close pipes
        self.front_pipe.close()
        self.back_pipe.close()

    def force_poll(self, method: str, **kwargs) -> None:
        """Immediately poll a device method once and write the result to the database via the queue."""
        try:
            val = self.get(method, **kwargs)
            dt = datetime.now(UTC)
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "query",
                        "data": (
                            f"INSERT INTO polling VALUES "
                            f"('{self.device_type}', '{self.device_name}',  "
                            f"'{method}', '{val}', '{dt_str}')"
                        ),
                    },
                )
            )
        except Exception as e:
            self.queue.put(
                (
                    self.metadata,
                    {
                        "type": "log",
                        "data": (
                            "error",
                            f"Force poll error: {self.device_type}, {self.device_name}, {method}, {str(e)}",
                        ),
                    },
                )
            )
