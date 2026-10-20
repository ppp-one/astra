import math
import threading
import time
from collections.abc import Callable
from typing import Any, List

import astra.utils.astelos_errors as astelos_errors
from astra.observatory import Observatory
from astra.paired_devices import PairedDevices


class SPECULOOS(Observatory):
    """Custom Observatory class for SPECULOOS observatories.

    Being ASTELCO made observatories, SPECULOOS telescopes are subject to certain
    quirks that require special handling. Specifically, they need custom error
    handling and some of its ASCOM methods not conforming asynchronous standards.

    By implementing this subclass, we can ensure that these observatories operate
    safely and effectively within the Astra framework.
    """

    OBSERVATORY_ALIASES: List[str] = ["ganymede", "europa", "io", "callisto"]

    # Dome shutter commands that the ttTCLM driver runs in a background thread.
    SHUTTER_COMMANDS = ("OpenShutter", "CloseShutter")
    # Minimum time between two shutter commands on one dome. One AsTelOS
    # power-up held its lock for 8.8 s on Europa (2026-10-18).
    SHUTTER_COMMAND_MIN_GAP = 15.0
    # How long TELESCOPE.READY_STATE must stay out of a power change before a
    # shutter command is sent. During a power-down, READY_STATE stays at 1.0 for
    # the whole lock: up to 5.75 s on Callisto (TSI-IO logs, Sept-Oct 2026).
    READY_STATE_SETTLE_TIME = 10.0
    READY_STATE_POLL_INTERVAL = 0.5
    # The driver gives up a power-up after 5 minutes.
    SHUTTER_GUARD_TIMEOUT = 300.0
    # How long a CloseShutter waits for an opening to end. AsTelOS locks the dome
    # target while the shutter moves, and refuses a close during an opening.
    # Longest power-up (78 s) plus shutter move (41 s) on Callisto, x1.5.
    OPENING_WAIT_TIMEOUT = 180.0
    # How often the watchdog checks that the driver still talks to AsTelOS.
    ASTELOS_LINK_CHECK_INTERVAL = 60.0

    def __init__(self, *args, **kwargs):
        # Set before Observatory.__init__, which can start threads.
        self._shutter_command_lock = threading.Lock()
        self._last_shutter_command: dict[str, float] = {}
        self._last_astelos_link_check = -math.inf
        super().__init__(*args, **kwargs)

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

        self.device_manager.pause_polls(["Dome", "Telescope", "Focuser"])

        # acknowledge errors if dome not closed, if any
        dome_names = self.device_manager.list_device_names("Dome", paired_devices)
        for dome_name in dome_names:
            dome = self.devices["Dome"][dome_name]
            ShutterStatus = dome.get("ShutterStatus")
            if ShutterStatus != 1:  # not closed
                self.speculoos_check_and_ack_error(close=True)

        all_telescopes_parked = super().close_observatory(
            paired_devices=paired_devices, error_sensitive=error_sensitive
        )

        self.device_manager.resume_polls(["Dome", "Telescope", "Focuser"])

        return all_telescopes_parked

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
        self.device_manager.pause_polls(["Dome", "Telescope", "Focuser"])
        self.speculoos_check_and_ack_error()

        if "Dome" in self.config:
            self._open_dome_shutters(paired_devices)

        self.speculoos_check_and_ack_error()

        if "Telescope" in self.config:
            self._unpark_telescopes(paired_devices)

        self.speculoos_check_and_ack_error()
        self.device_manager.resume_polls(["Dome", "Telescope", "Focuser"])
        self._wait_for_telescopes_ready()

    def _close_domes_on_error(self):
        for dome_config in self.config["Dome"]:
            if not dome_config.get("close_dome_on_telescope_error", False):
                continue

            self.speculoos_check_and_ack_error(close=True)

            device_name = dome_config["device_name"]
            self.logger.warning(f"Closing Dome {device_name} due to errors.")
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
                valid, all_errors, messages = astelos_errors.check_error(
                    telescope, close=close
                )

                if valid and len(all_errors) > 0:
                    self.logger.info(
                        f"Attempting to acknowledge AsTelOS errors for {telescope_name}: {messages}"
                    )
                    ack, messages = astelos_errors.ack_error(
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
                    if astelos_errors.is_status_reply(messages):
                        message = (
                            f"AsTelOS errors invalid for {telescope_name}: {messages}"
                        )
                    else:
                        message = self._astelos_link_lost_message(
                            telescope_name, messages
                        )
                    self.logger.report_device_issue(
                        device_type="Telescope",
                        device_name=telescope_name,
                        message=message,
                    )

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
        schedule_sensitive: bool = False,
    ) -> None:
        """Run a monitor-action, and guard dome shutter commands first.

        See Observatory.execute_and_monitor_device_task. For OpenShutter and
        CloseShutter, this first waits until no AsTelOS power change is in
        progress (see _wait_before_shutter_command).
        """
        is_shutter_command = (
            device_type == "Dome" and run_command in self.SHUTTER_COMMANDS
        )
        if is_shutter_command and not self._wait_before_shutter_command(
            device_name,
            run_command,
            desired_condition,
            error_sensitive=error_sensitive,
            weather_sensitive=weather_sensitive,
        ):
            return

        super().execute_and_monitor_device_task(
            device_type,
            monitor_command,
            desired_condition,
            run_command,
            device_name,
            run_command_type=run_command_type,
            abs_tol=abs_tol,
            log_message=log_message,
            timeout=timeout,
            error_sensitive=error_sensitive,
            weather_sensitive=weather_sensitive,
            schedule_sensitive=schedule_sensitive,
        )

    def _wait_before_shutter_command(
        self,
        dome_name: str,
        run_command: str,
        desired_condition: Any,
        error_sensitive: bool = True,
        weather_sensitive: bool = True,
    ) -> bool:
        """Wait until a dome shutter command is safe to send to the ttTCLM driver.

        The ttTCLM driver runs OpenShutter and CloseShutter in a background
        thread. The thread first sends TELESCOPE.READY=1.0, and AsTelOS locks
        TELESCOPE.READY until that power-up ends. If another READY command
        arrives during the lock, AsTelOS replies LOCKEDBY. The driver thread
        then raises an exception that nothing catches, and the driver closes
        its connection to AsTelOS for good. After that it keeps answering
        ASCOM calls with old values and gives no error.

        AsTelOS also locks AUXILIARY.DOME.TARGETPOS while the shutter moves. A
        CloseShutter during an opening is then refused, and the driver drops it
        without an error. AsTelOS does not handle repeated commands well, so
        the close is not repeated: it waits for the opening to end instead.

        So before a shutter command, this method waits until:
            1. For CloseShutter only: the shutter is no longer opening
               (ShutterStatus 2), for at most OPENING_WAIT_TIMEOUT,
            2. SHUTTER_COMMAND_MIN_GAP has passed since the last shutter
               command on this dome, and
            3. TELESCOPE.READY_STATE has been out of a power change
               (not between 0 and 1) for READY_STATE_SETTLE_TIME.

        Parameters:
            dome_name (str): Dome that gets the command.
            run_command (str): "OpenShutter" or "CloseShutter".
            desired_condition (Any): ShutterStatus the command must reach.
            error_sensitive (bool): Stop waiting if an error occurs.
            weather_sensitive (bool): Stop waiting if the weather is unsafe.

        Returns:
            bool: True if the command can be sent. False if it must not be
                sent: the wait was stopped, or an OpenShutter cannot be done
                safely.
        """
        opening = run_command == "OpenShutter"

        def stop_waiting() -> bool:
            return (weather_sensitive and not self.weather_safe) or (
                error_sensitive and not self.logger.error_free
            )

        # If the shutter is already in the desired state, the monitor-action
        # sends no command, so there is nothing to guard.
        try:
            if (
                self.devices["Dome"][dome_name].get("ShutterStatus")
                == desired_condition
            ):
                return True
        except Exception as e:
            # The monitor-action reads ShutterStatus again and reports errors.
            self.logger.debug(f"Could not read ShutterStatus of {dome_name}: {e}")

        with self._shutter_command_lock:
            # 1. A close waits for an opening to end
            if run_command == "CloseShutter" and not self._wait_while_opening(
                dome_name, stop_waiting
            ):
                return False

            deadline = time.monotonic() + self.SHUTTER_GUARD_TIMEOUT

            # 2. Minimum gap since the last shutter command on this dome
            last = self._last_shutter_command.get(dome_name)
            if last is not None:
                remaining = last + self.SHUTTER_COMMAND_MIN_GAP - time.monotonic()
                if remaining > 0:
                    self.logger.info(
                        f"Waiting {remaining:.1f} s before {run_command} on "
                        f"{dome_name}, because the last shutter command was "
                        f"{time.monotonic() - last:.1f} s ago"
                    )
                while time.monotonic() < last + self.SHUTTER_COMMAND_MIN_GAP:
                    if stop_waiting():
                        return False
                    time.sleep(self.READY_STATE_POLL_INTERVAL)

            # 3. No AsTelOS power change in progress
            result = self._wait_for_stable_ready_state(deadline, stop_waiting)
            if result is None:
                if opening:
                    self.logger.report_device_issue(
                        device_type="Dome",
                        device_name=dome_name,
                        message=f"Not sending {run_command} to {dome_name}: "
                        "TELESCOPE.READY_STATE could not be read",
                    )
                    return False
                self.logger.warning(
                    f"TELESCOPE.READY_STATE could not be read. Sending "
                    f"{run_command} to {dome_name} anyway."
                )
            elif result is False:
                if stop_waiting():
                    return False
                if opening:
                    self.logger.report_device_issue(
                        device_type="Dome",
                        device_name=dome_name,
                        message=f"Not sending {run_command} to {dome_name}: AsTelOS "
                        f"power change did not end within {self.SHUTTER_GUARD_TIMEOUT:.0f} s",
                    )
                    return False
                self.logger.warning(
                    f"AsTelOS power change did not end within "
                    f"{self.SHUTTER_GUARD_TIMEOUT:.0f} s. Sending {run_command} "
                    f"to {dome_name} anyway."
                )

            self._last_shutter_command[dome_name] = time.monotonic()
            return True

    def _wait_while_opening(
        self, dome_name: str, stop_waiting: Callable[[], bool]
    ) -> bool:
        """Wait while the dome shutter is opening, for at most OPENING_WAIT_TIMEOUT.

        After the time limit this returns True anyway, so that a close is never
        blocked for good, for example by a driver that reports "opening"
        forever.

        Parameters:
            dome_name (str): Dome to watch.
            stop_waiting (Callable[[], bool]): Returns True to stop waiting.

        Returns:
            bool: False if stop_waiting() stopped the wait, else True.
        """
        dome = self.devices["Dome"][dome_name]
        deadline = time.monotonic() + self.OPENING_WAIT_TIMEOUT
        logged = False
        while True:
            try:
                if dome.get("ShutterStatus") != 2:  # 2 = opening
                    return True
            except Exception as e:
                # Do not block the close; the monitor-action reports errors.
                self.logger.debug(f"Could not read ShutterStatus of {dome_name}: {e}")
                return True
            if stop_waiting():
                return False
            if time.monotonic() > deadline:
                self.logger.warning(
                    f"{dome_name} still opening after "
                    f"{self.OPENING_WAIT_TIMEOUT:.0f} s. Sending CloseShutter anyway."
                )
                return True
            if not logged:
                self.logger.info(
                    f"Waiting for {dome_name} to finish opening before CloseShutter"
                )
                logged = True
            time.sleep(self.READY_STATE_POLL_INTERVAL)

    def _wait_for_stable_ready_state(
        self, deadline: float, stop_waiting: Callable[[], bool]
    ) -> bool | None:
        """Wait until no telescope has TELESCOPE.READY_STATE between 0 and 1.

        READY_STATE is 0 when powered down and 1 when powered up. A value
        between 0 and 1 means a power change is in progress. A negative value
        is an error or local mode, not a power change.

        Parameters:
            deadline (float): time.monotonic() value at which to give up.
            stop_waiting (Callable[[], bool]): Returns True to stop waiting.

        Returns:
            bool | None: True when READY_STATE has been stable for
                READY_STATE_SETTLE_TIME. False at the deadline or when
                stop_waiting() is True. None if READY_STATE could not be read.
        """
        stable_since = None
        logged = False
        while True:
            if stop_waiting():
                return False

            states = self._astelos_ready_states()
            if states is None:
                return None

            now = time.monotonic()
            if all(state <= 0 or state >= 1 for state in states.values()):
                if stable_since is None:
                    stable_since = now
                if now - stable_since >= self.READY_STATE_SETTLE_TIME:
                    return True
            else:
                stable_since = None
                if not logged:
                    self.logger.info(
                        f"Waiting for AsTelOS power change to end before a dome "
                        f"shutter command (READY_STATE: {states})"
                    )
                    logged = True

            if now > deadline:
                return False
            time.sleep(self.READY_STATE_POLL_INTERVAL)

    def _astelos_ready_states(self) -> dict[str, float] | None:
        """Read TELESCOPE.READY_STATE of each telescope.

        Returns:
            dict[str, float] | None: READY_STATE per telescope name, or None if
                any value could not be read as a number.
        """
        states = {}
        for telescope_name, telescope in self.devices.get("Telescope", {}).items():
            try:
                reply = telescope.get(
                    "CommandString", Command="TELESCOPE.READY_STATE", Raw=True
                )
                states[telescope_name] = float(reply)
            except Exception:
                return None
        return states

    def _watchdog_step(self):
        """Run the watchdog step, then check the AsTelOS link now and then."""
        super()._watchdog_step()

        if not self.watchdog_running or not self.logger.error_free:
            return

        now = time.monotonic()
        if now - self._last_astelos_link_check < self.ASTELOS_LINK_CHECK_INTERVAL:
            return
        self._last_astelos_link_check = now
        self._check_astelos_link()

    def _check_astelos_link(self) -> None:
        """Report a telescope error if the driver no longer talks to AsTelOS.

        The ttTCLM driver can lose its connection to AsTelOS without an ASCOM
        error. It then returns old values for all properties, and
        CommandString returns "Client not Connected". Only CommandString shows
        the problem, so it is read here.
        """
        for telescope_name, telescope in self.devices.get("Telescope", {}).items():
            try:
                reply = telescope.get(
                    "CommandString", Command="TELESCOPE.STATUS.LIST", Raw=True
                )
            except Exception as e:
                self.logger.report_device_issue(
                    device_type="Telescope",
                    device_name=telescope_name,
                    message=f"Could not read AsTelOS status of {telescope_name}",
                    exception=e,
                )
                continue

            if not astelos_errors.is_status_reply(reply):
                self.logger.report_device_issue(
                    device_type="Telescope",
                    device_name=telescope_name,
                    message=self._astelos_link_lost_message(telescope_name, reply),
                )

    @staticmethod
    def _astelos_link_lost_message(telescope_name: str, reply: Any) -> str:
        return (
            f"ttTCLM driver has no working connection to AsTelOS for "
            f"{telescope_name}: TELESCOPE.STATUS.LIST returned {reply!r}. Values "
            "from this driver are old. Restart the driver: stop the "
            "ttTCLM_ASCOM*.exe processes, then restart ASCOM Remote."
        )
