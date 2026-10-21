import math
import threading
import time
from contextlib import contextmanager
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
    # ASCOM ShutterStatus values while the shutter moves.
    SHUTTER_OPENING = 2
    SHUTTER_CLOSING = 3
    # Longest wait for a running shutter command to end: longest power-up
    # (78 s) plus shutter move (41 s) on Callisto (TSI-IO logs, Sept-Oct 2026),
    # x1.5. After this, a close is sent anyway and an open is refused.
    SHUTTER_MOVE_TIMEOUT = 180.0
    SHUTTER_POLL_INTERVAL = 0.5
    # Pause after a move ends, so that AsTelOS has released its lock before
    # the next shutter command arrives.
    SHUTTER_SETTLE_AFTER_MOVE = 2.0
    # How often the watchdog checks that the driver still talks to AsTelOS, and
    # how long to wait before reading a bad status reply a second time.
    ASTELOS_LINK_CHECK_INTERVAL = 60.0
    ASTELOS_LINK_RECHECK_DELAY = 5.0

    def __init__(self, *args, **kwargs):
        # Set before Observatory.__init__, which can start threads.
        self._shutter_command_lock = threading.Lock()
        self._last_astelos_link_check = -math.inf
        self._link_check_thread: threading.Thread | None = None
        # Open/close sequences in progress; the link check waits for them.
        self._sequence_lock = threading.Lock()
        self._sequences_running = 0
        super().__init__(*args, **kwargs)

    @contextmanager
    def _sequence(self):
        """Pause Dome/Telescope/Focuser polls for an open or close sequence.

        Polls are resumed even if the sequence raises an exception, and the
        AsTelOS link check is skipped while a sequence runs.
        """
        with self._sequence_lock:
            self._sequences_running += 1
        self.device_manager.pause_polls(["Dome", "Telescope", "Focuser"])
        try:
            yield
        finally:
            self.device_manager.resume_polls(["Dome", "Telescope", "Focuser"])
            with self._sequence_lock:
                self._sequences_running -= 1

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

        with self._sequence():
            # acknowledge errors if dome not closed, if any
            dome_names = self.device_manager.list_device_names("Dome", paired_devices)
            for dome_name in dome_names:
                dome = self.devices["Dome"][dome_name]
                ShutterStatus = dome.get("ShutterStatus")
                if ShutterStatus != 1:  # not closed
                    self.speculoos_check_and_ack_error(close=True)

            return super().close_observatory(
                paired_devices=paired_devices, error_sensitive=error_sensitive
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
        with self._sequence():
            self.speculoos_check_and_ack_error()

            if "Dome" in self.config:
                self._open_dome_shutters(paired_devices)

            self.speculoos_check_and_ack_error()

            if "Telescope" in self.config:
                self._unpark_telescopes(paired_devices)

            self.speculoos_check_and_ack_error()

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
                        self.logger.report_device_issue(
                            device_type="Telescope",
                            device_name=telescope_name,
                            message=f"AsTelOS errors invalid for {telescope_name}: {messages}",
                        )
                    elif close:
                        # Do not stop a close: an error here would make the
                        # error-sensitive park and close steps return early.
                        # The watchdog link check reports a lost connection.
                        self.logger.warning(
                            self._astelos_link_lost_message(telescope_name, messages)
                        )
                    else:
                        self.logger.report_device_issue(
                            device_type="Telescope",
                            device_name=telescope_name,
                            message=self._astelos_link_lost_message(
                                telescope_name, messages
                            ),
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
        """Run a monitor-action. Run shutter commands one at a time.

        See Observatory.execute_and_monitor_device_task. For OpenShutter and
        CloseShutter, this first waits until the previous shutter command has
        ended (see _wait_before_shutter_command). The shutter lock is held
        until the monitor-action ends, so no other shutter command can be sent
        in between.
        """

        def run() -> None:
            super(SPECULOOS, self).execute_and_monitor_device_task(
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

        if device_type != "Dome" or run_command not in self.SHUTTER_COMMANDS:
            run()
            return

        with self._shutter_command_lock:
            if self._wait_before_shutter_command(
                device_name,
                run_command,
                error_sensitive=error_sensitive,
                weather_sensitive=weather_sensitive,
            ):
                run()

    def _wait_before_shutter_command(
        self,
        dome_name: str,
        run_command: str,
        error_sensitive: bool = True,
        weather_sensitive: bool = True,
    ) -> bool:
        """Wait until the previous shutter command has ended.

        The ttTCLM driver runs OpenShutter and CloseShutter in a background
        thread: it powers the telescope up if needed, then moves the shutter.
        Meanwhile AsTelOS refuses another shutter command (LOCKEDBY). During
        the power-up this makes the driver drop its AsTelOS connection for
        good; during the move the new command is lost. The driver reports this
        whole time as ShutterStatus opening (2) or closing (3), from the moment
        it accepts the command. So a new shutter command waits while the
        shutter is opening or closing, then pauses SHUTTER_SETTLE_AFTER_MOVE.
        Commands are never repeated, because AsTelOS does not handle that well.

        If the move ends in the state that run_command would reach, the
        monitor-action sees this and sends no command.

        Call with self._shutter_command_lock held.

        Parameters:
            dome_name (str): Dome that gets the command.
            run_command (str): "OpenShutter" or "CloseShutter".
            error_sensitive (bool): Stop waiting if an error occurs.
            weather_sensitive (bool): Stop waiting if the weather is unsafe.

        Returns:
            bool: True if the command can be sent. False if it must not be
                sent: the wait was stopped, or an open waited longer than
                SHUTTER_MOVE_TIMEOUT. After that time a close is sent anyway,
                so that a close is never blocked for long.
        """
        dome = self.devices["Dome"][dome_name]
        deadline = time.monotonic() + self.SHUTTER_MOVE_TIMEOUT
        waited = False

        while True:
            try:
                status = dome.get("ShutterStatus")
            except Exception as e:
                # The monitor-action reads ShutterStatus again and reports errors.
                self.logger.debug(f"Could not read ShutterStatus of {dome_name}: {e}")
                return True
            if status not in (self.SHUTTER_OPENING, self.SHUTTER_CLOSING):
                break

            if (weather_sensitive and not self.weather_safe) or (
                error_sensitive and not self.logger.error_free
            ):
                return False
            if time.monotonic() > deadline:
                if run_command == "OpenShutter":
                    self.logger.report_device_issue(
                        device_type="Dome",
                        device_name=dome_name,
                        message=f"Not sending {run_command} to {dome_name}: the "
                        f"shutter still moves after {self.SHUTTER_MOVE_TIMEOUT:.0f} s",
                    )
                    return False
                self.logger.warning(
                    f"{dome_name} still moves after {self.SHUTTER_MOVE_TIMEOUT:.0f} s. "
                    f"Sending {run_command} anyway."
                )
                return True
            if not waited:
                moving = "opening" if status == self.SHUTTER_OPENING else "closing"
                self.logger.info(
                    f"Waiting for {dome_name} to finish {moving} before {run_command}"
                )
                waited = True
            time.sleep(self.SHUTTER_POLL_INTERVAL)

        if waited:
            time.sleep(self.SHUTTER_SETTLE_AFTER_MOVE)
        return True

    def _watchdog_step(self):
        """Run the watchdog step, then start an AsTelOS link check now and then.

        The check runs in its own thread, because a call to a hung driver can
        block for minutes, and the watchdog must keep checking the weather.
        It is skipped while an open or close sequence or a shutter command is
        in progress, because SPECULOOS keeps other driver traffic low then.
        """
        super()._watchdog_step()

        if not self.watchdog_running or not self.logger.error_free:
            return
        now = time.monotonic()
        if now - self._last_astelos_link_check < self.ASTELOS_LINK_CHECK_INTERVAL:
            return
        if self._sequences_running or self._shutter_command_lock.locked():
            return
        if self._link_check_thread is not None and self._link_check_thread.is_alive():
            return

        self._last_astelos_link_check = now
        self._link_check_thread = threading.Thread(
            target=self._check_astelos_link, name="astelos_link_check", daemon=True
        )
        self._link_check_thread.start()

    def _check_astelos_link(self) -> None:
        """Report a telescope error if the driver no longer talks to AsTelOS.

        The ttTCLM driver can lose its connection to AsTelOS without an ASCOM
        error. It then returns old values for all properties, and
        CommandString returns "Client not Connected". Only CommandString shows
        the problem, so it is read here. A bad reply is read a second time
        before it is reported, so that one odd reply does not stop the night.
        """
        for telescope_name, telescope in self.devices.get("Telescope", {}).items():
            reply = self._read_astelos_status(telescope_name, telescope)
            if reply is None or astelos_errors.is_status_reply(reply):
                continue

            time.sleep(self.ASTELOS_LINK_RECHECK_DELAY)
            reply = self._read_astelos_status(telescope_name, telescope)
            if reply is None or astelos_errors.is_status_reply(reply):
                continue

            self.logger.report_device_issue(
                device_type="Telescope",
                device_name=telescope_name,
                message=self._astelos_link_lost_message(telescope_name, reply),
            )

    def _read_astelos_status(self, telescope_name: str, telescope: Any) -> Any:
        """Read TELESCOPE.STATUS.LIST. Return None if the call fails.

        A failed call is only logged as a warning here: the device process
        already reports an error for a call that fails after its retries.
        """
        try:
            return telescope.get(
                "CommandString", Command="TELESCOPE.STATUS.LIST", Raw=True
            )
        except Exception as e:
            self.logger.warning(
                f"Could not read AsTelOS status of {telescope_name}: {e}"
            )
            return None

    @staticmethod
    def _astelos_link_lost_message(telescope_name: str, reply: Any) -> str:
        return (
            f"ttTCLM driver has no working connection to AsTelOS for "
            f"{telescope_name}: TELESCOPE.STATUS.LIST returned {reply!r}. Values "
            "from this driver are old, and it does not recover by itself. Do not "
            "use 'reconnect devices' first: that can hang the driver. Close ASCOM "
            "Remote (no ttTCLM_ASCOM*.exe process may remain), start it again, "
            "then restart astra."
        )
