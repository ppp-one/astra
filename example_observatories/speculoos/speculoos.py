from typing import List

import astra.utils as utils
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
            if ShutterStatus == 0:  # open
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
