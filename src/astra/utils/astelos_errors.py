"""AsTelOS telescope error checking and acknowledgement."""

import time
from typing import Any, Tuple

import numpy as np
import pandas as pd


def check_error(telescope: Any, close: bool = False) -> Tuple[bool, pd.DataFrame, str]:
    """Check AsTelOS telescope status for known acceptable errors.

    Analyzes telescope status messages to identify errors and determines if they
    are in the list of known acceptable errors that can be safely acknowledged.

    Args:
        telescope (Any): Telescope object with get() method for status commands.
        close (bool): Whether to include slit closure errors in acceptable list.

    Returns:
        Tuple[bool, pd.DataFrame, str]: Error analysis as (valid, errors, messages):
            - valid: True if all errors are acceptable, False otherwise
            - errors: DataFrame with columns ['error', 'detail', 'level', 'component']
            - messages: Raw telescope status message string
    """

    allowed_err = [
        [
            "ERR_DeviceError",
            "axis (0) unexpectedly changed to powered on state",
            "2",
            "DOME[0]",
        ],
        [
            "ERR_DeviceError",
            "axis (0) unexpectedly changed to powered on state",
            "2",
            "DOME[1]",
        ],
        [
            "ERR_DeviceError",
            "axis (1) unexpectedly changed to powered on state",
            "2",
            "DOME[0]",
        ],
        [
            "ERR_DeviceError",
            "axis (1) unexpectedly changed to powered on state",
            "2",
            "DOME[1]",
        ],
        [
            "ERR_DeviceError",
            "axis #0\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "HA",
        ],
        [
            "ERR_DeviceError",
            "axis #0\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "DEC",
        ],
        [
            "ERR_DeviceError",
            "axis #1\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "HA",
        ],
        [
            "ERR_DeviceError",
            "axis #1\\| amplifier fault #07H\\| safe torque-off circuit fault",
            "2",
            "DEC",
        ],
        ["ERR_RunDevError", "Working pressure suddenly lost", "2", "HA"],
        ["ERR_RunDevError", "Working pressure suddenly lost", "2", "DEC"],
        ["ERR_DeviceWarn", "Malformed telegram from GPS", "4", "LOCAL"],
        ["ERR_DeviceError", "axis (1)\\| BOTH LIMITS (code=128)", "2", "DOME[0]"],
        ["ERR_DeviceError", "axis (1)\\| BOTH LIMITS (code=128)", "2", "DOME[1]"],
        ["ERR_DeviceError", "axis (1)\\| EXTERN (code=32)", "2", "DOME[0]"],
        ["ERR_DeviceError", "axis (1)\\| EXTERN (code=32)", "2", "DOME[1]"],
    ]
    if close:
        slit_error = []
        allowed_err.extend(slit_error)

    df_allowed = pd.DataFrame(
        allowed_err, columns=["error", "detail", "level", "component"]
    )
    df_list = pd.DataFrame(columns=["error", "detail", "level", "component"])

    messages = telescope.get("CommandString", Command="TELESCOPE.STATUS.LIST", Raw=True)
    # structure = "<group>|<level>[:<component>|<level>[;<component>...]][:<error>|<detail>|<level>|<component>[;<error>...]][,<group>...]"

    for message in messages.split(","):
        parts = message.split(":")

        # only look parts after "<group>|<level>"
        for part in parts[1:]:
            elements = part.split(";")

            for element in elements:
                error_detail = element.replace("\\|", "[ESCAPED_PIPE]").split("|")
                error_detail = [
                    item.replace("[ESCAPED_PIPE]", "\\|") for item in error_detail
                ]

                if len(error_detail) == 4:
                    if not error_detail[1].isdigit():
                        error = error_detail[0]
                        detail = error_detail[1]
                        error_level = error_detail[2]
                        component = error_detail[3]

                        df_list = pd.concat(
                            [
                                df_list,
                                pd.DataFrame(
                                    {
                                        "error": [error],
                                        "detail": [detail],
                                        "level": [error_level],
                                        "component": [component],
                                    }
                                ),
                            ],
                            ignore_index=True,
                        )

    # check all rows of df_list are in df_allowed
    compare_df = pd.merge(df_list, df_allowed, how="left", indicator="exists")
    exists = compare_df["exists"] == "both"

    # if all of exists is True
    if exists.all():
        return True, df_list, messages
    else:
        return False, df_list, messages


def ack_error(
    telescope: Any,
    valid: bool,
    all_errors: pd.DataFrame,
    messages: str,
    close: bool = False,
) -> Tuple[bool, str]:
    """Acknowledge acceptable AsTelOS telescope errors.

    Attempts to clear acceptable telescope errors by sending appropriate
    acknowledgement commands. Continues until all errors are cleared or
    unacceptable errors are encountered.

    Args:
        telescope (Any): Telescope object with get() method for commands.
        valid (bool): Whether errors are acceptable (from check_error).
        all_errors (pd.DataFrame): Error information with 'level' column.
        messages (str): Original telescope status messages.
        close (bool): Whether to include slit closure errors as acceptable.

    Returns:
        Tuple[bool, str]: Acknowledgement result as (success, final_messages):
            - success: True if all errors cleared, False if unacceptable errors remain
            - final_messages: Updated telescope status messages

    Raises:
        TimeoutError: If error clearing takes longer than 2 minutes.
    """

    start_time = time.time()

    while valid and len(all_errors) > 0:
        # derive system eror level
        sys_level = int(np.sum(np.unique(np.array(all_errors.level.astype(int)))))

        # clear errors
        telescope.get(
            "CommandBlind",
            Command=f"TELESCOPE.STATUS.CLEAR_ERROR={sys_level}",
            Raw=True,
        )
        time.sleep(2)

        # check telescope status
        valid, all_errors, messages = check_error(telescope, close=close)

        if time.time() - start_time > 120:  # 2 minutes hardcoded limit
            raise TimeoutError("Astelos error acknowledgement timed out")

    if not valid:
        return False, messages

    return True, messages
