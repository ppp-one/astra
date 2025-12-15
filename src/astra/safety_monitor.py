""" "Safety monitoring for observatory weather conditions.

Key capabilities:
    - Monitor external safety monitor device status
    - Evaluate internal weather conditions against safety limits
    - Determine overall weather safety status for observatory operations
    - Provide time estimates until conditions become safe again
"""

from datetime import UTC, datetime

import pandas as pd

from astra.config import ObservatoryConfig
from astra.database_manager import DatabaseManager
from astra.device_manager import DeviceManager
from astra.logger import ObservatoryLogger


class SafetyMonitor:
    def __init__(
        self,
        observatory_config: ObservatoryConfig,
        database_manager: DatabaseManager,
        logger: ObservatoryLogger,
        device_manager: DeviceManager,
    ):
        self.config = observatory_config
        self.database_manager = database_manager
        self.logger = logger
        self.device_manager = device_manager

        self._weather_safe: bool | None = None
        self._time_to_safe: float = 0
        self._weather_log_warning: bool = False

        # last known status
        self.last_external_status = None
        self.last_internal_status = None
        self.last_update = None

        # Load configuration
        if "SafetyMonitor" in observatory_config:
            cfg = observatory_config["SafetyMonitor"][0]
            self.max_safe_duration = cfg.get("max_safe_duration", 30)
            if "max_safe_duration" not in cfg:
                self.logger.warning(
                    f"No max_safe_duration in user config, defaulting to {self.max_safe_duration} minutes."
                )

            self.device_type = "SafetyMonitor"
            self.device_name = cfg["device_name"]
        else:
            self.max_safe_duration = 0
            self.device_type = None
            self.device_name = None
            self.logger.warning("No safety monitor found")

    @property
    def device(self):
        if self.device_type and self.device_name:
            return self.device_manager.devices[self.device_type][self.device_name]
        return None

    @property
    def weather_safe(self) -> bool | None:
        """Latest evaluated weather safety status."""
        return self._weather_safe

    @property
    def time_to_safe(self) -> float:
        """Minutes until conditions become safe again."""
        return self._time_to_safe

    def check_safety_monitor(self, max_safe_duration: int):
        """
        Polls the external SafetyMonitor and database history
        to determine if weather is currently safe.
        """
        sm_poll = self.device.poll_latest()

        # Handle case where poll data is unavailable (e.g., during blocking operations)
        if sm_poll is None or "IsSafe" not in sm_poll:
            self.logger.warning("Safety monitor poll data unavailable")
            return False, 0  # treat as unsafe when data unavailable

        # staleness check
        last_update = (
            datetime.now(UTC) - sm_poll["IsSafe"]["datetime"]
        ).total_seconds()
        if 3 < last_update < 30:
            self.logger.warning(f"Safety monitor {last_update}s stale")
        elif last_update > 30:
            self.logger.report_device_issue(
                device_type="SafetyMonitor",
                device_name=self.device_name,
                message=f"Stale data {last_update}s",
            )
            return False, 0  # treat as unsafe

        if sm_poll["IsSafe"]["value"] is False:
            self._weather_safe = False
            if not self._weather_log_warning:
                self.logger.warning("Weather unsafe from SafetyMonitor")

        # query unsafe history
        weather_unsafe_stats = self.database_manager.execute_select(
            f"SELECT COUNT(*), MAX(datetime) FROM polling WHERE "
            f"device_type = 'SafetyMonitor' AND device_value = 'False' "
            f"AND datetime > datetime('now', '-{max_safe_duration} minutes')"
        )
        return weather_unsafe_stats

    def check_internal_conditions(self) -> tuple[bool, float, float]:
        """
        Monitor internal safety systems and weather conditions.

        Evaluates weather conditions against configured safety limits and determines
        if operations can continue safely. Checks observing conditions parameters
        against their defined closing limits and calculates time to safe operation.

        Returns:
            tuple: A tuple containing:
                - bool: True if weather conditions are safe for operation
                - float: Time in seconds until conditions become safe (0 if already safe)
                - float: Maximum safe duration in seconds for current conditions

        The method examines each parameter in the closing_limits configuration:
            - Compares current values against upper and lower thresholds
            - Calculates time until conditions improve if currently unsafe
            - Determines maximum safe operating duration under current conditions

        Note:
            - Returns (True, 0, 0) if no ObservingConditions devices are configured
            - Used by the watchdog to make decisions about observatory operations
            - Critical for autonomous safety management
        """

        longest_time_to_safe = 0
        longest_max_safe_duration = 0

        if "ObservingConditions" not in self.config:
            return True, 0, 0

        if "closing_limits" not in self.config["ObservingConditions"][0]:
            return True, 0, 0

        closing_limits = self.config["ObservingConditions"][0]["closing_limits"]

        # find largest max_safe_duration
        max_safe_duration = max(
            limit.get("max_safe_duration", 0)
            for limits in closing_limits.values()
            for limit in limits
        )

        query = f"""SELECT * FROM polling WHERE device_type = 'ObservingConditions' 
            AND datetime > datetime('now', '-{max_safe_duration * 1.1} minutes')"""
        df = self.database_manager.execute_select_to_df(query, table="polling")

        if df.shape[0] == 0:
            self.logger.warning("No data found for internal safety weather monitor")
            return True, 0, 0

        # Pivot: datetime as index, device_command as columns
        df = df.pivot(index="datetime", columns="device_command", values="device_value")

        # Ensure datetime index and numeric values
        df.index = pd.to_datetime(df.index, utc=True)
        df = df.sort_index()
        df = df.apply(pd.to_numeric, errors="coerce")

        # interpolate
        df = df.interpolate(method="time")

        if "SkyTemperature" in df.columns and "Temperature" in df.columns:
            df["RelativeSkyTemp"] = df["SkyTemperature"] - df["Temperature"]

        # Check each parameter and its limits
        for parameter, limits in closing_limits.items():
            if parameter not in df.columns:
                self.logger.warning(f"Parameter '{parameter}' not found in DataFrame")
                continue

            for limit in limits:
                max_safe_duration = limit.get("max_safe_duration", 0)
                lower_limit = limit.get("lower")
                upper_limit = limit.get("upper")

                if lower_limit is not None and upper_limit is not None:
                    condition = (df[parameter] < lower_limit) | (
                        df[parameter] > upper_limit
                    )
                elif lower_limit is not None:
                    condition = df[parameter] < lower_limit
                elif upper_limit is not None:
                    condition = df[parameter] > upper_limit
                else:
                    continue  # no limits defined

                # Apply the condition to the DataFrame
                _df = df[
                    condition
                    & (
                        df.index
                        > (
                            pd.Timestamp.now(tz="UTC")
                            - pd.Timedelta(minutes=max_safe_duration)
                        )
                    )
                ]

                count = _df.shape[0]

                if count > 0:
                    max_datetime = _df.index.max()

                    time_since_last_unsafe = pd.to_datetime(
                        datetime.now(UTC)
                    ) - pd.to_datetime(max_datetime, utc=True)

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

    def update_status(self) -> bool | None:
        """Run external + internal checks and update weather_safe + time_to_safe."""
        if not self.device:
            self.logger.warning("No safety monitor device found")
            return None

        # External safety history
        weather_unsafe_stats = self.check_safety_monitor(self.max_safe_duration)

        # Internal safety checks
        (
            internal_safety,
            internal_time_to_safe,
            internal_max_safe_duration,
        ) = self.check_internal_conditions()

        # if internal safety monitor is False, act on it
        if internal_safety is False:
            self._weather_safe = False

            # log message saying weather unsafe
            if self._weather_log_warning is False:
                self.logger.warning("Weather unsafe from internal safety monitor")

        # Decide time_to_safe
        if weather_unsafe_stats[0][0] > 0 or internal_time_to_safe > 0:
            if weather_unsafe_stats[0][1] is not None:
                time_since_last_unsafe = pd.to_datetime(
                    datetime.now(UTC)
                ) - pd.to_datetime(weather_unsafe_stats[0][1], utc=True)
            else:
                time_since_last_unsafe = pd.to_timedelta(0)

            current_time_to_safe = (
                self.max_safe_duration - time_since_last_unsafe.total_seconds() / 60
            )

            if weather_unsafe_stats[0][0] == 0:
                self._time_to_safe = internal_time_to_safe
            else:
                self._time_to_safe = max(current_time_to_safe, internal_time_to_safe)
        else:
            self._time_to_safe = 0

        self.logger.debug(
            f"SafetyMonitor: {weather_unsafe_stats} instances of weather unsafe "
            f"found in last {max(self.max_safe_duration, internal_max_safe_duration)} minutes"
        )

        # Decide weather_safe flag
        if (weather_unsafe_stats[0][0] == 0) and internal_safety:
            self._weather_safe = True
            if self._weather_log_warning:
                self.logger.info(
                    f"Weather safe for the last "
                    f"{max(self.max_safe_duration, internal_max_safe_duration)} minutes"
                )
                self._weather_log_warning = False
        elif weather_unsafe_stats[0][0] > 0 and internal_safety:
            self._weather_safe = False
            if not self._weather_log_warning:
                self.logger.warning(
                    "Weather unsafe from SafetyMonitor IsSafe history, "
                    "internal safety monitor is True. Are the internal "
                    "safety monitor limits higher than SafetyMonitor values?"
                )
                self._weather_log_warning = True
        else:
            self._weather_log_warning = True

        return self._weather_safe
