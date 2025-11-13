import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Type, Union

import pandas as pd

from astra.action_configs import (
    ACTION_CONFIGS,
    BaseActionConfig,
)
from astra.logger import ObservatoryLogger


class ActionStatus(Enum):
    """Status of a scheduled action."""

    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


@dataclass
class Action:
    """A scheduled action for a device.

    Examples
    --------
    >>> from astra.scheduler import Action
    >>> from astra.action_configs import ObjectActionConfig
    >>> from datetime import datetime, UTC
    >>> action = Action(
    ...     device_name="test_camera",
    ...     action_type="object",
    ...     action_value=ObjectActionConfig(
    ...         object="M42",
    ...         exptime=10.0,
    ...     ),
    ...     start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    ...     end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC),
    ... )
    >>> action.validate()
    """

    device_name: str
    action_type: str
    action_value: BaseActionConfig
    start_time: datetime
    end_time: datetime
    completed: bool = False
    status: ActionStatus = ActionStatus.PENDING

    def __post__init__(self):
        # TODO enforce start_time, end_time are datetime / UTC?
        pass

    def get(self, key: str, default=None):
        return getattr(self, key, default)

    @property
    def duration(self):
        return self.end_time - self.start_time

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def validate(self):
        # Validate action_value
        try:
            self.action_value.validate()
        except Exception as e:
            raise ValueError(
                f"ActionConfig validation failed for {self.action_type} "
                f"on {self.device_name}: {e}"
            )
        # Validate time order
        if not isinstance(self.start_time, datetime) or not isinstance(
            self.end_time, datetime
        ):
            raise TypeError(
                f"Schedule error in {self.action_type} for device {self.device_name}: "
                f"start_time and end_time must be datetime objects."
            )
        if self.start_time >= self.end_time:
            raise ValueError(
                f"Schedule error in {self.action_type} for device {self.device_name}: "
                f"start_time {self.start_time} must be before end_time {self.end_time}."
            )

    def update_times(
        self, time_factor: float, new_start_time: datetime | None = None
    ) -> "Action":
        """Update the start and end times to present day factored by the time factor."""
        if new_start_time is None:
            new_start_time = datetime.now(UTC)
        new_end_time = new_start_time + self.duration / time_factor

        return Action(
            device_name=self.device_name,
            action_type=self.action_type,
            action_value=self.action_value,
            start_time=new_start_time,
            end_time=new_end_time,
            completed=self.completed,
        )

    def summary_string(self, verbose=False) -> str:
        if verbose:
            return (
                f"Running {self.action_type} sequence for {self.device_name}, "
                f"starting {self.start_time} and ending {self.end_time}"
            )
        return f"{self.device_name} {self.action_type} {self.action_value}"

    def set_status(self, status: ActionStatus | str):
        if isinstance(status, str):
            status = ActionStatus[status.upper()]
        self.status = status

    def to_dict(self, iso: bool = False) -> dict:
        """Return a JSON-serializable dict representation of this Action.

        iso: if True, format datetimes as ISO strings (for JSONL); otherwise
             keep datetime objects (for DataFrame).
        """
        av = getattr(self, "action_value", "")
        av = av if not hasattr(av, "to_jsonable") else av.to_jsonable()

        start = getattr(self, "start_time", None)
        end = getattr(self, "end_time", None)
        if iso:
            start = start.isoformat() if hasattr(start, "isoformat") else start
            end = end.isoformat() if hasattr(end, "isoformat") else end

        return {
            "device_name": getattr(self, "device_name", ""),
            "action_type": getattr(self, "action_type", ""),
            "action_value": av,
            "start_time": start,
            "end_time": end,
            "completed": getattr(self, "completed", False),
            "status": getattr(self, "status", ActionStatus.PENDING).value,
        }

    def __str__(self) -> str:
        # Every property on a new line, slightly indented
        return (
            "Action(\n"
            f"  device_name={self.device_name!r},\n"
            f"  action_type={self.action_type!r},\n"
            f"  action_value={self.action_value!r},\n"
            f"  start_time={self.start_time!r},\n"
            f"  end_time={self.end_time!r},\n"
            f"  completed={self.completed!r},\n"
            f"  status={self.status!r}\n"
            ")"
        )


class Schedule(list[Action]):
    """A list of scheduled actions.

    Examples
    --------
    >>> from astra.scheduler import Schedule, Action
    >>> from astra.action_configs import ObjectActionConfig, OpenActionConfig
    >>> from datetime import datetime, UTC
    >>> actions = [
    ...     Action(
    ...         device_name="test_camera",
    ...         action_type="open",
    ...         action_value=OpenActionConfig(),
    ...         start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    ...         end_time=datetime(2024, 1, 1, 12, 30, 0, tzinfo=UTC),
    ...     ),
    ...     Action(
    ...         device_name="test_camera",
    ...         action_type="object",
    ...         action_value=ObjectActionConfig(
    ...             object="M42",
    ...             exptime=10.0,
    ...         ),
    ...         start_time=datetime(2024, 1, 1, 12, 30, 0, tzinfo=UTC),
    ...         end_time=datetime(2024, 1, 1, 12, 35, 0, tzinfo=UTC),
    ...     ),
    ... ]
    >>> schedule = Schedule(actions)
    >>> schedule.validate()
    """

    ACTION_CONFIGS: Dict[str, Type[BaseActionConfig]] = ACTION_CONFIGS

    def __init__(self, actions: List[Action]):
        super().__init__(actions)
        self.sort_by_start_time()

    @classmethod
    def from_file(
        cls,
        filename: Union[str, Path],
    ) -> "Schedule":
        """
        Read a schedule file and return a Schedule instance with parsed schedule data.
        """
        schedule_path = Path(filename)
        if schedule_path.exists() is False:
            raise FileNotFoundError(f"File not found: {filename}")
        elif schedule_path.suffix == ".jsonl":
            data = []
            with open(schedule_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("//"):
                        continue
                    obj = json.loads(line)
                    data.append(obj)
            schedule = pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported file format: {schedule_path.suffix}")
        schedule["start_time"] = pd.to_datetime(
            schedule.start_time, utc=True, format="mixed"
        )
        schedule["end_time"] = pd.to_datetime(
            schedule.end_time, utc=True, format="mixed"
        )
        schedule = schedule.sort_values(by=["start_time"])
        return cls.from_dataframe(schedule)

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
    ) -> "Schedule":
        """
        Construct a Schedule instance from a pandas DataFrame.
        """
        actions = []
        for _, action in df.iterrows():
            action_type = action["action_type"]
            config_cls = cls.ACTION_CONFIGS.get(action_type)
            if config_cls is None:
                raise ValueError(f"Unknown action_type: {action_type}")
            action_value = {} if not action["action_value"] else action["action_value"]

            actions.append(
                Action(
                    device_name=action["device_name"],
                    action_type=action_type,
                    action_value=config_cls.from_dict(action_value),
                    start_time=action["start_time"],
                    end_time=action["end_time"],
                )
            )
        return cls(actions)

    def update_times(self, time_factor: float):
        """
        Update the start and end times of all actions in this Schedule to present day,
        factored by the time factor. Modifies the schedule in-place.
        """
        new_actions = []

        for i, action in enumerate(self):
            if i == 0:
                new_start_time = datetime.now(UTC)
            else:
                ss_time_diff = action.start_time - self[i - 1].start_time
                ss_time_diff = ss_time_diff / time_factor
                new_start_time = new_actions[-1].start_time + ss_time_diff

            new_actions.append(
                action.update_times(
                    time_factor=time_factor, new_start_time=new_start_time
                )
            )

        for i, updated_action in enumerate(new_actions):
            self[i] = updated_action

    def validate(self):
        for row in self:
            row.validate()

    def get_by_device(self, device_name: str) -> List[Action]:
        return [action for action in self if action.device_name == device_name]

    def sort_by_start_time(self):
        """
        Return a new Schedule sorted by start_time.
        """
        sorted(self, key=lambda action: action.start_time)

    def to_dataframe(self) -> pd.DataFrame:
        data = [action.to_dict(iso=False) for action in self]
        return pd.DataFrame(data)

    def save_to_csv(self, filename: Union[str, Path]):
        """Write the schedule to a CSV file."""
        if not isinstance(filename, str):
            filename = str(filename)
        if not filename.endswith(".csv"):
            filename = str(filename) + ".csv"

        df = self.to_dataframe()
        df.to_csv(filename, index=False)

    def is_completed(self) -> bool:
        return all(action.completed for action in self)

    def reset_completion(self):
        for action in self:
            action.completed = False
            action.set_status(ActionStatus.PENDING)

    def __str__(self) -> str:
        actions = ",\n".join(
            ["  " + str(action).replace("\n", "\n  ") for action in self]
        )
        return "Schedule([\n" + actions + "\n])"

    def to_jsonl_string(self) -> str:
        """Convert the schedule to a JSONL string."""
        lines = [json.dumps(action.to_dict(iso=True)) for action in self]
        return "\n".join(lines)

    def to_one_line_string(self) -> str:
        """Convert the schedule to a single line string."""
        actions = "; ".join(
            [
                f"{action.device_name} {action.action_type} {action.action_value} "
                f"from {action.start_time.isoformat()} to {action.end_time.isoformat()}"
                for action in self
            ]
        )
        return f"Schedule: {actions}"

    def save_to_jsonl(self, filename: Union[str, Path]):
        """Write the schedule to a JSONL file."""
        if not isinstance(filename, str):
            filename = str(filename)
        if not filename.endswith(".jsonl"):
            filename = str(filename) + ".jsonl"

        with open(filename, "w") as f:
            json_string = self.to_jsonl_string()
            f.write(json_string + "\n")

    @classmethod
    def convert_action_value_string(cls, action_value_str):
        """Read an old schedule CSV file and convert it to a Schedule instance."""
        import re

        # Replace single quotes with double quotes
        action_value_str = action_value_str.replace("'", '"')
        # Replace tuples (parentheses) with lists (brackets)
        action_value_str = re.sub(r"\(([^()]*)\)", r"[\1]", action_value_str)
        try:
            action_value = json.loads(action_value_str)
        except Exception:
            action_value = {}

        return action_value


class ScheduleManager:
    def __init__(
        self,
        schedule_path: str | Path,
        truncate_factor: float | None,
        logger: ObservatoryLogger,
    ):
        self.schedule = None
        self.schedule_path = Path(schedule_path)
        self.truncate_factor = truncate_factor
        self.logger = logger
        self.running = False
        self.schedule_mtime = self.get_mtime()

        if self.schedule_mtime != 0:
            self.read()

    def get_schedule(self) -> Schedule:
        if self.schedule is None:
            self.schedule = self.read()

        if self.schedule is None:
            raise ValueError("No valid schedule loaded.")
        return self.schedule

    def read(self) -> Schedule | None:
        """
        Read and process the observatory schedule from CSV file.

        Loads the schedule CSV file and converts it to a pandas DataFrame with
        proper datetime parsing. Automatically reloads the schedule if the file
        has been modified since the last read. Supports schedule truncation for
        development and testing purposes.

        Returns:
            pd.DataFrame or None: A DataFrame containing the schedule data with
                properly parsed 'start_time' and 'end_time' columns, or None if
                an error occurs during reading.

        Features:
        - Automatic file modification detection and reload
        - Datetime parsing for start_time and end_time columns
        - Optional schedule truncation for development (via truncate_factor)
        - Error handling with logging and error source tracking

        File Format:
        - CSV file with columns including start_time, end_time, device_name,
          action_type, and action_value
        - Datetime columns should be in ISO format compatible with pandas

        Note:
            - Schedule is sorted by start_time after loading
            - Truncation moves schedule to current time for testing
            - File modification time is tracked to enable automatic reloading
        """
        try:
            # self.schedule_manager.read()
            schedule_mtime = self.get_mtime()

            if (schedule_mtime > self.schedule_mtime) or (self.schedule is None):
                if self.running is True:
                    self.logger.warning(
                        "Schedule updating while the previous schedule is running. This will not take effect until the new schedule is run."
                    )

                self.logger.info("Reading schedule")
                self.schedule_mtime = schedule_mtime

                try:
                    schedule = Schedule.from_file(self.schedule_path)
                    schedule.validate()
                    self.logger.info(f"Schedule read: {schedule.to_one_line_string()}")
                    if self.truncate_factor is not None:
                        schedule.update_times(self.truncate_factor)
                        self.logger.info(
                            f"Schedule truncated by factor {self.truncate_factor}. "
                            f"Truncated schedule: {schedule.to_one_line_string()}"
                        )
                    self.schedule = schedule

                    return schedule
                except Exception as e:
                    self.logger.warning(
                        f"Warning: Issue processing schedule: {e}, please try again"
                    )
                    return None
            else:
                return self.schedule

        except Exception as e:
            self.logger.report_device_issue(
                "Schedule", "", "Error reading schedule", exception=e
            )

    def reload_if_updated(self) -> bool:
        """
        Reload the schedule if the schedule file has been modified.

        Checks if the schedule file has been updated since it was last read.
        If the file has been modified, it reloads the schedule and updates
        the internal state accordingly.

        Process:
        1. Checks if the schedule file modification time is greater than
           the last recorded modification time.
        2. If updated, calls the read() method to reload the schedule.
        3. Updates the internal schedule state.

        Note:
            - Does not interrupt a currently running schedule.
            - Logs a warning if attempting to update while running.
            - Useful for dynamic schedule updates during operation.
        """
        if self.is_schedule_updated() and not self.running:
            self.logger.info("Schedule file updated, reloading")
            self.read()
            return True
        return False

    def get_mtime(self) -> float:
        """
        Get the modification timestamp of the schedule file.

        Retrieves the last modification time of the schedule CSV file to enable
        automatic detection of schedule updates. Returns 0 if the file doesn't
        exist, which can be used to detect when no schedule is available.

        Returns:
            float: The Unix timestamp of the schedule file's last modification
                time, or 0.0 if the file does not exist.

        Note:
            - Used by read_schedule() to detect file changes
            - Enables automatic schedule reloading during operation
            - Returns 0 for non-existent files to simplify logic
        """
        if not self.schedule_path.exists():
            return 0
        else:
            return os.path.getmtime(self.schedule_path)

    def is_schedule_updated(self) -> bool:
        """Return True if the schedule file has been modified since last read."""
        return self.get_mtime() > self.schedule_mtime

    def stop_schedule(self, thread_manager) -> None:
        """
        Stop the currently running schedule execution thread.

        Safely stops the schedule execution by setting the schedule_running flag
        to False and waiting for the schedule thread to complete. This ensures
        that any ongoing actions can finish cleanly before the schedule stops.

        Process:
        1. Sets schedule_running flag to False (signals thread to stop)
        2. Finds the schedule thread in the threads list
        3. Waits for the thread to complete using join()
        4. Logs the stopping action

        Thread Safety:
        - Uses thread.join() to ensure clean shutdown
        - Schedule thread checks schedule_running flag regularly
        - Ongoing actions are allowed to complete before stopping

        Note:
            - If no schedule is running, logs a warning and returns
            - Essential for emergency stops and robotic switch operations
            - Used when weather becomes unsafe or errors occur
        """

        if self.running:
            self.running = False
            self.logger.info("Stopping schedule")
            thread_manager.stop_thread("schedule")
        else:
            self.logger.warning("Schedule not running")

    def get_completion_status(self) -> list:
        if self.schedule is None:
            return []
        return [action.completed for action in self.schedule]

    def get_completed_percentage(self) -> float:
        if self.schedule is None or len(self.schedule) == 0:
            return 0.0
        completed = sum(action.completed for action in self.schedule)
        return completed / len(self.schedule) * 100.0
