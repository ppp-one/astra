import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from astra import CONFIG


def update_times(df: pd.DataFrame, time_factor: float) -> pd.DataFrame:
    """Update the start and end times to present day factored by the time
    factor."""

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for i, row in df.iterrows():
        device_type, device_name, action_type, action_value, start_time, end_time = row

        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor

        new_start_time = datetime.now(UTC)

        if prev_end_time:
            ss_time_diff = start_time - prev_start_time
            ss_time_diff = ss_time_diff / time_factor

            new_start_time = prev_new_start_time + ss_time_diff

        new_end_time = new_start_time + se_time_diff

        new_row = [
            device_type,
            device_name,
            action_type,
            action_value,
            new_start_time,
            new_end_time,
        ]
        new_rows.append(new_row)

        prev_start_time = start_time
        prev_end_time = end_time

        prev_new_start_time = new_start_time

    return pd.DataFrame(new_rows, columns=df.columns)


def action_type_values(action):
    assert len(action) == 1, "action must have a single key containing the action type"
    action_type = list(action.keys())[0]
    action_values = action[action_type]

    return action_type, action_values


def action_dict_to_series(action: dict, observatory: str) -> pd.DataFrame:
    """
    Convert an action dictionary to a DataFrame row of the form:
    {
        device_type: Camera,
        device_name: camera_{observatory},
        action_type: {action_type},
        action_value: {action_value},
        start_time: {start_time},
        end_time: {end_time},
    }
    """
    assert len(action) == 1, "action must have a single key containing the action type"
    action = unflatten_action(action)
    action_type, action_values = action_type_values(action)
    device_type = "Camera"
    device_name = f"camera_{observatory}"
    start_time = action[action_type].get("start_time", None)
    end_time = action[action_type].get("end_time", None)

    # collect the action values
    series_action_value = {
        key: value
        for key, value in action[action_type].items()
        if not key in ["start_time", "end_time"]
    }

    series_action_value = json.dumps(series_action_value)

    return pd.Series(
        [
            device_type,
            device_name,
            action_type,
            series_action_value,
            start_time,
            end_time,
        ],
        index=[
            "device_type",
            "device_name",
            "action_type",
            "action_value",
            "start_time",
            "end_time",
        ],
    )


def action_series_to_dict(row: pd.Series) -> dict:
    """
    Convert a DataFrame row to an action dictionary of the form:
    {
        {action_type}: {
            start_time: None,
            end_time: None,
            filter: None,
            n: None,
            exptime: None,
            guiding: None,
            pointing: None,
    }
    """
    action = {}
    action[row["action_type"]] = {
        "start_time": row["start_time"],
        "end_time": row["end_time"],
    }

    action_value_dict = eval(row["action_value"].replace("'", '"'))

    if len(action_value_dict) > 0:
        action[row["action_type"]].update(
            {
                key: value
                for key, value in action_value_dict.items()
                if not key in ["start_time", "end_time"]
            }
        )

    return action


def unflatten_action(action: dict) -> dict:
    """Set ``filter`` and ``n`` of an action to a list if `filter` is simply a
    string."""
    action_type, action_values = action_type_values(action)
    if action_type in ["flats", "calib"]:
        if isinstance(action_values["filter"], str):
            action_values["filter"] = [action_values["filter"]]
            action_values["n"] = [action_values["n"]]

    return {action_type: action_values}


def check_action(action: dict) -> bool:
    """Check if an action (as dictionary) is valid."""
    message = None
    action_type, action_values = action_type_values(action)

    if not action_type in [
        "open",
        "close",
        "object",
        "flats",
        "calibration",
    ]:
        message = "action type must be one of 'open', 'close', 'object', 'flats', 'calibration'"

    if action_type in ["flats", "calib"]:
        if not "filter" in action_values:
            message = "filter must be specified"
        if not "n" in action_values:
            message = "n must be specified"
        if not "start_time" in action_values:
            message = "start_time must be specified"
        if not "end_time" in action_values:
            message = "end_time must be specified"

    if message is None:
        return True, "Valid schedule action"
    else:
        return False, message


def df_to_actions_list(df: pd.DataFrame) -> list:
    """Convert a DataFrame to a a list of actions (each action being a dict)"""
    schedule_list = []
    for _, row in df.iterrows():
        action = action_series_to_dict(row)
        schedule_list.append(action)
    return schedule_list


def actions_list_to_df(actions_list: list, observatory: str) -> pd.DataFrame:
    """Convert a list of actions (each action being a dict) to a DataFrame."""
    actions_series = [
        action_dict_to_series(action, observatory) for action in actions_list
    ]
    return pd.DataFrame(actions_series)


def check_object_action(action: dict) -> bool:
    return False


def check_flats_action(action: dict) -> bool:
    return False


def check_calibration_action(action: dict) -> bool:
    return False


def check_open_action(action: dict) -> bool:
    return False


def check_close_action(action: dict) -> bool:
    return False


ACTION_CHECKERS = {
    "object": check_object_action,
    "flats": check_flats_action,
    "calibration": check_calibration_action,
    "open": check_open_action,
    "close": check_close_action,
}


def check_duration(duration_str: str) -> timedelta:
    """Check duration string, either a float and seconds or a "HH:MM:SS"
    string.

    Parameters
    ----------
    duration_str : str
        string representing a duration

    Returns
    -------
    datetime.timedelta or None
        duration as a timedelta object or None if the string is not a valid duration
    """
    if isinstance(duration_str, str):
        duration = datetime.datetime.strptime(duration_str, "%H:%M:%S").time()
        duration = timedelta(
            hours=duration.hour, minutes=duration.minute, seconds=duration.second
        )
    elif isinstance(duration_str, float):
        duration = float(duration_str)
        duration = timedelta(seconds=duration)
    else:
        duration = None

    return duration


# TODO: WIP
def normalize_and_check_actions(actions_list: list) -> list:
    message = None
    new_actions_list = actions_list.copy()
    for i in range(len(actions_list)):
        action_type, action = action_type_values(new_actions_list[0])
        if i > 1:
            _, last_action = action_type_values(new_actions_list[0])
            if "start_time" not in action:
                if "end_time" in last_action:
                    action["start_time"] = new_actions_list[i - 1]["end_time"]
                else:
                    message = f"Schedule action {action_type} ({i}) missing start_time (and cannot be inferred from previous end_time)"
                    break

                start_time = CONFIG.as_datetime(action["start_time"])

            if "end_time" not in action:
                if "duration" in action:
                    duration = check_duration(action["duration"])

                    if duration is None:
                        message = (
                            f"Schedule action {action_type} ({i}) has invalid duration"
                        )
                        break

                    action["end_time"] = start_time + duration

    if message is not None:
        return False, message
    else:
        return True, "Schedule is valid"


def process_schedule(filename, observatory_name=None, truncate=False) -> pd.DataFrame:
    """Read a schedule file and return a DataFrame. The schedule file can be in
    CSV or YAML format. The yaml file directly exposes the actions as a list of
    dictionaries. If a CSV is provided, the actions are read as a DataFrame and
    then converted to a list of dictionaries. Finally, because the original
    output of Astra 0.2 was a DataFrame, the list of actions is converted back
    to a DataFrame and returned.

    Following steps are taken:

    - 1. Read the schedule file and convert to a list of actions, each action being a dict
    - 2. (this part is not implemented yet) Normalize the actions, i.e. convert filter and n to lists and explicitly set start_time and end_time
    - 3. (this part is not implemented yet) Check if each action is valid
    - 4. Convert the actions list to a DataFrame (for now)
    - Return the DataFrame

    Parameters
    ----------
    filename : _type_
        _description_
    observatory_name : _type_, optional
        _description_, by default None
    truncate : bool, optional
        _description_, by default False

    Returns
    -------
    pd.DataFrame
        _description_

    Raises
    ------
    FileNotFoundError
        _description_
    """
    schedule_path = Path(filename)

    if schedule_path.exists() is False:
        raise FileNotFoundError(f"File not found: {filename}")

    # 1. read schedule and convert to a DataFrame
    if schedule_path.suffix == ".csv":
        schedule = pd.read_csv(schedule_path)
        actions = df_to_actions_list(schedule)
        if observatory_name is None:
            # TODO: not good
            observatory_name = schedule.iloc[0]["device_name"].split("_")[1]
    elif schedule_path.suffix in [".yml", ".yaml"]:
        schedule = pd.DataFrame()
        with open(schedule_path, "r") as f:
            actions = yaml.safe_load(f)
        if observatory_name is None:
            observatory_name = schedule_path.stem

    else:
        raise ValueError(f"Unsupported file format: {schedule_path.suffix}")

    # 2. Normalize the actions
    # TODO: WIP

    # 3. Check the actions
    # TODO: WIP

    # 4. Convert the actions list to a DataFrame
    schedule = actions_list_to_df(actions, observatory_name)

    # at this point schedule must be a DataFrame
    schedule["start_time"] = pd.to_datetime(schedule.start_time)
    schedule["end_time"] = pd.to_datetime(schedule.end_time)

    # for development: Truncate the schedule if self.truncate_schedule is True
    if truncate:
        schedule = update_times(schedule, 10)

    schedule["completed"] = False

    return schedule


# def discover_schedule(self) -> None:
#     """
#     Discover the schedule files available
#     """

#     schedule_files_csv = list(CONFIG.paths.folder_schedule.glob(f"{self.name}.csv"))
#     schedule_files_yaml = list(CONFIG.paths.folder_schedule.glob(f"{self.name}.y*ml"))

#     if len(schedule_files_csv) > 0 and len(schedule_files_yaml) > 0:
#         raise ValueError(
#             f"Only one of {self.name}.csv or {self.name}.y*ml is expected in {CONFIG.paths.folder_schedule}"
#         )

#     elif len(schedule_files_csv) == 0 and len(schedule_files_yaml) == 0:
#         raise FileNotFoundError(
#             f"No schedule files found in {CONFIG.paths.folder_schedule}"
#         )

#     elif len(schedule_files_csv) > 0:
#         self.schedule_path = schedule_files_csv[0]
#     else:
#         self.schedule_path = schedule_files_yaml[0]

#     self.schedule_mtime = self.get_schedule_mtime()
#     self.logger.info(f"Schedule file found: {self.schedule_path}")
